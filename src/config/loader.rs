//! Load config from in-memory structs or from architect._sys_* tables in DB.

use crate::config::resolved::{ColumnInfo, IncludeDirection, IncludeSpec, PkType, ResolvedEntity, ResolvedModel};
use crate::config::types::*;
use crate::config::{default_schema_id, validate, FullConfig};
use crate::error::ConfigError;
use crate::store::qualified_sys_table;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};

/// Build resolved model from full config (call after validate).
pub fn resolve(config: &FullConfig) -> Result<ResolvedModel, ConfigError> {
    validate(config)?;
    let default_sid = default_schema_id(config)?;

    let schemas_by_id: HashMap<_, _> = config.schemas.iter().map(|s| (s.id.as_str(), s)).collect();
    let tables_by_id: HashMap<_, _> = config.tables.iter().map(|t| (t.id.as_str(), t)).collect();
    let columns_by_table: HashMap<_, Vec<&ColumnConfig>> = config
        .columns
        .iter()
        .fold(HashMap::new(), |mut m, c| {
            m.entry(c.table_id.as_str()).or_default().push(c);
            m
        });
    let column_id_to_name: HashMap<&str, &str> = config.columns.iter().map(|c| (c.id.as_str(), c.name.as_str())).collect();
    let table_id_to_path: HashMap<&str, &str> = config
        .api_entities
        .iter()
        .map(|api| (api.entity_id.as_str(), api.path_segment.as_str()))
        .collect();

    let mut entities = Vec::new();
    let mut entity_by_path = HashMap::new();

    for api in &config.api_entities {
        let table = tables_by_id
            .get(api.entity_id.as_str())
            .ok_or_else(|| ConfigError::MissingReference {
                kind: "table",
                id: api.entity_id.clone(),
            })?;
        let table_sid = table.schema_id.as_deref().unwrap_or(default_sid);
        let schema = schemas_by_id
            .get(table_sid)
            .ok_or_else(|| ConfigError::MissingReference {
                kind: "schema",
                id: table_sid.to_string(),
            })?;
        let table_columns = columns_by_table
            .get(table.id.as_str())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        let pk_names = match &table.primary_key {
            PrimaryKeyConfig::Single(s) => vec![s.clone()],
            PrimaryKeyConfig::Composite(v) => v.clone(),
        };
        let pk_col = table_columns
            .iter()
            .find(|c| c.name == pk_names[0])
            .ok_or_else(|| ConfigError::InvalidPrimaryKey {
                table_id: table.id.clone(),
                column: pk_names[0].clone(),
            })?;
        let pk_type = infer_pk_type(pk_col);

        let mut columns: Vec<ColumnInfo> = table_columns
            .iter()
            .map(|c| {
                let is_pk = pk_names.contains(&c.name);
                let pg_type = column_pg_type_name(&c.type_);
                ColumnInfo {
                    name: c.name.clone(),
                    pk_type: if is_pk { Some(pk_type.clone()) } else { None },
                    nullable: c.nullable,
                    has_default: c.default.is_some(),
                    pg_type,
                }
            })
            .collect();

        let config_col_names: HashSet<String> = columns.iter().map(|c| c.name.clone()).collect();
        for (name, nullable, has_default) in [
            ("created_at", false, true),
            ("updated_at", false, true),
            ("archived_at", true, false),
        ] {
            if !config_col_names.contains(name) {
                columns.push(ColumnInfo {
                    name: name.to_string(),
                    pk_type: None,
                    nullable,
                    has_default,
                    pg_type: Some("timestamptz".into()),
                });
            }
        }

        let sensitive_columns: HashSet<String> = api.sensitive_columns.iter().cloned().collect();
        let includes = build_includes_for_table(
            &table.id,
            &config.relationships,
            &column_id_to_name,
            &table_id_to_path,
        );
        let entity = ResolvedEntity {
            table_id: table.id.clone(),
            schema_name: schema.name.clone(),
            table_name: table.name.clone(),
            path_segment: api.path_segment.clone(),
            pk_columns: pk_names.clone(),
            pk_type: pk_type.clone(),
            columns: columns,
            operations: api.operations.clone(),
            sensitive_columns,
            includes,
            validation: api.validation.clone(),
        };
        entity_by_path.insert(api.path_segment.clone(), entity.clone());
        entities.push(entity);
    }

    Ok(ResolvedModel {
        entities,
        entity_by_path,
    })
}

fn build_includes_for_table(
    our_table_id: &str,
    relationships: &[RelationshipConfig],
    column_id_to_name: &HashMap<&str, &str>,
    table_id_to_path: &HashMap<&str, &str>,
) -> Vec<IncludeSpec> {
    let mut includes = Vec::new();
    for rel in relationships {
        let from_col = column_id_to_name.get(rel.from_column_id.as_str()).map(|s| s.to_string());
        let to_col = column_id_to_name.get(rel.to_column_id.as_str()).map(|s| s.to_string());
        let from_path = table_id_to_path.get(rel.from_table_id.as_str()).map(|s| s.to_string());
        let to_path = table_id_to_path.get(rel.to_table_id.as_str()).map(|s| s.to_string());
        if let (Some(our_key), Some(their_key), Some(related_path)) = (from_col.clone(), to_col.clone(), to_path.clone()) {
            if rel.from_table_id == our_table_id {
                includes.push(IncludeSpec {
                    name: related_path.clone(),
                    direction: IncludeDirection::ToOne,
                    related_path_segment: related_path,
                    our_key_column: our_key,
                    their_key_column: their_key,
                });
            }
        }
        if let (Some(our_key), Some(their_key), Some(related_path)) = (to_col, from_col, from_path) {
            if rel.to_table_id == our_table_id {
                includes.push(IncludeSpec {
                    name: related_path.clone(),
                    direction: IncludeDirection::ToMany,
                    related_path_segment: related_path,
                    our_key_column: our_key,
                    their_key_column: their_key,
                });
            }
        }
    }
    includes
}

fn column_pg_type_name(ty: &ColumnTypeConfig) -> Option<String> {
    let name = match ty {
        ColumnTypeConfig::Simple(s) => s.as_str(),
        ColumnTypeConfig::Parameterized { name, .. } => name.as_str(),
    };
    let lower = name.to_lowercase();
    if lower == "timestamptz" || lower == "timestamp with time zone" {
        Some("timestamptz".into())
    } else if lower == "timestamp" || lower.starts_with("timestamp ") {
        Some("timestamp".into())
    } else if lower == "date" {
        Some("date".into())
    } else if lower.contains("uuid") {
        Some("uuid".into())
    } else if name.contains('.') {
        // Schema-qualified custom type (e.g. sample.order_status); cast so text binds correctly
        Some(name.to_string())
    } else {
        None
    }
}

fn infer_pk_type(col: &ColumnConfig) -> PkType {
    let type_str = match &col.type_ {
        ColumnTypeConfig::Simple(s) => s.as_str(),
        ColumnTypeConfig::Parameterized { name, .. } => name.as_str(),
    };
    let type_lower = type_str.to_lowercase();
    if type_lower.contains("uuid") {
        PkType::Uuid
    } else if type_lower.contains("bigserial") || type_lower.contains("bigint") {
        PkType::BigInt
    } else if type_lower.contains("serial") || type_lower.contains("integer") || type_lower.contains("int") {
        PkType::Int
    } else {
        PkType::Text
    }
}

/// Load full config from architect._sys_* tables. Tables must already exist (ensure_sys_tables).
pub async fn load_from_pool(pool: &PgPool) -> Result<FullConfig, ConfigError> {
    let mut schemas = load_config_table::<SchemaConfig>(pool, &qualified_sys_table("_sys_schemas")).await?;
    if schemas.is_empty() {
        schemas = vec![SchemaConfig {
            id: "default".into(),
            name: "public".into(),
            comment: None,
        }];
    }
    let enums = load_config_table::<EnumConfig>(pool, &qualified_sys_table("_sys_enums")).await?;
    let tables = load_config_table::<TableConfig>(pool, &qualified_sys_table("_sys_tables")).await?;
    let columns = load_config_table::<ColumnConfig>(pool, &qualified_sys_table("_sys_columns")).await?;
    let indexes = load_config_table::<IndexConfig>(pool, &qualified_sys_table("_sys_indexes")).await?;
    let relationships = load_config_table::<RelationshipConfig>(pool, &qualified_sys_table("_sys_relationships")).await?;
    let api_entities = load_config_table::<ApiEntityConfig>(pool, &qualified_sys_table("_sys_api_entities")).await?;

    let config = FullConfig {
        schemas,
        enums,
        tables,
        columns,
        indexes,
        relationships,
        api_entities,
    };
    Ok(config)
}

async fn load_config_table<T>(pool: &PgPool, table: &str) -> Result<Vec<T>, ConfigError>
where
    T: for<'de> serde::Deserialize<'de>,
{
    let sql = format!("SELECT payload FROM {} ORDER BY id", table);
    tracing::debug!(sql = %sql, "query");
    let rows = sqlx::query_scalar::<_, serde_json::Value>(&sql)
    .fetch_all(pool)
    .await
    .map_err(|e| ConfigError::Load(e.to_string()))?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let value: T = serde_json::from_value(row).map_err(|e| ConfigError::Load(e.to_string()))?;
        out.push(value);
    }
    Ok(out)
}
