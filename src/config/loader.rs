//! Load config from in-memory structs or from _private_* tables in DB.

use crate::config::resolved::{ColumnInfo, PkType, ResolvedEntity, ResolvedModel};
use crate::config::types::*;
use crate::config::{validate, FullConfig};
use crate::error::ConfigError;
use sqlx::PgPool;
use std::collections::HashMap;

/// Build resolved model from full config (call after validate).
pub fn resolve(config: &FullConfig) -> Result<ResolvedModel, ConfigError> {
    validate(config)?;

    let schemas_by_id: HashMap<_, _> = config.schemas.iter().map(|s| (s.id.as_str(), s)).collect();
    let tables_by_id: HashMap<_, _> = config.tables.iter().map(|t| (t.id.as_str(), t)).collect();
    let columns_by_table: HashMap<_, Vec<&ColumnConfig>> = config
        .columns
        .iter()
        .fold(HashMap::new(), |mut m, c| {
            m.entry(c.table_id.as_str()).or_default().push(c);
            m
        });

    let mut entities = Vec::new();
    let mut entity_by_path = HashMap::new();

    for api in &config.api_entities {
        let table = tables_by_id
            .get(api.entity_id.as_str())
            .ok_or_else(|| ConfigError::MissingReference {
                kind: "table",
                id: api.entity_id.clone(),
            })?;
        let schema = schemas_by_id
            .get(table.schema_id.as_str())
            .ok_or_else(|| ConfigError::MissingReference {
                kind: "schema",
                id: table.schema_id.clone(),
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

        let columns: Vec<ColumnInfo> = table_columns
            .iter()
            .map(|c| {
                let is_pk = pk_names.contains(&c.name);
                ColumnInfo {
                    name: c.name.clone(),
                    pk_type: if is_pk { Some(pk_type.clone()) } else { None },
                    nullable: c.nullable,
                    has_default: c.default.is_some(),
                }
            })
            .collect();

        let entity = ResolvedEntity {
            table_id: table.id.clone(),
            schema_name: schema.name.clone(),
            table_name: table.name.clone(),
            path_segment: api.path_segment.clone(),
            pk_columns: pk_names.clone(),
            pk_type: pk_type.clone(),
            columns: columns.clone(),
            operations: api.operations.clone(),
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

/// Load full config from _private_* tables. Tables must already exist (ensure_private_tables).
pub async fn load_from_pool(pool: &PgPool) -> Result<FullConfig, ConfigError> {
    let schemas = load_config_table::<SchemaConfig>(pool, "_private_schemas").await?;
    let enums = load_config_table::<EnumConfig>(pool, "_private_enums").await?;
    let tables = load_config_table::<TableConfig>(pool, "_private_tables").await?;
    let columns = load_config_table::<ColumnConfig>(pool, "_private_columns").await?;
    let indexes = load_config_table::<IndexConfig>(pool, "_private_indexes").await?;
    let relationships = load_config_table::<RelationshipConfig>(pool, "_private_relationships").await?;
    let api_entities = load_config_table::<ApiEntityConfig>(pool, "_private_api_entities").await?;

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
    let rows = sqlx::query_scalar::<_, serde_json::Value>(&format!(
        "SELECT payload FROM {} ORDER BY id",
        table
    ))
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
