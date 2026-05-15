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

    // Build enum-value lookup keyed by lowercase type name (plain and schema-qualified).
    // Used to auto-populate ValidationRule.allowed for columns whose type is a known enum
    // so that adding an enum value in config is immediately reflected in request validation
    // without requiring the api_entity allowed list to be kept in sync manually.
    let mut enum_allowed_by_type: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    for e in &config.enums {
        let vals: Vec<serde_json::Value> = e.values.iter().map(|v| serde_json::Value::String(v.clone())).collect();
        enum_allowed_by_type.insert(e.name.to_lowercase(), vals.clone());
        if let Some(sid) = &e.schema_id {
            if let Some(schema) = schemas_by_id.get(sid.as_str()) {
                let qualified = format!("{}.{}", schema.name.to_lowercase(), e.name.to_lowercase());
                enum_allowed_by_type.insert(qualified, vals);
            }
        }
    }
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
                let type_str = match &c.type_ {
                    ColumnTypeConfig::Simple(s) => s.to_lowercase(),
                    ColumnTypeConfig::Parameterized { name, .. } => name.to_lowercase(),
                };
                let is_asset = type_str == "asset" || type_str == "asset[]";
                let asset_is_array = type_str == "asset[]";
                let pg_type = column_pg_type_name(&c.type_);
                ColumnInfo {
                    name: c.name.clone(),
                    pk_type: if is_pk { Some(pk_type.clone()) } else { None },
                    nullable: c.nullable,
                    has_default: c.default.is_some(),
                    pg_type,
                    is_asset,
                    asset_is_array,
                    asset_config: c.asset.clone(),
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
                    is_asset: false,
                    asset_is_array: false,
                    asset_config: None,
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

        // Start with explicitly configured validation rules, then fill in `allowed` from the
        // enum config for any column whose type maps to a known enum and has no explicit list.
        // This means enum value additions in config are immediately enforced/allowed by the
        // validator without requiring the api_entity validation block to be kept in sync.
        let mut validation = api.validation.clone();
        for col in table_columns {
            let type_key = match &col.type_ {
                ColumnTypeConfig::Simple(s) => s.to_lowercase(),
                ColumnTypeConfig::Parameterized { name, .. } => name.to_lowercase(),
            };
            if let Some(enum_vals) = enum_allowed_by_type.get(&type_key) {
                let rule = validation.entry(col.name.clone()).or_insert_with(ValidationRule::default);
                if rule.allowed.is_none() {
                    rule.allowed = Some(enum_vals.clone());
                }
            }
        }

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
            validation,
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
    // Asset pseudo-types must be intercepted BEFORE the generic `ends_with("[]")` guard
    // below, otherwise "asset[]" would be returned verbatim and PostgreSQL would reject the
    // cast with "type asset[] does not exist".
    if lower == "asset[]" {
        return Some("jsonb".into());
    }
    if lower == "asset" {
        return Some("text".into());
    }

    // Check array and schema-qualified types first so they are preserved verbatim
    // (e.g. `uuid[]` must not be shortened to `uuid`, `sample.order_status[]` keeps both).
    if name.ends_with("[]") {
        return Some(name.to_string());
    }
    if name.contains('.') {
        // Schema-qualified custom type (e.g. sample.order_status); cast so text binds correctly
        return Some(name.to_string());
    }
    if lower == "timestamptz" || lower == "timestamp with time zone" {
        Some("timestamptz".into())
    } else if lower == "timestamp" || lower.starts_with("timestamp ") {
        Some("timestamp".into())
    } else if lower == "date" {
        Some("date".into())
    } else if lower == "timetz" || lower == "time with time zone" {
        Some("timetz".into())
    } else if lower == "time" || lower.starts_with("time ") {
        Some("time".into())
    } else if lower == "boolean" || lower == "bool" {
        Some("boolean".into())
    } else if lower == "jsonb" {
        Some("jsonb".into())
    } else if lower == "json" {
        Some("json".into())
    } else if lower.contains("uuid") {
        Some("uuid".into())
    } else if lower == "numeric" || lower.starts_with("numeric(") || lower == "decimal" || lower.starts_with("decimal(") {
        Some("numeric".into())
    } else if lower == "smallint" || lower == "int2" || lower == "smallserial" || lower == "serial2" {
        Some("smallint".into())
    } else if lower == "integer" || lower == "int" || lower == "int4" || lower == "serial" || lower == "serial4" {
        Some("integer".into())
    } else if lower == "bigint" || lower == "int8" || lower == "bigserial" || lower == "serial8" {
        Some("bigint".into())
    } else if lower == "real" || lower == "float4" {
        Some("real".into())
    } else if lower == "double precision" || lower == "float8" {
        Some("double precision".into())
    } else if lower == "float" || lower.starts_with("float(") {
        // Postgres FLOAT(n): n<=24 is real, n>25 is double precision; default to double precision.
        Some("double precision".into())
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

/// Load full config from architect._sys_* tables for one package. Tables must already exist (ensure_sys_tables).
pub async fn load_from_pool(pool: &PgPool, package_id: &str) -> Result<FullConfig, ConfigError> {
    let mut schemas = load_config_table::<SchemaConfig>(pool, &qualified_sys_table("_sys_schemas"), package_id).await?;
    if schemas.is_empty() {
        schemas = vec![SchemaConfig {
            id: "default".into(),
            name: "public".into(),
            comment: None,
        }];
    }
    let enums = load_config_table::<EnumConfig>(pool, &qualified_sys_table("_sys_enums"), package_id).await?;
    let tables = load_config_table::<TableConfig>(pool, &qualified_sys_table("_sys_tables"), package_id).await?;
    let columns = load_config_table::<ColumnConfig>(pool, &qualified_sys_table("_sys_columns"), package_id).await?;
    let indexes = load_config_table::<IndexConfig>(pool, &qualified_sys_table("_sys_indexes"), package_id).await?;
    let relationships = load_config_table::<RelationshipConfig>(pool, &qualified_sys_table("_sys_relationships"), package_id).await?;
    let api_entities = load_config_table::<ApiEntityConfig>(pool, &qualified_sys_table("_sys_api_entities"), package_id).await?;
    let kv_stores = load_config_table::<KvStoreConfig>(pool, &qualified_sys_table("_sys_kv_stores"), package_id).await?;

    let config = FullConfig {
        schemas,
        enums,
        tables,
        columns,
        indexes,
        relationships,
        api_entities,
        kv_stores,
    };
    Ok(config)
}

async fn load_config_table<T>(pool: &PgPool, table: &str, package_id: &str) -> Result<Vec<T>, ConfigError>
where
    T: for<'de> serde::Deserialize<'de>,
{
    let sql = format!("SELECT payload FROM {} WHERE package_id = $1 ORDER BY id", table);
    tracing::debug!(sql = %sql, package_id = %package_id, "query");
    let rows = sqlx::query_scalar::<_, serde_json::Value>(&sql)
        .bind(package_id)
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
