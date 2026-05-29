//! Load config from in-memory structs or from architect._sys_* tables in DB.

use crate::config::resolved::{
    ColumnInfo, IncludeDirection, IncludeSpec, PkType, ResolvedEntity, ResolvedModel,
};
use crate::config::types::*;
use crate::config::{default_schema_id, validate, FullConfig};
use crate::db::pool::Pool;
use crate::db::{active_cast_name, parse_canonical};
use crate::error::ConfigError;
use crate::store::qualified_sys_table;
use std::collections::{HashMap, HashSet};

/// Build resolved model from full config (call after validate).
pub fn resolve(config: &FullConfig) -> Result<ResolvedModel, ConfigError> {
    validate(config)?;
    let default_sid = default_schema_id(config)?;

    let schemas_by_id: HashMap<_, _> = config.schemas.iter().map(|s| (s.id.as_str(), s)).collect();
    let tables_by_id: HashMap<_, _> = config.tables.iter().map(|t| (t.id.as_str(), t)).collect();
    let columns_by_table: HashMap<_, Vec<&ColumnConfig>> =
        config.columns.iter().fold(HashMap::new(), |mut m, c| {
            m.entry(c.table_id.as_str()).or_default().push(c);
            m
        });
    let column_id_to_name: HashMap<&str, &str> = config
        .columns
        .iter()
        .map(|c| (c.id.as_str(), c.name.as_str()))
        .collect();
    let table_id_to_path: HashMap<&str, &str> = config
        .api_entities
        .iter()
        .map(|api| (api.entity_id.as_str(), api.path_segment.as_str()))
        .collect();

    let mut entities = Vec::new();
    let mut entity_by_path = HashMap::new();

    for api in &config.api_entities {
        let table = tables_by_id.get(api.entity_id.as_str()).ok_or_else(|| {
            ConfigError::MissingReference {
                kind: "table",
                id: api.entity_id.clone(),
            }
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
                let canonical = parse_canonical(&c.type_);
                let is_asset = matches!(
                    canonical,
                    crate::db::CanonicalType::Asset | crate::db::CanonicalType::AssetArray
                );
                let asset_is_array = matches!(canonical, crate::db::CanonicalType::AssetArray);
                let pg_type = active_cast_name(&canonical);
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
        // Use active_cast_name so the cast is correct per dialect
        // (timestamptz for Postgres, None for SQLite/MySQL which need no cast).
        let ts_cast = active_cast_name(&crate::db::CanonicalType::Timestamp);
        let ts_cast_str: Option<&str> = ts_cast.as_deref();
        for (name, nullable, has_default, pg_type) in [
            ("created_at", false, true, ts_cast_str),
            ("updated_at", false, true, ts_cast_str),
            ("archived_at", true, false, ts_cast_str),
            ("created_by", true, false, None),
            ("updated_by", true, false, None),
        ] {
            if !config_col_names.contains(name) {
                columns.push(ColumnInfo {
                    name: name.to_string(),
                    pk_type: None,
                    nullable,
                    has_default,
                    pg_type: pg_type.map(str::to_owned),
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
        let entity = ResolvedEntity {
            table_id: table.id.clone(),
            schema_name: schema.name.clone(),
            table_name: table.name.clone(),
            path_segment: api.path_segment.clone(),
            pk_columns: pk_names.clone(),
            pk_type: pk_type.clone(),
            columns,
            operations: api.operations.clone(),
            sensitive_columns,
            includes,
            validation: api.validation.clone(),
            events: api.events.clone(),
            archive_field: api.archive_field.clone().or_else(|| {
                if api
                    .operations
                    .iter()
                    .any(|o| o == "archive" || o == "unarchive")
                {
                    Some("archived_at".to_string())
                } else {
                    None
                }
            }),
            package_id: String::new(),
            audit_log: table.audit_log,
            parent_ref_column: api.parent_ref_column.clone(),
        };
        entity_by_path.insert(api.path_segment.clone(), entity.clone());
        entities.push(entity);
    }

    // Synthesize read-only audit entities for every entity with audit_log enabled.
    // The companion `{table}_audit` table is created by apply_migrations; here we expose it
    // as `{path_segment}_audit` with only list + read operations.
    let audit_entities: Vec<ResolvedEntity> = entities
        .iter()
        .filter(|e| e.audit_log)
        .map(|e| {
            let audit_entity = ResolvedEntity {
                table_id: format!("{}_audit", e.table_id),
                schema_name: e.schema_name.clone(),
                table_name: format!("{}_audit", e.table_name),
                path_segment: format!("{}_audit", e.path_segment),
                pk_columns: vec!["audit_id".to_string()],
                pk_type: PkType::Uuid,
                columns: build_audit_columns(&e.columns),
                operations: vec!["list".to_string(), "read".to_string()],
                sensitive_columns: e.sensitive_columns.clone(),
                includes: Vec::new(),
                validation: HashMap::new(),
                events: Vec::new(),
                archive_field: None,
                package_id: e.package_id.clone(),
                audit_log: false,
                parent_ref_column: None,
            };
            audit_entity
        })
        .collect();
    for ae in audit_entities {
        entity_by_path.insert(ae.path_segment.clone(), ae.clone());
        entities.push(ae);
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
        let from_col = column_id_to_name
            .get(rel.from_column_id.as_str())
            .map(|s| s.to_string());
        let to_col = column_id_to_name
            .get(rel.to_column_id.as_str())
            .map(|s| s.to_string());
        let from_path = table_id_to_path
            .get(rel.from_table_id.as_str())
            .map(|s| s.to_string());
        let to_path = table_id_to_path
            .get(rel.to_table_id.as_str())
            .map(|s| s.to_string());
        if let (Some(our_key), Some(their_key), Some(related_path)) =
            (from_col.clone(), to_col.clone(), to_path.clone())
        {
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
        if let (Some(our_key), Some(their_key), Some(related_path)) = (to_col, from_col, from_path)
        {
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

/// Build the column list for a synthetic audit entity.
/// Prepends the five audit metadata columns then appends all source columns with pk_type cleared
/// (audit_id is the new PK, so source PKs become regular queryable columns).
fn build_audit_columns(source_columns: &[ColumnInfo]) -> Vec<ColumnInfo> {
    let mut cols = Vec::with_capacity(5 + source_columns.len());
    cols.push(ColumnInfo {
        name: "audit_id".to_string(),
        pk_type: Some(PkType::Uuid),
        nullable: false,
        has_default: true,
        pg_type: Some("uuid".to_string()),
        is_asset: false,
        asset_is_array: false,
        asset_config: None,
    });
    cols.push(ColumnInfo {
        name: "audit_action".to_string(),
        pk_type: None,
        nullable: false,
        has_default: false,
        pg_type: None,
        is_asset: false,
        asset_is_array: false,
        asset_config: None,
    });
    cols.push(ColumnInfo {
        name: "audit_at".to_string(),
        pk_type: None,
        nullable: false,
        has_default: true,
        pg_type: Some("timestamptz".to_string()),
        is_asset: false,
        asset_is_array: false,
        asset_config: None,
    });
    cols.push(ColumnInfo {
        name: "audit_by".to_string(),
        pk_type: None,
        nullable: true,
        has_default: false,
        pg_type: None,
        is_asset: false,
        asset_is_array: false,
        asset_config: None,
    });
    cols.push(ColumnInfo {
        name: "changed_fields".to_string(),
        pk_type: None,
        nullable: true,
        has_default: false,
        pg_type: Some("jsonb".to_string()),
        is_asset: false,
        asset_is_array: false,
        asset_config: None,
    });
    for col in source_columns {
        cols.push(ColumnInfo {
            name: col.name.clone(),
            pk_type: None,
            nullable: col.nullable,
            has_default: col.has_default,
            pg_type: col.pg_type.clone(),
            is_asset: col.is_asset,
            asset_is_array: col.asset_is_array,
            asset_config: col.asset_config.clone(),
        });
    }
    cols
}

fn infer_pk_type(col: &ColumnConfig) -> PkType {
    use crate::db::CanonicalType;
    match parse_canonical(&col.type_) {
        CanonicalType::Uuid => PkType::Uuid,
        CanonicalType::BigInt | CanonicalType::BigSerial => PkType::BigInt,
        CanonicalType::Int | CanonicalType::Serial | CanonicalType::SmallInt => PkType::Int,
        // Custom pass-through: fall back to string matching for raw SQL types.
        CanonicalType::Custom(s) => {
            let lower = s.to_lowercase();
            if lower.contains("uuid") {
                PkType::Uuid
            } else if lower.contains("bigserial") || lower.contains("bigint") {
                PkType::BigInt
            } else if lower.contains("serial") || lower.contains("int") {
                PkType::Int
            } else {
                PkType::Text
            }
        }
        _ => PkType::Text,
    }
}

/// Load full config from architect._sys_* tables for one package. Tables must already exist (ensure_sys_tables).
pub async fn load_from_pool(pool: &Pool, package_id: &str) -> Result<FullConfig, ConfigError> {
    let mut schemas =
        load_config_table::<SchemaConfig>(pool, &qualified_sys_table("_sys_schemas"), package_id)
            .await?;
    if schemas.is_empty() {
        schemas = vec![SchemaConfig {
            id: "default".into(),
            name: "public".into(),
            comment: None,
        }];
    }
    let enums =
        load_config_table::<EnumConfig>(pool, &qualified_sys_table("_sys_enums"), package_id)
            .await?;
    let tables =
        load_config_table::<TableConfig>(pool, &qualified_sys_table("_sys_tables"), package_id)
            .await?;
    let columns =
        load_config_table::<ColumnConfig>(pool, &qualified_sys_table("_sys_columns"), package_id)
            .await?;
    let indexes =
        load_config_table::<IndexConfig>(pool, &qualified_sys_table("_sys_indexes"), package_id)
            .await?;
    let relationships = load_config_table::<RelationshipConfig>(
        pool,
        &qualified_sys_table("_sys_relationships"),
        package_id,
    )
    .await?;
    let api_entities = load_config_table::<ApiEntityConfig>(
        pool,
        &qualified_sys_table("_sys_api_entities"),
        package_id,
    )
    .await?;
    let kv_stores = load_config_table::<KvStoreConfig>(
        pool,
        &qualified_sys_table("_sys_kv_stores"),
        package_id,
    )
    .await?;

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

async fn load_config_table<T>(
    pool: &Pool,
    table: &str,
    package_id: &str,
) -> Result<Vec<T>, ConfigError>
where
    T: for<'de> serde::Deserialize<'de>,
{
    let sql = format!(
        "SELECT payload FROM {} WHERE package_id = $1 ORDER BY id",
        table
    );
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
