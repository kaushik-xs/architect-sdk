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

        // Collect JSON/JSONB columns flagged `extensible: true` — the extensible-fields bags.
        // A non-JSON column flagged extensible is ignored (logged) since JSON-path access
        // only makes sense on a JSON document.
        let extensible_columns: Vec<String> = table_columns
            .iter()
            .filter(|c| c.extensible)
            .filter_map(|c| {
                let canonical = parse_canonical(&c.type_);
                if matches!(
                    canonical,
                    crate::db::CanonicalType::Json | crate::db::CanonicalType::Jsonb
                ) {
                    Some(c.name.clone())
                } else {
                    tracing::warn!(
                        table = %table.id,
                        column = %c.name,
                        "ignoring `extensible: true` on non-JSON column"
                    );
                    None
                }
            })
            .collect();

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
            global: table.global,
            parent_ref_column: api.parent_ref_column.clone(),
            versioning: table.versioning.clone(),
            mcp: api.mcp.clone(),
            extensible_columns,
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
                global: e.global,
                parent_ref_column: None,
                versioning: None,
                mcp: None,
                extensible_columns: Vec::new(),
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

/// Precomputed cross-package include map. Built from ALL installed packages' configs so that
/// `?include=<name>` can join a related entity that lives in a *different* package, in either
/// direction (to_one when the FK is in the requesting table, to_many when it points back to it).
///
/// Keyed by `(owning_table_id, include_name)`. The `include_name` is the related entity's
/// `path_segment`, matching the same-package include convention. Same-package relationships are
/// intentionally skipped — they are already resolved by [`resolve`] into `ResolvedEntity::includes`.
///
/// Limitation: the join executes against the request's tenant pool. For Database-strategy tenants
/// the related package must be physically migrated into that tenant's DB; if it is not, the join
/// fails at execution time rather than here.
#[derive(Clone, Debug, Default)]
pub struct CrossPackageIndex {
    entries: HashMap<(String, String), (IncludeSpec, ResolvedEntity)>,
}

impl CrossPackageIndex {
    /// Look up a cross-package include for `table_id` by include name (the related path_segment).
    pub fn get(&self, table_id: &str, name: &str) -> Option<&(IncludeSpec, ResolvedEntity)> {
        self.entries.get(&(table_id.to_string(), name.to_string()))
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Build the cross-package include index by loading every installed package's config from the
/// central config DB (`_sys_*` tables) and resolving cross-package relationships. Best-effort:
/// packages that fail to load or resolve are skipped rather than aborting the whole index.
pub async fn build_cross_package_index(pool: &Pool) -> CrossPackageIndex {
    let mut ids = crate::store::list_package_ids(pool)
        .await
        .unwrap_or_default();
    if !ids.iter().any(|i| i == crate::store::DEFAULT_PACKAGE_ID) {
        ids.push(crate::store::DEFAULT_PACKAGE_ID.to_string());
    }

    // Global maps spanning all packages.
    let mut col_name: HashMap<String, String> = HashMap::new();
    let mut table_to_path: HashMap<String, String> = HashMap::new();
    let mut table_to_pkg: HashMap<String, String> = HashMap::new();
    let mut entity_by_table: HashMap<String, ResolvedEntity> = HashMap::new();
    let mut all_rels: Vec<RelationshipConfig> = Vec::new();

    for id in ids {
        let cfg = match load_from_pool(pool, &id).await {
            Ok(c) => c,
            Err(_) => continue,
        };
        for c in &cfg.columns {
            col_name.insert(c.id.clone(), c.name.clone());
        }
        for api in &cfg.api_entities {
            table_to_path.insert(api.entity_id.clone(), api.path_segment.clone());
            table_to_pkg.insert(api.entity_id.clone(), id.clone());
        }
        if let Ok(model) = resolve(&cfg) {
            for e in model.with_package_id(&id).entities {
                entity_by_table.entry(e.table_id.clone()).or_insert(e);
            }
        }
        all_rels.extend(cfg.relationships.iter().cloned());
    }

    CrossPackageIndex {
        entries: cross_package_entries(
            &col_name,
            &table_to_path,
            &table_to_pkg,
            &entity_by_table,
            &all_rels,
        ),
    }
}

/// Pure relationship → cross-package include mapping. Separated from DB loading so it can be
/// unit-tested. Only relationships whose two sides live in *different* packages produce entries;
/// same-package relationships are handled by [`resolve`].
fn cross_package_entries(
    col_name: &HashMap<String, String>,
    table_to_path: &HashMap<String, String>,
    table_to_pkg: &HashMap<String, String>,
    entity_by_table: &HashMap<String, ResolvedEntity>,
    relationships: &[RelationshipConfig],
) -> HashMap<(String, String), (IncludeSpec, ResolvedEntity)> {
    let mut entries: HashMap<(String, String), (IncludeSpec, ResolvedEntity)> = HashMap::new();
    for rel in relationships {
        let (Some(from_pkg), Some(to_pkg)) = (
            table_to_pkg.get(&rel.from_table_id),
            table_to_pkg.get(&rel.to_table_id),
        ) else {
            continue;
        };
        if from_pkg == to_pkg {
            continue; // same-package relationships are handled by resolve()
        }
        let (Some(from_col), Some(to_col), Some(from_path), Some(to_path)) = (
            col_name.get(&rel.from_column_id),
            col_name.get(&rel.to_column_id),
            table_to_path.get(&rel.from_table_id),
            table_to_path.get(&rel.to_table_id),
        ) else {
            continue;
        };

        // to_one: the requesting (from) table holds the FK and includes the to-side entity.
        if let Some(related) = entity_by_table.get(&rel.to_table_id) {
            let spec = IncludeSpec {
                name: to_path.clone(),
                direction: IncludeDirection::ToOne,
                related_path_segment: to_path.clone(),
                our_key_column: from_col.clone(),
                their_key_column: to_col.clone(),
            };
            entries.insert(
                (rel.from_table_id.clone(), to_path.clone()),
                (spec, related.clone()),
            );
        }
        // to_many: the to-side table includes the rows that point back to it via the FK.
        if let Some(related) = entity_by_table.get(&rel.from_table_id) {
            let spec = IncludeSpec {
                name: from_path.clone(),
                direction: IncludeDirection::ToMany,
                related_path_segment: from_path.clone(),
                our_key_column: to_col.clone(),
                their_key_column: from_col.clone(),
            };
            entries.insert(
                (rel.to_table_id.clone(), from_path.clone()),
                (spec, related.clone()),
            );
        }
    }
    entries
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ent(table_id: &str, path: &str, schema: &str, pkg: &str) -> ResolvedEntity {
        ResolvedEntity {
            table_id: table_id.into(),
            schema_name: schema.into(),
            table_name: table_id.into(),
            path_segment: path.into(),
            pk_columns: vec!["id".into()],
            pk_type: PkType::Uuid,
            columns: vec![],
            operations: vec!["list".into(), "read".into()],
            sensitive_columns: HashSet::new(),
            includes: vec![],
            validation: HashMap::new(),
            events: vec![],
            archive_field: None,
            package_id: pkg.into(),
            audit_log: false,
            global: false,
            parent_ref_column: None,
            versioning: None,
            mcp: None,
            extensible_columns: vec![],
        }
    }

    fn rel(id: &str, from_t: &str, from_c: &str, to_t: &str, to_c: &str) -> RelationshipConfig {
        RelationshipConfig {
            id: id.into(),
            from_schema_id: None,
            from_table_id: from_t.into(),
            from_column_id: from_c.into(),
            to_package_id: None,
            to_schema_id: None,
            to_table_id: to_t.into(),
            to_column_id: to_c.into(),
            on_update: None,
            on_delete: None,
            name: None,
        }
    }

    /// A cross-package FK (users in pkg A → orgs in pkg B) yields BOTH directions:
    /// users can include `orgs` (to_one) and orgs can include `users` (to_many).
    #[test]
    fn cross_package_relationship_builds_both_directions() {
        let col_name = HashMap::from([
            ("c_org_id".to_string(), "org_id".to_string()),
            ("c_org_pk".to_string(), "id".to_string()),
        ]);
        let table_to_path = HashMap::from([
            ("t_users".to_string(), "users".to_string()),
            ("t_orgs".to_string(), "orgs".to_string()),
        ]);
        let table_to_pkg = HashMap::from([
            ("t_users".to_string(), "pkg_a".to_string()),
            ("t_orgs".to_string(), "pkg_b".to_string()),
        ]);
        let entity_by_table = HashMap::from([
            ("t_users".to_string(), ent("t_users", "users", "a", "pkg_a")),
            ("t_orgs".to_string(), ent("t_orgs", "orgs", "b", "pkg_b")),
        ]);
        let rels = vec![rel("r1", "t_users", "c_org_id", "t_orgs", "c_org_pk")];

        let entries = cross_package_entries(
            &col_name,
            &table_to_path,
            &table_to_pkg,
            &entity_by_table,
            &rels,
        );

        // to_one: users?include=orgs
        let (one_spec, one_rel) = entries
            .get(&("t_users".to_string(), "orgs".to_string()))
            .expect("users → orgs to_one entry");
        assert!(matches!(one_spec.direction, IncludeDirection::ToOne));
        assert_eq!(one_spec.our_key_column, "org_id");
        assert_eq!(one_spec.their_key_column, "id");
        assert_eq!(one_rel.schema_name, "b");

        // to_many: orgs?include=users
        let (many_spec, many_rel) = entries
            .get(&("t_orgs".to_string(), "users".to_string()))
            .expect("orgs → users to_many entry");
        assert!(matches!(many_spec.direction, IncludeDirection::ToMany));
        assert_eq!(many_spec.our_key_column, "id");
        assert_eq!(many_spec.their_key_column, "org_id");
        assert_eq!(many_rel.schema_name, "a");
    }

    /// Same-package relationships must NOT appear in the cross-package index (resolve() owns them).
    #[test]
    fn same_package_relationship_is_skipped() {
        let col_name = HashMap::from([
            ("c_fk".to_string(), "user_id".to_string()),
            ("c_pk".to_string(), "id".to_string()),
        ]);
        let table_to_path = HashMap::from([
            ("t_orders".to_string(), "orders".to_string()),
            ("t_users".to_string(), "users".to_string()),
        ]);
        // Both tables in the same package.
        let table_to_pkg = HashMap::from([
            ("t_orders".to_string(), "pkg_a".to_string()),
            ("t_users".to_string(), "pkg_a".to_string()),
        ]);
        let entity_by_table = HashMap::from([
            (
                "t_orders".to_string(),
                ent("t_orders", "orders", "a", "pkg_a"),
            ),
            ("t_users".to_string(), ent("t_users", "users", "a", "pkg_a")),
        ]);
        let rels = vec![rel("r1", "t_orders", "c_fk", "t_users", "c_pk")];

        let entries = cross_package_entries(
            &col_name,
            &table_to_path,
            &table_to_pkg,
            &entity_by_table,
            &rels,
        );
        assert!(entries.is_empty(), "same-package rel should be skipped");
    }

    /// When the related table has no API entity (not exposed), no include is built for it.
    #[test]
    fn missing_related_path_yields_no_entry() {
        let col_name = HashMap::from([
            ("c_org_id".to_string(), "org_id".to_string()),
            ("c_org_pk".to_string(), "id".to_string()),
        ]);
        // t_orgs has a package mapping but no path_segment (no api_entity).
        let table_to_path = HashMap::from([("t_users".to_string(), "users".to_string())]);
        let table_to_pkg = HashMap::from([
            ("t_users".to_string(), "pkg_a".to_string()),
            ("t_orgs".to_string(), "pkg_b".to_string()),
        ]);
        let entity_by_table =
            HashMap::from([("t_users".to_string(), ent("t_users", "users", "a", "pkg_a"))]);
        let rels = vec![rel("r1", "t_users", "c_org_id", "t_orgs", "c_org_pk")];

        let entries = cross_package_entries(
            &col_name,
            &table_to_path,
            &table_to_pkg,
            &entity_by_table,
            &rels,
        );
        assert!(entries.is_empty());
    }
}
