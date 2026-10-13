//! Config validation: referential integrity and API consistency.

use crate::config::types::ColumnTypeConfig;
use crate::config::{FullConfig, PrimaryKeyConfig};
use crate::error::ConfigError;
use std::collections::{HashMap, HashSet};

/// The raw, user-authored type string for a column (before canonicalization).
fn raw_type_str(t: &ColumnTypeConfig) -> &str {
    match t {
        ColumnTypeConfig::Simple(s) => s.as_str(),
        ColumnTypeConfig::Parameterized { name, .. } => name.as_str(),
    }
}

/// Default schema id when configs omit schema_id (manifest-driven schema).
pub fn default_schema_id(config: &FullConfig) -> Result<&str, ConfigError> {
    config
        .schemas
        .first()
        .map(|s| s.id.as_str())
        .ok_or_else(|| {
            ConfigError::Validation("at least one schema required (set manifest.schema)".into())
        })
}

pub fn validate(config: &FullConfig) -> Result<(), ConfigError> {
    let default_sid = default_schema_id(config)?;
    let schema_ids: HashSet<&str> = config.schemas.iter().map(|s| s.id.as_str()).collect();
    let table_ids: HashSet<&str> = config.tables.iter().map(|t| t.id.as_str()).collect();
    let column_ids: HashSet<&str> = config.columns.iter().map(|c| c.id.as_str()).collect();

    for e in &config.enums {
        let sid = e.schema_id.as_deref().unwrap_or(default_sid);
        if !schema_ids.contains(sid) {
            return Err(ConfigError::MissingReference {
                kind: "schema",
                id: sid.to_string(),
            });
        }
    }

    for t in &config.tables {
        let sid = t.schema_id.as_deref().unwrap_or(default_sid);
        if !schema_ids.contains(sid) {
            return Err(ConfigError::MissingReference {
                kind: "schema",
                id: sid.to_string(),
            });
        }
        if let Some(ref v) = t.versioning {
            if v.enabled {
                if let Some(kv) = v.keep_versions {
                    if kv < 1 {
                        return Err(ConfigError::Validation(format!(
                            "table '{}': versioning.keep_versions must be ≥ 1 (got {})",
                            t.id, kv
                        )));
                    }
                }
            }
        }
        let pk_cols = match &t.primary_key {
            PrimaryKeyConfig::Single(s) => vec![s.as_str()],
            PrimaryKeyConfig::Composite(v) => v.iter().map(String::as_str).collect::<Vec<_>>(),
        };
        let table_columns: HashSet<&str> = config
            .columns
            .iter()
            .filter(|c| c.table_id == t.id)
            .map(|c| c.name.as_str())
            .collect();
        for pk in &pk_cols {
            if !table_columns.contains(pk) {
                return Err(ConfigError::InvalidPrimaryKey {
                    table_id: t.id.clone(),
                    column: (*pk).to_string(),
                });
            }
        }
    }

    for c in &config.columns {
        if !table_ids.contains(c.table_id.as_str()) {
            return Err(ConfigError::MissingReference {
                kind: "table",
                id: c.table_id.clone(),
            });
        }
    }
    validate_generated_columns(config)?;

    // Validate schema-qualified enum-typed columns. A column type like
    // `manufacturing_essential.material_unit` must reference an enum that this config actually
    // defines under that schema. The common failure is a typo in the schema prefix (e.g.
    // `manufactring_essential.material_unit`): the enum name is real but the qualified name is
    // wrong, so `CREATE TABLE` later fails with "type ... does not exist", leaving a partial
    // install. We catch it here, before any DDL or `_sys_*` writes.
    let schema_name_by_id: HashMap<&str, &str> = config
        .schemas
        .iter()
        .map(|s| (s.id.as_str(), s.name.as_str()))
        .collect();
    let mut enum_qnames: HashSet<String> = HashSet::new();
    let mut enum_names: HashSet<&str> = HashSet::new();
    for e in &config.enums {
        let sid = e.schema_id.as_deref().unwrap_or(default_sid);
        let sname = schema_name_by_id.get(sid).copied().unwrap_or(sid);
        enum_qnames.insert(format!("{}.{}", sname, e.name));
        enum_names.insert(e.name.as_str());
    }
    for c in &config.columns {
        // Strip a trailing array marker so `schema.enum[]` is checked as `schema.enum`.
        let base = raw_type_str(&c.type_).trim();
        let base = base.strip_suffix("[]").unwrap_or(base).trim();
        if let Some((_, type_part)) = base.rsplit_once('.') {
            // Only flag types whose bare name matches a known enum: any other dotted type is an
            // external/native type (e.g. `public.citext`) we cannot validate locally.
            if enum_names.contains(type_part) && !enum_qnames.contains(base) {
                let expected: Vec<String> = enum_qnames
                    .iter()
                    .filter(|q| q.ends_with(&format!(".{}", type_part)))
                    .cloned()
                    .collect();
                return Err(ConfigError::Validation(format!(
                    "column '{}' (table '{}') has type '{}', but no enum is defined with that \
                     qualified name. Enum '{}' exists under a different schema — expected [{}]. \
                     Check the schema prefix for a typo.",
                    c.name,
                    c.table_id,
                    raw_type_str(&c.type_),
                    type_part,
                    expected.join(", "),
                )));
            }
        }
    }

    for idx in &config.indexes {
        let sid = idx.schema_id.as_deref().unwrap_or(default_sid);
        if !schema_ids.contains(sid) || !table_ids.contains(idx.table_id.as_str()) {
            return Err(ConfigError::MissingReference {
                kind: "schema or table",
                id: format!("{} / {}", sid, idx.table_id),
            });
        }
    }

    for r in &config.relationships {
        let from_sid = r.from_schema_id.as_deref().unwrap_or(default_sid);
        // For cross-package relationships the to_* side lives in another package's config;
        // skip local referential checks for it and only validate the from_* side.
        let is_cross_package = r.to_package_id.is_some();
        let local_to_sid = r.to_schema_id.as_deref().unwrap_or(default_sid);
        let to_side_valid = is_cross_package
            || (schema_ids.contains(local_to_sid)
                && table_ids.contains(r.to_table_id.as_str())
                && column_ids.contains(r.to_column_id.as_str()));
        if !schema_ids.contains(from_sid)
            || !table_ids.contains(r.from_table_id.as_str())
            || !column_ids.contains(r.from_column_id.as_str())
            || !to_side_valid
        {
            return Err(ConfigError::MissingReference {
                kind: "relationship",
                id: r.id.clone(),
            });
        }
    }

    validate_views(config, default_sid, &schema_ids, &table_ids)?;
    let views_by_id: HashMap<&str, &crate::config::ViewConfig> =
        config.views.iter().map(|v| (v.id.as_str(), v)).collect();

    let mut path_segments = HashSet::new();
    for api in &config.api_entities {
        if let Some(view) = views_by_id.get(api.entity_id.as_str()) {
            if let Some(op) = api
                .operations
                .iter()
                .find(|op| !VIEW_OPERATIONS.contains(&op.as_str()))
            {
                return Err(ConfigError::Validation(format!(
                    "api entity '{}' exposes view '{}': views are read-only, operation '{}' is \
                     not allowed (use {})",
                    api.path_segment,
                    view.id,
                    op,
                    VIEW_OPERATIONS.join(", ")
                )));
            }
            if view.primary_key.is_none() {
                return Err(ConfigError::Validation(format!(
                    "view '{}' is exposed as api entity '{}' and needs a primary_key",
                    view.id, api.path_segment
                )));
            }
        } else if !table_ids.contains(api.entity_id.as_str()) {
            return Err(ConfigError::MissingReference {
                kind: "table",
                id: api.entity_id.clone(),
            });
        }
        if !path_segments.insert(api.path_segment.as_str()) {
            return Err(ConfigError::DuplicatePathSegment(api.path_segment.clone()));
        }
    }

    Ok(())
}

/// Generated columns: a non-empty expression, known dialect overrides, and nothing that would
/// require writing the column (default, primary key, asset upload, extensible fields).
fn validate_generated_columns(config: &FullConfig) -> Result<(), ConfigError> {
    let pk_by_table: HashMap<&str, Vec<&str>> = config
        .tables
        .iter()
        .map(|t| {
            let pk = match &t.primary_key {
                PrimaryKeyConfig::Single(s) => vec![s.as_str()],
                PrimaryKeyConfig::Composite(v) => v.iter().map(String::as_str).collect(),
            };
            (t.id.as_str(), pk)
        })
        .collect();
    for c in &config.columns {
        let Some(g) = &c.generated else { continue };
        let invalid = |msg: &str| {
            ConfigError::Validation(format!(
                "column '{}' (table '{}'): generated column {}",
                c.name, c.table_id, msg
            ))
        };
        if g.expression.trim().is_empty()
            || g.expression_by_dialect
                .values()
                .any(|e| e.trim().is_empty())
        {
            return Err(invalid("expression must not be empty"));
        }
        if let Some(key) = g
            .expression_by_dialect
            .keys()
            .find(|k| !VIEW_DIALECTS.contains(&k.as_str()))
        {
            return Err(invalid(&format!(
                "expression_by_dialect key '{}' is not one of {}",
                key,
                VIEW_DIALECTS.join(", ")
            )));
        }
        if c.default.is_some() {
            return Err(invalid("cannot have a default"));
        }
        if pk_by_table
            .get(c.table_id.as_str())
            .is_some_and(|pk| pk.contains(&c.name.as_str()))
        {
            return Err(invalid("cannot be part of the primary key"));
        }
        if c.asset.is_some() || c.extensible {
            return Err(invalid("cannot be an asset or extensible column"));
        }
    }
    Ok(())
}

/// Operations a view may expose through `api_entities`.
pub const VIEW_OPERATIONS: &[&str] = &["list", "read"];

/// Smallest `refresh.max_age_secs`: stale refreshes run from a schedule that fires at most once a
/// minute.
const MIN_REFRESH_MAX_AGE_SECS: u64 = 60;

/// Dialect names accepted as `sql_by_dialect` keys.
const VIEW_DIALECTS: &[&str] = &["postgres", "mysql", "sqlite"];

fn validate_views(
    config: &FullConfig,
    default_sid: &str,
    schema_ids: &HashSet<&str>,
    table_ids: &HashSet<&str>,
) -> Result<(), ConfigError> {
    let mut view_ids: HashSet<&str> = HashSet::new();
    for v in &config.views {
        let invalid = |msg: String| ConfigError::Validation(format!("view '{}': {}", v.id, msg));
        // api_entities.entity_id may name a table or a view, so the two share one id space.
        if table_ids.contains(v.id.as_str()) || !view_ids.insert(v.id.as_str()) {
            return Err(invalid(
                "id is already used by another table or view".into(),
            ));
        }
        let sid = v.schema_id.as_deref().unwrap_or(default_sid);
        if !schema_ids.contains(sid) {
            return Err(ConfigError::MissingReference {
                kind: "schema",
                id: sid.to_string(),
            });
        }
        if let Some(r) = &v.refresh {
            if !v.materialized {
                return Err(invalid(
                    "refresh applies only to materialized views (a plain view is always live)"
                        .into(),
                ));
            }
            if r.max_age_secs < MIN_REFRESH_MAX_AGE_SECS {
                return Err(invalid(format!(
                    "refresh.max_age_secs must be at least {}",
                    MIN_REFRESH_MAX_AGE_SECS
                )));
            }
        }
        if v.materialized && v.primary_key.is_none() {
            return Err(invalid(
                "a materialized view needs a primary_key (it keys the stored rows)".into(),
            ));
        }
        if v.sql.trim().is_empty() {
            return Err(invalid("sql must not be empty".into()));
        }
        if let Some(key) = v
            .sql_by_dialect
            .keys()
            .find(|k| !VIEW_DIALECTS.contains(&k.as_str()))
        {
            return Err(invalid(format!(
                "sql_by_dialect key '{}' is not one of {}",
                key,
                VIEW_DIALECTS.join(", ")
            )));
        }
        if v.columns.is_empty() {
            return Err(invalid(
                "columns must list the view's output columns".into(),
            ));
        }
        let mut names: HashSet<&str> = HashSet::new();
        for c in &v.columns {
            if !names.insert(c.name.as_str()) {
                return Err(invalid(format!("column '{}' is listed twice", c.name)));
            }
            if v.materialized && c.name == crate::migration::RLS_TENANT_COLUMN {
                return Err(invalid(format!(
                    "'{}' is managed by the SDK for materialized views: return it from the sql \
                     ({{{{tenant_id:<alias>}}}} AS {}) but do not declare it",
                    c.name, c.name
                )));
            }
        }
        for pk in v.primary_key_columns() {
            if !names.contains(pk.as_str()) {
                return Err(invalid(format!(
                    "primary_key column '{}' is not one of its columns",
                    pk
                )));
            }
        }
        if let Some(r) = v.reads.iter().find(|r| r.split_once('.').is_none()) {
            return Err(invalid(format!(
                "reads entry '{}' must be schema-qualified (schema.name)",
                r
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::types::{
        ApiEntityConfig, ColumnConfig, ColumnTypeConfig, EnumConfig, FullConfig, PrimaryKeyConfig,
        SchemaConfig, TableConfig,
    };

    fn schema(id: &str) -> SchemaConfig {
        SchemaConfig {
            id: id.into(),
            name: id.into(),
            comment: None,
        }
    }

    fn enum_def(id: &str, schema_id: Option<&str>, name: &str) -> EnumConfig {
        EnumConfig {
            id: id.into(),
            schema_id: schema_id.map(Into::into),
            name: name.into(),
            values: vec!["a".into(), "b".into()],
            comment: None,
        }
    }

    fn typed_column(id: &str, table_id: &str, name: &str, ty: &str) -> ColumnConfig {
        ColumnConfig {
            type_: ColumnTypeConfig::Simple(ty.into()),
            ..column(id, table_id, name)
        }
    }

    fn table(id: &str, schema_id: Option<&str>, pk: &str) -> TableConfig {
        TableConfig {
            id: id.into(),
            schema_id: schema_id.map(Into::into),
            name: id.into(),
            comment: None,
            primary_key: PrimaryKeyConfig::Single(pk.into()),
            unique: vec![],
            check: vec![],
            audit_log: false,
            versioning: None,
            global: false,
        }
    }

    fn column(id: &str, table_id: &str, name: &str) -> ColumnConfig {
        ColumnConfig {
            id: id.into(),
            table_id: table_id.into(),
            name: name.into(),
            type_: ColumnTypeConfig::Simple("text".into()),
            nullable: true,
            default: None,
            comment: None,
            asset: None,
            extensible: false,
            generated: None,
        }
    }

    fn api_entity(table_id: &str, path: &str) -> ApiEntityConfig {
        ApiEntityConfig {
            entity_id: table_id.into(),
            path_segment: path.into(),
            operations: vec!["list".into()],
            sensitive_columns: vec![],
            validation: Default::default(),
            archive_field: None,
            events: vec![],
            parent_ref_column: None,
            mcp: None,
        }
    }

    fn minimal_config() -> FullConfig {
        let mut c = FullConfig::default();
        c.schemas.push(schema("s1"));
        c.tables.push(table("t1", None, "id"));
        c.columns.push(column("c1", "t1", "id"));
        c.api_entities.push(api_entity("t1", "items"));
        c
    }

    // --- happy path ---

    #[test]
    fn valid_config_passes() {
        assert!(validate(&minimal_config()).is_ok());
    }

    // --- generated columns ---

    fn generated(expr: &str) -> Option<crate::config::types::GeneratedColumnConfig> {
        Some(crate::config::types::GeneratedColumnConfig {
            expression: expr.into(),
            expression_by_dialect: Default::default(),
        })
    }

    #[test]
    fn generated_column_passes() {
        let mut c = minimal_config();
        c.columns.push(ColumnConfig {
            generated: generated("upper(id)"),
            ..column("c2", "t1", "id_upper")
        });
        assert!(validate(&c).is_ok());
    }

    #[test]
    fn invalid_generated_columns_fail() {
        use crate::config::types::ColumnDefaultConfig;
        let cases: Vec<(ColumnConfig, &str)> = vec![
            (
                ColumnConfig {
                    generated: generated("  "),
                    ..column("c2", "t1", "g")
                },
                "expression must not be empty",
            ),
            (
                ColumnConfig {
                    generated: generated("upper(id)"),
                    default: Some(ColumnDefaultConfig::Literal("'x'".into())),
                    ..column("c2", "t1", "g")
                },
                "cannot have a default",
            ),
            (
                ColumnConfig {
                    generated: generated("upper(id)").map(|mut g| {
                        g.expression_by_dialect.insert("oracle".into(), "x".into());
                        g
                    }),
                    ..column("c2", "t1", "g")
                },
                "expression_by_dialect key 'oracle'",
            ),
            (
                ColumnConfig {
                    generated: generated("1"),
                    extensible: true,
                    ..column("c2", "t1", "g")
                },
                "asset or extensible",
            ),
        ];
        for (col, expected) in cases {
            let mut c = minimal_config();
            c.columns.push(col);
            let msg = validate(&c).unwrap_err().to_string();
            assert!(
                msg.contains(expected),
                "{} does not mention {}",
                msg,
                expected
            );
        }
        let mut c = minimal_config();
        c.columns[0].generated = generated("1");
        assert!(validate(&c)
            .unwrap_err()
            .to_string()
            .contains("primary key"));
    }

    // --- empty schemas ---

    #[test]
    fn no_schemas_fails() {
        let mut c = minimal_config();
        c.schemas.clear();
        assert!(matches!(validate(&c), Err(ConfigError::Validation(_))));
    }

    // --- missing table for api_entity ---

    #[test]
    fn api_entity_missing_table_fails() {
        let mut c = minimal_config();
        c.api_entities[0].entity_id = "nonexistent".into();
        assert!(matches!(
            validate(&c),
            Err(ConfigError::MissingReference { kind: "table", .. })
        ));
    }

    // --- duplicate path segments ---

    #[test]
    fn duplicate_path_segment_fails() {
        let mut c = minimal_config();
        c.tables.push(table("t2", None, "id"));
        c.columns.push(column("c2", "t2", "id"));
        c.api_entities.push(api_entity("t2", "items")); // same path as t1
        assert!(matches!(
            validate(&c),
            Err(ConfigError::DuplicatePathSegment(_))
        ));
    }

    // --- column references nonexistent table ---

    #[test]
    fn column_missing_table_fails() {
        let mut c = minimal_config();
        c.columns.push(column("c99", "no_such_table", "name"));
        assert!(matches!(
            validate(&c),
            Err(ConfigError::MissingReference { kind: "table", .. })
        ));
    }

    // --- primary key column must exist ---

    #[test]
    fn pk_column_missing_fails() {
        let mut c = FullConfig::default();
        c.schemas.push(schema("s1"));
        c.tables.push(table("t1", None, "id")); // pk="id" but no column "id"
                                                // Add a column with a *different* name so the table itself passes the column table_id check
        c.columns.push(column("c1", "t1", "not_id"));
        c.api_entities.push(api_entity("t1", "items"));
        assert!(matches!(
            validate(&c),
            Err(ConfigError::InvalidPrimaryKey { .. })
        ));
    }

    // --- schema reference on table ---

    #[test]
    fn table_missing_schema_fails() {
        let mut c = minimal_config();
        c.tables[0].schema_id = Some("nonexistent_schema".into());
        assert!(matches!(
            validate(&c),
            Err(ConfigError::MissingReference { kind: "schema", .. })
        ));
    }

    // --- versioning ---

    #[test]
    fn versioning_keep_versions_zero_fails() {
        use crate::config::types::VersioningConfig;
        let mut c = minimal_config();
        c.tables[0].versioning = Some(VersioningConfig {
            enabled: true,
            keep_versions: Some(0),
        });
        assert!(matches!(validate(&c), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn versioning_keep_versions_negative_fails() {
        use crate::config::types::VersioningConfig;
        let mut c = minimal_config();
        c.tables[0].versioning = Some(VersioningConfig {
            enabled: true,
            keep_versions: Some(-1),
        });
        assert!(matches!(validate(&c), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn versioning_keep_versions_one_passes() {
        use crate::config::types::VersioningConfig;
        let mut c = minimal_config();
        c.tables[0].versioning = Some(VersioningConfig {
            enabled: true,
            keep_versions: Some(1),
        });
        assert!(validate(&c).is_ok());
    }

    #[test]
    fn versioning_keep_versions_none_passes() {
        use crate::config::types::VersioningConfig;
        let mut c = minimal_config();
        c.tables[0].versioning = Some(VersioningConfig {
            enabled: true,
            keep_versions: None,
        });
        assert!(validate(&c).is_ok());
    }

    #[test]
    fn versioning_disabled_with_zero_keep_passes() {
        // keep_versions validation skipped when enabled = false
        use crate::config::types::VersioningConfig;
        let mut c = minimal_config();
        c.tables[0].versioning = Some(VersioningConfig {
            enabled: false,
            keep_versions: Some(0),
        });
        assert!(validate(&c).is_ok());
    }

    // --- enum-typed column schema-prefix validation ---

    #[test]
    fn enum_column_correct_schema_passes() {
        // schema "s1" has name "s1"; enum defined under it; column references "s1.status".
        let mut c = minimal_config();
        c.enums.push(enum_def("e1", None, "status"));
        c.columns
            .push(typed_column("c2", "t1", "state", "s1.status"));
        assert!(validate(&c).is_ok());
    }

    #[test]
    fn enum_column_typo_schema_prefix_fails() {
        // enum "status" exists under "s1", but column references it under a typo'd schema.
        let mut c = minimal_config();
        c.enums.push(enum_def("e1", None, "status"));
        c.columns
            .push(typed_column("c2", "t1", "state", "s_typo.status"));
        assert!(matches!(validate(&c), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn enum_array_column_typo_schema_prefix_fails() {
        // Array marker is stripped before the check, so "s_typo.status[]" is still caught.
        let mut c = minimal_config();
        c.enums.push(enum_def("e1", None, "status"));
        c.columns
            .push(typed_column("c2", "t1", "state", "s_typo.status[]"));
        assert!(matches!(validate(&c), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn dotted_external_type_passes() {
        // A dotted type whose bare name is not a defined enum (e.g. an extension type) is skipped.
        let mut c = minimal_config();
        c.columns
            .push(typed_column("c2", "t1", "name2", "public.citext"));
        assert!(validate(&c).is_ok());
    }

    // --- default_schema_id ---

    #[test]
    fn default_schema_id_returns_first() {
        let c = minimal_config();
        assert_eq!(default_schema_id(&c).unwrap(), "s1");
    }

    #[test]
    fn default_schema_id_empty_fails() {
        let c = FullConfig::default();
        assert!(default_schema_id(&c).is_err());
    }
}

#[cfg(test)]
mod view_validation_tests {
    use super::*;
    use crate::config::resolve;
    use crate::config::types::{
        ApiEntityConfig, ColumnConfig, ColumnTypeConfig, SchemaConfig, TableConfig,
        ViewColumnConfig, ViewConfig,
    };

    fn view() -> ViewConfig {
        ViewConfig {
            id: "v_sales".into(),
            schema_id: None,
            name: "sales".into(),
            comment: None,
            sql: "SELECT id, total FROM shop.orders".into(),
            sql_by_dialect: Default::default(),
            columns: vec![
                ViewColumnConfig {
                    name: "id".into(),
                    type_: ColumnTypeConfig::Simple("uuid".into()),
                    nullable: false,
                    comment: None,
                },
                ViewColumnConfig {
                    name: "total".into(),
                    type_: ColumnTypeConfig::Simple("numeric".into()),
                    nullable: true,
                    comment: None,
                },
            ],
            primary_key: Some(PrimaryKeyConfig::Single("id".into())),
            reads: vec!["shop.orders".into()],
            materialized: false,
            refresh: None,
        }
    }

    fn api(entity_id: &str, ops: &[&str]) -> ApiEntityConfig {
        ApiEntityConfig {
            entity_id: entity_id.into(),
            path_segment: "sales".into(),
            operations: ops.iter().map(|o| o.to_string()).collect(),
            sensitive_columns: vec!["total".into()],
            validation: Default::default(),
            archive_field: None,
            events: vec![],
            parent_ref_column: None,
            mcp: None,
        }
    }

    fn config(v: ViewConfig, apis: Vec<ApiEntityConfig>) -> FullConfig {
        FullConfig {
            schemas: vec![SchemaConfig {
                id: "s1".into(),
                name: "an_sales".into(),
                comment: None,
            }],
            views: vec![v],
            api_entities: apis,
            ..Default::default()
        }
    }

    fn err(cfg: &FullConfig) -> String {
        validate(cfg).unwrap_err().to_string()
    }

    #[test]
    fn a_valid_view_entity_resolves_read_only() {
        let cfg = config(view(), vec![api("v_sales", &["list", "read"])]);
        let model = resolve(&cfg).unwrap();
        let e = model.entity_by_path("sales").unwrap();
        assert_eq!(e.schema_name, "an_sales");
        assert_eq!(e.table_name, "sales");
        assert_eq!(e.pk_columns, vec!["id"]);
        assert!(matches!(e.pk_type, crate::config::PkType::Uuid));
        // Only the declared columns: no synthesized created_at/updated_at/archived_at.
        let cols: Vec<&str> = e.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(cols, vec!["id", "total"]);
        assert!(e.sensitive_columns.contains("total"));
        assert!(e.archive_field.is_none() && e.includes.is_empty());
    }

    #[test]
    fn write_operations_on_a_view_are_rejected() {
        let cfg = config(view(), vec![api("v_sales", &["list", "create"])]);
        assert!(err(&cfg).contains("operation 'create' is not allowed"));
    }

    #[test]
    fn an_exposed_view_needs_a_primary_key() {
        let v = ViewConfig {
            primary_key: None,
            ..view()
        };
        assert!(validate(&config(v.clone(), vec![])).is_ok());
        assert!(err(&config(v, vec![api("v_sales", &["list"])])).contains("needs a primary_key"));
    }

    #[test]
    fn invalid_views_are_rejected() {
        let mut cases: Vec<(ViewConfig, &str)> = vec![
            (
                ViewConfig {
                    materialized: true,
                    primary_key: None,
                    ..view()
                },
                "materialized view needs a primary_key",
            ),
            (
                ViewConfig {
                    materialized: true,
                    columns: vec![ViewColumnConfig {
                        name: "tenant_id".into(),
                        type_: ColumnTypeConfig::Simple("text".into()),
                        nullable: true,
                        comment: None,
                    }],
                    primary_key: Some(PrimaryKeyConfig::Single("tenant_id".into())),
                    ..view()
                },
                "managed by the SDK",
            ),
            (
                ViewConfig {
                    sql: " ".into(),
                    ..view()
                },
                "sql must not be empty",
            ),
            (
                ViewConfig {
                    columns: vec![],
                    ..view()
                },
                "columns must list",
            ),
            (
                ViewConfig {
                    primary_key: Some(PrimaryKeyConfig::Single("nope".into())),
                    ..view()
                },
                "primary_key column 'nope'",
            ),
            (
                ViewConfig {
                    reads: vec!["orders".into()],
                    ..view()
                },
                "must be schema-qualified",
            ),
            (
                ViewConfig {
                    sql_by_dialect: [("oracle".to_string(), "SELECT 1".to_string())].into(),
                    ..view()
                },
                "sql_by_dialect key 'oracle'",
            ),
        ];
        let refresh = |secs| Some(crate::config::ViewRefreshConfig { max_age_secs: secs });
        cases.push((
            ViewConfig {
                refresh: refresh(3600),
                ..view()
            },
            "refresh applies only to materialized views",
        ));
        cases.push((
            ViewConfig {
                materialized: true,
                refresh: refresh(30),
                ..view()
            },
            "max_age_secs must be at least 60",
        ));
        for (v, expected) in cases {
            let msg = err(&config(v, vec![]));
            assert!(
                msg.contains(expected),
                "{} does not mention {}",
                msg,
                expected
            );
        }
        let mut dup = view();
        dup.columns.push(dup.columns[0].clone());
        assert!(err(&config(dup, vec![])).contains("listed twice"));
    }

    #[test]
    fn view_ids_must_not_collide_with_tables() {
        let mut cfg = config(view(), vec![]);
        cfg.tables.push(TableConfig {
            id: "v_sales".into(),
            schema_id: None,
            name: "t".into(),
            comment: None,
            primary_key: PrimaryKeyConfig::Single("id".into()),
            unique: vec![],
            check: vec![],
            audit_log: false,
            versioning: None,
            global: false,
        });
        cfg.columns.push(ColumnConfig {
            id: "c".into(),
            table_id: "v_sales".into(),
            name: "id".into(),
            type_: ColumnTypeConfig::Simple("uuid".into()),
            nullable: false,
            default: None,
            comment: None,
            asset: None,
            extensible: false,
            generated: None,
        });
        assert!(err(&cfg).contains("already used"));
    }

    #[test]
    fn unknown_entity_ids_are_still_rejected() {
        let cfg = config(view(), vec![api("nope", &["list"])]);
        assert!(matches!(
            validate(&cfg),
            Err(ConfigError::MissingReference { kind: "table", .. })
        ));
    }
}
