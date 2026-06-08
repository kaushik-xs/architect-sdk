//! Per-tenant **extensible fields** for extensible JSON/JSONB columns.
//!
//! A column declared `"extensible": true` in config becomes a *extensible-fields bag*: a JSON
//! document whose keys are defined per tenant in a KV-stored **registry**, not in the schema.
//! Those keys become first-class RSQL-filterable/sortable fields via the `<column>.<key>`
//! dotted syntax (e.g. `q=attributes.warrantyMonths=ge=12`, `sort=-attributes.voltage`).
//!
//! ## Registry storage
//! Definitions live in `_sys_kv_data` under the reserved namespace [`REGISTRY_NAMESPACE`],
//! one row per entity keyed by the entity's `path_segment`. The stored value maps each
//! extensible column name to its list of field definitions:
//!
//! ```json
//! {
//!   "attributes": [
//!     { "key": "warrantyMonths", "type": "int",  "filterable": true, "sortable": true },
//!     { "key": "voltage",        "type": "decimal" }
//!   ]
//! }
//! ```
//!
//! Field keys are stored verbatim (no case conversion), so the convention is **camelCase**
//! — that is exactly how they round-trip to API clients.

use crate::config::types::ColumnTypeConfig;
use crate::config::ResolvedEntity;
use crate::db::{parse_canonical, CanonicalType, Dialect};
use crate::error::AppError;
use crate::store::qualified_sys_table;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

/// Reserved KV namespace under which extensible-field registries are stored.
pub const REGISTRY_NAMESPACE: &str = "__extensible_fields__";

/// How long a cached registry stays valid before it is reloaded from the config DB.
/// Bounds cross-instance staleness when another node updates the registry (single-instance
/// updates are evicted immediately on write).
pub const REGISTRY_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// A cached registry plus the instant it was loaded (for TTL expiry).
#[derive(Clone)]
pub struct CachedRegistry {
    pub registry: ExtensibleRegistry,
    pub loaded_at: std::time::Instant,
}

/// Process-shared, tenant-scoped registry cache keyed by `(tenant_id, package_id, path_segment)`.
/// Lives on `AppState`; read-through on load, evicted on admin write.
pub type RegistryCache = std::sync::Arc<
    std::sync::RwLock<std::collections::HashMap<(String, String, String), CachedRegistry>>,
>;

fn default_true() -> bool {
    true
}

/// One extensible-field definition: its key, declared type, and validation/query flags.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensibleFieldDef {
    /// Inner JSON key (camelCase by convention).
    pub key: String,
    /// Declared canonical type string (e.g. `"int"`, `"decimal"`, `"text"`, `"boolean"`).
    /// Drives the SQL cast used for type-correct comparison/sorting.
    #[serde(rename = "type")]
    pub type_: String,
    /// Whether this field must be present on create.
    #[serde(default)]
    pub required: bool,
    /// Whether this field may be used in RSQL `q=` filters. Defaults to true.
    #[serde(default = "default_true")]
    pub filterable: bool,
    /// Whether this field may be used in `sort=`. Defaults to true.
    #[serde(default = "default_true")]
    pub sortable: bool,
    /// Allowed value set (enum-style).
    #[serde(default)]
    pub allowed: Option<Vec<Value>>,
    /// Numeric lower bound (inclusive).
    #[serde(default)]
    pub min: Option<f64>,
    /// Numeric upper bound (inclusive).
    #[serde(default)]
    pub max: Option<f64>,
    /// Maximum string length.
    #[serde(default)]
    pub max_length: Option<u32>,
    /// Minimum string length.
    #[serde(default)]
    pub min_length: Option<u32>,
    /// Regex the string value must fully match.
    #[serde(default)]
    pub pattern: Option<String>,
}

impl ExtensibleFieldDef {
    /// Resolve the declared type string to a [`CanonicalType`].
    pub fn canonical(&self) -> CanonicalType {
        parse_canonical(&ColumnTypeConfig::Simple(self.type_.clone()))
    }
}

/// Resolved extensible-field registry for a single entity: extensible column → (field key → def).
#[derive(Clone, Debug, Default)]
pub struct ExtensibleRegistry {
    bags: HashMap<String, HashMap<String, ExtensibleFieldDef>>,
}

impl ExtensibleRegistry {
    /// True when no extensible column has any declared field.
    pub fn is_empty(&self) -> bool {
        self.bags.is_empty()
    }

    /// All declared fields for one bag column, if any.
    pub fn bag(&self, column: &str) -> Option<&HashMap<String, ExtensibleFieldDef>> {
        self.bags.get(column)
    }

    /// A single field definition by (column, key), if declared.
    pub fn field(&self, column: &str, key: &str) -> Option<&ExtensibleFieldDef> {
        self.bags.get(column).and_then(|b| b.get(key))
    }

    /// Build from the raw KV value shape `{ "<column>": [defs...] }`.
    pub fn from_value(v: Value) -> Result<Self, AppError> {
        let raw: HashMap<String, Vec<ExtensibleFieldDef>> =
            serde_json::from_value(v).map_err(|e| {
                AppError::Validation(format!("invalid extensible-fields registry: {}", e))
            })?;
        let mut bags = HashMap::new();
        for (column, defs) in raw {
            let mut by_key = HashMap::new();
            for def in defs {
                by_key.insert(def.key.clone(), def);
            }
            bags.insert(column, by_key);
        }
        Ok(ExtensibleRegistry { bags })
    }
}

/// Load the extensible-field registry for one entity from the KV store.
///
/// Returns an empty registry when no row exists (the feature is opt-in and must never error
/// merely because a tenant has declared no extensible fields).
pub async fn load_registry(
    pool: &crate::db::pool::Pool,
    dialect: &dyn Dialect,
    tenant_id: &str,
    package_id: &str,
    path_segment: &str,
) -> Result<ExtensibleRegistry, AppError> {
    let q_table = qualified_sys_table("_sys_kv_data");
    let sql = format!(
        "SELECT value FROM {} WHERE tenant_id = {} AND package_id = {} AND namespace = {} AND key = {}",
        q_table,
        dialect.placeholder(1),
        dialect.placeholder(2),
        dialect.placeholder(3),
        dialect.placeholder(4),
    );
    let row: Option<(Value,)> = sqlx::query_as(&sql)
        .bind(tenant_id)
        .bind(package_id)
        .bind(REGISTRY_NAMESPACE)
        .bind(path_segment)
        .fetch_optional(pool)
        .await?;
    match row {
        Some((v,)) => ExtensibleRegistry::from_value(v),
        None => Ok(ExtensibleRegistry::default()),
    }
}

/// Read the raw registry document for one entity (the value stored in `_sys_kv_data`), or
/// `None` when no registry has been defined. Unlike [`load_registry`], this returns the
/// untouched JSON for display in the admin API rather than the parsed/indexed structure.
pub async fn load_registry_raw(
    pool: &crate::db::pool::Pool,
    dialect: &dyn Dialect,
    tenant_id: &str,
    package_id: &str,
    path_segment: &str,
) -> Result<Option<Value>, AppError> {
    let q_table = qualified_sys_table("_sys_kv_data");
    let sql = format!(
        "SELECT value FROM {} WHERE tenant_id = {} AND package_id = {} AND namespace = {} AND key = {}",
        q_table,
        dialect.placeholder(1),
        dialect.placeholder(2),
        dialect.placeholder(3),
        dialect.placeholder(4),
    );
    let row: Option<(Value,)> = sqlx::query_as(&sql)
        .bind(tenant_id)
        .bind(package_id)
        .bind(REGISTRY_NAMESPACE)
        .bind(path_segment)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|(v,)| v))
}

/// Upsert the registry document for one entity into `_sys_kv_data` under the reserved
/// [`REGISTRY_NAMESPACE`], keyed by `path_segment`. Writes directly, bypassing the
/// `_sys_kv_stores` namespace check the public KV API enforces.
pub async fn store_registry(
    pool: &crate::db::pool::Pool,
    dialect: &dyn Dialect,
    tenant_id: &str,
    package_id: &str,
    path_segment: &str,
    value: &Value,
) -> Result<(), AppError> {
    let q_table = qualified_sys_table("_sys_kv_data");
    let now = dialect.now_fn();
    let (p1, p2, p3, p4, p5) = (
        dialect.placeholder(1),
        dialect.placeholder(2),
        dialect.placeholder(3),
        dialect.placeholder(4),
        dialect.placeholder(5),
    );

    // UPDATE-then-INSERT rather than an ON CONFLICT upsert: the latter would reuse a
    // placeholder in the SET clause, which breaks on positional-placeholder dialects
    // (SQLite/MySQL `?`) by introducing an unbound parameter. Each statement here binds
    // exactly its placeholders, so it is correct on every dialect.
    let update_sql = format!(
        "UPDATE {tbl} SET value = {p1}, updated_at = {now} \
         WHERE tenant_id = {p2} AND package_id = {p3} AND namespace = {p4} AND key = {p5}",
        tbl = q_table,
    );
    let affected = sqlx::query(&update_sql)
        .bind(value)
        .bind(tenant_id)
        .bind(package_id)
        .bind(REGISTRY_NAMESPACE)
        .bind(path_segment)
        .execute(pool)
        .await?
        .rows_affected();

    if affected == 0 {
        let insert_sql = format!(
            "INSERT INTO {tbl} (tenant_id, package_id, namespace, key, value, updated_at) \
             VALUES ({p1}, {p2}, {p3}, {p4}, {p5}, {now})",
            tbl = q_table,
        );
        sqlx::query(&insert_sql)
            .bind(tenant_id)
            .bind(package_id)
            .bind(REGISTRY_NAMESPACE)
            .bind(path_segment)
            .bind(value)
            .execute(pool)
            .await?;
    }
    Ok(())
}

/// Delete the registry document for one entity. Returns `true` when a row was removed.
pub async fn delete_registry(
    pool: &crate::db::pool::Pool,
    dialect: &dyn Dialect,
    tenant_id: &str,
    package_id: &str,
    path_segment: &str,
) -> Result<bool, AppError> {
    let q_table = qualified_sys_table("_sys_kv_data");
    let sql = format!(
        "DELETE FROM {} WHERE tenant_id = {} AND package_id = {} AND namespace = {} AND key = {}",
        q_table,
        dialect.placeholder(1),
        dialect.placeholder(2),
        dialect.placeholder(3),
        dialect.placeholder(4),
    );
    let result = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(package_id)
        .bind(REGISTRY_NAMESPACE)
        .bind(path_segment)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Validate a raw registry document intended for `entity`: it must be a JSON object whose
/// top-level keys are all extensible columns on the entity, and whose values parse as field
/// definition lists. Returns the validated registry on success (HTTP 422 on any problem).
pub fn validate_registry_document(
    value: &Value,
    extensible_columns: &[String],
    path_segment: &str,
) -> Result<ExtensibleRegistry, AppError> {
    let obj = value.as_object().ok_or_else(|| {
        AppError::Validation(
            "registry must be a JSON object mapping column name -> field definitions".into(),
        )
    })?;
    for column in obj.keys() {
        if !extensible_columns.iter().any(|c| c == column) {
            return Err(AppError::Validation(format!(
                "'{}' is not an extensible column on '{}' (declare it with \"extensible\": true)",
                column, path_segment
            )));
        }
    }
    // Shape-validates each definition (key + type required, flags well-typed).
    ExtensibleRegistry::from_value(value.clone())
}

/// Build `CREATE INDEX` statements for every **filterable or sortable** extensible field in the
/// registry, one per (column, key), using the dialect's typed JSON extraction so the index
/// matches the expression the query builder emits.
///
/// - `schema`/`table`: the entity's resolved schema and table names.
/// - `rls_predicate`: `Some((tenant_column, tenant_id))` for RLS shared tables — produces a
///   **partial index** scoped to one tenant so it doesn't bloat with other tenants' rows.
///   `None` for per-tenant databases (Database strategy).
///
/// Statements use `IF NOT EXISTS` where the dialect supports it (Postgres/SQLite); MySQL omits it
/// (callers treat "already exists" as benign). These are intended to be reviewed and applied
/// deliberately — at scale, `CREATE INDEX` on a large table is a heavy operation.
pub fn index_ddl(
    schema: &str,
    table: &str,
    registry: &ExtensibleRegistry,
    dialect: &dyn Dialect,
    rls_predicate: Option<(&str, &str)>,
) -> Vec<String> {
    let qualified = if dialect.supports_schemas() {
        format!(
            "{}.{}",
            dialect.quote_ident(schema),
            dialect.quote_ident(table)
        )
    } else {
        dialect.quote_ident(table)
    };
    let if_not_exists = if dialect.name() == "mysql" {
        ""
    } else {
        "IF NOT EXISTS "
    };
    let where_clause = rls_predicate
        .map(|(col, tid)| {
            format!(
                " WHERE {} = '{}'",
                dialect.quote_ident(col),
                tid.replace('\'', "''")
            )
        })
        .unwrap_or_default();

    let mut out = Vec::new();
    // Deterministic order (column, then key) so generated DDL is stable across runs.
    let mut columns: Vec<&String> = registry.bags.keys().collect();
    columns.sort();
    for column in columns {
        let bag = &registry.bags[column];
        let mut keys: Vec<&String> = bag.keys().collect();
        keys.sort();
        for key in keys {
            let def = &bag[key];
            if !def.filterable && !def.sortable {
                continue;
            }
            let canonical = def.canonical();
            let expr = dialect.json_extract_typed(&dialect.quote_ident(column), key, &canonical);
            let tenant_suffix = rls_predicate.map(|(_, tid)| tid).unwrap_or("");
            let name = index_name(table, column, key, tenant_suffix);
            out.push(format!(
                "CREATE INDEX {}{} ON {} ({}){}",
                if_not_exists,
                dialect.quote_ident(&name),
                qualified,
                expr,
                where_clause
            ));
        }
    }
    out
}

/// Sanitized, length-bounded index identifier: `xf_<table>_<column>_<key>[_<tenant>]`.
fn index_name(table: &str, column: &str, key: &str, tenant: &str) -> String {
    let mut raw = format!("xf_{}_{}_{}", table, column, key);
    if !tenant.is_empty() {
        raw.push('_');
        raw.push_str(tenant);
    }
    let mut s: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    // Postgres/MySQL/SQLite identifier limit is ~63/64 chars; stay safely under.
    if s.len() > 60 {
        s.truncate(60);
    }
    s
}

/// Execute index DDL statements against `pool`, best-effort. Returns `(applied, errors)`:
/// statements that succeeded, and `(statement, message)` for those that failed (e.g. a MySQL
/// "duplicate key name" when the index already exists). Never fails the whole batch on one error.
pub async fn apply_indexes(
    pool: &crate::db::pool::Pool,
    statements: &[String],
) -> (Vec<String>, Vec<(String, String)>) {
    let mut applied = Vec::new();
    let mut errors = Vec::new();
    for stmt in statements {
        match sqlx::query(stmt).execute(pool).await {
            Ok(_) => applied.push(stmt.clone()),
            Err(e) => errors.push((stmt.clone(), e.to_string())),
        }
    }
    (applied, errors)
}

/// Validation mode: `Full` enforces required extensible fields (create); `Partial` validates only
/// the fields present in the request (update/PATCH).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ValidateMode {
    Full,
    Partial,
}

/// Validate the extensible-fields bags in a (snake_cased top-level) request body against the
/// per-tenant registry. Rejects unknown keys, type mismatches, and constraint violations
/// with HTTP 422. Plain (non-extensible) JSON columns are ignored entirely.
pub fn validate_extensible_fields(
    body: &HashMap<String, Value>,
    entity: &ResolvedEntity,
    registry: &ExtensibleRegistry,
    mode: ValidateMode,
) -> Result<(), AppError> {
    for column in &entity.extensible_columns {
        let present = body.get(column);

        // A bag value, when present, must be a JSON object.
        let obj = match present {
            Some(Value::Null) | None => None,
            Some(Value::Object(o)) => Some(o),
            Some(_) => {
                return Err(AppError::Validation(format!(
                    "extensible-fields column '{}' must be a JSON object",
                    column
                )))
            }
        };

        let bag = registry.bag(column);

        // If the request carries extensible fields but no registry is declared, reject — we cannot
        // validate undeclared fields and silent acceptance would defeat typo protection.
        if obj.is_some_and(|o| !o.is_empty()) && bag.is_none() {
            return Err(AppError::Validation(format!(
                "no extensible-field registry declared for column '{}' (namespace '{}', key '{}')",
                column, REGISTRY_NAMESPACE, entity.path_segment
            )));
        }

        // Validate every provided key against its definition.
        if let (Some(o), Some(bag)) = (obj, bag) {
            for (key, val) in o {
                let def = bag.get(key).ok_or_else(|| {
                    AppError::Validation(format!("unknown extensible field '{}.{}'", column, key))
                })?;
                validate_one(column, def, val)?;
            }
        }

        // Enforce required fields on create.
        if mode == ValidateMode::Full {
            if let Some(bag) = bag {
                for def in bag.values().filter(|d| d.required) {
                    let provided = obj.and_then(|o| o.get(&def.key));
                    if matches!(provided, None | Some(Value::Null)) {
                        return Err(AppError::Validation(format!(
                            "missing required extensible field '{}.{}'",
                            column, def.key
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_one(column: &str, def: &ExtensibleFieldDef, val: &Value) -> Result<(), AppError> {
    if val.is_null() {
        return Ok(());
    }
    let label = format!("{}.{}", column, def.key);
    let canonical = def.canonical();
    let category = crate::db::type_category(&canonical);
    use crate::db::TypeCategory;

    match category {
        TypeCategory::Int | TypeCategory::Float => {
            let n = val.as_f64().ok_or_else(|| {
                AppError::Validation(format!("extensible field '{}' must be a number", label))
            })?;
            if category == TypeCategory::Int && val.as_i64().is_none() && n.fract() != 0.0 {
                return Err(AppError::Validation(format!(
                    "extensible field '{}' must be an integer",
                    label
                )));
            }
            if let Some(min) = def.min {
                if n < min {
                    return Err(AppError::Validation(format!(
                        "extensible field '{}' must be >= {}",
                        label, min
                    )));
                }
            }
            if let Some(max) = def.max {
                if n > max {
                    return Err(AppError::Validation(format!(
                        "extensible field '{}' must be <= {}",
                        label, max
                    )));
                }
            }
        }
        TypeCategory::Bool => {
            if !val.is_boolean() {
                return Err(AppError::Validation(format!(
                    "extensible field '{}' must be a boolean",
                    label
                )));
            }
        }
        TypeCategory::Text
        | TypeCategory::Uuid
        | TypeCategory::Date
        | TypeCategory::Timestamp
        | TypeCategory::Time => {
            let s = val.as_str().ok_or_else(|| {
                AppError::Validation(format!("extensible field '{}' must be a string", label))
            })?;
            if let Some(maxl) = def.max_length {
                if s.chars().count() > maxl as usize {
                    return Err(AppError::Validation(format!(
                        "extensible field '{}' exceeds max length {}",
                        label, maxl
                    )));
                }
            }
            if let Some(minl) = def.min_length {
                if s.chars().count() < minl as usize {
                    return Err(AppError::Validation(format!(
                        "extensible field '{}' is shorter than min length {}",
                        label, minl
                    )));
                }
            }
            if let Some(pat) = &def.pattern {
                let re = regex::Regex::new(pat).map_err(|e| {
                    AppError::Validation(format!(
                        "extensible field '{}' has an invalid pattern: {}",
                        label, e
                    ))
                })?;
                if !re.is_match(s) {
                    return Err(AppError::Validation(format!(
                        "extensible field '{}' does not match required pattern",
                        label
                    )));
                }
            }
        }
        // Json / Bytes / Other: accept any JSON shape.
        _ => {}
    }

    if let Some(allowed) = &def.allowed {
        if !allowed.iter().any(|a| a == val) {
            return Err(AppError::Validation(format!(
                "extensible field '{}' has a value that is not allowed",
                label
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::resolved::{PkType, ResolvedEntity};
    use serde_json::json;
    use std::collections::HashSet;

    fn entity_with_bag(column: &str) -> ResolvedEntity {
        ResolvedEntity {
            table_id: "t".into(),
            schema_name: "s".into(),
            table_name: "products".into(),
            path_segment: "products".into(),
            pk_columns: vec!["id".into()],
            pk_type: PkType::Uuid,
            columns: vec![],
            operations: vec![],
            sensitive_columns: HashSet::new(),
            includes: vec![],
            validation: HashMap::new(),
            events: vec![],
            archive_field: None,
            package_id: "_default".into(),
            audit_log: false,
            parent_ref_column: None,
            versioning: None,
            mcp: None,
            extensible_columns: vec![column.into()],
        }
    }

    fn registry() -> ExtensibleRegistry {
        ExtensibleRegistry::from_value(json!({
            "attributes": [
                {"key": "warrantyMonths", "type": "int", "min": 0, "required": true},
                {"key": "energyRating", "type": "text", "maxLength": 3},
                {"key": "notes", "type": "text", "sortable": false, "filterable": false}
            ]
        }))
        .unwrap()
    }

    fn body(attrs: Value) -> HashMap<String, Value> {
        let mut m = HashMap::new();
        m.insert("attributes".to_string(), attrs);
        m
    }

    #[test]
    fn accepts_valid_extensible_fields() {
        let e = entity_with_bag("attributes");
        let reg = registry();
        let b = body(json!({"warrantyMonths": 24, "energyRating": "A++"}));
        assert!(validate_extensible_fields(&b, &e, &reg, ValidateMode::Full).is_ok());
    }

    #[test]
    fn rejects_unknown_key() {
        let e = entity_with_bag("attributes");
        let reg = registry();
        let b = body(json!({"warrantyMonths": 24, "bogus": 1}));
        let err = validate_extensible_fields(&b, &e, &reg, ValidateMode::Partial).unwrap_err();
        assert!(format!("{:?}", err).contains("unknown extensible field"));
    }

    #[test]
    fn rejects_type_mismatch_and_bounds() {
        let e = entity_with_bag("attributes");
        let reg = registry();
        // non-numeric for an int field
        let b = body(json!({"warrantyMonths": "x"}));
        assert!(validate_extensible_fields(&b, &e, &reg, ValidateMode::Partial).is_err());
        // below min
        let b = body(json!({"warrantyMonths": -1}));
        assert!(validate_extensible_fields(&b, &e, &reg, ValidateMode::Partial).is_err());
        // over max length
        let b = body(json!({"energyRating": "TOOLONG"}));
        assert!(validate_extensible_fields(&b, &e, &reg, ValidateMode::Partial).is_err());
    }

    #[test]
    fn enforces_required_on_create_only() {
        let e = entity_with_bag("attributes");
        let reg = registry();
        let b = body(json!({"energyRating": "A"}));
        // Full (create) requires warrantyMonths
        assert!(validate_extensible_fields(&b, &e, &reg, ValidateMode::Full).is_err());
        // Partial (update) does not
        assert!(validate_extensible_fields(&b, &e, &reg, ValidateMode::Partial).is_ok());
    }

    #[test]
    fn rejects_extensible_fields_without_registry() {
        let e = entity_with_bag("attributes");
        let empty = ExtensibleRegistry::default();
        let b = body(json!({"warrantyMonths": 24}));
        assert!(validate_extensible_fields(&b, &e, &empty, ValidateMode::Partial).is_err());
    }

    #[test]
    fn ignores_absent_bag_when_no_required() {
        let mut e = entity_with_bag("attributes");
        e.extensible_columns = vec!["other".into()]; // no registry, no body → fine
        let reg = registry();
        let b: HashMap<String, Value> = HashMap::new();
        assert!(validate_extensible_fields(&b, &e, &reg, ValidateMode::Partial).is_ok());
    }

    // ── validate_registry_document (admin write path) ──────────────────────────

    #[test]
    fn registry_document_accepts_known_columns() {
        let cols = vec!["attributes".to_string(), "specs".to_string()];
        let doc = json!({
            "attributes": [{"key": "warrantyMonths", "type": "int"}],
            "specs": [{"key": "voltage", "type": "decimal"}]
        });
        assert!(validate_registry_document(&doc, &cols, "products").is_ok());
    }

    #[test]
    fn registry_document_rejects_unknown_column() {
        let cols = vec!["attributes".to_string()];
        let doc = json!({ "not_a_bag": [{"key": "x", "type": "int"}] });
        let err = validate_registry_document(&doc, &cols, "products").unwrap_err();
        assert!(format!("{:?}", err).contains("not an extensible column"));
    }

    #[test]
    fn registry_document_rejects_non_object() {
        let cols = vec!["attributes".to_string()];
        assert!(validate_registry_document(&json!([1, 2, 3]), &cols, "products").is_err());
    }

    #[test]
    fn registry_document_rejects_malformed_def() {
        let cols = vec!["attributes".to_string()];
        // missing required `type` on the def
        let doc = json!({ "attributes": [{"key": "warrantyMonths"}] });
        assert!(validate_registry_document(&doc, &cols, "products").is_err());
    }

    // ── index_ddl ──────────────────────────────────────────────────────────────

    fn index_registry() -> ExtensibleRegistry {
        ExtensibleRegistry::from_value(json!({
            "attributes": [
                {"key": "warrantyMonths", "type": "int", "filterable": true, "sortable": true},
                {"key": "internalNote",   "type": "text", "filterable": false, "sortable": false}
            ]
        }))
        .unwrap()
    }

    #[test]
    fn index_ddl_covers_only_queryable_fields() {
        let dialect = crate::db::active_dialect();
        let stmts = index_ddl(
            "main",
            "products",
            &index_registry(),
            dialect.as_ref(),
            None,
        );
        // warrantyMonths is filterable+sortable → 1 index; internalNote is neither → skipped.
        assert_eq!(stmts.len(), 1, "got: {:?}", stmts);
        assert!(stmts[0].contains("CREATE INDEX"));
        assert!(stmts[0].contains("warrantyMonths"), "got: {}", stmts[0]);
        assert!(
            !stmts[0].contains("internalNote"),
            "non-queryable field must not be indexed"
        );
    }

    #[test]
    fn index_ddl_adds_partial_predicate_for_rls() {
        let dialect = crate::db::active_dialect();
        let stmts = index_ddl(
            "main",
            "products",
            &index_registry(),
            dialect.as_ref(),
            Some(("tenant_id", "acme")),
        );
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("WHERE"), "got: {}", stmts[0]);
        assert!(stmts[0].contains("acme"), "got: {}", stmts[0]);
    }

    #[test]
    fn index_ddl_escapes_tenant_in_predicate() {
        let dialect = crate::db::active_dialect();
        let stmts = index_ddl(
            "main",
            "products",
            &index_registry(),
            dialect.as_ref(),
            Some(("tenant_id", "a'b")),
        );
        assert!(stmts[0].contains("'a''b'"), "got: {}", stmts[0]);
    }

    // ── cache mechanics ────────────────────────────────────────────────────────

    #[test]
    fn registry_cache_insert_get_evict_and_ttl() {
        let cache: RegistryCache = Default::default();
        let key = (
            "acme".to_string(),
            "_default".to_string(),
            "products".to_string(),
        );
        let entry = CachedRegistry {
            registry: index_registry(),
            loaded_at: std::time::Instant::now(),
        };
        cache.write().unwrap().insert(key.clone(), entry);

        // Hit: present and fresh (well within TTL).
        {
            let c = cache.read().unwrap();
            let got = c.get(&key).expect("entry present");
            assert!(got.loaded_at.elapsed() < REGISTRY_CACHE_TTL);
            assert!(got.registry.field("attributes", "warrantyMonths").is_some());
        }

        // Evict: a removed entry is a miss (forces reload).
        cache.write().unwrap().remove(&key);
        assert!(cache.read().unwrap().get(&key).is_none());
    }
}
