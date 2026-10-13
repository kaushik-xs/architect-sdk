//! The `Dialect` trait — every database backend implements this.
//!
//! All methods are on `&self` so the same dialect object can be stored in `Arc<dyn Dialect>`.
//! The trait is object-safe: no generics, no associated types.

use super::types::{CanonicalType, TypeCategory, TypeSupport};

/// A database dialect encapsulates all SQL syntax and type-mapping differences between
/// database engines. The SDK calls dialect methods instead of hardcoding Postgres strings,
/// so adding a new database is a matter of implementing this trait.
pub trait Dialect: Send + Sync + 'static {
    /// Short name for log messages and errors (e.g. "postgres", "mysql", "sqlite").
    fn name(&self) -> &'static str;

    // ── Type system ───────────────────────────────────────────────────────────

    /// DDL type string for CREATE TABLE (e.g. "TIMESTAMPTZ", "DATETIME", "TEXT").
    fn ddl_type(&self, t: &CanonicalType) -> String;

    /// Type name used in parameter cast expressions, or `None` when no cast is needed.
    /// Postgres: becomes `$n::cast`.  MySQL/SQLite: cast is omitted (binding handles type).
    fn cast_name(&self, t: &CanonicalType) -> Option<String>;

    /// Broad category for RSQL operator validation.
    fn type_category(&self, t: &CanonicalType) -> TypeCategory;

    /// How well this dialect supports the canonical type.
    fn type_support(&self, t: &CanonicalType) -> TypeSupport;

    // ── Identifier quoting ────────────────────────────────────────────────────

    /// Wrap an identifier in dialect-specific delimiters.
    /// Postgres/SQLite: double-quotes.  MySQL: backticks.
    fn quote_ident(&self, s: &str) -> String;

    // ── Parameter placeholders ────────────────────────────────────────────────

    /// Positional placeholder for the n-th parameter (1-based).
    /// Postgres: `$1`.  MySQL/SQLite: `?`.
    fn placeholder(&self, n: usize) -> String;

    /// Wrap a placeholder with a type cast where required.
    /// Postgres: `$1::uuid`.  MySQL/SQLite: placeholder returned unchanged.
    fn cast_expr(&self, placeholder: &str, cast: &str) -> String;

    // ── SQL functions ─────────────────────────────────────────────────────────

    /// Current-timestamp function name/expression.
    fn now_fn(&self) -> &'static str;

    /// Expression that generates a random UUID as a column DEFAULT.
    fn uuid_default_expr(&self) -> &'static str;

    // ── DML clauses ───────────────────────────────────────────────────────────

    /// RETURNING clause appended to INSERT/UPDATE/DELETE, or empty string when unsupported.
    fn returning_clause(&self, cols: &str) -> String;

    /// Upsert conflict suffix.
    /// `conflict_cols`: columns that identify the conflict.
    /// `set_pairs`: pre-built "col = value" pairs for the update branch.
    fn upsert_conflict(&self, conflict_cols: &[&str], set_pairs: &str) -> String;

    // ── JSON aggregation (related-entity includes) ────────────────────────────

    /// Build a scalar subquery returning a single JSON object for a to-one include.
    /// `col_exprs`: already-quoted column expressions.
    /// `from_clause`: `"schema"."table" WHERE ...` fragment.
    fn to_one_subquery(&self, col_exprs: &[String], from_clause: &str) -> String;

    /// Build a scalar subquery returning a JSON array for a to-many include.
    fn to_many_subquery(&self, col_exprs: &[String], from_clause: &str) -> String;

    // ── JSON path extraction (extensible fields) ───────────────────────────────────

    /// Extract a top-level key from a JSON/JSONB column as text.
    /// `col` is an already-quoted column expression; `key` is the raw JSON key (escaped here).
    /// Postgres: `(col ->> 'key')`.  MySQL/SQLite: `col->>'$.key'`.
    fn json_extract_text(&self, col: &str, key: &str) -> String;

    /// Extract a JSON key and cast it to `t` so comparisons and ORDER BY are type-correct.
    /// For text-like types this is equivalent to [`Dialect::json_extract_text`].
    fn json_extract_typed(&self, col: &str, key: &str, t: &CanonicalType) -> String;

    /// Case-insensitive LIKE comparison fragment: `col <ci-like> placeholder`.
    /// Postgres: `col ILIKE ph`.  MySQL: `LOWER(col) LIKE LOWER(ph)`.  SQLite: `col LIKE ph`
    /// (SQLite LIKE is case-insensitive for ASCII by default).
    fn case_insensitive_like(&self, col: &str, placeholder: &str) -> String;

    // ── System-table DDL helpers ──────────────────────────────────────────────

    /// DDL fragment for a JSON/JSONB payload column (e.g. "JSONB", "JSON", "TEXT").
    fn sys_json_type(&self) -> &'static str;

    /// Timestamp type name (without NOT NULL / DEFAULT).
    fn sys_timestamp_type(&self) -> &'static str;

    /// NOT NULL timestamp column with a now() default — convenience built from above.
    fn sys_timestamp_default(&self) -> String {
        format!(
            "{} NOT NULL DEFAULT {}",
            self.sys_timestamp_type(),
            self.now_fn()
        )
    }

    /// Auto-incrementing large integer for surrogate PKs.
    /// e.g. "BIGSERIAL", "BIGINT AUTO_INCREMENT", "INTEGER".
    fn sys_bigserial_type(&self) -> &'static str;

    /// DDL type for a raw binary payload column (e.g. "BYTEA", "BLOB").
    fn sys_bytes_type(&self) -> &'static str;

    /// Timestamp type used in audit table columns (no DEFAULT — values supplied explicitly).
    fn audit_timestamp_type(&self) -> &'static str;

    // ── Multi-tenancy ─────────────────────────────────────────────────────────

    /// Whether this dialect supports `CREATE SCHEMA` DDL.
    /// Postgres: true. MySQL: false (uses databases). SQLite: false (no user-defined schemas).
    fn supports_schemas(&self) -> bool {
        true
    }

    /// DDL fragment for a column that holds a timestamp defaulting to N hours from now.
    /// Returns `None` when the dialect has no constant-expression equivalent (SQLite).
    /// Callers should make the column nullable and omit the DEFAULT when `None` is returned.
    fn default_now_plus_hours(&self, hours: u32) -> Option<String> {
        Some(format!("NOW() + INTERVAL '{} hours'", hours))
    }

    /// Whether this dialect natively supports row-level security (CREATE POLICY etc.).
    fn supports_rls(&self) -> bool;

    /// Whether this dialect supports named enum types (CREATE TYPE … AS ENUM).
    fn supports_named_enum_types(&self) -> bool;

    /// Whether this dialect supports INCLUDE columns on indexes (Postgres 11+).
    fn supports_index_include(&self) -> bool;

    /// SQL statement that sets a session-local tenant identifier before a query.
    /// Returns `None` when the dialect has no such mechanism.
    fn set_tenant_session_sql(&self, tenant_id: &str) -> Option<String>;

    // ── Read-only query sandbox (saved queries) ─────────────────────────────────────
    // Statements issued inside a transaction to sandbox an ad-hoc read-only query. Each returns
    // `None` when the dialect has no equivalent, in which case the caller simply omits it (the
    // guarantee then rests on the other layers, e.g. a read-only DB role).

    /// Make the current transaction read-only (blocks writes and writable CTEs).
    fn set_read_only_sql(&self) -> Option<String> {
        None
    }

    /// Bound the running time of statements in the current transaction, in milliseconds.
    fn set_statement_timeout_sql(&self, _ms: u64) -> Option<String> {
        None
    }

    /// Drop to a specific (read-only) DB role for the current transaction. `role` is a bare
    /// identifier supplied by an operator via env var, quoted by the dialect.
    fn set_role_sql(&self, _role: &str) -> Option<String> {
        None
    }

    // ── Idempotent DDL ────────────────────────────────────────────────────────

    /// Whether `ALTER TABLE … ADD COLUMN IF NOT EXISTS` is valid syntax.
    /// Postgres: true. MySQL/SQLite: false (they need a pre-flight existence check).
    fn supports_add_column_if_not_exists(&self) -> bool {
        false
    }

    /// Whether a database error code (SQLSTATE for Postgres, error number for MySQL) means
    /// "the object I tried to create already exists" — a migration step that is already applied.
    fn is_duplicate_object_code(&self, _code: &str) -> bool {
        false
    }

    // ── Generated columns ─────────────────────────────────────────────────────

    /// Column-definition clause for a database-computed column, placed right after the type.
    /// `adding_column` is true inside `ALTER TABLE … ADD COLUMN` (SQLite cannot add a STORED
    /// generated column there and falls back to VIRTUAL).
    fn generated_column_clause(&self, expression: &str, _adding_column: bool) -> String {
        format!("GENERATED ALWAYS AS ({}) STORED", expression)
    }

    /// Statement that changes an existing generated column's expression in place (keeping its
    /// indexes), or `None` when the dialect cannot. `column_def` is the full new column
    /// definition (`"name" TYPE GENERATED ALWAYS AS (…) STORED [NOT NULL]`).
    fn alter_generated_expression_sql(
        &self,
        _full_table: &str,
        _column: &str,
        _expression: &str,
        _column_def: &str,
    ) -> Option<String> {
        None
    }

    /// Statement that turns a generated column into a regular one, keeping its current values,
    /// or `None` when the dialect cannot. `column_def` is the new plain column definition.
    fn drop_generated_expression_sql(
        &self,
        _full_table: &str,
        _column: &str,
        _column_def: &str,
    ) -> Option<String> {
        None
    }

    // ── Introspection ─────────────────────────────────────────────────────────
    //
    // These queries feed `db::introspect`, which lets the migration executor skip steps whose
    // effect is already present in the physical database. Every selected value must come back
    // as text so one decoding path works for all dialects.

    /// One row per column in `schema`:
    /// `(table_name, column_name, data_type, is_nullable 'YES'/'NO', has_default 'YES'/'NO')`.
    fn introspect_columns_sql(&self, schema: &str) -> String {
        format!(
            "SELECT table_name, column_name, data_type, is_nullable, \
             CASE WHEN column_default IS NULL THEN 'NO' ELSE 'YES' END \
             FROM information_schema.columns WHERE table_schema = '{}'",
            escape_literal(schema)
        )
    }

    /// One row per index in `schema`: `(index_name)`. `None` when the dialect cannot report them.
    fn introspect_indexes_sql(&self, _schema: &str) -> Option<String> {
        None
    }

    /// One row per table constraint in `schema`: `(table_name, constraint_name)`.
    /// `None` when the dialect cannot report them.
    fn introspect_constraints_sql(&self, schema: &str) -> Option<String> {
        Some(format!(
            "SELECT table_name, constraint_name FROM information_schema.table_constraints \
             WHERE constraint_schema = '{}'",
            escape_literal(schema)
        ))
    }

    /// Views (plain or materialized) that read any of `tables`, given as `(schema, table)` pairs,
    /// directly or through other views. One row per view and table column it ultimately reads:
    /// `(view_schema, view_name, table_schema, table_name, column_name)`, the table being one of
    /// `tables`; `column_name` is `''` for a dependency on the table as a whole (e.g.
    /// `count(*)`). A view reading another view inherits every column that view reads.
    /// Feeds the package-uninstall guard (on Postgres `DROP … CASCADE` would silently drop those
    /// views) and the upgrade guard (a view blocks retyping or dropping a column it reads).
    /// `None` when the dialect cannot report view dependencies; `tables` is never empty.
    fn dependent_views_sql(&self, _tables: &[(String, String)]) -> Option<String> {
        None
    }

    // ── Views ─────────────────────────────────────────────────────────────────

    /// Create (or replace) a view. `qualified` is the already-quoted `schema.name`; `body` is the
    /// view's SELECT. Replacing must keep working when the body changes but the output columns do
    /// not; migrations drop and recreate a view whose columns change.
    fn create_view_sql(&self, qualified: &str, body: &str) -> String {
        format!("CREATE OR REPLACE VIEW {} AS {}", qualified, body)
    }

    /// Whether `create_view_sql` replaces an existing view. When false, callers drop the view
    /// first so a changed body takes effect.
    fn create_view_replaces(&self) -> bool {
        true
    }

    /// Drop a view if it exists. Never cascades: a view other objects still depend on must fail
    /// to drop rather than take them with it.
    fn drop_view_sql(&self, qualified: &str) -> String {
        format!("DROP VIEW IF EXISTS {}", qualified)
    }

    // ── Materialized views ────────────────────────────────────────────────────
    //
    // A materialized view is stored in a backing relation (`qualified` below) that holds every
    // tenant's rows; the SDK reads it through a plain wrapper view that filters by tenant.
    // Dialects without native materialized views (the default) back it with a table.

    /// Create the backing relation, populated, if it does not exist.
    fn create_materialized_view_sql(&self, qualified: &str, body: &str) -> String {
        format!("CREATE TABLE IF NOT EXISTS {} AS {}", qualified, body)
    }

    /// Unique index on the backing relation, when the dialect needs one to refresh without
    /// blocking readers. `index` is the quoted index name; `columns` the quoted column list.
    fn materialized_view_key_sql(
        &self,
        _index: &str,
        _qualified: &str,
        _columns: &str,
    ) -> Option<String> {
        None
    }

    fn drop_materialized_view_sql(&self, qualified: &str) -> String {
        format!("DROP TABLE IF EXISTS {}", qualified)
    }

    /// Statements that recompute the backing relation from `body`. Several statements run in one
    /// transaction, so readers see the old rows until the new ones are committed.
    fn refresh_materialized_view_sql(&self, qualified: &str, body: &str) -> Vec<String> {
        vec![
            format!("DELETE FROM {}", qualified),
            format!(
                "INSERT INTO {} SELECT * FROM ({}) AS architect_mv_refresh",
                qualified, body
            ),
        ]
    }

    /// Row filter the wrapper view applies so each caller sees only its tenant's rows, keyed off
    /// the session tenant (`app.tenant_id`). `None` when the dialect has no session tenant (no
    /// shared-database tenants), in which case the wrapper does not filter.
    fn tenant_session_filter_sql(&self, _column: &str) -> Option<String> {
        None
    }

    /// A typed NULL used for the tenant column in databases that have none.
    fn null_text_expr(&self) -> &'static str {
        "CAST(NULL AS TEXT)"
    }
}

/// `('s1', 't1'), ('s2', 't2')` — a row-value list for `(schema, table) IN (…)` filters.
pub fn table_pairs_literal(tables: &[(String, String)]) -> String {
    tables
        .iter()
        .map(|(s, t)| format!("('{}', '{}')", escape_literal(s), escape_literal(t)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Escape a value for inlining into a single-quoted SQL literal.
/// Introspection queries take schema names from config, never from request input.
pub fn escape_literal(s: &str) -> String {
    s.replace('\'', "''")
}
