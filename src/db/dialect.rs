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
}
