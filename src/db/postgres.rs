//! PostgreSQL dialect: maps canonical types to DDL strings, SQL cast names, and type categories.
//!
//! Free functions are kept for zero-overhead internal use. `PostgresDialect` implements the
//! `Dialect` trait for use via `Arc<dyn Dialect>` in AppState.

use super::dialect::Dialect;
use super::types::{CanonicalType, TypeCategory, TypeSupport};

/// Zero-sized marker for the PostgreSQL dialect.
pub struct PostgresDialect;

// ─── DDL type ────────────────────────────────────────────────────────────────

/// Return the PostgreSQL DDL type string for use in `CREATE TABLE`.
///
/// Called once per column during migration — not on the hot path.
pub fn ddl_type(t: &CanonicalType) -> String {
    match t {
        CanonicalType::Text => "TEXT".to_string(),
        CanonicalType::Varchar(Some(n)) => format!("VARCHAR({})", n),
        CanonicalType::Varchar(None) => "TEXT".to_string(),
        CanonicalType::Char(Some(n)) => format!("CHAR({})", n),
        CanonicalType::Char(None) => "CHAR(1)".to_string(),
        CanonicalType::SmallInt => "SMALLINT".to_string(),
        CanonicalType::Int => "INTEGER".to_string(),
        CanonicalType::BigInt => "BIGINT".to_string(),
        CanonicalType::Real => "REAL".to_string(),
        CanonicalType::Double => "DOUBLE PRECISION".to_string(),
        CanonicalType::Decimal(Some((p, s))) => format!("NUMERIC({}, {})", p, s),
        CanonicalType::Decimal(None) => "NUMERIC".to_string(),
        CanonicalType::Boolean => "BOOLEAN".to_string(),
        CanonicalType::Uuid => "UUID".to_string(),
        // Prefer JSONB over JSON in Postgres: binary storage, indexable.
        CanonicalType::Json => "JSONB".to_string(),
        CanonicalType::Jsonb => "JSONB".to_string(),
        CanonicalType::Timestamp => "TIMESTAMPTZ".to_string(),
        CanonicalType::TimestampNtz => "TIMESTAMP".to_string(),
        CanonicalType::Date => "DATE".to_string(),
        CanonicalType::Time => "TIME".to_string(),
        CanonicalType::Timetz => "TIMETZ".to_string(),
        CanonicalType::Bytes => "BYTEA".to_string(),
        CanonicalType::Serial => "SERIAL".to_string(),
        CanonicalType::BigSerial => "BIGSERIAL".to_string(),
        CanonicalType::Asset => "TEXT".to_string(),
        CanonicalType::AssetArray => "JSONB".to_string(),
        CanonicalType::Array(inner) => format!("{}[]", ddl_type(inner)),
        CanonicalType::Custom(s) => s.clone(),
    }
}

// ─── Cast name ───────────────────────────────────────────────────────────────

/// Return the Postgres type name used in `$n::type` parameter casts, or `None` when no cast
/// is needed (e.g. integers bind directly without a cast).
///
/// This drives `ColumnInfo.pg_type` in the resolved model and is used throughout the query
/// builder whenever a value needs an explicit type annotation in SQL.
pub fn cast_name(t: &CanonicalType) -> Option<String> {
    let s = match t {
        CanonicalType::Uuid => "uuid",
        CanonicalType::Json | CanonicalType::Jsonb | CanonicalType::AssetArray => "jsonb",
        CanonicalType::Timestamp => "timestamptz",
        CanonicalType::TimestampNtz => "timestamp",
        CanonicalType::Date => "date",
        CanonicalType::Time => "time",
        CanonicalType::Timetz => "timetz",
        CanonicalType::Decimal(_) => "numeric",
        CanonicalType::Boolean => "boolean",
        CanonicalType::Bytes => "bytea",
        // Arrays: cast to the element type followed by [].
        // Fall back to the DDL type name for inner types that need no scalar cast (e.g. text, int).
        CanonicalType::Array(inner) => {
            let inner_cast = cast_name(inner).unwrap_or_else(|| ddl_type(inner).to_lowercase());
            return Some(format!("{}[]", inner_cast));
        }
        // Schema-qualified custom types (enums like schema.my_enum) cast directly to the type name.
        CanonicalType::Custom(s) if s.contains('.') => {
            return Some(s.clone());
        }
        CanonicalType::Custom(_) => return None,
        // Text, numeric, serial types bind fine without an explicit cast.
        _ => return None,
    };
    Some(s.to_string())
}

// ─── Type support ─────────────────────────────────────────────────────────────

/// Describe how well Postgres supports a canonical type.
/// Callers log a warning for [`TypeSupport::Degraded`] at startup.
pub fn type_support(t: &CanonicalType) -> TypeSupport {
    match t {
        CanonicalType::Text
        | CanonicalType::Varchar(_)
        | CanonicalType::Char(_)
        | CanonicalType::SmallInt
        | CanonicalType::Int
        | CanonicalType::BigInt
        | CanonicalType::Real
        | CanonicalType::Double
        | CanonicalType::Decimal(_)
        | CanonicalType::Boolean
        | CanonicalType::Uuid
        | CanonicalType::Jsonb
        | CanonicalType::Timestamp
        | CanonicalType::TimestampNtz
        | CanonicalType::Date
        | CanonicalType::Time
        | CanonicalType::Timetz
        | CanonicalType::Bytes
        | CanonicalType::Serial
        | CanonicalType::BigSerial
        | CanonicalType::Array(_)
        | CanonicalType::Custom(_) => TypeSupport::Native(ddl_type(t).leak()),
        // Json is stored as JSONB — same semantics, richer storage.
        CanonicalType::Json => TypeSupport::Native("JSONB"),
        // SDK pseudo-types are emulated via TEXT / JSONB.
        CanonicalType::Asset => TypeSupport::Emulated("TEXT"),
        CanonicalType::AssetArray => TypeSupport::Emulated("JSONB"),
    }
}

// ─── Type category ────────────────────────────────────────────────────────────

/// Delegates to the shared impl in [`super::types`].
pub fn type_category(t: &CanonicalType) -> TypeCategory {
    super::types::type_category(t)
}

/// Delegates to the shared impl in [`super::types`].
pub fn type_category_from_cast(cast: &str) -> TypeCategory {
    super::types::type_category_from_cast(cast)
}

// ─── Dialect impl ─────────────────────────────────────────────────────────────

impl Dialect for PostgresDialect {
    fn name(&self) -> &'static str {
        "postgres"
    }

    fn ddl_type(&self, t: &CanonicalType) -> String {
        ddl_type(t)
    }

    fn cast_name(&self, t: &CanonicalType) -> Option<String> {
        cast_name(t)
    }

    fn type_category(&self, t: &CanonicalType) -> TypeCategory {
        type_category(t)
    }

    fn type_support(&self, t: &CanonicalType) -> TypeSupport {
        type_support(t)
    }

    fn quote_ident(&self, s: &str) -> String {
        format!("\"{}\"", s.replace('"', "\"\""))
    }

    fn placeholder(&self, n: usize) -> String {
        format!("${}", n)
    }

    fn cast_expr(&self, placeholder: &str, cast: &str) -> String {
        format!("{}::{}", placeholder, cast)
    }

    fn now_fn(&self) -> &'static str {
        "NOW()"
    }

    fn uuid_default_expr(&self) -> &'static str {
        "gen_random_uuid()"
    }

    fn returning_clause(&self, cols: &str) -> String {
        format!("RETURNING {}", cols)
    }

    fn upsert_conflict(&self, conflict_cols: &[&str], set_pairs: &str) -> String {
        let cols = conflict_cols
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        format!("ON CONFLICT ({}) DO UPDATE SET {}", cols, set_pairs)
    }

    fn to_one_subquery(&self, col_exprs: &[String], from_clause: &str) -> String {
        let cols = col_exprs.join(", ");
        format!(
            "(SELECT row_to_json(sub) FROM (SELECT {} FROM {}) sub)",
            cols, from_clause
        )
    }

    fn to_many_subquery(&self, col_exprs: &[String], from_clause: &str) -> String {
        let cols = col_exprs.join(", ");
        format!(
            "(SELECT COALESCE(json_agg(row_to_json(sub)), '[]'::json) FROM (SELECT {} FROM {}) sub)",
            cols, from_clause
        )
    }

    fn sys_json_type(&self) -> &'static str {
        "JSONB"
    }

    fn sys_timestamp_type(&self) -> &'static str {
        "TIMESTAMPTZ"
    }

    fn sys_bigserial_type(&self) -> &'static str {
        "BIGSERIAL"
    }

    fn audit_timestamp_type(&self) -> &'static str {
        "TIMESTAMPTZ"
    }

    fn supports_rls(&self) -> bool {
        true
    }

    fn supports_named_enum_types(&self) -> bool {
        true
    }

    fn supports_index_include(&self) -> bool {
        true
    }

    fn set_tenant_session_sql(&self, tenant_id: &str) -> Option<String> {
        Some(format!(
            "SET LOCAL app.tenant_id = '{}'",
            tenant_id.replace('\'', "''")
        ))
    }
}
