//! PostgreSQL dialect: maps canonical types to DDL strings, SQL cast names, and type categories.
//!
//! All functions are free functions (no vtable, no allocation overhead) that the compiler
//! inlines at call sites — zero runtime cost compared with writing Postgres strings directly.

use super::types::{CanonicalType, TypeCategory, TypeSupport};

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
        CanonicalType::Array(inner) => {
            return cast_name(inner).map(|c| format!("{}[]", c));
        }
        // Schema-qualified custom types need a ::text cast so string params bind cleanly.
        CanonicalType::Custom(s) if s.contains('.') => {
            return Some(format!("{}[]", s).replace("[][]", "[]")); // schema.type — text binding
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

/// Classify a canonical type into a broad [`TypeCategory`] used by the RSQL query builder
/// to validate which filter operators are legal for each column.
pub fn type_category(t: &CanonicalType) -> TypeCategory {
    match t {
        CanonicalType::Text
        | CanonicalType::Varchar(_)
        | CanonicalType::Char(_)
        | CanonicalType::Asset => TypeCategory::Text,

        CanonicalType::SmallInt
        | CanonicalType::Int
        | CanonicalType::BigInt
        | CanonicalType::Serial
        | CanonicalType::BigSerial => TypeCategory::Int,

        CanonicalType::Real | CanonicalType::Double | CanonicalType::Decimal(_) => {
            TypeCategory::Float
        }

        CanonicalType::Boolean => TypeCategory::Bool,
        CanonicalType::Uuid => TypeCategory::Uuid,
        CanonicalType::Date => TypeCategory::Date,
        CanonicalType::Timestamp | CanonicalType::TimestampNtz => TypeCategory::Timestamp,
        CanonicalType::Time | CanonicalType::Timetz => TypeCategory::Time,
        CanonicalType::Json | CanonicalType::Jsonb | CanonicalType::AssetArray => {
            TypeCategory::Json
        }
        CanonicalType::Bytes => TypeCategory::Bytes,
        CanonicalType::Array(_) | CanonicalType::Custom(_) => TypeCategory::Other,
    }
}

/// Classify a Postgres cast-name string (as stored in `ColumnInfo.pg_type`) into a
/// [`TypeCategory`]. Used as a fallback when only the cast name is available (e.g. for
/// synthetic audit columns whose `CanonicalType` is not stored).
pub fn type_category_from_cast(cast: &str) -> TypeCategory {
    let base = cast
        .trim_end_matches("[]")
        .split('(')
        .next()
        .unwrap_or(cast)
        .trim()
        .to_lowercase();
    match base.as_str() {
        "text" | "varchar" | "char" | "bpchar" | "citext" | "name" | "character varying"
        | "character" => TypeCategory::Text,
        "int2" | "int4" | "int8" | "integer" | "bigint" | "smallint" | "serial" | "bigserial"
        | "smallserial" => TypeCategory::Int,
        "float4" | "float8" | "numeric" | "decimal" | "real" | "money" | "double precision" => {
            TypeCategory::Float
        }
        "bool" | "boolean" => TypeCategory::Bool,
        "uuid" => TypeCategory::Uuid,
        "date" => TypeCategory::Date,
        "timestamp" | "timestamptz" | "timestamp with time zone"
        | "timestamp without time zone" => TypeCategory::Timestamp,
        "time" | "timetz" | "time with time zone" | "time without time zone" => TypeCategory::Time,
        "json" | "jsonb" => TypeCategory::Json,
        "bytea" => TypeCategory::Bytes,
        _ => TypeCategory::Other,
    }
}
