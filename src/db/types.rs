//! Database-agnostic type system for architect-sdk.
//!
//! Package configs express columns using `CanonicalType` values (resolved from the JSON
//! `"type"` field). Each database dialect then maps these to its own DDL types, cast names,
//! and operator support. This keeps package JSON portable across database backends.

use crate::config::types::ColumnTypeConfig;

/// Standard logical types that package configs may declare.
///
/// All string aliases accepted in JSON are normalised to one of these variants by
/// [`parse_canonical`]. Unknown strings fall through to [`CanonicalType::Custom`] so that
/// existing packages using raw SQL type names continue to work unchanged.
#[derive(Clone, Debug, PartialEq)]
pub enum CanonicalType {
    /// Unbounded unicode text (TEXT / VARCHAR without limit).
    Text,
    /// Variable-length text with optional length cap.
    Varchar(Option<u32>),
    /// Fixed-length text.
    Char(Option<u32>),
    /// 16-bit integer.
    SmallInt,
    /// 32-bit integer.
    Int,
    /// 64-bit integer.
    BigInt,
    /// 32-bit floating point.
    Real,
    /// 64-bit floating point (default for `"float"`).
    Double,
    /// Fixed-precision decimal. Params: `(precision, scale)`.
    Decimal(Option<(u8, u8)>),
    /// Boolean true/false.
    Boolean,
    /// UUID (128-bit universally unique identifier).
    Uuid,
    /// JSON document. Dialects may use a richer binary form (e.g. JSONB in Postgres).
    Json,
    /// Explicitly request the binary JSON form where available; degrades to JSON/TEXT elsewhere.
    Jsonb,
    /// Timestamp with time zone. Always stores timezone information.
    Timestamp,
    /// Timestamp without time zone (use sparingly — prefer [`CanonicalType::Timestamp`]).
    TimestampNtz,
    /// Calendar date (no time component).
    Date,
    /// Time of day without time zone.
    Time,
    /// Time of day with time zone.
    Timetz,
    /// Binary data.
    Bytes,
    /// Auto-incrementing 32-bit integer primary key.
    Serial,
    /// Auto-incrementing 64-bit integer primary key.
    BigSerial,
    /// SDK pseudo-type: a single asset stored as a relative path string.
    Asset,
    /// SDK pseudo-type: a list of assets stored as a JSON array of path strings.
    AssetArray,
    /// Typed array of another canonical type (e.g. `text[]`, `uuid[]`).
    Array(Box<CanonicalType>),
    /// Pass-through for schema-qualified enums (e.g. `"myschema.status"`) and any raw SQL
    /// type string not matched by the canonical parser. Rendered verbatim in DDL.
    Custom(String),
}

/// Broad category used by the query builder to validate RSQL operators per column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TypeCategory {
    Text,
    Int,
    Float,
    Bool,
    Uuid,
    Date,
    Timestamp,
    Time,
    Json,
    Bytes,
    /// Enums, arrays, custom types: permit all operators.
    Other,
}

/// How well a target database supports a canonical type.
#[derive(Clone, Debug)]
pub enum TypeSupport {
    /// Full native support; DDL string is the type to use in CREATE TABLE.
    Native(&'static str),
    /// No native type but semantically equivalent storage exists. Package still installs cleanly.
    Emulated(&'static str),
    /// Type exists but a feature is lost on this database (e.g. JSON indexing).
    Degraded(&'static str, &'static str),
    /// The type cannot be supported on this database at all.
    Unsupported,
}

/// Parse a [`ColumnTypeConfig`] (from JSON) into a [`CanonicalType`].
///
/// Accepts all historical aliases (both lower and upper case) so that existing packages
/// with raw SQL type strings continue to load without changes. Unrecognised strings become
/// [`CanonicalType::Custom`].
pub fn parse_canonical(ty: &ColumnTypeConfig) -> CanonicalType {
    match ty {
        ColumnTypeConfig::Simple(s) => parse_canonical_str(s, None),
        ColumnTypeConfig::Parameterized { name, params } => {
            parse_canonical_str(name, params.as_deref())
        }
    }
}

fn parse_canonical_str(s: &str, params: Option<&[u32]>) -> CanonicalType {
    let lower = s.trim().to_lowercase();

    // SDK pseudo-types — checked before the generic array guard.
    if lower == "asset[]" {
        return CanonicalType::AssetArray;
    }
    if lower == "asset" {
        return CanonicalType::Asset;
    }

    // Array suffix: strip "[]" and recurse.
    if lower.ends_with("[]") {
        let inner_str = &s[..s.len() - 2];
        let inner = parse_canonical_str(inner_str, None);
        return CanonicalType::Array(Box::new(inner));
    }

    // Schema-qualified custom type (e.g. "sample.order_status").
    if lower.contains('.') {
        return CanonicalType::Custom(s.to_string());
    }

    // Extract optional inline parameter from strings like "varchar(255)" or "numeric(10,2)".
    let (base, inline_params) = split_inline_params(&lower);

    match base {
        // Text family
        "text" => CanonicalType::Text,
        "varchar" | "character varying" => {
            let n = first_param(params, &inline_params);
            CanonicalType::Varchar(n)
        }
        "char" | "character" | "bpchar" => {
            let n = first_param(params, &inline_params);
            CanonicalType::Char(n)
        }
        "citext" | "name" => CanonicalType::Text,

        // Integer family
        "smallint" | "int2" => CanonicalType::SmallInt,
        "smallserial" | "serial2" => CanonicalType::SmallInt, // auto-inc smallint
        "int" | "integer" | "int4" => CanonicalType::Int,
        "serial" | "serial4" => CanonicalType::Serial,
        "bigint" | "int8" => CanonicalType::BigInt,
        "bigserial" | "serial8" => CanonicalType::BigSerial,

        // Float family
        "real" | "float4" => CanonicalType::Real,
        "double" | "double precision" | "float8" => CanonicalType::Double,
        "float" => CanonicalType::Double,
        "money" => CanonicalType::Decimal(None),

        // Decimal
        "numeric" | "decimal" => {
            let params_parsed = two_params(params, &inline_params);
            CanonicalType::Decimal(params_parsed)
        }

        // Boolean
        "boolean" | "bool" => CanonicalType::Boolean,

        // UUID
        "uuid" => CanonicalType::Uuid,

        // JSON
        "json" => CanonicalType::Json,
        "jsonb" => CanonicalType::Jsonb,

        // Date/time family
        "timestamptz" | "timestamp with time zone" => CanonicalType::Timestamp,
        "timestamp" | "timestamp without time zone" => CanonicalType::Timestamp,
        "timestamp_ntz" => CanonicalType::TimestampNtz,
        "date" => CanonicalType::Date,
        "time" | "time without time zone" => CanonicalType::Time,
        "timetz" | "time with time zone" => CanonicalType::Timetz,

        // Binary
        "bytea" | "bytes" => CanonicalType::Bytes,

        // Anything else passes through verbatim.
        _ => CanonicalType::Custom(s.to_string()),
    }
}

/// Split "varchar(255)" into ("varchar", Some(vec![255])).
fn split_inline_params(s: &str) -> (&str, Option<Vec<u32>>) {
    if let Some(paren) = s.find('(') {
        let base = s[..paren].trim();
        let inner = s[paren + 1..].trim_end_matches(')').trim();
        let nums: Vec<u32> = inner
            .split(',')
            .filter_map(|p| p.trim().parse::<u32>().ok())
            .collect();
        let params = if nums.is_empty() { None } else { Some(nums) };
        (base, params)
    } else {
        (s, None)
    }
}

fn first_param(explicit: Option<&[u32]>, inline: &Option<Vec<u32>>) -> Option<u32> {
    explicit
        .and_then(|p| p.first().copied())
        .or_else(|| inline.as_ref().and_then(|p| p.first().copied()))
}

fn two_params(explicit: Option<&[u32]>, inline: &Option<Vec<u32>>) -> Option<(u8, u8)> {
    let src = explicit
        .filter(|p| p.len() >= 2)
        .or_else(|| inline.as_deref().filter(|p| p.len() >= 2))?;
    Some((src[0] as u8, src[1] as u8))
}

// ─── Dialect-agnostic helpers ─────────────────────────────────────────────────

/// Classify a [`CanonicalType`] into a [`TypeCategory`] for RSQL operator validation.
/// This logic is shared across all dialects via `Dialect::type_category`.
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

/// Classify a cast-name string (as stored in `ColumnInfo.pg_type`) into a [`TypeCategory`].
/// Used as a fallback when the [`CanonicalType`] is not directly available (e.g. synthetic
/// audit columns).
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
        "timestamp"
        | "timestamptz"
        | "timestamp with time zone"
        | "timestamp without time zone" => TypeCategory::Timestamp,
        "time" | "timetz" | "time with time zone" | "time without time zone" => TypeCategory::Time,
        "json" | "jsonb" => TypeCategory::Json,
        "bytea" => TypeCategory::Bytes,
        _ => TypeCategory::Other,
    }
}

/// Return the cast name for a canonical type using the compiled-in dialect.
///
/// Populated from the active dialect at compile time so that `resolve()` (which has no
/// dialect parameter) can fill `ColumnInfo.pg_type` correctly without a runtime lookup.
pub fn active_cast_name(t: &CanonicalType) -> Option<String> {
    #[cfg(feature = "postgres")]
    return crate::db::postgres::cast_name(t);

    #[cfg(not(feature = "postgres"))]
    {
        let _ = t;
        None
    }
}
