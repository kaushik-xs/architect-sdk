//! SQLite dialect implementation.
//!
//! SQLite is dynamically typed — affinity rules apply. RETURNING supported from 3.35 (2021).
//! RLS and named enum types are not supported.

use super::dialect::Dialect;
use super::types::{CanonicalType, TypeCategory, TypeSupport};

pub struct SqliteDialect;

impl Dialect for SqliteDialect {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn ddl_type(&self, t: &CanonicalType) -> String {
        match t {
            CanonicalType::Text
            | CanonicalType::Varchar(_)
            | CanonicalType::Char(_)
            | CanonicalType::Uuid     // stored as TEXT
            | CanonicalType::Asset => "TEXT".to_string(),

            CanonicalType::SmallInt | CanonicalType::Int | CanonicalType::BigInt => {
                "INTEGER".to_string()
            }
            // INTEGER PRIMARY KEY is auto-incrementing in SQLite.
            CanonicalType::Serial | CanonicalType::BigSerial => "INTEGER".to_string(),

            CanonicalType::Real | CanonicalType::Double => "REAL".to_string(),
            CanonicalType::Decimal(_) => "NUMERIC".to_string(),
            CanonicalType::Boolean => "INTEGER".to_string(), // 0 / 1
            CanonicalType::Json | CanonicalType::Jsonb => "TEXT".to_string(),
            CanonicalType::Timestamp | CanonicalType::TimestampNtz => "TEXT".to_string(),
            CanonicalType::Date => "TEXT".to_string(),
            CanonicalType::Time | CanonicalType::Timetz => "TEXT".to_string(),
            CanonicalType::Bytes => "BLOB".to_string(),
            CanonicalType::AssetArray | CanonicalType::Array(_) => "TEXT".to_string(),
            CanonicalType::Custom(s) => s.clone(),
        }
    }

    fn cast_name(&self, _t: &CanonicalType) -> Option<String> {
        None
    }

    fn type_category(&self, t: &CanonicalType) -> TypeCategory {
        super::types::type_category(t)
    }

    fn type_support(&self, t: &CanonicalType) -> TypeSupport {
        match t {
            CanonicalType::Jsonb => {
                TypeSupport::Degraded("TEXT", "JSONB not available on SQLite; using TEXT")
            }
            CanonicalType::Timetz => TypeSupport::Degraded(
                "TEXT",
                "SQLite has no TIME WITH TIME ZONE; storing as ISO-8601 TEXT",
            ),
            CanonicalType::Array(_) => TypeSupport::Degraded(
                "TEXT",
                "SQLite has no native array type; stored as JSON TEXT",
            ),
            CanonicalType::Asset => TypeSupport::Emulated("TEXT"),
            CanonicalType::AssetArray => TypeSupport::Emulated("TEXT"),
            _ => TypeSupport::Native(self.ddl_type(t).leak()),
        }
    }

    fn quote_ident(&self, s: &str) -> String {
        format!("\"{}\"", s.replace('"', "\"\""))
    }

    fn placeholder(&self, _n: usize) -> String {
        "?".to_string()
    }

    fn cast_expr(&self, placeholder: &str, _cast: &str) -> String {
        placeholder.to_string()
    }

    fn now_fn(&self) -> &'static str {
        // CURRENT_TIMESTAMP works as both a DDL column DEFAULT and inside DML expressions.
        // datetime('now') is a function call and is rejected by SQLite as a DEFAULT value.
        "CURRENT_TIMESTAMP"
    }

    fn uuid_default_expr(&self) -> &'static str {
        // Portable UUID v4 via SQLite's randomblob().
        "lower(hex(randomblob(4)))||'-'||lower(hex(randomblob(2)))||'-4'||\
         substr(lower(hex(randomblob(2))),2)||'-'||\
         substr('89ab',abs(random())%4+1,1)||\
         substr(lower(hex(randomblob(2))),2)||'-'||lower(hex(randomblob(6)))"
    }

    fn returning_clause(&self, cols: &str) -> String {
        format!("RETURNING {}", cols)
    }

    fn upsert_conflict(&self, conflict_cols: &[&str], set_pairs: &str) -> String {
        let cols = conflict_cols
            .iter()
            .map(|c| self.quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        format!("ON CONFLICT ({}) DO UPDATE SET {}", cols, set_pairs)
    }

    fn to_one_subquery(&self, col_exprs: &[String], from_clause: &str) -> String {
        let pairs = col_exprs
            .iter()
            .map(|c| format!("'{}', {}", c.trim_matches('"'), c))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "(SELECT json_object({}) FROM {} LIMIT 1)",
            pairs, from_clause
        )
    }

    fn to_many_subquery(&self, col_exprs: &[String], from_clause: &str) -> String {
        let pairs = col_exprs
            .iter()
            .map(|c| format!("'{}', {}", c.trim_matches('"'), c))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "(SELECT COALESCE(json_group_array(json_object({})), '[]') FROM {})",
            pairs, from_clause
        )
    }

    fn sys_json_type(&self) -> &'static str {
        "TEXT"
    }

    fn sys_timestamp_type(&self) -> &'static str {
        "TEXT"
    }

    fn sys_bigserial_type(&self) -> &'static str {
        "INTEGER"
    }

    fn sys_bytes_type(&self) -> &'static str {
        "BLOB"
    }

    fn audit_timestamp_type(&self) -> &'static str {
        "TEXT"
    }

    fn supports_schemas(&self) -> bool {
        false
    }

    fn default_now_plus_hours(&self, _hours: u32) -> Option<String> {
        // SQLite has no constant-expression interval arithmetic; caller makes the column nullable.
        None
    }

    fn supports_rls(&self) -> bool {
        false
    }

    fn supports_named_enum_types(&self) -> bool {
        false
    }

    fn supports_index_include(&self) -> bool {
        false
    }

    fn set_tenant_session_sql(&self, _tenant_id: &str) -> Option<String> {
        None
    }
}
