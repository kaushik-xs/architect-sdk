//! MySQL dialect implementation.

use super::dialect::Dialect;
use super::types::{CanonicalType, TypeCategory, TypeSupport};

pub struct MySqlDialect;

impl Dialect for MySqlDialect {
    fn name(&self) -> &'static str {
        "mysql"
    }

    fn ddl_type(&self, t: &CanonicalType) -> String {
        match t {
            CanonicalType::Text => "TEXT".to_string(),
            CanonicalType::Varchar(Some(n)) => format!("VARCHAR({})", n),
            CanonicalType::Varchar(None) => "TEXT".to_string(),
            CanonicalType::Char(Some(n)) => format!("CHAR({})", n),
            CanonicalType::Char(None) => "CHAR(1)".to_string(),
            CanonicalType::SmallInt => "SMALLINT".to_string(),
            CanonicalType::Int => "INT".to_string(),
            CanonicalType::BigInt => "BIGINT".to_string(),
            CanonicalType::Real => "FLOAT".to_string(),
            CanonicalType::Double => "DOUBLE".to_string(),
            CanonicalType::Decimal(Some((p, s))) => format!("DECIMAL({}, {})", p, s),
            CanonicalType::Decimal(None) => "DECIMAL".to_string(),
            CanonicalType::Boolean => "TINYINT(1)".to_string(),
            // UUID has no native MySQL type — store as CHAR(36).
            CanonicalType::Uuid => "CHAR(36)".to_string(),
            CanonicalType::Json | CanonicalType::Jsonb => "JSON".to_string(),
            CanonicalType::Timestamp | CanonicalType::TimestampNtz => "DATETIME(6)".to_string(),
            CanonicalType::Date => "DATE".to_string(),
            CanonicalType::Time => "TIME".to_string(),
            // MySQL TIME has no timezone.
            CanonicalType::Timetz => "TIME".to_string(),
            CanonicalType::Bytes => "BLOB".to_string(),
            CanonicalType::Serial => "INT AUTO_INCREMENT".to_string(),
            CanonicalType::BigSerial => "BIGINT AUTO_INCREMENT".to_string(),
            CanonicalType::Asset => "TEXT".to_string(),
            // Arrays stored as JSON.
            CanonicalType::AssetArray | CanonicalType::Array(_) => "JSON".to_string(),
            CanonicalType::Custom(s) => s.clone(),
        }
    }

    fn cast_name(&self, _t: &CanonicalType) -> Option<String> {
        // MySQL infers types from bound values — SQL casts not needed in param placeholders.
        None
    }

    fn type_category(&self, t: &CanonicalType) -> TypeCategory {
        super::types::type_category(t)
    }

    fn type_support(&self, t: &CanonicalType) -> TypeSupport {
        match t {
            CanonicalType::Jsonb => TypeSupport::Degraded(
                "JSON",
                "JSONB binary storage / GIN indexes unavailable on MySQL; using JSON",
            ),
            CanonicalType::Timetz => {
                TypeSupport::Degraded("TIME", "MySQL TIME does not store timezone offset")
            }
            CanonicalType::Array(_) => TypeSupport::Degraded(
                "JSON",
                "MySQL has no native array type; array stored as JSON",
            ),
            CanonicalType::Asset => TypeSupport::Emulated("TEXT"),
            CanonicalType::AssetArray => TypeSupport::Emulated("JSON"),
            _ => TypeSupport::Native(self.ddl_type(t).leak()),
        }
    }

    fn quote_ident(&self, s: &str) -> String {
        format!("`{}`", s.replace('`', "``"))
    }

    fn placeholder(&self, _n: usize) -> String {
        "?".to_string()
    }

    fn cast_expr(&self, placeholder: &str, _cast: &str) -> String {
        // MySQL binding handles types — no cast syntax needed in SQL.
        placeholder.to_string()
    }

    fn now_fn(&self) -> &'static str {
        "NOW(6)"
    }

    fn uuid_default_expr(&self) -> &'static str {
        "UUID()"
    }

    fn returning_clause(&self, _cols: &str) -> String {
        // MySQL does not support RETURNING. Callers re-query after mutation.
        String::new()
    }

    fn upsert_conflict(&self, _conflict_cols: &[&str], set_pairs: &str) -> String {
        format!("ON DUPLICATE KEY UPDATE {}", set_pairs)
    }

    fn to_one_subquery(&self, col_exprs: &[String], from_clause: &str) -> String {
        let pairs = col_exprs
            .iter()
            .map(|c| format!("'{}', {}", c.trim_matches('`'), c))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "(SELECT JSON_OBJECT({}) FROM {} LIMIT 1)",
            pairs, from_clause
        )
    }

    fn to_many_subquery(&self, col_exprs: &[String], from_clause: &str) -> String {
        let pairs = col_exprs
            .iter()
            .map(|c| format!("'{}', {}", c.trim_matches('`'), c))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT({})), JSON_ARRAY()) FROM {})",
            pairs, from_clause
        )
    }

    fn sys_json_type(&self) -> &'static str {
        "JSON"
    }

    fn sys_timestamp_type(&self) -> &'static str {
        "DATETIME(6)"
    }

    fn sys_bigserial_type(&self) -> &'static str {
        "BIGINT AUTO_INCREMENT"
    }

    fn audit_timestamp_type(&self) -> &'static str {
        "DATETIME(6)"
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

    fn set_tenant_session_sql(&self, tenant_id: &str) -> Option<String> {
        Some(format!(
            "SET @tenant_id = '{}'",
            tenant_id.replace('\'', "''")
        ))
    }
}
