//! Convert serde_json::Value to types that sqlx can bind.

use serde_json::Value;
use sqlx::encode::{Encode, IsNull};
use sqlx::postgres::{PgTypeInfo, Postgres};
use sqlx::Database;

/// A value that can be bound to a PostgreSQL query. Converts from serde_json::Value.
#[derive(Clone, Debug)]
pub enum PgBindValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Uuid(uuid::Uuid),
    Json(Value),
}

impl PgBindValue {
    pub fn from_json(v: &Value) -> Result<Self, crate::error::AppError> {
        Ok(match v {
            Value::Null => PgBindValue::Null,
            Value::Bool(b) => PgBindValue::Bool(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    PgBindValue::I64(i)
                } else if let Some(f) = n.as_f64() {
                    PgBindValue::F64(f)
                } else {
                    PgBindValue::I64(n.as_i64().unwrap_or(0))
                }
            }
            Value::String(s) => {
                if let Ok(u) = uuid::Uuid::parse_str(s) {
                    PgBindValue::Uuid(u)
                } else {
                    PgBindValue::String(s.clone())
                }
            }
            Value::Array(_) | Value::Object(_) => PgBindValue::Json(v.clone()),
        })
    }
}

impl<'q> Encode<'q, Postgres> for PgBindValue {
    fn encode_by_ref(
        &self,
        buf: &mut <Postgres as Database>::ArgumentBuffer<'q>,
    ) -> Result<IsNull, Box<dyn std::error::Error + Send + Sync>> {
        Ok(match self {
            PgBindValue::Null => <Option::<i32> as Encode<Postgres>>::encode_by_ref(&None, buf)?,
            PgBindValue::Bool(b) => <bool as Encode<Postgres>>::encode_by_ref(b, buf)?,
            PgBindValue::I64(n) => <i64 as Encode<Postgres>>::encode_by_ref(n, buf)?,
            PgBindValue::F64(n) => <f64 as Encode<Postgres>>::encode_by_ref(n, buf)?,
            PgBindValue::String(s) => {
                let s_ref: &str = s.as_str();
                <&str as Encode<Postgres>>::encode_by_ref(&s_ref, buf)?
            }
            PgBindValue::Uuid(u) => {
                let u_str = u.to_string();
                <&str as Encode<Postgres>>::encode_by_ref(&u_str.as_str(), buf)?
            }
            PgBindValue::Json(v) => <serde_json::Value as Encode<Postgres>>::encode_by_ref(v, buf)?,
        })
    }
}

impl sqlx::Type<Postgres> for PgBindValue {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("TEXT")
    }
}
