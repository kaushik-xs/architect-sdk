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
        // type_info() is TEXT for all variants, so parameters must be valid UTF-8 text.
        // Encoding I64/F64 as binary would break; use decimal strings + $n::numeric casts.
        // Booleans: "true"/"false" + $n::boolean. JSON objects/arrays: serde_json::to_string
        // + $n::jsonb (or ::json) so jsonb columns accept TEXT-shaped params.
        Ok(match self {
            PgBindValue::Null => <Option::<i32> as Encode<Postgres>>::encode_by_ref(&None, buf)?,
            PgBindValue::Bool(b) => {
                let s: &str = if *b { "true" } else { "false" };
                <&str as Encode<Postgres>>::encode_by_ref(&s, buf)?
            }
            PgBindValue::I64(n) => {
                let s = n.to_string();
                let s_ref = s.as_str();
                <&str as Encode<Postgres>>::encode_by_ref(&s_ref, buf)?
            }
            PgBindValue::F64(n) => {
                let s = format!("{}", n);
                let s_ref = s.as_str();
                <&str as Encode<Postgres>>::encode_by_ref(&s_ref, buf)?
            }
            PgBindValue::String(s) => {
                let s_ref: &str = s.as_str();
                <&str as Encode<Postgres>>::encode_by_ref(&s_ref, buf)?
            }
            PgBindValue::Uuid(u) => {
                let u_str = u.to_string();
                <&str as Encode<Postgres>>::encode_by_ref(&u_str.as_str(), buf)?
            }
            PgBindValue::Json(v) => {
                let s = serde_json::to_string(v).map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                })?;
                <&str as Encode<Postgres>>::encode_by_ref(&s.as_str(), buf)?
            }
        })
    }
}

impl sqlx::Type<Postgres> for PgBindValue {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("TEXT")
    }
}
