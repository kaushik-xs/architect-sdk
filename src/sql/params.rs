//! Convert serde_json::Value to bindable SQL parameter values across all dialects.

use serde_json::Value;

/// A value that can be bound to a SQL query parameter.
/// Variants are dialect-agnostic; feature-gated `Encode` impls below handle the wire format.
#[derive(Clone, Debug)]
pub enum BindValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Uuid(uuid::Uuid),
    Json(Value),
}

impl BindValue {
    pub fn from_json(v: &Value) -> Result<Self, crate::error::AppError> {
        Ok(match v {
            Value::Null => BindValue::Null,
            Value::Bool(b) => BindValue::Bool(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    BindValue::I64(i)
                } else if let Some(f) = n.as_f64() {
                    BindValue::F64(f)
                } else {
                    BindValue::I64(0)
                }
            }
            Value::String(s) => {
                if let Ok(u) = uuid::Uuid::parse_str(s) {
                    BindValue::Uuid(u)
                } else {
                    BindValue::String(s.clone())
                }
            }
            Value::Array(_) | Value::Object(_) => BindValue::Json(v.clone()),
        })
    }
}

// ─── PostgreSQL ───────────────────────────────────────────────────────────────

#[cfg(feature = "postgres")]
mod pg_impl {
    use super::BindValue;
    use sqlx::encode::{Encode, IsNull};
    use sqlx::postgres::{PgTypeInfo, Postgres};
    use sqlx::Database;

    impl<'q> Encode<'q, Postgres> for BindValue {
        fn encode_by_ref(
            &self,
            buf: &mut <Postgres as Database>::ArgumentBuffer<'q>,
        ) -> Result<IsNull, Box<dyn std::error::Error + Send + Sync>> {
            Ok(match self {
                BindValue::Null => <Option<i32> as Encode<Postgres>>::encode_by_ref(&None, buf)?,
                BindValue::Bool(b) => {
                    let s: &str = if *b { "true" } else { "false" };
                    <&str as Encode<Postgres>>::encode_by_ref(&s, buf)?
                }
                BindValue::I64(n) => {
                    let s = n.to_string();
                    <&str as Encode<Postgres>>::encode_by_ref(&s.as_str(), buf)?
                }
                BindValue::F64(n) => {
                    let s = format!("{}", n);
                    <&str as Encode<Postgres>>::encode_by_ref(&s.as_str(), buf)?
                }
                BindValue::String(s) => {
                    <&str as Encode<Postgres>>::encode_by_ref(&s.as_str(), buf)?
                }
                BindValue::Uuid(u) => {
                    let s = u.to_string();
                    <&str as Encode<Postgres>>::encode_by_ref(&s.as_str(), buf)?
                }
                BindValue::Json(v) => {
                    let s = serde_json::to_string(v)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                    <&str as Encode<Postgres>>::encode_by_ref(&s.as_str(), buf)?
                }
            })
        }
    }

    impl sqlx::Type<Postgres> for BindValue {
        fn type_info() -> PgTypeInfo {
            // OID 705 = pg_catalog.unknown — lets PostgreSQL infer the type from
            // the column/expression context, avoiding "text vs integer" cast errors.
            PgTypeInfo::with_name("unknown")
        }

        fn compatible(_ty: &PgTypeInfo) -> bool {
            true
        }
    }
}

// ─── MySQL ────────────────────────────────────────────────────────────────────

#[cfg(feature = "mysql")]
mod mysql_impl {
    use super::BindValue;
    use sqlx::encode::{Encode, IsNull};
    use sqlx::mysql::{MySql, MySqlTypeInfo};
    use sqlx::Database;

    impl<'q> Encode<'q, MySql> for BindValue {
        fn encode_by_ref(
            &self,
            buf: &mut <MySql as Database>::ArgumentBuffer<'q>,
        ) -> Result<IsNull, Box<dyn std::error::Error + Send + Sync>> {
            Ok(match self {
                BindValue::Null => <Option<i32> as Encode<MySql>>::encode_by_ref(&None, buf)?,
                BindValue::Bool(b) => <i32 as Encode<MySql>>::encode_by_ref(&(*b as i32), buf)?,
                BindValue::I64(n) => <i64 as Encode<MySql>>::encode_by_ref(n, buf)?,
                BindValue::F64(n) => <f64 as Encode<MySql>>::encode_by_ref(n, buf)?,
                BindValue::String(s) => <String as Encode<MySql>>::encode_by_ref(s, buf)?,
                BindValue::Uuid(u) => {
                    let s = u.to_string();
                    <String as Encode<MySql>>::encode_by_ref(&s, buf)?
                }
                BindValue::Json(v) => {
                    let s = serde_json::to_string(v)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                    <String as Encode<MySql>>::encode_by_ref(&s, buf)?
                }
            })
        }
    }

    impl sqlx::Type<MySql> for BindValue {
        fn type_info() -> MySqlTypeInfo {
            <String as sqlx::Type<MySql>>::type_info()
        }
    }
}

// ─── SQLite ───────────────────────────────────────────────────────────────────

#[cfg(feature = "sqlite")]
mod sqlite_impl {
    use super::BindValue;
    use sqlx::encode::{Encode, IsNull};
    use sqlx::sqlite::{Sqlite, SqliteTypeInfo};
    use sqlx::Database;

    impl<'q> Encode<'q, Sqlite> for BindValue {
        fn encode_by_ref(
            &self,
            buf: &mut <Sqlite as Database>::ArgumentBuffer<'q>,
        ) -> Result<IsNull, Box<dyn std::error::Error + Send + Sync>> {
            Ok(match self {
                BindValue::Null => <Option<i32> as Encode<Sqlite>>::encode_by_ref(&None, buf)?,
                BindValue::Bool(b) => <i32 as Encode<Sqlite>>::encode_by_ref(&(*b as i32), buf)?,
                BindValue::I64(n) => <i64 as Encode<Sqlite>>::encode_by_ref(n, buf)?,
                BindValue::F64(n) => <f64 as Encode<Sqlite>>::encode_by_ref(n, buf)?,
                BindValue::String(s) => <String as Encode<Sqlite>>::encode_by_ref(s, buf)?,
                BindValue::Uuid(u) => {
                    let s = u.to_string();
                    <String as Encode<Sqlite>>::encode_by_ref(&s, buf)?
                }
                BindValue::Json(v) => {
                    let s = serde_json::to_string(v)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                    <String as Encode<Sqlite>>::encode_by_ref(&s, buf)?
                }
            })
        }
    }

    impl sqlx::Type<Sqlite> for BindValue {
        fn type_info() -> SqliteTypeInfo {
            <String as sqlx::Type<Sqlite>>::type_info()
        }
    }
}

/// Backward-compat alias — existing call sites referencing PgBindValue continue to compile.
#[cfg(feature = "postgres")]
pub type PgBindValue = BindValue;
