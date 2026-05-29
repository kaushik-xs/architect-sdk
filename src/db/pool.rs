//! Feature-gated type aliases for the active database pool and connection.
//!
//! Only one dialect feature may be active at a time (enforced by build.rs).
//! Code that imports `db::pool::Pool` compiles to the concrete sqlx type with zero overhead.

#[cfg(feature = "postgres")]
pub use sqlx::postgres::PgRow as DbRow;
#[cfg(feature = "postgres")]
pub use sqlx::PgConnection as Connection;
#[cfg(feature = "postgres")]
pub use sqlx::PgPool as Pool;
#[cfg(feature = "postgres")]
pub type DbConnection = sqlx::pool::PoolConnection<sqlx::Postgres>;

#[cfg(feature = "mysql")]
pub use sqlx::mysql::MySqlRow as DbRow;
#[cfg(feature = "mysql")]
pub use sqlx::MySqlConnection as Connection;
#[cfg(feature = "mysql")]
pub use sqlx::MySqlPool as Pool;
#[cfg(feature = "mysql")]
pub type DbConnection = sqlx::pool::PoolConnection<sqlx::MySql>;

#[cfg(feature = "sqlite")]
pub use sqlx::sqlite::SqliteRow as DbRow;
#[cfg(feature = "sqlite")]
pub use sqlx::SqliteConnection as Connection;
#[cfg(feature = "sqlite")]
pub use sqlx::SqlitePool as Pool;
#[cfg(feature = "sqlite")]
pub type DbConnection = sqlx::pool::PoolConnection<sqlx::Sqlite>;
