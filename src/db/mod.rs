//! Database dialect abstraction for architect-sdk.
//!
//! Package configs use canonical type names (see [`types::CanonicalType`]). Each dialect
//! module maps those to database-specific DDL strings, SQL casts, and operator rules.
//!
//! # Adding a new dialect
//! 1. Add a Cargo feature (e.g. `mysql`).
//! 2. Create `src/db/your_dialect.rs` implementing [`Dialect`].
//! 3. Gate it with `#[cfg(feature = "your_dialect")]` below.
//! 4. Add it to `active_dialect()`.
//! 5. Add to Cargo.toml features.

pub mod dialect;
pub mod pool;
pub mod types;

pub use dialect::Dialect;
pub use types::{
    active_cast_name, parse_canonical, type_category, type_category_from_cast, CanonicalType,
    TypeCategory, TypeSupport,
};

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "postgres")]
pub use postgres::PostgresDialect;

#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "mysql")]
pub use mysql::MySqlDialect;

#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteDialect;

/// Construct the compiled-in dialect as a shared reference.
/// The dialect is determined at compile time by the active feature flag.
pub fn active_dialect() -> std::sync::Arc<dyn Dialect> {
    _active_dialect_impl()
}

#[cfg(feature = "postgres")]
fn _active_dialect_impl() -> std::sync::Arc<dyn Dialect> {
    std::sync::Arc::new(PostgresDialect)
}

#[cfg(feature = "mysql")]
fn _active_dialect_impl() -> std::sync::Arc<dyn Dialect> {
    std::sync::Arc::new(MySqlDialect)
}

#[cfg(feature = "sqlite")]
fn _active_dialect_impl() -> std::sync::Arc<dyn Dialect> {
    std::sync::Arc::new(SqliteDialect)
}

#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
fn _active_dialect_impl() -> std::sync::Arc<dyn Dialect> {
    panic!("No database dialect feature enabled. Enable one of: postgres, mysql, sqlite.");
}
