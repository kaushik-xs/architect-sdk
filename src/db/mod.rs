//! Database dialect abstraction for architect-sdk.
//!
//! Package configs use canonical type names (see [`types::CanonicalType`]). Each dialect
//! module maps those to database-specific DDL strings, SQL casts, and operator rules.
//!
//! # Adding a new dialect
//! 1. Add a Cargo feature (e.g. `mysql`).
//! 2. Create `src/db/mysql.rs` implementing the same surface as `postgres.rs`.
//! 3. Gate it with `#[cfg(feature = "mysql")]`.
//! 4. Update callers in `config/loader.rs`, `migration.rs`, and `sql/builder.rs`.

pub mod types;
pub use types::{parse_canonical, CanonicalType, TypeCategory, TypeSupport};

#[cfg(feature = "postgres")]
pub mod postgres;
