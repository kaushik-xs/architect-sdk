//! `foundry-rs` — configuration-driven REST backend library for Rust with PostgreSQL.
//!
//! Define your schemas, tables, columns, and API entities in JSON. Get a fully working,
//! production-grade REST API with multi-tenancy, validation, and OpenAPI docs — no
//! entity-specific business logic required.
//!
//! # Quick Start
//!
//! Add to your `Cargo.toml`:
//! ```toml
//! [dependencies]
//! foundry-rs = "0.1"
//! ```
//!
//! See the [repository](https://github.com/kaushik-xs/architect-sdk) for full examples.

pub mod authrs;
pub mod case;
pub mod config;
pub mod db;
pub mod error;
pub mod events;
pub mod extractors;
pub mod handlers;
pub mod migration;
pub mod openapi;
pub mod response;
pub mod routes;
pub mod service;
pub mod sql;
pub mod state;
pub mod storage;
pub mod store;
pub mod tenant;

pub use config::{load_from_pool, resolve, FullConfig, ResolvedEntity, ResolvedModel};
pub use error::{AppError, ConfigError};
pub use migration::{
    apply_migrations, compute_migration_plan, execute_migration_plan, MigrationOperation,
    MigrationPlan, MigrationRisk, MigrationSafety, MigrationStep, MigrationSummary,
};
pub use response::{error_body, success_many, success_one};
pub use routes::{common_routes, common_routes_with_ready, config_routes, entity_routes};
pub use service::{CrudService, TenantExecutor};
pub use state::AppState;
pub use storage::{init_storage_provider, StorageProvider};
pub use store::{create_pool, ensure_database_exists, ensure_sys_tables, DEFAULT_PACKAGE_ID};
pub use tenant::{load_registry_from_pool, TenantEntry, TenantRegistry, TenantStrategy};
