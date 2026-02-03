//! Architect SDK: configuration-driven REST backend library.

pub mod config;
pub mod error;
pub mod migration;
pub mod response;
pub mod sql;
pub mod state;
pub mod store;
pub mod service;
pub mod handlers;
pub mod routes;

pub use config::{resolve, load_from_pool, FullConfig, ResolvedModel, ResolvedEntity};
pub use error::{AppError, ConfigError};
pub use migration::apply_migrations;
pub use response::{success_one, success_many, error_body};
pub use state::AppState;
pub use store::{ensure_database_exists, ensure_private_tables};
pub use routes::{common_routes, common_routes_with_ready, config_routes, entity_routes};
pub use service::CrudService;
