//! Shared application state for all routes. Model is reloadable after plugin install.

use crate::config::ResolvedModel;
use sqlx::PgPool;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    /// Reloaded after plugin install so new entities are available without restart.
    pub model: Arc<RwLock<ResolvedModel>>,
}
