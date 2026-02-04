//! Shared application state for all routes. Model is reloadable after module install.

use crate::config::ResolvedModel;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    /// Default/active model (used for /api/v1/:path_segment). Reloaded after module install.
    pub model: Arc<RwLock<ResolvedModel>>,
    /// Resolved model per module_id (used for /api/v1/module/:module_id/:path_segment). Populated on module install and on first request (lazy load).
    pub module_models: Arc<RwLock<HashMap<String, ResolvedModel>>>,
}
