//! Shared application state for all routes. Model is reloadable after package install.

use crate::config::ResolvedModel;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    /// Default/active model (used for /api/v1/:path_segment). Reloaded after package install.
    pub model: Arc<RwLock<ResolvedModel>>,
    /// Resolved model per package_id (used for /api/v1/package/:package_id/:path_segment). Populated on package install and on first request (lazy load).
    pub package_models: Arc<RwLock<HashMap<String, ResolvedModel>>>,
}
