//! Shared application state for all routes.

use crate::config::ResolvedModel;
use sqlx::PgPool;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub model: Arc<ResolvedModel>,
}
