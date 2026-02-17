//! Shared application state for all routes. Model is reloadable after package install.

use crate::config::ResolvedModel;
use crate::tenant::TenantRegistry;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct AppState {
    /// Default/central pool (from DATABASE_URL). Used for non-tenant, schema, and rls strategies; also for config/_sys_* when config is in central DB.
    pub pool: PgPool,
    /// Default/active model (used for /api/v1/:path_segment). Reloaded after package install.
    pub model: Arc<RwLock<ResolvedModel>>,
    /// Resolved model per package_id (used for /api/v1/package/:package_id/:path_segment). For database tenants, key is "package_id:tenant_id". Populated on package install and on first request (lazy load).
    pub package_models: Arc<RwLock<HashMap<String, ResolvedModel>>>,
    /// Pools for database-strategy tenants, keyed by tenant_id. Created on first request for that tenant.
    pub tenant_pools: Arc<RwLock<HashMap<String, PgPool>>>,
    /// Tenant registry (strategy + config per tenant), loaded from central DB at startup.
    pub tenant_registry: Arc<TenantRegistry>,
}
