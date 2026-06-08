//! Shared application state for all routes. Model is reloadable after package install.

use crate::authrs::AuthrsClient;
use crate::config::ResolvedModel;
use crate::db::{pool::Pool, Dialect};
use crate::events::DecisionHubClient;
use crate::storage::StorageProvider;
use crate::tenant::TenantRegistry;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct AppState {
    /// Default/central pool (from DATABASE_URL).
    pub pool: Pool,
    /// Default/active model (used for /api/v1/:path_segment). Reloaded after package install.
    pub model: Arc<RwLock<ResolvedModel>>,
    /// Resolved model per package_id. For database tenants, key is "package_id:tenant_id".
    pub package_models: Arc<RwLock<HashMap<String, ResolvedModel>>>,
    /// Pools for database-strategy tenants, keyed by tenant_id.
    pub tenant_pools: Arc<RwLock<HashMap<String, Pool>>>,
    /// Tenant registry (strategy + config per tenant), loaded from central DB at startup.
    pub tenant_registry: Arc<TenantRegistry>,
    /// Optional blob storage provider for asset columns.
    pub storage: Option<Arc<dyn StorageProvider>>,
    /// Optional decision-hub client. None when DECISION_HUB_URL is not set.
    pub event_client: Option<Arc<DecisionHubClient>>,
    /// Optional authrs permission-check client. None when AUTHRS_URL or SERVICE_NAME is not set.
    pub authrs_client: Option<Arc<AuthrsClient>>,
    /// Active database dialect (set at startup via `db::active_dialect()`).
    pub dialect: Arc<dyn Dialect>,
    /// Per-tenant extensible-field registry cache (read-through, TTL-bounded, evicted on write).
    /// Construct with `Default::default()`.
    pub extensible_cache: crate::extensible_fields::RegistryCache,
}
