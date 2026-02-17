//! Multi-tenant registry: strategy and config per tenant, loaded from central DB.

use crate::error::AppError;
use crate::store::qualified_sys_table;
use sqlx::PgPool;
use std::collections::HashMap;

/// Tenant isolation strategy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TenantStrategy {
    /// Tenant has its own PostgreSQL database (own pool).
    Database,
    /// Tenant shares DB and schema; isolation via RLS and app.tenant_id.
    Rls,
}

impl std::str::FromStr for TenantStrategy {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "database" => Ok(TenantStrategy::Database),
            "rls" => Ok(TenantStrategy::Rls),
            _ => Err(AppError::BadRequest(format!(
                "invalid tenant strategy: {} (expected database or rls)",
                s
            ))),
        }
    }
}

/// Per-tenant config from _sys_tenants.
#[derive(Clone, Debug)]
pub struct TenantEntry {
    pub strategy: TenantStrategy,
    /// Required when strategy = Database. Optional for RLS (when set, app data uses that DB; config stays in architect DB).
    pub database_url: Option<String>,
}

/// In-memory tenant registry loaded from central DB. Thread-safe via Arc.
#[derive(Clone, Default)]
pub struct TenantRegistry {
    by_id: HashMap<String, TenantEntry>,
}

impl TenantRegistry {
    pub fn new() -> Self {
        TenantRegistry {
            by_id: HashMap::new(),
        }
    }

    pub fn get(&self, tenant_id: &str) -> Option<&TenantEntry> {
        self.by_id.get(tenant_id)
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

/// Load tenant registry from architect._sys_tenants. Invalid rows are skipped (missing database_url for database strategy).
pub async fn load_registry_from_pool(pool: &PgPool) -> Result<TenantRegistry, AppError> {
    let q_table = qualified_sys_table("_sys_tenants");
    let sql = format!(
        "SELECT id, strategy, database_url FROM {} ORDER BY id",
        q_table
    );
    let rows = sqlx::query_as::<_, (String, String, Option<String>)>(&sql)
        .fetch_all(pool)
        .await?;

    let mut by_id = HashMap::new();
    for (id, strategy_str, database_url) in rows {
        if strategy_str.eq_ignore_ascii_case("schema") {
            tracing::warn!("tenant {}: strategy 'schema' is no longer supported, skipping", id);
            continue;
        }
        let strategy: TenantStrategy = strategy_str.parse().map_err(|e: AppError| e)?;
        if matches!(&strategy, TenantStrategy::Database) && database_url.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
            tracing::warn!("tenant {}: strategy database requires database_url, skipping", id);
            continue;
        }
        by_id.insert(
            id,
            TenantEntry {
                strategy,
                database_url: database_url.filter(|s| !s.is_empty()),
            },
        );
    }

    Ok(TenantRegistry { by_id })
}
