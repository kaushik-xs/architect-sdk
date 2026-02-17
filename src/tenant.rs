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
    /// Tenant shares DB but has its own schema (same pool, schema override).
    Schema,
    /// Tenant shares DB and schema; isolation via RLS and app.tenant_id.
    Rls,
}

impl std::str::FromStr for TenantStrategy {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "database" => Ok(TenantStrategy::Database),
            "schema" => Ok(TenantStrategy::Schema),
            "rls" => Ok(TenantStrategy::Rls),
            _ => Err(AppError::BadRequest(format!(
                "invalid tenant strategy: {} (expected database, schema, or rls)",
                s
            ))),
        }
    }
}

/// Per-tenant config from _sys_tenants.
#[derive(Clone, Debug)]
pub struct TenantEntry {
    pub strategy: TenantStrategy,
    /// Required when strategy = Database.
    pub database_url: Option<String>,
    /// Required when strategy = Schema.
    pub schema_name: Option<String>,
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

/// Load tenant registry from architect._sys_tenants. Invalid rows are skipped
/// (missing database_url for database, missing schema_name for schema).
pub async fn load_registry_from_pool(pool: &PgPool) -> Result<TenantRegistry, AppError> {
    let q_table = qualified_sys_table("_sys_tenants");
    let sql = format!(
        "SELECT id, strategy, database_url, schema_name FROM {} ORDER BY id",
        q_table
    );
    let rows = sqlx::query_as::<_, (String, String, Option<String>, Option<String>)>(&sql)
        .fetch_all(pool)
        .await?;

    let mut by_id = HashMap::new();
    for (id, strategy_str, database_url, schema_name) in rows {
        let strategy: TenantStrategy = strategy_str.parse().map_err(|e: AppError| e)?;
        match &strategy {
            TenantStrategy::Database if database_url.as_ref().map(|s| s.is_empty()).unwrap_or(true) => {
                tracing::warn!("tenant {}: strategy database requires database_url, skipping", id);
                continue;
            }
            TenantStrategy::Schema if schema_name.as_ref().map(|s| s.is_empty()).unwrap_or(true) => {
                tracing::warn!("tenant {}: strategy schema requires schema_name, skipping", id);
                continue;
            }
            _ => {}
        }
        by_id.insert(
            id,
            TenantEntry {
                strategy,
                database_url: database_url.filter(|s| !s.is_empty()),
                schema_name: schema_name.filter(|s| !s.is_empty()),
            },
        );
    }

    Ok(TenantRegistry { by_id })
}
