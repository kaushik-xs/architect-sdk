//! Multi-tenant registry: strategy and config per tenant, loaded from central DB.

use crate::db::pool::Pool;
use crate::error::AppError;
use crate::store::qualified_sys_table;
use std::collections::HashMap;

/// Default tenant id that identifies the Platform Admin — the only principal allowed to write
/// `global` tables. Overridable via the `ARCHITECT_PLATFORM_TENANT` env var. A normal RLS request
/// runs `SET LOCAL app.tenant_id = '<tenant>'`; only this id satisfies the write policies that
/// `migration::apply_rls_to_tables` installs on global tables, so non-admin tenants get read-only
/// access enforced at the database level.
pub const DEFAULT_PLATFORM_TENANT_ID: &str = "_platform";

/// The configured Platform Admin tenant id (env `ARCHITECT_PLATFORM_TENANT`, else `_platform`).
/// Used both when generating RLS policies (migration) and when authorizing writes (handlers), so
/// the two must agree across processes that share a database.
pub fn platform_tenant_id() -> String {
    std::env::var("ARCHITECT_PLATFORM_TENANT")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_PLATFORM_TENANT_ID.to_string())
}

/// Optional app-wide tenant-strategy override read from the `ARCHITECT_TENANT_STRATEGY` env var
/// (`"rls"` or `"database"`). When set, **every** tenant runs under this single strategy regardless
/// of its `_sys_tenants.strategy` value — useful to pin a whole deployment to one model. When unset
/// (the default), each tenant uses its own stored strategy. Unrecognized values are ignored
/// (treated as unset). See [`load_registry_from_pool`] for how the override is applied.
pub fn forced_tenant_strategy() -> Option<TenantStrategy> {
    std::env::var("ARCHITECT_TENANT_STRATEGY")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse().ok())
}

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

    /// All Database-strategy tenants as (tenant_id, database_url).
    /// Used by the DDL broadcast to know which dedicated databases need migration.
    pub fn database_tenant_targets(&self) -> Vec<(String, String)> {
        self.by_id
            .iter()
            .filter_map(|(id, entry)| {
                if matches!(entry.strategy, TenantStrategy::Database) {
                    entry
                        .database_url
                        .as_ref()
                        .map(|url| (id.clone(), url.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// True if any RLS tenants share the central architect DB (no database_url).
    /// When true, the broadcast must run DDL on the central pool once for all such tenants.
    pub fn has_shared_rls_tenants(&self) -> bool {
        self.by_id
            .values()
            .any(|e| matches!(e.strategy, TenantStrategy::Rls) && e.database_url.is_none())
    }

    /// RLS tenants that have their own dedicated database_url (not the central DB).
    /// DDL is run per unique URL with rls_tenant_column enabled.
    pub fn rls_dedicated_db_targets(&self) -> Vec<(String, String)> {
        self.by_id
            .iter()
            .filter_map(|(id, entry)| {
                if matches!(entry.strategy, TenantStrategy::Rls) {
                    entry
                        .database_url
                        .as_ref()
                        .map(|url| (id.clone(), url.clone()))
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Load tenant registry from architect._sys_tenants. Invalid rows are skipped (missing database_url for database strategy).
pub async fn load_registry_from_pool(pool: &Pool) -> Result<TenantRegistry, AppError> {
    let q_table = qualified_sys_table("_sys_tenants");
    let sql = format!(
        "SELECT id, strategy, database_url FROM {} ORDER BY id",
        q_table
    );
    let rows = sqlx::query_as::<_, (String, String, Option<String>)>(&sql)
        .fetch_all(pool)
        .await?;

    let forced = forced_tenant_strategy();
    if let Some(s) = &forced {
        let name = match s {
            TenantStrategy::Database => "database",
            TenantStrategy::Rls => "rls",
        };
        tracing::info!(
            "ARCHITECT_TENANT_STRATEGY override active: all tenants run as '{}' strategy (per-tenant _sys_tenants.strategy ignored)",
            name
        );
    }

    let mut by_id = HashMap::new();
    for (id, strategy_str, database_url) in rows {
        // Effective strategy: the app-wide override when set, else the per-tenant stored value.
        let strategy = match &forced {
            Some(s) => s.clone(),
            None => {
                if strategy_str.eq_ignore_ascii_case("schema") {
                    tracing::warn!(
                        "tenant {}: strategy 'schema' is no longer supported, skipping",
                        id
                    );
                    continue;
                }
                strategy_str.parse().map_err(|e: AppError| e)?
            }
        };
        // Under forced RLS we run a single shared central DB (greenfield), so any per-tenant
        // database_url is ignored and every tenant shares the architect DB with RLS policies.
        // Otherwise keep the configured URL (dedicated DB for Database strategy, or a dedicated
        // RLS DB when set per tenant).
        let database_url = if matches!(&forced, Some(TenantStrategy::Rls)) {
            None
        } else {
            database_url.filter(|s| !s.is_empty())
        };
        if matches!(&strategy, TenantStrategy::Database) && database_url.is_none() {
            tracing::warn!(
                "tenant {}: database strategy requires database_url, skipping",
                id
            );
            continue;
        }
        by_id.insert(
            id,
            TenantEntry {
                strategy,
                database_url,
            },
        );
    }

    Ok(TenantRegistry { by_id })
}

#[cfg(test)]
mod strategy_override_tests {
    use super::*;

    #[test]
    fn strategy_parses_rls_and_database_case_insensitively() {
        assert_eq!(
            "rls".parse::<TenantStrategy>().unwrap(),
            TenantStrategy::Rls
        );
        assert_eq!(
            "DATABASE".parse::<TenantStrategy>().unwrap(),
            TenantStrategy::Database
        );
        assert!("bogus".parse::<TenantStrategy>().is_err());
    }

    // Mutates a process-global env var; no other test reads ARCHITECT_TENANT_STRATEGY, so this is
    // safe. Sets, asserts, and restores the prior value.
    #[test]
    fn forced_strategy_reads_env() {
        let prev = std::env::var("ARCHITECT_TENANT_STRATEGY").ok();

        std::env::remove_var("ARCHITECT_TENANT_STRATEGY");
        assert!(forced_tenant_strategy().is_none(), "unset = no override");

        std::env::set_var("ARCHITECT_TENANT_STRATEGY", "rls");
        assert_eq!(forced_tenant_strategy(), Some(TenantStrategy::Rls));

        std::env::set_var("ARCHITECT_TENANT_STRATEGY", "  database  ");
        assert_eq!(forced_tenant_strategy(), Some(TenantStrategy::Database));

        std::env::set_var("ARCHITECT_TENANT_STRATEGY", "nonsense");
        assert!(
            forced_tenant_strategy().is_none(),
            "unrecognized value is ignored"
        );

        match prev {
            Some(v) => std::env::set_var("ARCHITECT_TENANT_STRATEGY", v),
            None => std::env::remove_var("ARCHITECT_TENANT_STRATEGY"),
        }
    }
}
