//! Resolved entity model: config validated and flattened for runtime use.

use crate::config::types::{
    AssetColumnConfig, EntityEventTrigger, McpEntityConfig, VersioningConfig,
};
use crate::config::ValidationRule;
use std::collections::{HashMap, HashSet};

/// Direction of a related-include: to_one (we have FK to them) or to_many (they have FK to us).
#[derive(Clone, Debug)]
pub enum IncludeDirection {
    ToOne,
    ToMany,
}

/// Spec for including a related entity in list/read responses. Name is the related entity's path_segment (e.g. "orders", "users").
#[derive(Clone, Debug)]
pub struct IncludeSpec {
    /// API name for the include (path_segment of the related entity).
    pub name: String,
    pub direction: IncludeDirection,
    /// Path segment of the related entity (for lookup in model).
    pub related_path_segment: String,
    /// Our column used in the join (our FK for to_one; our PK for to_many).
    pub our_key_column: String,
    /// Their column used in the join (their PK for to_one; their FK for to_many).
    pub their_key_column: String,
}

/// Primary key type for parsing path/body ids.
#[derive(Clone, Debug)]
pub enum PkType {
    Uuid,
    BigInt,
    Int,
    Text,
}

#[derive(Clone, Debug)]
pub struct ColumnInfo {
    pub name: String,
    pub pk_type: Option<PkType>,
    pub nullable: bool,
    /// Whether the column has a DB default (e.g. gen_random_uuid(), NOW()).
    pub has_default: bool,
    /// PostgreSQL type name for SQL casts (e.g. "timestamptz") when binding string values.
    pub pg_type: Option<String>,
    /// True when the column was declared with type "asset" or "asset[]".
    pub is_asset: bool,
    /// True when the column was declared with type "asset[]" (stores a JSONB array of paths).
    pub asset_is_array: bool,
    /// Storage config for asset columns (prefix template, compression).
    pub asset_config: Option<AssetColumnConfig>,
}

#[derive(Clone, Debug)]
pub struct ResolvedEntity {
    pub table_id: String,
    pub schema_name: String,
    pub table_name: String,
    pub path_segment: String,
    pub pk_columns: Vec<String>,
    pub pk_type: PkType,
    pub columns: Vec<ColumnInfo>,
    pub operations: Vec<String>,
    /// Column names to strip from all API responses (sensitive data).
    pub sensitive_columns: HashSet<String>,
    /// Available includes (related entities) for ?include= name1,name2. Built from relationships.
    pub includes: Vec<IncludeSpec>,
    pub validation: HashMap<String, ValidationRule>,
    /// Decision-hub event triggers. Empty when no events are configured.
    pub events: Vec<EntityEventTrigger>,
    /// Column whose null→non-null transition signals an archive (for on:"archive" triggers).
    pub archive_field: Option<String>,
    /// Package id this entity belongs to. Set via ResolvedModel::with_package_id().
    pub package_id: String,
    /// When true, a companion `{table}_audit` table exists and every write is journaled there.
    pub audit_log: bool,
    /// When true, this entity's table is shared across all RLS tenants: every tenant may read it,
    /// but only the Platform Admin tenant may write. Carried from `TableConfig.global`. Writes by
    /// non-admin tenants are rejected with 403 in handlers (and blocked by RLS at the DB level).
    pub global: bool,
    /// Natural-key column used to resolve `parentRef` in bulk create (e.g. `"location_id"`).
    pub parent_ref_column: Option<String>,
    /// Row-level versioning config, carried from TableConfig.
    pub versioning: Option<VersioningConfig>,
    /// MCP exposure config, carried from ApiEntityConfig. None when not set.
    pub mcp: Option<McpEntityConfig>,
    /// Names of JSON/JSONB columns flagged `extensible: true`. Each is a extensible-fields bag
    /// whose per-tenant field definitions live in the KV registry and whose keys are
    /// RSQL-filterable/sortable via the `<column>.<key>` syntax. Empty when none configured.
    pub extensible_columns: Vec<String>,
}

/// A report whose named params have been translated to positional placeholders and whose
/// validation rules/defaults are indexed by param name. Built from [`crate::config::types::ReportConfig`]
/// during `resolve()`. Reports produce no [`ResolvedEntity`] — they are data-plane only, looked up
/// by id when a run request arrives.
#[derive(Clone, Debug)]
pub struct ResolvedReport {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    /// Package id this report belongs to. Set via ResolvedModel::with_package_id().
    pub package_id: String,
    /// SQL schemas the query references.
    pub schemas: Vec<String>,
    /// SQL with named params already translated to positional placeholders (`$1`, `$2`, …).
    pub sql: String,
    /// Param names in positional order (index 0 → `$1`). Repeated named params are deduplicated
    /// and share a single placeholder.
    pub param_order: Vec<String>,
    /// Validation rules per param name (reuses the entity ValidationRule engine).
    pub rules: HashMap<String, ValidationRule>,
    /// Default values per param name, applied when the param is absent from the request.
    pub defaults: HashMap<String, serde_json::Value>,
    /// Optional SQL cast per param name (e.g. "timestamptz").
    pub casts: HashMap<String, String>,
    /// Whether to EXPLAIN-validate the SQL at registration time.
    pub validate_on_register: bool,
    /// Per-report result-cache TTL override (seconds). `None` = use the global default TTL;
    /// `Some(0)` = never cache this report.
    pub cache_ttl_secs: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct ResolvedModel {
    pub entities: Vec<ResolvedEntity>,
    pub entity_by_path: HashMap<String, ResolvedEntity>,
    /// Reports available for execution, keyed by report id.
    pub reports: HashMap<String, ResolvedReport>,
}

impl ResolvedModel {
    pub fn entity_by_path(&self, path: &str) -> Option<&ResolvedEntity> {
        self.entity_by_path.get(path)
    }

    /// Look up a report by id.
    pub fn report(&self, id: &str) -> Option<&ResolvedReport> {
        self.reports.get(id)
    }

    /// Backfill `package_id` on all contained entities and reports. Call this after `resolve()`
    /// when the package id is known (e.g. from manifest.id or the route parameter).
    pub fn with_package_id(mut self, package_id: &str) -> Self {
        for e in &mut self.entities {
            e.package_id = package_id.to_string();
        }
        for e in self.entity_by_path.values_mut() {
            e.package_id = package_id.to_string();
        }
        for r in self.reports.values_mut() {
            r.package_id = package_id.to_string();
        }
        self
    }
}
