//! Raw config types matching the JSON schema (postgres-config-schema + api_entities).

use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaConfig {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub comment: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnumConfig {
    pub id: String,
    #[serde(default)]
    pub schema_id: Option<String>,
    pub name: String,
    pub values: Vec<String>,
    #[serde(default)]
    pub comment: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableCheck {
    pub name: String,
    pub expression: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PrimaryKeyConfig {
    Single(String),
    Composite(Vec<String>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableConfig {
    pub id: String,
    #[serde(default)]
    pub schema_id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub comment: Option<String>,
    pub primary_key: PrimaryKeyConfig,
    #[serde(default)]
    pub unique: Vec<Vec<String>>,
    #[serde(default)]
    pub check: Vec<TableCheck>,
    /// When true, a companion `{table}_audit` table is created and every create/update/delete
    /// is recorded there with the full row snapshot, action type, timestamp, and actor.
    #[serde(default)]
    pub audit_log: bool,
    /// Row-level versioning: when enabled, a `{table}_history` table is created and a snapshot
    /// of the row is written there before every UPDATE and DELETE.
    #[serde(default)]
    pub versioning: Option<VersioningConfig>,
    /// When true, this table holds data shared across all RLS tenants instead of being
    /// tenant-isolated. Under the RLS strategy it gets asymmetric row-level-security policies:
    /// every tenant may read all rows, but only the Platform Admin tenant
    /// (see `tenant::platform_tenant_id`) may insert/update/delete. Has no effect under the
    /// Database strategy (tenants are physically separate databases). Default false.
    #[serde(default)]
    pub global: bool,
}

/// Configuration for row-level versioning on a table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersioningConfig {
    pub enabled: bool,
    /// Maximum number of historical versions to retain per row (None = keep all).
    /// Must be ≥ 1 when set.
    #[serde(default)]
    pub keep_versions: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ColumnTypeConfig {
    Simple(String),
    Parameterized {
        name: String,
        params: Option<Vec<u32>>,
    },
}

#[derive(Clone, Debug, Serialize)]
pub enum ColumnDefaultConfig {
    Literal(String),
    Expression { expression: String },
}

impl<'de> Deserialize<'de> for ColumnDefaultConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = serde_json::Value::deserialize(deserializer)?;
        match v {
            serde_json::Value::String(s) => Ok(ColumnDefaultConfig::Literal(s)),
            serde_json::Value::Object(mut obj) => {
                if let Some(serde_json::Value::String(s)) = obj.remove("expression") {
                    return Ok(ColumnDefaultConfig::Expression { expression: s });
                }
                if let Some(serde_json::Value::String(s)) = obj.remove("value").or_else(|| obj.remove("literal")) {
                    return Ok(ColumnDefaultConfig::Literal(s));
                }
                Err(serde::de::Error::custom(format!(
                    "column default must be a string, {{ \"expression\": \"...\" }}, or {{ \"value\": \"...\" }}; got object with keys: {:?}",
                    obj.keys().collect::<Vec<_>>()
                )))
            }
            serde_json::Value::Bool(b) => Ok(ColumnDefaultConfig::Literal(b.to_string())),
            serde_json::Value::Number(n) => Ok(ColumnDefaultConfig::Literal(n.to_string())),
            other => Err(serde::de::Error::custom(format!(
                "column default must be a string, boolean, number, or {{ \"expression\": \"...\" }}; got {}",
                type_name_of_json(&other)
            ))),
        }
    }
}

fn type_name_of_json(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnConfig {
    pub id: String,
    pub table_id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: ColumnTypeConfig,
    #[serde(default = "default_true")]
    pub nullable: bool,
    #[serde(default)]
    pub default: Option<ColumnDefaultConfig>,
    #[serde(default)]
    pub comment: Option<String>,
    #[serde(default)]
    pub asset: Option<AssetColumnConfig>,
    /// When true, this JSON/JSONB column is an extensible "extensible fields" bag: per-tenant
    /// field definitions are stored in the KV registry and its keys become RSQL
    /// filterable/sortable via the `<column>.<key>` dotted syntax. Ignored (with a warning)
    /// for non-JSON columns.
    #[serde(default)]
    pub extensible: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IndexColumnEntry {
    Name(String),
    Spec {
        name: String,
        direction: Option<String>,
        nulls: Option<String>,
    },
    Expression {
        expression: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexConfig {
    pub id: String,
    #[serde(default)]
    pub schema_id: Option<String>,
    pub table_id: String,
    pub name: String,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub unique: bool,
    pub columns: Vec<IndexColumnEntry>,
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default, rename = "where")]
    pub where_: Option<String>,
    #[serde(default)]
    pub comment: Option<String>,
}

impl IndexConfig {
    pub fn where_clause(&self) -> Option<&str> {
        self.where_.as_deref()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelationshipConfig {
    pub id: String,
    /// Defaults to the owning package's schema when absent.
    #[serde(default)]
    pub from_schema_id: Option<String>,
    pub from_table_id: String,
    pub from_column_id: String,
    /// When set, this relationship crosses into another installed package.
    /// The `to_schema_id` and `to_table_id` are resolved from that package's config.
    #[serde(default)]
    pub to_package_id: Option<String>,
    /// Defaults to the owning package's schema when absent (or to the target package's schema
    /// for cross-package relationships).
    #[serde(default)]
    pub to_schema_id: Option<String>,
    pub to_table_id: String,
    pub to_column_id: String,
    #[serde(default)]
    pub on_update: Option<String>,
    #[serde(default)]
    pub on_delete: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ValidationRule {
    #[serde(default)]
    pub required: Option<bool>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub max_length: Option<u32>,
    #[serde(default)]
    pub min_length: Option<u32>,
    #[serde(default)]
    pub pattern: Option<String>,
    #[serde(default)]
    pub allowed: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    pub minimum: Option<f64>,
    #[serde(default)]
    pub maximum: Option<f64>,
    // Asset-specific validation (only applied when the column type is "asset")
    #[serde(default)]
    pub allowed_mime_types: Option<Vec<String>>,
    #[serde(default)]
    pub allowed_extensions: Option<Vec<String>>,
    #[serde(default)]
    pub max_size_mb: Option<f64>,
    #[serde(default)]
    pub min_size_kb: Option<f64>,
    #[serde(default)]
    pub max_filename_length: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssetColumnConfig {
    /// Path prefix template. Supports {yyyy}, {mm}, {dd}, {hh}, {tenant_id}, {entity}.
    #[serde(default)]
    pub prefix: Option<String>,
    /// Byte-level compression before upload: "none" | "gzip" | "zstd". Default: "none".
    #[serde(default)]
    pub compression: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventCondition {
    /// Column name (snake_case) to inspect on the saved row.
    pub field: String,
    /// Fire when the field's new value equals this (post-update check).
    #[serde(default)]
    pub changed_to: Option<serde_json::Value>,
    /// Fire when the field's current value equals this.
    #[serde(default)]
    pub equals: Option<serde_json::Value>,
    /// true = fire when field is non-null; false = fire when null.
    #[serde(default)]
    pub not_null: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntityEventTrigger {
    pub id: String,
    /// Lifecycle hook: "create" | "update" | "delete" | "archive".
    pub on: String,
    /// Suffix of the event type sent to decision-hub.
    /// Defaults to "created" / "updated" / "deleted" / "archived" when omitted.
    #[serde(default)]
    pub event_name: Option<String>,
    /// Only fire when this condition is satisfied against the saved row (snake_case keys).
    #[serde(default)]
    pub condition: Option<EventCondition>,
}

/// Configuration for exposing a selected API entity as an MCP tool.
/// Only takes effect when the `mcp` feature is enabled.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct McpEntityConfig {
    /// Opt-in to MCP exposure. Default false.
    #[serde(default)]
    pub enabled: bool,
    /// Subset of the entity's REST operations to expose as MCP tools.
    /// Defaults to all operations on the entity when omitted.
    /// Valid values: "list", "read", "create", "update", "delete".
    #[serde(default)]
    pub operations: Vec<String>,
    /// Prefix for generated tool names. Defaults to `path_segment`.
    #[serde(default)]
    pub tool_prefix: Option<String>,
    /// Human-readable description injected into each tool's MCP description.
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiEntityConfig {
    pub entity_id: String,
    pub path_segment: String,
    pub operations: Vec<String>,
    /// Column names that must never be exposed in API responses (e.g. password hashes, secrets).
    #[serde(default)]
    pub sensitive_columns: Vec<String>,
    #[serde(default)]
    pub validation: std::collections::HashMap<String, ValidationRule>,
    /// Column whose null→non-null transition signals an archive. Required for on:"archive" triggers.
    #[serde(default)]
    pub archive_field: Option<String>,
    /// Decision-hub event triggers for this entity.
    #[serde(default)]
    pub events: Vec<EntityEventTrigger>,
    /// Column holding the human-readable natural key used to resolve `parentRef` during bulk
    /// create (e.g. `"location_id"` for locations, `"product_id"` for products). When set, bulk
    /// create accepts a virtual `parentRef` field; the SDK resolves it to a UUID and writes
    /// `parent_id` in a second pass after all rows are inserted.
    #[serde(default)]
    pub parent_ref_column: Option<String>,
    /// MCP tool exposure config. Only effective when the `mcp` feature is enabled.
    #[serde(default)]
    pub mcp: Option<McpEntityConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KvStoreConfig {
    pub id: String,
    pub namespace: String,
    #[serde(default)]
    pub comment: Option<String>,
}

/// All config types in one struct for in-memory loading.
#[derive(Clone, Debug, Default)]
pub struct FullConfig {
    pub schemas: Vec<SchemaConfig>,
    pub enums: Vec<EnumConfig>,
    pub tables: Vec<TableConfig>,
    pub columns: Vec<ColumnConfig>,
    pub indexes: Vec<IndexConfig>,
    pub relationships: Vec<RelationshipConfig>,
    pub api_entities: Vec<ApiEntityConfig>,
    pub kv_stores: Vec<KvStoreConfig>,
}
