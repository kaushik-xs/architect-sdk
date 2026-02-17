//! Raw config types matching the JSON schema (postgres-config-schema + api_entities).

use serde::{Deserialize, Serialize};

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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ColumnTypeConfig {
    Simple(String),
    Parameterized { name: String, params: Option<Vec<u32>> },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ColumnDefaultConfig {
    Literal(String),
    Expression { expression: String },
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
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IndexColumnEntry {
    Name(String),
    Spec { name: String, direction: Option<String>, nulls: Option<String> },
    Expression { expression: String },
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
    pub from_schema_id: String,
    pub from_table_id: String,
    pub from_column_id: String,
    pub to_schema_id: String,
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
