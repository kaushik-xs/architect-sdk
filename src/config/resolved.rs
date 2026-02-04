//! Resolved entity model: config validated and flattened for runtime use.

use crate::config::ValidationRule;
use std::collections::{HashMap, HashSet};

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
    pub validation: HashMap<String, ValidationRule>,
}

#[derive(Clone, Debug)]
pub struct ResolvedModel {
    pub entities: Vec<ResolvedEntity>,
    pub entity_by_path: HashMap<String, ResolvedEntity>,
}

impl ResolvedModel {
    pub fn entity_by_path(&self, path: &str) -> Option<&ResolvedEntity> {
        self.entity_by_path.get(path)
    }
}
