//! Resolved entity model: config validated and flattened for runtime use.

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
