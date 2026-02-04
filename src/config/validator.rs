//! Config validation: referential integrity and API consistency.

use crate::config::{FullConfig, PrimaryKeyConfig};
use crate::error::ConfigError;
use std::collections::HashSet;

/// Default schema id when configs omit schema_id (manifest-driven schema).
pub fn default_schema_id(config: &FullConfig) -> Result<&str, ConfigError> {
    config
        .schemas
        .first()
        .map(|s| s.id.as_str())
        .ok_or_else(|| ConfigError::Validation("at least one schema required (set manifest.schema)".into()))
}

pub fn validate(config: &FullConfig) -> Result<(), ConfigError> {
    let default_sid = default_schema_id(config)?;
    let schema_ids: HashSet<&str> = config.schemas.iter().map(|s| s.id.as_str()).collect();
    let table_ids: HashSet<&str> = config.tables.iter().map(|t| t.id.as_str()).collect();
    let column_ids: HashSet<&str> = config.columns.iter().map(|c| c.id.as_str()).collect();

    for e in &config.enums {
        let sid = e.schema_id.as_deref().unwrap_or(default_sid);
        if !schema_ids.contains(sid) {
            return Err(ConfigError::MissingReference {
                kind: "schema",
                id: sid.to_string(),
            });
        }
    }

    for t in &config.tables {
        let sid = t.schema_id.as_deref().unwrap_or(default_sid);
        if !schema_ids.contains(sid) {
            return Err(ConfigError::MissingReference {
                kind: "schema",
                id: sid.to_string(),
            });
        }
        let pk_cols = match &t.primary_key {
            PrimaryKeyConfig::Single(s) => vec![s.as_str()],
            PrimaryKeyConfig::Composite(v) => v.iter().map(String::as_str).collect::<Vec<_>>(),
        };
        let table_columns: HashSet<&str> = config
            .columns
            .iter()
            .filter(|c| c.table_id == t.id)
            .map(|c| c.name.as_str())
            .collect();
        for pk in &pk_cols {
            if !table_columns.contains(pk) {
                return Err(ConfigError::InvalidPrimaryKey {
                    table_id: t.id.clone(),
                    column: (*pk).to_string(),
                });
            }
        }
    }

    for c in &config.columns {
        if !table_ids.contains(c.table_id.as_str()) {
            return Err(ConfigError::MissingReference {
                kind: "table",
                id: c.table_id.clone(),
            });
        }
    }

    for idx in &config.indexes {
        let sid = idx.schema_id.as_deref().unwrap_or(default_sid);
        if !schema_ids.contains(sid) || !table_ids.contains(idx.table_id.as_str()) {
            return Err(ConfigError::MissingReference {
                kind: "schema or table",
                id: format!("{} / {}", sid, idx.table_id),
            });
        }
    }

    for r in &config.relationships {
        let from_sid = r.from_schema_id.as_str();
        let to_sid = r.to_schema_id.as_str();
        if !schema_ids.contains(from_sid)
            || !schema_ids.contains(to_sid)
            || !table_ids.contains(r.from_table_id.as_str())
            || !table_ids.contains(r.to_table_id.as_str())
            || !column_ids.contains(r.from_column_id.as_str())
            || !column_ids.contains(r.to_column_id.as_str())
        {
            return Err(ConfigError::MissingReference {
                kind: "relationship",
                id: r.id.clone(),
            });
        }
    }

    let mut path_segments = HashSet::new();
    for api in &config.api_entities {
        if !table_ids.contains(api.entity_id.as_str()) {
            return Err(ConfigError::MissingReference {
                kind: "table",
                id: api.entity_id.clone(),
            });
        }
        if !path_segments.insert(api.path_segment.as_str()) {
            return Err(ConfigError::DuplicatePathSegment(api.path_segment.clone()));
        }
    }

    Ok(())
}
