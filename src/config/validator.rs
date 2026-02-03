//! Config validation: referential integrity and API consistency.

use crate::config::{FullConfig, PrimaryKeyConfig};
use crate::error::ConfigError;
use std::collections::HashSet;

pub fn validate(config: &FullConfig) -> Result<(), ConfigError> {
    let schema_ids: HashSet<&str> = config.schemas.iter().map(|s| s.id.as_str()).collect();
    let table_ids: HashSet<&str> = config.tables.iter().map(|t| t.id.as_str()).collect();
    let column_ids: HashSet<&str> = config.columns.iter().map(|c| c.id.as_str()).collect();

    for e in &config.enums {
        if !schema_ids.contains(e.schema_id.as_str()) {
            return Err(ConfigError::MissingReference {
                kind: "schema",
                id: e.schema_id.clone(),
            });
        }
    }

    for t in &config.tables {
        if !schema_ids.contains(t.schema_id.as_str()) {
            return Err(ConfigError::MissingReference {
                kind: "schema",
                id: t.schema_id.clone(),
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
        if !schema_ids.contains(idx.schema_id.as_str()) || !table_ids.contains(idx.table_id.as_str()) {
            return Err(ConfigError::MissingReference {
                kind: "schema or table",
                id: format!("{} / {}", idx.schema_id, idx.table_id),
            });
        }
    }

    for r in &config.relationships {
        if !schema_ids.contains(r.from_schema_id.as_str())
            || !schema_ids.contains(r.to_schema_id.as_str())
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
