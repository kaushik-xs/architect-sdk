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
        .ok_or_else(|| {
            ConfigError::Validation("at least one schema required (set manifest.schema)".into())
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::types::{
        ApiEntityConfig, ColumnConfig, ColumnTypeConfig, FullConfig, PrimaryKeyConfig,
        SchemaConfig, TableConfig,
    };

    fn schema(id: &str) -> SchemaConfig {
        SchemaConfig {
            id: id.into(),
            name: id.into(),
            comment: None,
        }
    }

    fn table(id: &str, schema_id: Option<&str>, pk: &str) -> TableConfig {
        TableConfig {
            id: id.into(),
            schema_id: schema_id.map(Into::into),
            name: id.into(),
            comment: None,
            primary_key: PrimaryKeyConfig::Single(pk.into()),
            unique: vec![],
            check: vec![],
            audit_log: false,
        }
    }

    fn column(id: &str, table_id: &str, name: &str) -> ColumnConfig {
        ColumnConfig {
            id: id.into(),
            table_id: table_id.into(),
            name: name.into(),
            type_: ColumnTypeConfig::Simple("text".into()),
            nullable: true,
            default: None,
            comment: None,
            asset: None,
        }
    }

    fn api_entity(table_id: &str, path: &str) -> ApiEntityConfig {
        ApiEntityConfig {
            entity_id: table_id.into(),
            path_segment: path.into(),
            operations: vec!["list".into()],
            sensitive_columns: vec![],
            validation: Default::default(),
            archive_field: None,
            events: vec![],
            parent_ref_column: None,
        }
    }

    fn minimal_config() -> FullConfig {
        let mut c = FullConfig::default();
        c.schemas.push(schema("s1"));
        c.tables.push(table("t1", None, "id"));
        c.columns.push(column("c1", "t1", "id"));
        c.api_entities.push(api_entity("t1", "items"));
        c
    }

    // --- happy path ---

    #[test]
    fn valid_config_passes() {
        assert!(validate(&minimal_config()).is_ok());
    }

    // --- empty schemas ---

    #[test]
    fn no_schemas_fails() {
        let mut c = minimal_config();
        c.schemas.clear();
        assert!(matches!(validate(&c), Err(ConfigError::Validation(_))));
    }

    // --- missing table for api_entity ---

    #[test]
    fn api_entity_missing_table_fails() {
        let mut c = minimal_config();
        c.api_entities[0].entity_id = "nonexistent".into();
        assert!(matches!(
            validate(&c),
            Err(ConfigError::MissingReference { kind: "table", .. })
        ));
    }

    // --- duplicate path segments ---

    #[test]
    fn duplicate_path_segment_fails() {
        let mut c = minimal_config();
        c.tables.push(table("t2", None, "id"));
        c.columns.push(column("c2", "t2", "id"));
        c.api_entities.push(api_entity("t2", "items")); // same path as t1
        assert!(matches!(
            validate(&c),
            Err(ConfigError::DuplicatePathSegment(_))
        ));
    }

    // --- column references nonexistent table ---

    #[test]
    fn column_missing_table_fails() {
        let mut c = minimal_config();
        c.columns.push(column("c99", "no_such_table", "name"));
        assert!(matches!(
            validate(&c),
            Err(ConfigError::MissingReference { kind: "table", .. })
        ));
    }

    // --- primary key column must exist ---

    #[test]
    fn pk_column_missing_fails() {
        let mut c = FullConfig::default();
        c.schemas.push(schema("s1"));
        c.tables.push(table("t1", None, "id")); // pk="id" but no column "id"
                                                // Add a column with a *different* name so the table itself passes the column table_id check
        c.columns.push(column("c1", "t1", "not_id"));
        c.api_entities.push(api_entity("t1", "items"));
        assert!(matches!(
            validate(&c),
            Err(ConfigError::InvalidPrimaryKey { .. })
        ));
    }

    // --- schema reference on table ---

    #[test]
    fn table_missing_schema_fails() {
        let mut c = minimal_config();
        c.tables[0].schema_id = Some("nonexistent_schema".into());
        assert!(matches!(
            validate(&c),
            Err(ConfigError::MissingReference { kind: "schema", .. })
        ));
    }

    // --- default_schema_id ---

    #[test]
    fn default_schema_id_returns_first() {
        let c = minimal_config();
        assert_eq!(default_schema_id(&c).unwrap(), "s1");
    }

    #[test]
    fn default_schema_id_empty_fails() {
        let c = FullConfig::default();
        assert!(default_schema_id(&c).is_err());
    }
}
