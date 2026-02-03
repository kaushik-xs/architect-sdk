//! Apply config to the database: DDL for schemas, enums, tables, indexes, and foreign keys.
//! Order follows PostgreSQL dependencies (see docs/postgres-config-schema.md § 3.5).

use crate::config::types::*;
use crate::config::{validate, FullConfig};
use crate::error::AppError;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};

fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

/// Apply full config to the database: CREATE SCHEMA, CREATE TYPE, CREATE TABLE, CREATE INDEX, ADD FK.
/// Validates config first. Idempotent for schemas and types (IF NOT EXISTS); tables are CREATE TABLE only (fails if exists).
pub async fn apply_migrations(pool: &PgPool, config: &FullConfig) -> Result<(), AppError> {
    validate(config)?;

    let schemas_by_id: HashMap<_, _> = config.schemas.iter().map(|s| (s.id.as_str(), s)).collect();
    let tables_by_id: HashMap<_, _> = config.tables.iter().map(|t| (t.id.as_str(), t)).collect();
    let columns_by_table: HashMap<_, Vec<&ColumnConfig>> = config.columns.iter().fold(
        HashMap::new(),
        |mut m, c| {
            m.entry(c.table_id.as_str()).or_default().push(c);
            m
        },
    );

    for s in &config.schemas {
        let name = quote(&s.name);
        let comment = s
            .comment
            .as_ref()
            .map(|c| format!("COMMENT ON SCHEMA {} IS '{}'", name, c.replace('\'', "''")));
        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {}", name))
            .execute(pool)
            .await?;
        if let Some(sql) = comment {
            let _ = sqlx::query(&sql).execute(pool).await;
        }
    }

    for e in &config.enums {
        let schema = schemas_by_id
            .get(e.schema_id.as_str())
            .ok_or_else(|| AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "schema",
                id: e.schema_id.clone(),
            }))?;
        let schema_name = quote(&schema.name);
        let type_name = quote(&e.name);
        let values: Vec<String> = e.values.iter().map(|v| format!("'{}'", v.replace('\'', "''"))).collect();
        let sql = format!(
            "CREATE TYPE {}.{} AS ENUM ({})",
            schema_name,
            type_name,
            values.join(", ")
        );
        let _ = sqlx::query(&sql).execute(pool).await;
    }

    for t in &config.tables {
        let schema = schemas_by_id
            .get(t.schema_id.as_str())
            .ok_or_else(|| AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "schema",
                id: t.schema_id.clone(),
            }))?;
        let schema_name = quote(&schema.name);
        let table_name = quote(&t.name);
        let full_name = format!("{}.{}", schema_name, table_name);

        let cols = columns_by_table
            .get(t.id.as_str())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let mut col_defs: Vec<String> = Vec::new();
        for c in cols {
            let typ = type_str(&c.type_, &schemas_by_id);
            let mut def = format!("{} {}", quote(&c.name), typ);
            if !c.nullable {
                def.push_str(" NOT NULL");
            }
            if let Some(ref d) = c.default {
                def.push_str(" DEFAULT ");
                match d {
                    ColumnDefaultConfig::Literal(s) => def.push_str(s),
                    ColumnDefaultConfig::Expression { expression } => def.push_str(expression),
                }
            }
            col_defs.push(def);
        }

        let config_col_names: HashSet<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        for (name, def_suffix) in [
            ("created_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
            ("updated_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
            ("archived_at", "TIMESTAMPTZ"),
        ] {
            if !config_col_names.contains(name) {
                col_defs.push(format!("{} {}", quote(name), def_suffix));
            }
        }

        let pk_cols = match &t.primary_key {
            PrimaryKeyConfig::Single(s) => vec![quote(s)],
            PrimaryKeyConfig::Composite(v) => v.iter().map(|s| quote(s)).collect::<Vec<_>>(),
        };
        let pk_def = format!("PRIMARY KEY ({})", pk_cols.join(", "));
        col_defs.push(pk_def);

        for u in &t.unique {
            let cols: Vec<String> = u.iter().map(|s| quote(s)).collect();
            col_defs.push(format!("UNIQUE ({})", cols.join(", ")));
        }
        for ch in &t.check {
            col_defs.push(format!(
                "CONSTRAINT {} CHECK ({})",
                quote(&ch.name),
                ch.expression
            ));
        }

        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (\n  {}\n)",
            full_name,
            col_defs.join(",\n  ")
        );
        sqlx::query(&sql).execute(pool).await?;
    }

    for idx in &config.indexes {
        let schema = schemas_by_id.get(idx.schema_id.as_str()).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "schema",
                id: idx.schema_id.clone(),
            })
        })?;
        let table = tables_by_id.get(idx.table_id.as_str()).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "table",
                id: idx.table_id.clone(),
            })
        })?;
        let schema_name = quote(&schema.name);
        let table_name = quote(&table.name);
        let full_table = format!("{}.{}", schema_name, table_name);
        let index_name = quote(&idx.name);

        let mut col_parts: Vec<String> = Vec::new();
        for col in &idx.columns {
            match col {
                IndexColumnEntry::Name(n) => col_parts.push(quote(n)),
                IndexColumnEntry::Spec { name, direction, .. } => {
                    let dir = direction
                        .as_deref()
                        .map(|d| format!(" {}", d.to_uppercase()))
                        .unwrap_or_default();
                    col_parts.push(format!("{}{}", quote(name), dir));
                }
                IndexColumnEntry::Expression { expression } => col_parts.push(expression.clone()),
            }
        }
        let method = idx.method.as_deref().unwrap_or("btree");
        let unique = if idx.unique { "UNIQUE " } else { "" };
        let include: String = if idx.include.is_empty() {
            String::new()
        } else {
            let inc: Vec<String> = idx.include.iter().map(|s| quote(s)).collect();
            format!(" INCLUDE ({})", inc.join(", "))
        };
        let where_clause: String = idx
            .where_
            .as_ref()
            .map(|w| format!(" WHERE {}", w))
            .unwrap_or_default();

        let sql = format!(
            "CREATE {}INDEX IF NOT EXISTS {} ON {} USING {} ({}){}{}",
            unique,
            index_name,
            full_table,
            method,
            col_parts.join(", "),
            include,
            where_clause
        );
        let _ = sqlx::query(&sql).execute(pool).await;
    }

    for rel in &config.relationships {
        let from_schema = schemas_by_id.get(rel.from_schema_id.as_str()).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "schema",
                id: rel.from_schema_id.clone(),
            })
        })?;
        let from_table = tables_by_id.get(rel.from_table_id.as_str()).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "table",
                id: rel.from_table_id.clone(),
            })
        })?;
        let to_schema = schemas_by_id.get(rel.to_schema_id.as_str()).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "schema",
                id: rel.to_schema_id.clone(),
            })
        })?;
        let to_table = tables_by_id.get(rel.to_table_id.as_str()).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "table",
                id: rel.to_table_id.clone(),
            })
        })?;

        let from_col = config
            .columns
            .iter()
            .find(|c| c.id == rel.from_column_id)
            .map(|c| c.name.as_str())
            .ok_or_else(|| AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "column",
                id: rel.from_column_id.clone(),
            }))?;
        let to_col = config
            .columns
            .iter()
            .find(|c| c.id == rel.to_column_id)
            .map(|c| c.name.as_str())
            .ok_or_else(|| AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "column",
                id: rel.to_column_id.clone(),
            }))?;

        let from_full = format!("{}.{}", quote(&from_schema.name), quote(&from_table.name));
        let to_full = format!("{}.{}", quote(&to_schema.name), quote(&to_table.name));
        let constraint_name = rel
            .name
            .as_deref()
            .unwrap_or(&rel.id);
        let on_update = rel
            .on_update
            .as_deref()
            .unwrap_or("NO ACTION");
        let on_delete = rel
            .on_delete
            .as_deref()
            .unwrap_or("NO ACTION");

        let sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}) ON UPDATE {} ON DELETE {}",
            from_full,
            quote(constraint_name),
            quote(from_col),
            to_full,
            quote(to_col),
            on_update,
            on_delete
        );
        let _ = sqlx::query(&sql).execute(pool).await;
    }

    Ok(())
}

fn type_str(ty: &ColumnTypeConfig, _schemas_by_id: &HashMap<&str, &SchemaConfig>) -> String {
    match ty {
        ColumnTypeConfig::Simple(s) => {
            if s.contains('.') {
                s.clone()
            } else {
                s.clone()
            }
        }
        ColumnTypeConfig::Parameterized { name, params } => {
            let p = params
                .as_ref()
                .map(|v| v.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(", "))
                .unwrap_or_default();
            if p.is_empty() {
                name.clone()
            } else {
                format!("{}({})", name, p)
            }
        }
    }
}
