//! Physical-database introspection.
//!
//! Migration plans are computed by diffing two package configs — they never look at the
//! database. When the physical database has drifted from what the old config describes (a
//! partially applied upgrade, a tenant database provisioned from a newer template, columns added
//! by RLS reconciliation or by hand), replaying such a plan fails on statements whose effect is
//! already present: `ADD COLUMN "project_id"` on a table that already has it.
//!
//! [`DbSnapshot`] captures what actually exists so the migration executor can skip those steps
//! instead of aborting. Everything here is best-effort: if introspection fails the snapshot is
//! left empty ([`DbSnapshot::introspected`] stays `false`) and every step executes as before.

use std::collections::{HashMap, HashSet};

use sqlx::Row;

use super::dialect::Dialect;
use super::pool::Pool;

/// What the database reports about one column.
#[derive(Debug, Clone)]
pub struct ColumnFacts {
    /// Dialect-reported type string (e.g. `text`, `character varying(64)`), for diagnostics only.
    pub data_type: String,
    pub nullable: bool,
    pub has_default: bool,
}

/// Physical state of one or more schemas: which tables, columns, indexes and constraints exist.
///
/// Identifier comparison is case-insensitive. The SDK generates every identifier from config and
/// never creates two objects in one table whose names differ only by case, so folding case is
/// safe here and keeps MySQL (case-insensitive column names) correct.
#[derive(Debug, Clone, Default)]
pub struct DbSnapshot {
    /// `schema\u{1}table` → column name → facts. All keys lowercased.
    columns: HashMap<String, HashMap<String, ColumnFacts>>,
    /// `schema\u{1}index`, lowercased.
    indexes: HashSet<String>,
    /// `schema\u{1}table\u{1}constraint`, lowercased.
    constraints: HashSet<String>,
    /// Whether at least one introspection query succeeded. When `false`, absence of an object in
    /// this snapshot proves nothing and callers must not skip steps on that basis.
    pub introspected: bool,
    /// Whether index names were readable for this dialect.
    pub indexes_known: bool,
    /// Whether constraint names were readable for this dialect.
    pub constraints_known: bool,
}

fn table_key(schema: &str, table: &str) -> String {
    format!("{}\u{1}{}", schema.to_lowercase(), table.to_lowercase())
}

impl DbSnapshot {
    /// True when this table was seen in the database.
    pub fn has_table(&self, schema: &str, table: &str) -> bool {
        self.columns.contains_key(&table_key(schema, table))
    }

    pub fn has_column(&self, schema: &str, table: &str, column: &str) -> bool {
        self.column(schema, table, column).is_some()
    }

    pub fn column(&self, schema: &str, table: &str, column: &str) -> Option<&ColumnFacts> {
        self.columns
            .get(&table_key(schema, table))
            .and_then(|cols| cols.get(&column.to_lowercase()))
    }

    pub fn has_index(&self, schema: &str, index: &str) -> bool {
        self.indexes.contains(&format!(
            "{}\u{1}{}",
            schema.to_lowercase(),
            index.to_lowercase()
        ))
    }

    pub fn has_constraint(&self, schema: &str, table: &str, constraint: &str) -> bool {
        self.constraints.contains(&format!(
            "{}\u{1}{}",
            table_key(schema, table),
            constraint.to_lowercase()
        ))
    }

    // ── Mutators — keep the snapshot in step with DDL as it is executed ───────

    pub fn add_column(&mut self, schema: &str, table: &str, column: &str, facts: ColumnFacts) {
        self.columns
            .entry(table_key(schema, table))
            .or_default()
            .insert(column.to_lowercase(), facts);
    }

    pub fn add_table(&mut self, schema: &str, table: &str) {
        self.columns.entry(table_key(schema, table)).or_default();
    }

    pub fn remove_column(&mut self, schema: &str, table: &str, column: &str) {
        if let Some(cols) = self.columns.get_mut(&table_key(schema, table)) {
            cols.remove(&column.to_lowercase());
        }
    }

    pub fn rename_column(&mut self, schema: &str, table: &str, from: &str, to: &str) {
        if let Some(cols) = self.columns.get_mut(&table_key(schema, table)) {
            if let Some(facts) = cols.remove(&from.to_lowercase()) {
                cols.insert(to.to_lowercase(), facts);
            }
        }
    }

    /// Update a column's nullability, if the column is known.
    pub fn set_nullable(&mut self, schema: &str, table: &str, column: &str, nullable: bool) {
        if let Some(cols) = self.columns.get_mut(&table_key(schema, table)) {
            if let Some(f) = cols.get_mut(&column.to_lowercase()) {
                f.nullable = nullable;
            }
        }
    }

    /// Update whether a column carries a DEFAULT, if the column is known.
    pub fn set_has_default(&mut self, schema: &str, table: &str, column: &str, has_default: bool) {
        if let Some(cols) = self.columns.get_mut(&table_key(schema, table)) {
            if let Some(f) = cols.get_mut(&column.to_lowercase()) {
                f.has_default = has_default;
            }
        }
    }

    pub fn add_index(&mut self, schema: &str, index: &str) {
        self.indexes.insert(format!(
            "{}\u{1}{}",
            schema.to_lowercase(),
            index.to_lowercase()
        ));
    }

    pub fn remove_index(&mut self, schema: &str, index: &str) {
        self.indexes.remove(&format!(
            "{}\u{1}{}",
            schema.to_lowercase(),
            index.to_lowercase()
        ));
    }

    pub fn add_constraint(&mut self, schema: &str, table: &str, constraint: &str) {
        self.constraints.insert(format!(
            "{}\u{1}{}",
            table_key(schema, table),
            constraint.to_lowercase()
        ));
    }

    pub fn remove_constraint(&mut self, schema: &str, table: &str, constraint: &str) {
        self.constraints.remove(&format!(
            "{}\u{1}{}",
            table_key(schema, table),
            constraint.to_lowercase()
        ));
    }
}

/// Read the physical state of `schemas` from `pool`.
///
/// Never fails: a query that errors (missing schema, no privileges, unsupported catalog) is
/// logged and leaves that part of the snapshot empty.
pub async fn introspect(pool: &Pool, dialect: &dyn Dialect, schemas: &[String]) -> DbSnapshot {
    let mut snap = DbSnapshot {
        indexes_known: true,
        constraints_known: true,
        ..Default::default()
    };

    for schema in schemas {
        let sql = dialect.introspect_columns_sql(schema);
        match sqlx::query(&sql).fetch_all(pool).await {
            Ok(rows) => {
                snap.introspected = true;
                for row in rows {
                    let (table, column) =
                        match (row.try_get::<String, _>(0), row.try_get::<String, _>(1)) {
                            (Ok(t), Ok(c)) => (t, c),
                            _ => continue,
                        };
                    let facts = ColumnFacts {
                        data_type: row.try_get::<String, _>(2).unwrap_or_default(),
                        nullable: row
                            .try_get::<String, _>(3)
                            .map(|v| v.eq_ignore_ascii_case("YES"))
                            .unwrap_or(true),
                        has_default: row
                            .try_get::<String, _>(4)
                            .map(|v| v.eq_ignore_ascii_case("YES"))
                            .unwrap_or(false),
                    };
                    snap.add_column(schema, &table, &column, facts);
                }
            }
            Err(e) => {
                tracing::warn!(schema = %schema, error = %e, "column introspection failed — migration steps for this schema will not be skipped");
            }
        }

        match dialect.introspect_indexes_sql(schema) {
            Some(sql) => match sqlx::query(&sql).fetch_all(pool).await {
                Ok(rows) => {
                    for row in rows {
                        if let Ok(name) = row.try_get::<String, _>(0) {
                            snap.add_index(schema, &name);
                        }
                    }
                }
                Err(e) => {
                    snap.indexes_known = false;
                    tracing::warn!(schema = %schema, error = %e, "index introspection failed");
                }
            },
            None => snap.indexes_known = false,
        }

        match dialect.introspect_constraints_sql(schema) {
            Some(sql) => match sqlx::query(&sql).fetch_all(pool).await {
                Ok(rows) => {
                    for row in rows {
                        match (row.try_get::<String, _>(0), row.try_get::<String, _>(1)) {
                            (Ok(table), Ok(name)) => snap.add_constraint(schema, &table, &name),
                            _ => continue,
                        }
                    }
                }
                Err(e) => {
                    snap.constraints_known = false;
                    tracing::warn!(schema = %schema, error = %e, "constraint introspection failed");
                }
            },
            None => snap.constraints_known = false,
        }
    }

    snap
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts() -> ColumnFacts {
        ColumnFacts {
            data_type: "text".into(),
            nullable: true,
            has_default: false,
        }
    }

    #[test]
    fn lookups_are_case_insensitive() {
        let mut snap = DbSnapshot::default();
        snap.add_column("App", "Orders", "ProjectId", facts());
        assert!(snap.has_column("app", "orders", "projectid"));
        assert!(snap.has_table("APP", "ORDERS"));
        assert!(!snap.has_column("app", "orders", "other"));
    }

    #[test]
    fn empty_snapshot_knows_nothing() {
        let snap = DbSnapshot::default();
        assert!(!snap.introspected);
        assert!(!snap.has_table("app", "orders"));
    }

    #[test]
    fn rename_moves_facts_to_the_new_name() {
        let mut snap = DbSnapshot::default();
        snap.add_column("app", "orders", "note", facts());
        snap.rename_column("app", "orders", "note", "remark");
        assert!(!snap.has_column("app", "orders", "note"));
        assert!(snap.has_column("app", "orders", "remark"));
    }

    #[test]
    fn index_and_constraint_membership() {
        let mut snap = DbSnapshot::default();
        snap.add_index("app", "orders_user_idx");
        snap.add_constraint("app", "orders", "fk_orders_user");
        assert!(snap.has_index("app", "ORDERS_USER_IDX"));
        assert!(snap.has_constraint("APP", "Orders", "fk_orders_user"));
        assert!(!snap.has_constraint("app", "users", "fk_orders_user"));
    }
}
