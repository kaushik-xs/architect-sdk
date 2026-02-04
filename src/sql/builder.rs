//! Builds parameterized INSERT, SELECT, UPDATE, DELETE from resolved entity.

use crate::config::ResolvedEntity;
use serde_json::Value;
use std::collections::HashMap;

/// Quote identifier for PostgreSQL (safe: only from config).
fn quoted(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Full qualified table name.
fn qualified_table(schema: &str, table: &str) -> String {
    format!("{}.{}", quoted(schema), quoted(table))
}

pub struct QueryBuf {
    pub sql: String,
    pub params: Vec<Value>,
}

impl QueryBuf {
    fn new() -> Self {
        QueryBuf {
            sql: String::new(),
            params: Vec::new(),
        }
    }

    fn push_param(&mut self, v: Value) -> u32 {
        let n = self.params.len() as u32 + 1;
        self.params.push(v);
        n
    }
}

/// SELECT list: each column as-is, except custom enum (schema.typename) columns as col::text so sqlx returns String.
fn select_column_list(entity: &ResolvedEntity) -> String {
    entity
        .columns
        .iter()
        .map(|c| {
            let q = quoted(&c.name);
            if c.pg_type.as_deref().map_or(false, |t| t.contains('.')) {
                format!("{}::text", q)
            } else {
                q
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// SELECT by primary key (single column PK only). Caller adds id as sole param.
pub fn select_by_id(entity: &ResolvedEntity) -> QueryBuf {
    let mut q = QueryBuf::new();
    let table = qualified_table(&entity.schema_name, &entity.table_name);
    let pk = &entity.pk_columns[0];
    let cols = select_column_list(entity);
    q.sql = format!("SELECT {} FROM {} WHERE {} = $1", cols, table, quoted(pk));
    q
}

/// SELECT list with optional filters (exact match per column), ORDER BY pk, optional LIMIT/OFFSET.
/// filters: only (col, value) where col is in entity.columns; params bound in filter order.
pub fn select_list(
    entity: &ResolvedEntity,
    filters: &[(String, Value)],
    limit: Option<u32>,
    offset: Option<u32>,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let col_names: std::collections::HashSet<&str> = entity.columns.iter().map(|c| c.name.as_str()).collect();
    let table = qualified_table(&entity.schema_name, &entity.table_name);
    let pk = &entity.pk_columns[0];

    let mut where_parts = Vec::new();
    for (col, val) in filters {
        if col_names.contains(col.as_str()) {
            let param_num = q.push_param(val.clone());
            let ph = entity
                .columns
                .iter()
                .find(|c| c.name == *col)
                .and_then(|c| c.pg_type.as_deref())
                .map(|t| format!("${}::{}", param_num, t))
                .unwrap_or_else(|| format!("${}", param_num));
            where_parts.push(format!("{} = {}", quoted(col), ph));
        }
    }

    let where_clause = if where_parts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_parts.join(" AND "))
    };
    let order_clause = format!(" ORDER BY {}", quoted(pk));
    let limit_clause = limit.map(|n| format!(" LIMIT {}", n.min(1000))).unwrap_or_default();
    let offset_clause = offset.map(|n| format!(" OFFSET {}", n)).unwrap_or_default();
    let cols = select_column_list(entity);
    q.sql = format!(
        "SELECT {} FROM {}{}{}{}{}",
        cols,
        table,
        where_clause,
        order_clause,
        limit_clause,
        offset_clause
    );
    q
}

/// SELECT * FROM entity WHERE column IN ($1, $2, ...) ORDER BY pk. Used for batch-fetching related rows (to_many or to_one by key).
pub fn select_by_column_in(
    entity: &ResolvedEntity,
    column_name: &str,
    values: &[Value],
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let table = qualified_table(&entity.schema_name, &entity.table_name);
    let pk = &entity.pk_columns[0];
    if values.is_empty() {
        let cols = select_column_list(entity);
        q.sql = format!("SELECT {} FROM {} WHERE 1 = 0", cols, table);
        return q;
    }
    let placeholders: Vec<String> = values
        .iter()
        .map(|v| {
            let n = q.push_param(v.clone());
            entity
                .columns
                .iter()
                .find(|c| c.name == column_name)
                .and_then(|c| c.pg_type.as_deref())
                .map(|t| format!("${}::{}", n, t))
                .unwrap_or_else(|| format!("${}", n))
        })
        .collect();
    let cols = select_column_list(entity);
    q.sql = format!(
        "SELECT {} FROM {} WHERE {} IN ({}) ORDER BY {}",
        cols,
        table,
        quoted(column_name),
        placeholders.join(", "),
        quoted(pk)
    );
    q
}

/// INSERT: columns and placeholders from entity; values from body. Excludes PK if has_default.
/// Omits columns with DB default when body does not provide a value (so DB uses default).
/// Uses SQL cast (e.g. $n::timestamptz) for timestamp columns so string values bind correctly.
pub fn insert(
    entity: &ResolvedEntity,
    body: &HashMap<String, Value>,
    include_pk: bool,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let table = qualified_table(&entity.schema_name, &entity.table_name);
    let mut cols = Vec::new();
    let mut placeholders = Vec::new();
    for c in &entity.columns {
        let name = &c.name;
        if c.pk_type.is_some() && !include_pk {
            continue;
        }
        let val = body.get(name).cloned();
        if val.is_none() && c.has_default {
            continue;
        }
        let val = val.unwrap_or(Value::Null);
        let param_num = q.push_param(val);
        let ph = c
            .pg_type
            .as_deref()
            .map(|t| format!("${}::{}", param_num, t))
            .unwrap_or_else(|| format!("${}", param_num));
        cols.push(quoted(name));
        placeholders.push(ph);
    }
    let returning = select_column_list(entity);
    q.sql = format!(
        "INSERT INTO {} ({}) VALUES ({}) RETURNING {}",
        table,
        cols.join(", "),
        placeholders.join(", "),
        returning
    );
    q
}

/// UPDATE by id: SET only columns present in body (and in entity columns).
/// Uses SQL cast for timestamp columns so string values bind correctly.
pub fn update(entity: &ResolvedEntity, id: &Value, body: &HashMap<String, Value>) -> QueryBuf {
    let mut q = QueryBuf::new();
    let table = qualified_table(&entity.schema_name, &entity.table_name);
    let pk = &entity.pk_columns[0];
    let col_by_name: std::collections::HashMap<_, _> = entity.columns.iter().map(|c| (c.name.as_str(), c)).collect();
    let mut sets = Vec::new();
    for (k, v) in body {
        if *k == *pk {
            continue;
        }
        let Some(c) = col_by_name.get(k.as_str()) else { continue };
        let param_num = q.push_param(v.clone());
        let rhs = c
            .pg_type
            .as_deref()
            .map(|t| format!("${}::{}", param_num, t))
            .unwrap_or_else(|| format!("${}", param_num));
        sets.push(format!("{} = {}", quoted(k), rhs));
    }
    sets.push(format!("{} = NOW()", quoted("updated_at")));
    if sets.is_empty() {
        let cols = select_column_list(entity);
        q.sql = format!("SELECT {} FROM {} WHERE {} = $1", cols, table, quoted(pk));
        q.params.push(id.clone());
        return q;
    }
    let set_clause = sets.join(", ");
    let id_param = q.params.len() + 1;
    q.params.push(id.clone());
    let returning = select_column_list(entity);
    q.sql = format!(
        "UPDATE {} SET {} WHERE {} = ${} RETURNING {}",
        table,
        set_clause,
        quoted(pk),
        id_param,
        returning
    );
    q
}

/// DELETE by id.
pub fn delete(entity: &ResolvedEntity) -> QueryBuf {
    let mut q = QueryBuf::new();
    let table = qualified_table(&entity.schema_name, &entity.table_name);
    let pk = &entity.pk_columns[0];
    let returning = select_column_list(entity);
    q.params.push(Value::Null);
    q.sql = format!("DELETE FROM {} WHERE {} = $1 RETURNING {}", table, quoted(pk), returning);
    q
}
