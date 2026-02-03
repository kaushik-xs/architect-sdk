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

/// SELECT by primary key (single column PK only). Caller adds id as sole param.
pub fn select_by_id(entity: &ResolvedEntity) -> QueryBuf {
    let mut q = QueryBuf::new();
    let cols: Vec<String> = entity.columns.iter().map(|c| quoted(&c.name)).collect();
    let table = qualified_table(&entity.schema_name, &entity.table_name);
    let pk = &entity.pk_columns[0];
    q.sql = format!(
        "SELECT {} FROM {} WHERE {} = $1",
        cols.join(", "),
        table,
        quoted(pk)
    );
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
    let cols: Vec<String> = entity.columns.iter().map(|c| quoted(&c.name)).collect();
    let pk = &entity.pk_columns[0];

    let mut where_parts = Vec::new();
    for (col, val) in filters {
        if col_names.contains(col.as_str()) {
            let param_num = q.push_param(val.clone());
            let ph = match entity.columns.iter().find(|c| c.name == *col) {
                Some(c) => match c.pg_type.as_deref() {
                    Some("timestamptz") | Some("timestamp") | Some("date") => {
                        format!("${}::{}", param_num, c.pg_type.as_deref().unwrap())
                    }
                    _ => format!("${}", param_num),
                },
                None => format!("${}", param_num),
            };
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

    q.sql = format!(
        "SELECT {} FROM {}{}{}{}{}",
        cols.join(", "),
        table,
        where_clause,
        order_clause,
        limit_clause,
        offset_clause
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
        let ph = match c.pg_type.as_deref() {
            Some("timestamptz") | Some("timestamp") | Some("date") => format!("${}::{}", param_num, c.pg_type.as_deref().unwrap()),
            _ => format!("${}", param_num),
        };
        cols.push(quoted(name));
        placeholders.push(ph);
    }
    q.sql = format!(
        "INSERT INTO {} ({}) VALUES ({}) RETURNING *",
        table,
        cols.join(", "),
        placeholders.join(", ")
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
        let rhs = match c.pg_type.as_deref() {
            Some("timestamptz") | Some("timestamp") | Some("date") => format!("${}::{}", param_num, c.pg_type.as_deref().unwrap()),
            _ => format!("${}", param_num),
        };
        sets.push(format!("{} = {}", quoted(k), rhs));
    }
    sets.push(format!("{} = NOW()", quoted("updated_at")));
    if sets.is_empty() {
        q.sql = format!("SELECT * FROM {} WHERE {} = $1", table, quoted(pk));
        q.params.push(id.clone());
        return q;
    }
    let set_clause = sets.join(", ");
    let id_param = q.params.len() + 1;
    q.params.push(id.clone());
    q.sql = format!(
        "UPDATE {} SET {} WHERE {} = ${} RETURNING *",
        table,
        set_clause,
        quoted(pk),
        id_param
    );
    q
}

/// DELETE by id.
pub fn delete(entity: &ResolvedEntity) -> QueryBuf {
    let mut q = QueryBuf::new();
    let table = qualified_table(&entity.schema_name, &entity.table_name);
    let pk = &entity.pk_columns[0];
    q.params.push(Value::Null);
    q.sql = format!("DELETE FROM {} WHERE {} = $1 RETURNING *", table, quoted(pk));
    q
}
