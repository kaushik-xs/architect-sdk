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

/// INSERT: columns and placeholders from entity; values from body. Excludes PK if has_default.
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
        let val = body.get(name).cloned().unwrap_or(Value::Null);
        cols.push(quoted(name));
        placeholders.push(format!("${}", q.push_param(val)));
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
pub fn update(entity: &ResolvedEntity, id: &Value, body: &HashMap<String, Value>) -> QueryBuf {
    let mut q = QueryBuf::new();
    let table = qualified_table(&entity.schema_name, &entity.table_name);
    let pk = &entity.pk_columns[0];
    let col_names: std::collections::HashSet<_> = entity.columns.iter().map(|c| c.name.as_str()).collect();
    let mut sets = Vec::new();
    for (k, v) in body {
        if col_names.contains(k.as_str()) && *k != *pk {
            sets.push(format!("{} = ${}", quoted(k), q.push_param(v.clone())));
        }
    }
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
