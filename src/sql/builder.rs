//! Builds parameterized INSERT, SELECT, UPDATE, DELETE from resolved entity.

use crate::config::{IncludeDirection, ResolvedEntity};
use serde_json::Value;
use std::collections::HashMap;

/// Describes one include for single-query list: name, direction, related entity, our key column, their key column.
pub struct IncludeSelect<'a> {
    pub name: &'a str,
    pub direction: IncludeDirection,
    pub related: &'a ResolvedEntity,
    pub our_key: &'a str,
    pub their_key: &'a str,
}

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

/// SELECT list: each column as-is, except custom enum (schema.typename) as col::text and numeric as col::text so sqlx returns String.
fn select_column_list(entity: &ResolvedEntity) -> String {
    entity
        .columns
        .iter()
        .map(|c| {
            let q = quoted(&c.name);
            let pg_type = c.pg_type.as_deref().unwrap_or("");
            if pg_type.contains('.') {
                format!("{}::text", q)
            } else if pg_type == "numeric" {
                format!("{}::text", q)
            } else {
                q
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Resolve schema: override if present, else entity's schema.
fn resolve_schema<'a>(entity: &'a ResolvedEntity, schema_override: Option<&'a str>) -> &'a str {
    schema_override.unwrap_or(&entity.schema_name)
}

/// SELECT by primary key (single column PK only). Caller adds id as sole param.
pub fn select_by_id(entity: &ResolvedEntity, schema_override: Option<&str>) -> QueryBuf {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let table = qualified_table(schema, &entity.table_name);
    let pk = &entity.pk_columns[0];
    let cols = select_column_list(entity);
    q.sql = format!("SELECT {} FROM {} WHERE {} = $1", cols, table, quoted(pk));
    q
}

/// SELECT list with includes in a single query: main table aliased as "main", each include as a scalar subquery (json_agg for to_many, row_to_json for to_one).
pub fn select_list_with_includes(
    entity: &ResolvedEntity,
    filters: &[(String, Value)],
    limit: Option<u32>,
    offset: Option<u32>,
    includes: &[IncludeSelect<'_>],
    schema_override: Option<&str>,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let col_names: std::collections::HashSet<&str> = entity.columns.iter().map(|c| c.name.as_str()).collect();
    let schema = resolve_schema(entity, schema_override);
    let table = qualified_table(schema, &entity.table_name);
    let pk = &entity.pk_columns[0];
    const MAIN_ALIAS: &str = "main";

    let main_cols: Vec<String> = entity
        .columns
        .iter()
        .map(|c| {
            let q = quoted(&c.name);
            let pg_type = c.pg_type.as_deref().unwrap_or("");
            let expr = if pg_type.contains('.') || pg_type == "numeric" {
                format!("{}.{}::text", MAIN_ALIAS, q)
            } else {
                format!("{}.{}", MAIN_ALIAS, q)
            };
            format!("{} AS {}", expr, q)
        })
        .collect();

    let mut select_parts = main_cols;
    for inc in includes {
        let rel_schema = resolve_schema(inc.related, schema_override);
        let rel_table = qualified_table(rel_schema, &inc.related.table_name);
        let rel_cols = select_column_list(inc.related);
        let sub_from = format!("{} WHERE {} = {}.{}", rel_table, quoted(inc.their_key), MAIN_ALIAS, quoted(inc.our_key));
        let subquery = match inc.direction {
            IncludeDirection::ToOne => format!(
                "(SELECT row_to_json(sub) FROM (SELECT {} FROM {}) sub)",
                rel_cols, sub_from
            ),
            IncludeDirection::ToMany => format!(
                "(SELECT COALESCE(json_agg(row_to_json(sub)), '[]'::json) FROM (SELECT {} FROM {}) sub)",
                rel_cols, sub_from
            ),
        };
        select_parts.push(format!("{} AS {}", subquery, quoted(inc.name)));
    }

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
            where_parts.push(format!("{}.{} = {}", MAIN_ALIAS, quoted(col), ph));
        }
    }
    let where_clause = if where_parts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_parts.join(" AND "))
    };
    let order_clause = format!(" ORDER BY {}.{}", MAIN_ALIAS, quoted(pk));
    let limit_clause = limit.map(|n| format!(" LIMIT {}", n.min(1000))).unwrap_or_default();
    let offset_clause = offset.map(|n| format!(" OFFSET {}", n)).unwrap_or_default();

    q.sql = format!(
        "SELECT {} FROM {} {}{}{}{}{}",
        select_parts.join(", "),
        table,
        MAIN_ALIAS,
        where_clause,
        order_clause,
        limit_clause,
        offset_clause
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
    schema_override: Option<&str>,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let col_names: std::collections::HashSet<&str> = entity.columns.iter().map(|c| c.name.as_str()).collect();
    let schema = resolve_schema(entity, schema_override);
    let table = qualified_table(schema, &entity.table_name);
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
    schema_override: Option<&str>,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let table = qualified_table(schema, &entity.table_name);
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
/// When `rls_tenant_id` is Some, appends tenant_id column and value (for RLS strategy).
pub fn insert(
    entity: &ResolvedEntity,
    body: &HashMap<String, Value>,
    include_pk: bool,
    schema_override: Option<&str>,
    rls_tenant_id: Option<&str>,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let table = qualified_table(schema, &entity.table_name);
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
    if let Some(tid) = rls_tenant_id {
        let param_num = q.push_param(Value::String(tid.to_string()));
        cols.push(quoted("tenant_id"));
        placeholders.push(format!("${}", param_num));
    }
    let mut returning = select_column_list(entity);
    if rls_tenant_id.is_some() {
        returning.push_str(", ");
        returning.push_str(&quoted("tenant_id"));
    }
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
pub fn update(
    entity: &ResolvedEntity,
    id: &Value,
    body: &HashMap<String, Value>,
    schema_override: Option<&str>,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let table = qualified_table(schema, &entity.table_name);
    let pk = &entity.pk_columns[0];
    let col_by_name: std::collections::HashMap<_, _> = entity.columns.iter().map(|c| (c.name.as_str(), c)).collect();
    let mut sets = Vec::new();
    for (k, v) in body {
        if *k == *pk {
            continue;
        }
        if k == "tenant_id" {
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
pub fn delete(entity: &ResolvedEntity, schema_override: Option<&str>) -> QueryBuf {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let table = qualified_table(schema, &entity.table_name);
    let pk = &entity.pk_columns[0];
    let returning = select_column_list(entity);
    q.params.push(Value::Null);
    q.sql = format!("DELETE FROM {} WHERE {} = $1 RETURNING {}", table, quoted(pk), returning);
    q
}
