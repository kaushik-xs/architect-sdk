//! Generic CRUD execution against PostgreSQL.

use crate::config::ResolvedEntity;
use crate::error::AppError;
use crate::sql::{archive, delete, insert, select_by_column_in, select_by_id, select_list, select_list_with_includes, unarchive, IncludeSelect, update, FilterNode, SortSpec, PgBindValue, QueryBuf};
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashMap;

/// Execution target: either a pool (for database/schema strategy) or a single connection (for RLS, with SET LOCAL already applied).
pub enum TenantExecutor<'a> {
    Pool(&'a PgPool),
    Conn(&'a mut sqlx::PgConnection),
}

pub struct CrudService;

impl CrudService {
    /// List rows with optional RSQL filter and sort, limit (default 100, max 1000), offset (default 0).
    /// `filter_includes` supplies related-entity metadata for dotted-field EXISTS filters; pass `&[]` when unused.
    pub async fn list<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        filter: Option<&FilterNode>,
        sort: &[SortSpec],
        limit: Option<u32>,
        offset: Option<u32>,
        filter_includes: &[IncludeSelect<'_>],
        schema_override: Option<&str>,
    ) -> Result<Vec<Value>, AppError> {
        const DEFAULT_LIMIT: u32 = 100;
        let limit = limit.unwrap_or(DEFAULT_LIMIT).min(1000);
        let offset = offset.unwrap_or(0);
        let q = select_list(entity, filter, sort, Some(limit), Some(offset), filter_includes, schema_override)?;
        Self::query_many_exec(executor, &q.sql, &q.params).await
    }

    /// List rows with includes in a single query (scalar subqueries with json_agg/row_to_json). Returns rows with include keys already set (JSON).
    /// `includes` drives scalar subqueries for response data; `filter_includes` is the superset used for EXISTS generation.
    pub async fn list_with_includes<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        filter: Option<&FilterNode>,
        sort: &[SortSpec],
        limit: Option<u32>,
        offset: Option<u32>,
        includes: &[IncludeSelect<'_>],
        filter_includes: &[IncludeSelect<'_>],
        schema_override: Option<&str>,
    ) -> Result<Vec<Value>, AppError> {
        const DEFAULT_LIMIT: u32 = 100;
        let limit = limit.unwrap_or(DEFAULT_LIMIT).min(1000);
        let offset = offset.unwrap_or(0);
        let q = select_list_with_includes(entity, filter, sort, Some(limit), Some(offset), includes, filter_includes, schema_override)?;
        Self::query_many_exec(executor, &q.sql, &q.params).await
    }

    /// Fetch one row by primary key. Returns JSON object or None.
    pub async fn read<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        id: &Value,
        schema_override: Option<&str>,
    ) -> Result<Option<Value>, AppError> {
        let q = select_by_id(entity, schema_override);
        Self::query_one_exec(executor, &q.sql, &[id.clone()]).await
    }

    /// Fetch rows from entity where column IN (values). Used for batch-loading related rows.
    pub async fn fetch_where_column_in<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        column_name: &str,
        values: &[Value],
        schema_override: Option<&str>,
    ) -> Result<Vec<Value>, AppError> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let q = select_by_column_in(entity, column_name, values, schema_override);
        Self::query_many_exec(executor, &q.sql, &q.params).await
    }

    /// Insert one row; body may include or omit PK (if has default). Returns created row.
    /// When rls_tenant_id is Some (RLS strategy), tenant_id column is set automatically.
    /// When caller_user_id is Some, created_by is set to that value.
    pub async fn create<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        body: &HashMap<String, Value>,
        schema_override: Option<&str>,
        rls_tenant_id: Option<&str>,
        caller_user_id: Option<&str>,
    ) -> Result<Value, AppError> {
        let include_pk = body.contains_key(&entity.pk_columns[0]);
        let q = insert(entity, body, include_pk, schema_override, rls_tenant_id, caller_user_id);
        let row = Self::execute_returning_one_exec(executor, &q).await?
            .ok_or_else(|| AppError::Db(sqlx::Error::RowNotFound))?;
        if entity.audit_log {
            Self::insert_audit(executor, entity, "create", &row, None, caller_user_id, schema_override).await?;
        }
        Ok(row)
    }

    /// Update one row by id. Returns updated row.
    /// When caller_user_id is Some, updated_by is set to that value.
    pub async fn update<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        id: &Value,
        body: &HashMap<String, Value>,
        schema_override: Option<&str>,
        caller_user_id: Option<&str>,
    ) -> Result<Option<Value>, AppError> {
        let pre_row = if entity.audit_log {
            let q = select_by_id(entity, schema_override);
            Self::query_one_exec(executor, &q.sql, &[id.clone()]).await?
        } else {
            None
        };
        let q = update(entity, id, body, schema_override, caller_user_id);
        let result = Self::execute_returning_one_exec(executor, &q).await?;
        if entity.audit_log {
            if let Some(ref post_row) = result {
                Self::insert_audit(executor, entity, "update", post_row, pre_row.as_ref(), caller_user_id, schema_override).await?;
            }
        }
        Ok(result)
    }

    /// Delete one row by id. Returns deleted row or None.
    /// When caller_user_id is Some, audit_by is set on the audit record.
    pub async fn delete<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        id: &Value,
        schema_override: Option<&str>,
        caller_user_id: Option<&str>,
    ) -> Result<Option<Value>, AppError> {
        let q = delete(entity, schema_override);
        let result = Self::execute_returning_one_with_params_exec(executor, &q.sql, &[id.clone()]).await?;
        if entity.audit_log {
            if let Some(ref deleted_row) = result {
                Self::insert_audit(executor, entity, "delete", deleted_row, None, caller_user_id, schema_override).await?;
            }
        }
        Ok(result)
    }

    /// Archive one row by id: stamps archive_field with NOW() if it is currently NULL.
    /// Returns the updated row, or None if the record was not found or already archived.
    pub async fn archive<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        archive_field: &str,
        id: &Value,
        schema_override: Option<&str>,
    ) -> Result<Option<Value>, AppError> {
        let q = archive(entity, archive_field, schema_override);
        Self::execute_returning_one_with_params_exec(executor, &q.sql, &[id.clone()]).await
    }

    /// Unarchive one row by id: clears archive_field (sets to NULL) if it is currently NOT NULL.
    /// Returns the updated row, or None if the record was not found or not archived.
    pub async fn unarchive<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        archive_field: &str,
        id: &Value,
        schema_override: Option<&str>,
    ) -> Result<Option<Value>, AppError> {
        let q = unarchive(entity, archive_field, schema_override);
        Self::execute_returning_one_with_params_exec(executor, &q.sql, &[id.clone()]).await
    }

    /// Bulk create in a transaction (when using pool) or on the same connection (when using conn). Returns vec of created rows.
    /// When rls_tenant_id is Some (RLS strategy), tenant_id column is set automatically on each row.
    /// When caller_user_id is Some, created_by is set on each row.
    pub async fn bulk_create<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        items: &[HashMap<String, Value>],
        schema_override: Option<&str>,
        rls_tenant_id: Option<&str>,
        caller_user_id: Option<&str>,
    ) -> Result<Vec<Value>, AppError> {
        const BULK_LIMIT: usize = 100;
        if items.len() > BULK_LIMIT {
            return Err(AppError::BadRequest(format!(
                "bulk create limited to {} items",
                BULK_LIMIT
            )));
        }
        let mut out = Vec::with_capacity(items.len());
        match executor {
            TenantExecutor::Pool(pool) => {
                let mut tx = pool.begin().await?;
                for body in items {
                    let include_pk = body.contains_key(&entity.pk_columns[0]);
                    let q = insert(entity, body, include_pk, schema_override, rls_tenant_id, caller_user_id);
                    let row = Self::execute_returning_one_tx(&mut tx, &q).await?.unwrap_or(Value::Null);
                    out.push(row);
                }
                tx.commit().await?;
            }
            TenantExecutor::Conn(conn) => {
                for body in items {
                    let include_pk = body.contains_key(&entity.pk_columns[0]);
                    let q = insert(entity, body, include_pk, schema_override, rls_tenant_id, caller_user_id);
                    let row = Self::execute_returning_one_conn(&mut **conn, &q).await?.unwrap_or(Value::Null);
                    out.push(row);
                }
            }
        }
        Ok(out)
    }

    /// Like `bulk_create` but uses savepoints to isolate per-row DB errors.
    /// Returns `(successful_rows, row_errors)`. If any errors occur the transaction is
    /// rolled back and successful_rows will be empty — call site decides how to surface errors.
    pub async fn bulk_create_collecting<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        items: &[HashMap<String, Value>],
        schema_override: Option<&str>,
        rls_tenant_id: Option<&str>,
        caller_user_id: Option<&str>,
    ) -> Result<(Vec<Value>, Vec<(usize, AppError)>), AppError> {
        const BULK_LIMIT: usize = 100;
        if items.len() > BULK_LIMIT {
            return Err(AppError::BadRequest(format!("bulk create limited to {} items", BULK_LIMIT)));
        }
        let mut out = Vec::with_capacity(items.len());
        let mut row_errors: Vec<(usize, AppError)> = Vec::new();
        match executor {
            TenantExecutor::Pool(pool) => {
                let mut tx = pool.begin().await?;
                for (idx, body) in items.iter().enumerate() {
                    let sp = format!("sp_{}", idx);
                    sqlx::query(&format!("SAVEPOINT {}", sp)).execute(&mut *tx).await?;
                    let include_pk = body.contains_key(&entity.pk_columns[0]);
                    let q = insert(entity, body, include_pk, schema_override, rls_tenant_id, caller_user_id);
                    match Self::execute_returning_one_tx(&mut tx, &q).await {
                        Ok(row) => {
                            sqlx::query(&format!("RELEASE SAVEPOINT {}", sp)).execute(&mut *tx).await?;
                            out.push(row.unwrap_or(Value::Null));
                        }
                        Err(e) => {
                            sqlx::query(&format!("ROLLBACK TO SAVEPOINT {}", sp)).execute(&mut *tx).await?;
                            row_errors.push((idx, e));
                        }
                    }
                }
                if row_errors.is_empty() {
                    tx.commit().await?;
                } else {
                    tx.rollback().await?;
                    out.clear();
                }
            }
            TenantExecutor::Conn(conn) => {
                for (idx, body) in items.iter().enumerate() {
                    let sp = format!("sp_{}", idx);
                    sqlx::query(&format!("SAVEPOINT {}", sp)).execute(&mut **conn).await?;
                    let include_pk = body.contains_key(&entity.pk_columns[0]);
                    let q = insert(entity, body, include_pk, schema_override, rls_tenant_id, caller_user_id);
                    match Self::execute_returning_one_conn(&mut **conn, &q).await {
                        Ok(row) => {
                            sqlx::query(&format!("RELEASE SAVEPOINT {}", sp)).execute(&mut **conn).await?;
                            out.push(row.unwrap_or(Value::Null));
                        }
                        Err(e) => {
                            sqlx::query(&format!("ROLLBACK TO SAVEPOINT {}", sp)).execute(&mut **conn).await?;
                            row_errors.push((idx, e));
                        }
                    }
                }
                if !row_errors.is_empty() {
                    out.clear();
                }
            }
        }
        Ok((out, row_errors))
    }

    /// Bulk update in a transaction (when using pool) or on the same connection (when using conn). Each item must have id. Returns vec of updated rows.
    /// When caller_user_id is Some, updated_by is set on each row.
    pub async fn bulk_update<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        items: &[HashMap<String, Value>],
        schema_override: Option<&str>,
        caller_user_id: Option<&str>,
    ) -> Result<Vec<Value>, AppError> {
        const BULK_LIMIT: usize = 100;
        if items.len() > BULK_LIMIT {
            return Err(AppError::BadRequest(format!(
                "bulk update limited to {} items",
                BULK_LIMIT
            )));
        }
        let pk = &entity.pk_columns[0];
        let mut out = Vec::with_capacity(items.len());
        match executor {
            TenantExecutor::Pool(pool) => {
                let mut tx = pool.begin().await?;
                for body in items {
                    let id = body.get(pk).ok_or_else(|| AppError::Validation(format!("each item must have '{}'", pk)))?;
                    let mut body_without_pk = body.clone();
                    body_without_pk.remove(pk);
                    let q = update(entity, id, &body_without_pk, schema_override, caller_user_id);
                    if let Some(row) = Self::execute_returning_one_tx(&mut tx, &q).await? {
                        out.push(row);
                    }
                }
                tx.commit().await?;
            }
            TenantExecutor::Conn(conn) => {
                for body in items {
                    let id = body.get(pk).ok_or_else(|| AppError::Validation(format!("each item must have '{}'", pk)))?;
                    let mut body_without_pk = body.clone();
                    body_without_pk.remove(pk);
                    let q = update(entity, id, &body_without_pk, schema_override, caller_user_id);
                    if let Some(row) = Self::execute_returning_one_conn(&mut **conn, &q).await? {
                        out.push(row);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Like `bulk_update` but uses savepoints to isolate per-row DB errors.
    /// Missing pk on an item is recorded as a row error rather than aborting early.
    /// Returns `(successful_rows, row_errors)`. If any errors occur the transaction is
    /// rolled back and successful_rows will be empty.
    pub async fn bulk_update_collecting<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        items: &[HashMap<String, Value>],
        schema_override: Option<&str>,
        caller_user_id: Option<&str>,
    ) -> Result<(Vec<Value>, Vec<(usize, AppError)>), AppError> {
        const BULK_LIMIT: usize = 100;
        if items.len() > BULK_LIMIT {
            return Err(AppError::BadRequest(format!("bulk update limited to {} items", BULK_LIMIT)));
        }
        let pk = entity.pk_columns[0].clone();
        let mut out = Vec::with_capacity(items.len());
        let mut row_errors: Vec<(usize, AppError)> = Vec::new();
        match executor {
            TenantExecutor::Pool(pool) => {
                let mut tx = pool.begin().await?;
                for (idx, body) in items.iter().enumerate() {
                    let id = match body.get(&pk) {
                        Some(id) => id.clone(),
                        None => {
                            row_errors.push((idx, AppError::Validation(format!("each item must have '{}'", pk))));
                            continue;
                        }
                    };
                    let sp = format!("sp_{}", idx);
                    sqlx::query(&format!("SAVEPOINT {}", sp)).execute(&mut *tx).await?;
                    let mut body_without_pk = body.clone();
                    body_without_pk.remove(&pk);
                    let q = update(entity, &id, &body_without_pk, schema_override, caller_user_id);
                    match Self::execute_returning_one_tx(&mut tx, &q).await {
                        Ok(Some(row)) => {
                            sqlx::query(&format!("RELEASE SAVEPOINT {}", sp)).execute(&mut *tx).await?;
                            out.push(row);
                        }
                        Ok(None) => {
                            sqlx::query(&format!("RELEASE SAVEPOINT {}", sp)).execute(&mut *tx).await?;
                        }
                        Err(e) => {
                            sqlx::query(&format!("ROLLBACK TO SAVEPOINT {}", sp)).execute(&mut *tx).await?;
                            row_errors.push((idx, e));
                        }
                    }
                }
                if row_errors.is_empty() {
                    tx.commit().await?;
                } else {
                    tx.rollback().await?;
                    out.clear();
                }
            }
            TenantExecutor::Conn(conn) => {
                for (idx, body) in items.iter().enumerate() {
                    let id = match body.get(&pk) {
                        Some(id) => id.clone(),
                        None => {
                            row_errors.push((idx, AppError::Validation(format!("each item must have '{}'", pk))));
                            continue;
                        }
                    };
                    let sp = format!("sp_{}", idx);
                    sqlx::query(&format!("SAVEPOINT {}", sp)).execute(&mut **conn).await?;
                    let mut body_without_pk = body.clone();
                    body_without_pk.remove(&pk);
                    let q = update(entity, &id, &body_without_pk, schema_override, caller_user_id);
                    match Self::execute_returning_one_conn(&mut **conn, &q).await {
                        Ok(Some(row)) => {
                            sqlx::query(&format!("RELEASE SAVEPOINT {}", sp)).execute(&mut **conn).await?;
                            out.push(row);
                        }
                        Ok(None) => {
                            sqlx::query(&format!("RELEASE SAVEPOINT {}", sp)).execute(&mut **conn).await?;
                        }
                        Err(e) => {
                            sqlx::query(&format!("ROLLBACK TO SAVEPOINT {}", sp)).execute(&mut **conn).await?;
                            row_errors.push((idx, e));
                        }
                    }
                }
                if !row_errors.is_empty() {
                    out.clear();
                }
            }
        }
        Ok((out, row_errors))
    }

    async fn query_one_exec<'a>(
        executor: &mut TenantExecutor<'a>,
        sql: &str,
        params: &[Value],
    ) -> Result<Option<Value>, AppError> {
        tracing::debug!(sql = %sql, params = ?params, "query");
        let bind = Self::to_sqlx_param(&params[0]);
        let row = match executor {
            TenantExecutor::Pool(pool) => sqlx::query(sql).bind(bind).fetch_optional(*pool).await?,
            TenantExecutor::Conn(conn) => sqlx::query(sql).bind(bind).fetch_optional(&mut **conn).await?,
        };
        Ok(row.map(|r| row_to_json(&r)))
    }

    async fn query_many_exec<'a>(
        executor: &mut TenantExecutor<'a>,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<Value>, AppError> {
        tracing::debug!(sql = %sql, params = ?params, "query");
        let mut query = sqlx::query(sql);
        for p in params {
            query = query.bind(Self::to_sqlx_param(p));
        }
        let rows = match executor {
            TenantExecutor::Pool(pool) => query.fetch_all(*pool).await?,
            TenantExecutor::Conn(conn) => query.fetch_all(&mut **conn).await?,
        };
        Ok(rows.iter().map(row_to_json).collect())
    }

    async fn execute_returning_one_exec<'a>(
        executor: &mut TenantExecutor<'a>,
        q: &QueryBuf,
    ) -> Result<Option<Value>, AppError> {
        tracing::debug!(sql = %q.sql, params = ?q.params, "query");
        let mut query = sqlx::query(&q.sql);
        for p in &q.params {
            query = query.bind(Self::to_sqlx_param(p));
        }
        let row = match executor {
            TenantExecutor::Pool(pool) => query.fetch_optional(*pool).await?,
            TenantExecutor::Conn(conn) => query.fetch_optional(&mut **conn).await?,
        };
        Ok(row.map(|r| row_to_json(&r)))
    }

    async fn execute_returning_one_with_params_exec<'a>(
        executor: &mut TenantExecutor<'a>,
        sql: &str,
        params: &[Value],
    ) -> Result<Option<Value>, AppError> {
        tracing::debug!(sql = %sql, params = ?params, "query");
        let mut query = sqlx::query(sql);
        for p in params {
            query = query.bind(Self::to_sqlx_param(p));
        }
        let row = match executor {
            TenantExecutor::Pool(pool) => query.fetch_optional(*pool).await?,
            TenantExecutor::Conn(conn) => query.fetch_optional(&mut **conn).await?,
        };
        Ok(row.map(|r| row_to_json(&r)))
    }

    async fn execute_returning_one_conn(
        conn: &mut sqlx::PgConnection,
        q: &QueryBuf,
    ) -> Result<Option<Value>, AppError> {
        tracing::debug!(sql = %q.sql, params = ?q.params, "query (conn)");
        let mut query = sqlx::query(&q.sql);
        for p in &q.params {
            query = query.bind(Self::to_sqlx_param(p));
        }
        let row = query.fetch_optional(conn).await?;
        Ok(row.map(|r| row_to_json(&r)))
    }

    async fn execute_returning_one_tx(
        tx: &mut sqlx::PgConnection,
        q: &QueryBuf,
    ) -> Result<Option<Value>, AppError> {
        tracing::debug!(sql = %q.sql, params = ?q.params, "query (tx)");
        let mut query = sqlx::query(&q.sql);
        for p in &q.params {
            query = query.bind(Self::to_sqlx_param(p));
        }
        let row = query.fetch_optional(&mut *tx).await?;
        Ok(row.map(|r| row_to_json(&r)))
    }

    fn to_sqlx_param(v: &Value) -> PgBindValue {
        PgBindValue::from_json(v).unwrap_or(PgBindValue::Null)
    }

    async fn insert_audit<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        action: &str,
        row: &Value,
        pre_row: Option<&Value>,
        audit_by: Option<&str>,
        schema_override: Option<&str>,
    ) -> Result<(), AppError> {
        let schema = schema_override.unwrap_or(&entity.schema_name);
        let audit_table = format!(
            "\"{}\".\"{}\"",
            schema.replace('"', "\"\""),
            format!("{}_audit", entity.table_name).replace('"', "\"\"")
        );

        let changed = if action == "update" {
            pre_row.map(|pre| compute_changed_fields(pre, row, entity))
        } else {
            None
        };

        let mut col_names: Vec<String> = vec![
            "\"audit_action\"".to_string(),
            "\"audit_by\"".to_string(),
            "\"changed_fields\"".to_string(),
        ];
        let mut placeholders: Vec<String> = Vec::new();
        let mut params: Vec<Value> = Vec::new();

        params.push(Value::String(action.to_string()));
        placeholders.push(format!("${}", params.len()));

        params.push(audit_by.map(|s| Value::String(s.to_string())).unwrap_or(Value::Null));
        placeholders.push(format!("${}", params.len()));

        params.push(changed.unwrap_or(Value::Null));
        placeholders.push(format!("${}::jsonb", params.len()));

        let row_obj = row.as_object();
        for col in &entity.columns {
            let val = row_obj.and_then(|o| o.get(&col.name)).cloned().unwrap_or(Value::Null);
            let param_num = params.len() + 1;
            let ph = col
                .pg_type
                .as_deref()
                .map(|t| format!("${}::{}", param_num, t))
                .unwrap_or_else(|| format!("${}", param_num));
            col_names.push(format!("\"{}\"", col.name));
            placeholders.push(ph);
            params.push(val);
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            audit_table,
            col_names.join(", "),
            placeholders.join(", ")
        );
        tracing::debug!(sql = %sql, "audit insert");

        let mut query = sqlx::query(&sql);
        for p in &params {
            query = query.bind(Self::to_sqlx_param(p));
        }
        match executor {
            TenantExecutor::Pool(pool) => { query.execute(*pool).await?; }
            TenantExecutor::Conn(conn) => { query.execute(&mut **conn).await?; }
        }
        Ok(())
    }
}

fn compute_changed_fields(pre: &Value, post: &Value, entity: &ResolvedEntity) -> Value {
    let pre_obj = match pre.as_object() { Some(o) => o, None => return Value::Null };
    let post_obj = match post.as_object() { Some(o) => o, None => return Value::Null };
    let mut changes = serde_json::Map::new();
    for col in &entity.columns {
        let pre_val = pre_obj.get(&col.name).unwrap_or(&Value::Null);
        let post_val = post_obj.get(&col.name).unwrap_or(&Value::Null);
        if pre_val != post_val {
            let mut diff = serde_json::Map::new();
            diff.insert("old".to_string(), pre_val.clone());
            diff.insert("new".to_string(), post_val.clone());
            changes.insert(col.name.clone(), Value::Object(diff));
        }
    }
    Value::Object(changes)
}

fn row_to_json(row: &sqlx::postgres::PgRow) -> Value {
    use sqlx::Row;
    use sqlx::Column;
    let mut map = serde_json::Map::new();
    for col in row.columns() {
        let name = col.name();
        let v = cell_to_value(row, name);
        map.insert(name.to_string(), v);
    }
    Value::Object(map)
}

fn cell_to_value(row: &sqlx::postgres::PgRow, name: &str) -> Value {
    use sqlx::Row;
    if let Ok(v) = row.try_get::<Option<i16>, _>(name) {
        if let Some(n) = v {
            return Value::Number(n.into());
        }
    }
    if let Ok(v) = row.try_get::<Option<i32>, _>(name) {
        if let Some(n) = v {
            return Value::Number(n.into());
        }
    }
    if let Ok(v) = row.try_get::<Option<i64>, _>(name) {
        if let Some(n) = v {
            return Value::Number(n.into());
        }
    }
    if let Ok(v) = row.try_get::<Option<f32>, _>(name) {
        if let Some(n) = v {
            if let Some(n) = serde_json::Number::from_f64(n as f64) {
                return Value::Number(n);
            }
        }
    }
    if let Ok(v) = row.try_get::<Option<f64>, _>(name) {
        if let Some(n) = v {
            if let Some(n) = serde_json::Number::from_f64(n) {
                return Value::Number(n);
            }
        }
    }
    if let Ok(v) = row.try_get::<Option<bool>, _>(name) {
        if let Some(b) = v {
            return Value::Bool(b);
        }
    }
    if let Ok(v) = row.try_get::<Option<Vec<String>>, _>(name) {
        if let Some(vec) = v {
            return Value::Array(vec.into_iter().map(Value::String).collect());
        }
    }
    if let Ok(v) = row.try_get::<Option<Vec<uuid::Uuid>>, _>(name) {
        if let Some(vec) = v {
            return Value::Array(vec.into_iter().map(|u| Value::String(u.to_string())).collect());
        }
    }
    if let Ok(v) = row.try_get::<Option<Vec<i64>>, _>(name) {
        if let Some(vec) = v {
            return Value::Array(vec.into_iter().map(|n| Value::Number(n.into())).collect());
        }
    }
    if let Ok(v) = row.try_get::<Option<uuid::Uuid>, _>(name) {
        if let Some(u) = v {
            return Value::String(u.to_string());
        }
    }
    if let Ok(v) = row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(name) {
        if let Some(d) = v {
            return Value::String(d.to_rfc3339());
        }
    }
    if let Ok(v) = row.try_get::<Option<chrono::NaiveDateTime>, _>(name) {
        if let Some(d) = v {
            return Value::String(d.format("%Y-%m-%dT%H:%M:%S%.f").to_string());
        }
    }
    if let Ok(v) = row.try_get::<Option<chrono::NaiveDate>, _>(name) {
        if let Some(d) = v {
            return Value::String(d.format("%Y-%m-%d").to_string());
        }
    }
    if let Ok(v) = row.try_get::<Option<String>, _>(name) {
        if let Some(s) = v {
            // Numeric columns are selected as ::text; parse so we return a JSON number not string
            if let Ok(n) = s.trim().parse::<f64>() {
                if let Some(num) = serde_json::Number::from_f64(n) {
                    return Value::Number(num);
                }
            }
            return Value::String(s);
        }
    }
    if let Ok(v) = row.try_get::<Option<serde_json::Value>, _>(name) {
        if let Some(j) = v {
            return j;
        }
    }
    Value::Null
}
