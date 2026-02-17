//! Generic CRUD execution against PostgreSQL.

use crate::config::ResolvedEntity;
use crate::error::AppError;
use crate::sql::{delete, insert, select_by_column_in, select_by_id, select_list, select_list_with_includes, IncludeSelect, update, PgBindValue, QueryBuf};
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
    /// List rows with optional filters (exact match), limit (default 100, max 1000), offset (default 0).
    pub async fn list<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        filters: &[(String, Value)],
        limit: Option<u32>,
        offset: Option<u32>,
        schema_override: Option<&str>,
    ) -> Result<Vec<Value>, AppError> {
        const DEFAULT_LIMIT: u32 = 100;
        let limit = limit.unwrap_or(DEFAULT_LIMIT).min(1000);
        let offset = offset.unwrap_or(0);
        let q = select_list(entity, filters, Some(limit), Some(offset), schema_override);
        Self::query_many_exec(executor, &q.sql, &q.params).await
    }

    /// List rows with includes in a single query (scalar subqueries with json_agg/row_to_json). Returns rows with include keys already set (JSON).
    pub async fn list_with_includes<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        filters: &[(String, Value)],
        limit: Option<u32>,
        offset: Option<u32>,
        includes: &[IncludeSelect<'_>],
        schema_override: Option<&str>,
    ) -> Result<Vec<Value>, AppError> {
        const DEFAULT_LIMIT: u32 = 100;
        let limit = limit.unwrap_or(DEFAULT_LIMIT).min(1000);
        let offset = offset.unwrap_or(0);
        let q = select_list_with_includes(entity, filters, Some(limit), Some(offset), includes, schema_override);
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
    pub async fn create<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        body: &HashMap<String, Value>,
        schema_override: Option<&str>,
        rls_tenant_id: Option<&str>,
    ) -> Result<Value, AppError> {
        let include_pk = body.contains_key(&entity.pk_columns[0]);
        let q = insert(entity, body, include_pk, schema_override, rls_tenant_id);
        let row = Self::execute_returning_one_exec(executor, &q).await?
            .ok_or_else(|| AppError::Db(sqlx::Error::RowNotFound))?;
        Ok(row)
    }

    /// Update one row by id. Returns updated row.
    pub async fn update<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        id: &Value,
        body: &HashMap<String, Value>,
        schema_override: Option<&str>,
    ) -> Result<Option<Value>, AppError> {
        let q = update(entity, id, body, schema_override);
        Self::execute_returning_one_exec(executor, &q).await
    }

    /// Delete one row by id. Returns deleted row or None.
    pub async fn delete<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        id: &Value,
        schema_override: Option<&str>,
    ) -> Result<Option<Value>, AppError> {
        let q = delete(entity, schema_override);
        Self::execute_returning_one_with_params_exec(executor, &q.sql, &[id.clone()]).await
    }

    /// Bulk create in a transaction (when using pool) or on the same connection (when using conn). Returns vec of created rows.
    /// When rls_tenant_id is Some (RLS strategy), tenant_id column is set automatically on each row.
    pub async fn bulk_create<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        items: &[HashMap<String, Value>],
        schema_override: Option<&str>,
        rls_tenant_id: Option<&str>,
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
                    let q = insert(entity, body, include_pk, schema_override, rls_tenant_id);
                    let row = Self::execute_returning_one_tx(&mut tx, &q).await?.unwrap_or(Value::Null);
                    out.push(row);
                }
                tx.commit().await?;
            }
            TenantExecutor::Conn(conn) => {
                for body in items {
                    let include_pk = body.contains_key(&entity.pk_columns[0]);
                    let q = insert(entity, body, include_pk, schema_override, rls_tenant_id);
                    let row = Self::execute_returning_one_conn(&mut **conn, &q).await?.unwrap_or(Value::Null);
                    out.push(row);
                }
            }
        }
        Ok(out)
    }

    /// Bulk update in a transaction (when using pool) or on the same connection (when using conn). Each item must have id. Returns vec of updated rows.
    pub async fn bulk_update<'a>(
        executor: &mut TenantExecutor<'a>,
        entity: &ResolvedEntity,
        items: &[HashMap<String, Value>],
        schema_override: Option<&str>,
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
                    let q = update(entity, id, &body_without_pk, schema_override);
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
                    let q = update(entity, id, &body_without_pk, schema_override);
                    if let Some(row) = Self::execute_returning_one_conn(&mut **conn, &q).await? {
                        out.push(row);
                    }
                }
            }
        }
        Ok(out)
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
