//! KV store data API: list keys, get, set, delete by package_id and namespace.
//! All data is tenant-isolated: rows in _sys_kv_data are keyed by (tenant_id, package_id, namespace, key).

use crate::error::AppError;
use crate::extractors::tenant::TenantId;
use crate::handlers::entity::resolve_tenant_context;
use crate::response::success_one_ok;
use crate::state::AppState;
use crate::store::qualified_sys_table;
use axum::extract::{Path, State};
use axum::Json;
use serde_json::Value;

async fn validate_namespace(
    pool: &crate::db::pool::Pool,
    dialect: &dyn crate::db::Dialect,
    package_id: &str,
    namespace: &str,
) -> Result<(), AppError> {
    let q = qualified_sys_table("_sys_kv_stores");
    let exists: Option<(String,)> = sqlx::query_as(&format!(
        "SELECT id FROM {} WHERE id = {} AND package_id = {}",
        q,
        dialect.placeholder(1),
        dialect.placeholder(2)
    ))
    .bind(namespace)
    .bind(package_id)
    .fetch_optional(pool)
    .await?;
    exists.ok_or_else(|| {
        AppError::NotFound(format!(
            "kv namespace '{}' not found in package '{}'",
            namespace, package_id
        ))
    })?;
    Ok(())
}

/// GET /api/v1/package/:package_id/kv/:namespace — list keys (and values) in namespace.
pub async fn kv_list_keys(
    TenantId(tenant_id_opt): TenantId,
    State(state): State<AppState>,
    Path((package_id, namespace)): Path<(String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let tenant_id = tenant_id_opt
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("X-Tenant-ID header is required".into()))?;
    state
        .tenant_registry
        .get(tenant_id)
        .ok_or_else(|| AppError::NotFound(format!("tenant not found: {}", tenant_id)))?;
    let _ctx = resolve_tenant_context(&state, Some(tenant_id), None, Some(&package_id)).await?;
    let pool = &state.pool;
    validate_namespace(pool, state.dialect.as_ref(), &package_id, &namespace).await?;
    let q_table = qualified_sys_table("_sys_kv_data");
    let d = state.dialect.as_ref();
    let sql = format!(
        "SELECT key, value FROM {} WHERE tenant_id = {} AND package_id = {} AND namespace = {} ORDER BY key",
        q_table, d.placeholder(1), d.placeholder(2), d.placeholder(3)
    );
    let rows: Vec<(String, Value)> = sqlx::query_as(&sql)
        .bind(tenant_id)
        .bind(&package_id)
        .bind(&namespace)
        .fetch_all(pool)
        .await?;
    let data: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(k, v)| serde_json::json!({ "key": k, "value": v }))
        .collect();
    Ok(crate::response::success_many(data))
}

/// GET /api/v1/package/:package_id/kv/:namespace/:key — get one value.
pub async fn kv_get(
    TenantId(tenant_id_opt): TenantId,
    State(state): State<AppState>,
    Path((package_id, namespace, key)): Path<(String, String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let tenant_id = tenant_id_opt
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("X-Tenant-ID header is required".into()))?;
    state
        .tenant_registry
        .get(tenant_id)
        .ok_or_else(|| AppError::NotFound(format!("tenant not found: {}", tenant_id)))?;
    let _ctx = resolve_tenant_context(&state, Some(tenant_id), None, Some(&package_id)).await?;
    let pool = &state.pool;
    validate_namespace(pool, state.dialect.as_ref(), &package_id, &namespace).await?;
    let q_table = qualified_sys_table("_sys_kv_data");
    let d = state.dialect.as_ref();
    let sql = format!(
        "SELECT value FROM {} WHERE tenant_id = {} AND package_id = {} AND namespace = {} AND key = {}",
        q_table, d.placeholder(1), d.placeholder(2), d.placeholder(3), d.placeholder(4)
    );
    let row: Option<(Value,)> = sqlx::query_as(&sql)
        .bind(tenant_id)
        .bind(&package_id)
        .bind(&namespace)
        .bind(&key)
        .fetch_optional(pool)
        .await?;
    let value = row
        .ok_or_else(|| AppError::NotFound(format!("kv key not found: {} / {}", namespace, key)))?
        .0;
    Ok(success_one_ok(value))
}

/// PUT /api/v1/package/:package_id/kv/:namespace/:key — set value (upsert).
pub async fn kv_put(
    TenantId(tenant_id_opt): TenantId,
    State(state): State<AppState>,
    Path((package_id, namespace, key)): Path<(String, String, String)>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let tenant_id = tenant_id_opt
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("X-Tenant-ID header is required".into()))?;
    state
        .tenant_registry
        .get(tenant_id)
        .ok_or_else(|| AppError::NotFound(format!("tenant not found: {}", tenant_id)))?;
    let _ctx = resolve_tenant_context(&state, Some(tenant_id), None, Some(&package_id)).await?;
    let pool = &state.pool;
    validate_namespace(pool, state.dialect.as_ref(), &package_id, &namespace).await?;
    let q_table = qualified_sys_table("_sys_kv_data");
    let d = state.dialect.as_ref();
    let now = d.now_fn();
    let (p1, p2, p3, p4, p5) = (
        d.placeholder(1),
        d.placeholder(2),
        d.placeholder(3),
        d.placeholder(4),
        d.placeholder(5),
    );

    // UPDATE-then-INSERT rather than an ON CONFLICT upsert: the upsert form would reuse a
    // placeholder in the SET clause, which breaks on positional-placeholder dialects
    // (SQLite/MySQL `?`) by introducing an unbound parameter — the conflicting write then sets
    // `value` to NULL and fails the NOT NULL constraint. Each statement here binds exactly its
    // own placeholders, so it is correct on every dialect.
    let update_sql = format!(
        "UPDATE {tbl} SET value = {p1}, updated_at = {now} \
         WHERE tenant_id = {p2} AND package_id = {p3} AND namespace = {p4} AND key = {p5}",
        tbl = q_table,
    );
    let affected = sqlx::query(&update_sql)
        .bind(&body)
        .bind(tenant_id)
        .bind(&package_id)
        .bind(&namespace)
        .bind(&key)
        .execute(pool)
        .await?
        .rows_affected();

    if affected == 0 {
        let insert_sql = format!(
            "INSERT INTO {tbl} (tenant_id, package_id, namespace, key, value, updated_at) \
             VALUES ({p1}, {p2}, {p3}, {p4}, {p5}, {now})",
            tbl = q_table,
        );
        sqlx::query(&insert_sql)
            .bind(tenant_id)
            .bind(&package_id)
            .bind(&namespace)
            .bind(&key)
            .bind(&body)
            .execute(pool)
            .await?;
    }
    Ok(success_one_ok(body))
}

/// DELETE /api/v1/package/:package_id/kv/:namespace/:key — delete key.
pub async fn kv_delete(
    TenantId(tenant_id_opt): TenantId,
    State(state): State<AppState>,
    Path((package_id, namespace, key)): Path<(String, String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let tenant_id = tenant_id_opt
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("X-Tenant-ID header is required".into()))?;
    state
        .tenant_registry
        .get(tenant_id)
        .ok_or_else(|| AppError::NotFound(format!("tenant not found: {}", tenant_id)))?;
    let _ctx = resolve_tenant_context(&state, Some(tenant_id), None, Some(&package_id)).await?;
    let pool = &state.pool;
    validate_namespace(pool, state.dialect.as_ref(), &package_id, &namespace).await?;
    let q_table = qualified_sys_table("_sys_kv_data");
    let d = state.dialect.as_ref();
    let sql =
        format!(
        "DELETE FROM {} WHERE tenant_id = {} AND package_id = {} AND namespace = {} AND key = {}",
        q_table, d.placeholder(1), d.placeholder(2), d.placeholder(3), d.placeholder(4)
    );
    let result = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(&package_id)
        .bind(&namespace)
        .bind(&key)
        .execute(pool)
        .await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(format!(
            "kv key not found: {} / {}",
            namespace, key
        )));
    }
    Ok((axum::http::StatusCode::NO_CONTENT, ()))
}
