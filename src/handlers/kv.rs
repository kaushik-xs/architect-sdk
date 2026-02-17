//! KV store data API: list keys, get, set, delete by package_id and namespace.

use crate::error::AppError;
use crate::extractors::tenant::TenantId;
use crate::handlers::entity::resolve_tenant_context;
use crate::response::success_one_ok;
use crate::state::AppState;
use crate::store::qualified_sys_table;
use axum::extract::{Path, State};
use axum::Json;
use serde_json::Value;

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
    let ctx = resolve_tenant_context(&state, Some(tenant_id), Some(&package_id)).await?;
    let pool = ctx.migration_pool();
    let q_table = qualified_sys_table("_sys_kv_data");
    let sql = format!(
        "SELECT key, value FROM {} WHERE package_id = $1 AND namespace = $2 ORDER BY key",
        q_table
    );
    let rows: Vec<(String, Value)> = sqlx::query_as(&sql)
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
    let ctx = resolve_tenant_context(&state, Some(tenant_id), Some(&package_id)).await?;
    let pool = ctx.migration_pool();
    let q_table = qualified_sys_table("_sys_kv_data");
    let sql = format!(
        "SELECT value FROM {} WHERE package_id = $1 AND namespace = $2 AND key = $3",
        q_table
    );
    let row: Option<(Value,)> = sqlx::query_as(&sql)
        .bind(&package_id)
        .bind(&namespace)
        .bind(&key)
        .fetch_optional(pool)
        .await?;
    let value = row.ok_or_else(|| AppError::NotFound(format!("kv key not found: {} / {}", namespace, key)))?.0;
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
    let ctx = resolve_tenant_context(&state, Some(tenant_id), Some(&package_id)).await?;
    let pool = ctx.migration_pool();
    let q_table = qualified_sys_table("_sys_kv_data");
    let sql = format!(
        r#"
        INSERT INTO {} (package_id, namespace, key, value, updated_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (package_id, namespace, key)
        DO UPDATE SET value = $4, updated_at = NOW()
        "#,
        q_table
    );
    sqlx::query(&sql)
        .bind(&package_id)
        .bind(&namespace)
        .bind(&key)
        .bind(&body)
        .execute(pool)
        .await?;
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
    let ctx = resolve_tenant_context(&state, Some(tenant_id), Some(&package_id)).await?;
    let pool = ctx.migration_pool();
    let q_table = qualified_sys_table("_sys_kv_data");
    let sql = format!(
        "DELETE FROM {} WHERE package_id = $1 AND namespace = $2 AND key = $3",
        q_table
    );
    let result: sqlx::postgres::PgQueryResult = sqlx::query(&sql)
        .bind(&package_id)
        .bind(&namespace)
        .bind(&key)
        .execute(pool)
        .await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(format!("kv key not found: {} / {}", namespace, key)));
    }
    Ok((axum::http::StatusCode::NO_CONTENT, ()))
}
