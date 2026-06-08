//! Admin API for per-tenant extensible-field registries.
//!
//! Routes (all require `X-Tenant-ID`):
//! - `GET    /api/v1/:entity/extensible-fields` — return the current registry document
//! - `PUT    /api/v1/:entity/extensible-fields` — replace the registry (validated)
//! - `DELETE /api/v1/:entity/extensible-fields` — clear the registry
//!
//! The registry is stored in `_sys_kv_data` (config pool) under the reserved namespace
//! `__extensible_fields__`, keyed by the entity's `path_segment`. This endpoint addresses it
//! by entity, validates the document shape against the entity's `extensible` columns, and
//! writes directly — bypassing the `_sys_kv_stores` namespace check the generic KV API applies.
//!
//! ## Authorization
//! When an authrs client is configured, each route is gated by a **distinct, privileged action**
//! so managing field *definitions* is a separate grant from row CRUD:
//! `getExtensibleFields<Table>`, `putExtensibleFields<Table>`, `deleteExtensibleFields<Table>`.
//! When authrs is not configured the checks are no-ops.

use crate::authrs::check_entity_permission_opt;
use crate::config::ResolvedEntity;
use crate::error::AppError;
use crate::extensible_fields::{
    apply_indexes, delete_registry, index_ddl, load_registry, load_registry_raw, store_registry,
    validate_registry_document,
};
use crate::extractors::tenant::TenantId;
use crate::extractors::user::UserId;
use crate::handlers::entity::{evict_extensible_registry, resolve_tenant_context, TenantContext};
use crate::response::success_one_ok;
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};

/// Resolve the tenant (required) and the entity by path segment. Errors if the tenant or
/// entity is unknown, or if the entity declares no extensible columns.
fn resolve(
    state: &AppState,
    tenant_id_opt: Option<&str>,
    path_segment: &str,
) -> Result<(String, ResolvedEntity), AppError> {
    let tenant_id = tenant_id_opt
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("X-Tenant-ID header is required".into()))?;
    state
        .tenant_registry
        .get(tenant_id)
        .ok_or_else(|| AppError::NotFound(format!("tenant not found: {}", tenant_id)))?;

    let entity = state
        .model
        .read()
        .map_err(|_| AppError::BadRequest("state lock".into()))?
        .entity_by_path(path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment.to_string()))?;

    if entity.extensible_columns.is_empty() {
        return Err(AppError::BadRequest(format!(
            "entity '{}' has no extensible columns (declare a JSON column with \"extensible\": true)",
            path_segment
        )));
    }
    Ok((tenant_id.to_string(), entity))
}

/// GET /api/v1/:entity/extensible-fields — current registry document (or `{}` when unset).
pub async fn get_registry(
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    State(state): State<AppState>,
    Path(path_segment): Path<String>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let (tenant_id, entity) = resolve(&state, tenant_id_opt.as_deref(), &path_segment)?;
    check_entity_permission_opt(
        &state.authrs_client,
        Some(&tenant_id),
        user_id_opt.as_deref(),
        &entity,
        "getExtensibleFields",
    )
    .await?;
    let value = load_registry_raw(
        &state.pool,
        state.dialect.as_ref(),
        &tenant_id,
        &entity.package_id,
        &path_segment,
    )
    .await?
    .unwrap_or_else(|| json!({}));
    Ok(success_one_ok(value))
}

/// PUT /api/v1/:entity/extensible-fields — validate and replace the registry document.
pub async fn put_registry(
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    State(state): State<AppState>,
    Path(path_segment): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let (tenant_id, entity) = resolve(&state, tenant_id_opt.as_deref(), &path_segment)?;
    check_entity_permission_opt(
        &state.authrs_client,
        Some(&tenant_id),
        user_id_opt.as_deref(),
        &entity,
        "putExtensibleFields",
    )
    .await?;
    // Shape + allow-list validation; rejects unknown bag columns and malformed defs with 422.
    validate_registry_document(&body, &entity.extensible_columns, &path_segment)?;
    store_registry(
        &state.pool,
        state.dialect.as_ref(),
        &tenant_id,
        &entity.package_id,
        &path_segment,
        &body,
    )
    .await?;
    evict_extensible_registry(&state, &tenant_id, &entity.package_id, &path_segment);
    Ok(success_one_ok(body))
}

/// DELETE /api/v1/:entity/extensible-fields — clear the registry document.
pub async fn delete_registry_handler(
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    State(state): State<AppState>,
    Path(path_segment): Path<String>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let (tenant_id, entity) = resolve(&state, tenant_id_opt.as_deref(), &path_segment)?;
    check_entity_permission_opt(
        &state.authrs_client,
        Some(&tenant_id),
        user_id_opt.as_deref(),
        &entity,
        "deleteExtensibleFields",
    )
    .await?;
    let removed = delete_registry(
        &state.pool,
        state.dialect.as_ref(),
        &tenant_id,
        &entity.package_id,
        &path_segment,
    )
    .await?;
    evict_extensible_registry(&state, &tenant_id, &entity.package_id, &path_segment);
    if !removed {
        return Err(AppError::NotFound(format!(
            "no extensible-field registry defined for '{}'",
            path_segment
        )));
    }
    Ok((axum::http::StatusCode::NO_CONTENT, ()))
}

/// Resolve the tenant data context and build the `CREATE INDEX` statements for the entity's
/// queryable extensible fields. Returns `(statements, data_pool)` so callers can review or apply.
async fn build_index_statements(
    state: &AppState,
    tenant_id: &str,
    entity: &ResolvedEntity,
) -> Result<(Vec<String>, crate::db::pool::Pool), AppError> {
    // Definitions live on the config pool; the index target lives in the tenant's data DB.
    let registry = load_registry(
        &state.pool,
        state.dialect.as_ref(),
        tenant_id,
        &entity.package_id,
        &entity.path_segment,
    )
    .await?;

    let ctx = resolve_tenant_context(state, Some(tenant_id), None).await?;
    let schema = ctx
        .schema_override()
        .map(str::to_string)
        .unwrap_or_else(|| entity.schema_name.clone());
    let rls_predicate = match (ctx.rls_tenant_column(), ctx.rls_tenant_id()) {
        (Some(col), Some(tid)) => Some((col, tid)),
        _ => None,
    };
    let statements = index_ddl(
        &schema,
        &entity.table_name,
        &registry,
        state.dialect.as_ref(),
        rls_predicate,
    );
    let data_pool = match &ctx {
        TenantContext::Pool { pool, .. } | TenantContext::Rls { pool, .. } => pool.clone(),
    };
    Ok((statements, data_pool))
}

/// GET /api/v1/:entity/extensible-fields/indexes — suggested `CREATE INDEX` statements for the
/// entity's queryable extensible fields. Review and apply deliberately (large-table DDL is heavy).
pub async fn get_indexes(
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    State(state): State<AppState>,
    Path(path_segment): Path<String>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let (tenant_id, entity) = resolve(&state, tenant_id_opt.as_deref(), &path_segment)?;
    check_entity_permission_opt(
        &state.authrs_client,
        Some(&tenant_id),
        user_id_opt.as_deref(),
        &entity,
        "getExtensibleFields",
    )
    .await?;
    let (statements, _pool) = build_index_statements(&state, &tenant_id, &entity).await?;
    Ok(success_one_ok(json!({
        "dialect": state.dialect.name(),
        "statements": statements,
    })))
}

/// POST /api/v1/:entity/extensible-fields/indexes — apply the suggested indexes to the tenant's
/// data table. Best-effort and idempotent (`IF NOT EXISTS` where supported); returns which
/// statements were applied and which failed.
pub async fn apply_indexes_handler(
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    State(state): State<AppState>,
    Path(path_segment): Path<String>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let (tenant_id, entity) = resolve(&state, tenant_id_opt.as_deref(), &path_segment)?;
    check_entity_permission_opt(
        &state.authrs_client,
        Some(&tenant_id),
        user_id_opt.as_deref(),
        &entity,
        "putExtensibleFields",
    )
    .await?;
    let (statements, pool) = build_index_statements(&state, &tenant_id, &entity).await?;
    let (applied, errors) = apply_indexes(&pool, &statements).await;
    Ok(success_one_ok(json!({
        "applied": applied,
        "errors": errors.into_iter().map(|(stmt, msg)| json!({ "statement": stmt, "error": msg })).collect::<Vec<_>>(),
    })))
}
