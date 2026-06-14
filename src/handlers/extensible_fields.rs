//! Admin API for per-tenant extensible-field registries.
//!
//! Routes (all require `X-Tenant-ID`), in both default-model and package-scoped forms:
//! - `GET/PUT/DELETE  /api/v1/:entity/extensible-fields`
//! - `GET/POST        /api/v1/:entity/extensible-fields/indexes`
//! - `GET/PUT/DELETE  /api/v1/package/:package_id/:entity/extensible-fields`
//! - `GET/POST        /api/v1/package/:package_id/:entity/extensible-fields/indexes`
//!
//! The registry is stored in `_sys_kv_data` (config pool) under the reserved namespace
//! `__extensible_fields__`, keyed by `(tenant_id, package_id, path_segment)`. The unprefixed
//! routes resolve the entity from the default/active model; the package-scoped routes resolve it
//! from that package's model (so the registry is keyed by the correct package).
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
use crate::handlers::entity::{
    evict_extensible_registry, get_or_load_package_model, resolve_tenant_context, TenantContext,
};
use crate::response::success_one_ok;
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};

// ── Entity resolution ───────────────────────────────────────────────────────

/// Require the tenant and ensure the entity declares at least one extensible column.
fn require_tenant<'a>(
    state: &AppState,
    tenant_id_opt: Option<&'a str>,
) -> Result<&'a str, AppError> {
    let tenant_id = tenant_id_opt
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("X-Tenant-ID header is required".into()))?;
    state
        .tenant_registry
        .get(tenant_id)
        .ok_or_else(|| AppError::NotFound(format!("tenant not found: {}", tenant_id)))?;
    Ok(tenant_id)
}

fn ensure_extensible(entity: &ResolvedEntity) -> Result<(), AppError> {
    if entity.extensible_columns.is_empty() {
        return Err(AppError::BadRequest(format!(
            "entity '{}' has no extensible columns (declare a JSON column with \"extensible\": true)",
            entity.path_segment
        )));
    }
    Ok(())
}

/// Resolve `(tenant_id, entity)` from the **default/active** model.
fn resolve_default(
    state: &AppState,
    tenant_id_opt: Option<&str>,
    path_segment: &str,
) -> Result<(String, ResolvedEntity), AppError> {
    let tenant_id = require_tenant(state, tenant_id_opt)?;
    let entity = state
        .model
        .read()
        .map_err(|_| AppError::BadRequest("state lock".into()))?
        .entity_by_path(path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment.to_string()))?;
    ensure_extensible(&entity)?;
    Ok((tenant_id.to_string(), entity))
}

/// Resolve `(tenant_id, entity)` from a specific **package's** model. The resolved entity carries
/// `package_id`, so the registry is keyed by the correct package.
async fn resolve_package(
    state: &AppState,
    tenant_id_opt: Option<&str>,
    package_id: &str,
    path_segment: &str,
) -> Result<(String, ResolvedEntity), AppError> {
    let tenant_id = require_tenant(state, tenant_id_opt)?.to_string();
    let ctx = resolve_tenant_context(state, Some(&tenant_id), None, Some(package_id)).await?;
    let model = get_or_load_package_model(
        state,
        ctx.config_pool(),
        ctx.package_cache_key(),
        package_id,
    )
    .await?;
    let entity = model
        .entity_by_path(path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment.to_string()))?;
    ensure_extensible(&entity)?;
    Ok((tenant_id, entity))
}

// ── Core operations (shared by default-model and package-scoped wrappers) ────

async fn do_get_registry(
    state: &AppState,
    tenant_id: &str,
    entity: &ResolvedEntity,
    user_id: Option<&str>,
) -> Result<Value, AppError> {
    check_entity_permission_opt(
        &state.authrs_client,
        Some(tenant_id),
        user_id,
        entity,
        "getExtensibleFields",
    )
    .await?;
    Ok(load_registry_raw(
        &state.pool,
        state.dialect.as_ref(),
        tenant_id,
        &entity.package_id,
        &entity.path_segment,
    )
    .await?
    .unwrap_or_else(|| json!({})))
}

async fn do_put_registry(
    state: &AppState,
    tenant_id: &str,
    entity: &ResolvedEntity,
    user_id: Option<&str>,
    body: Value,
) -> Result<Value, AppError> {
    check_entity_permission_opt(
        &state.authrs_client,
        Some(tenant_id),
        user_id,
        entity,
        "putExtensibleFields",
    )
    .await?;
    // Shape + allow-list validation; rejects unknown bag columns and malformed defs with 422.
    validate_registry_document(&body, &entity.extensible_columns, &entity.path_segment)?;
    store_registry(
        &state.pool,
        state.dialect.as_ref(),
        tenant_id,
        &entity.package_id,
        &entity.path_segment,
        &body,
    )
    .await?;
    evict_extensible_registry(state, tenant_id, &entity.package_id, &entity.path_segment);
    Ok(body)
}

async fn do_delete_registry(
    state: &AppState,
    tenant_id: &str,
    entity: &ResolvedEntity,
    user_id: Option<&str>,
) -> Result<bool, AppError> {
    check_entity_permission_opt(
        &state.authrs_client,
        Some(tenant_id),
        user_id,
        entity,
        "deleteExtensibleFields",
    )
    .await?;
    let removed = delete_registry(
        &state.pool,
        state.dialect.as_ref(),
        tenant_id,
        &entity.package_id,
        &entity.path_segment,
    )
    .await?;
    evict_extensible_registry(state, tenant_id, &entity.package_id, &entity.path_segment);
    Ok(removed)
}

/// Resolve the tenant data context and build `CREATE INDEX` statements for the entity's queryable
/// extensible fields. Returns `(statements, data_pool)`.
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

    let ctx = resolve_tenant_context(state, Some(tenant_id), None, None).await?;
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

async fn do_get_indexes(
    state: &AppState,
    tenant_id: &str,
    entity: &ResolvedEntity,
    user_id: Option<&str>,
) -> Result<Value, AppError> {
    check_entity_permission_opt(
        &state.authrs_client,
        Some(tenant_id),
        user_id,
        entity,
        "getExtensibleFields",
    )
    .await?;
    let (statements, _pool) = build_index_statements(state, tenant_id, entity).await?;
    Ok(json!({ "dialect": state.dialect.name(), "statements": statements }))
}

async fn do_apply_indexes(
    state: &AppState,
    tenant_id: &str,
    entity: &ResolvedEntity,
    user_id: Option<&str>,
) -> Result<Value, AppError> {
    check_entity_permission_opt(
        &state.authrs_client,
        Some(tenant_id),
        user_id,
        entity,
        "putExtensibleFields",
    )
    .await?;
    let (statements, pool) = build_index_statements(state, tenant_id, entity).await?;
    let (applied, errors) = apply_indexes(&pool, &statements).await;
    Ok(json!({
        "applied": applied,
        "errors": errors.into_iter().map(|(stmt, msg)| json!({ "statement": stmt, "error": msg })).collect::<Vec<_>>(),
    }))
}

// ── Default-model handlers ───────────────────────────────────────────────────

pub async fn get_registry(
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    State(state): State<AppState>,
    Path(path_segment): Path<String>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let (tenant_id, entity) = resolve_default(&state, tenant_id_opt.as_deref(), &path_segment)?;
    let value = do_get_registry(&state, &tenant_id, &entity, user_id_opt.as_deref()).await?;
    Ok(success_one_ok(value))
}

pub async fn put_registry(
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    State(state): State<AppState>,
    Path(path_segment): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let (tenant_id, entity) = resolve_default(&state, tenant_id_opt.as_deref(), &path_segment)?;
    let value = do_put_registry(&state, &tenant_id, &entity, user_id_opt.as_deref(), body).await?;
    Ok(success_one_ok(value))
}

pub async fn delete_registry_handler(
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    State(state): State<AppState>,
    Path(path_segment): Path<String>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let (tenant_id, entity) = resolve_default(&state, tenant_id_opt.as_deref(), &path_segment)?;
    let removed = do_delete_registry(&state, &tenant_id, &entity, user_id_opt.as_deref()).await?;
    not_found_if_absent(removed, &entity)?;
    Ok((axum::http::StatusCode::NO_CONTENT, ()))
}

pub async fn get_indexes(
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    State(state): State<AppState>,
    Path(path_segment): Path<String>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let (tenant_id, entity) = resolve_default(&state, tenant_id_opt.as_deref(), &path_segment)?;
    let value = do_get_indexes(&state, &tenant_id, &entity, user_id_opt.as_deref()).await?;
    Ok(success_one_ok(value))
}

pub async fn apply_indexes_handler(
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    State(state): State<AppState>,
    Path(path_segment): Path<String>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let (tenant_id, entity) = resolve_default(&state, tenant_id_opt.as_deref(), &path_segment)?;
    let value = do_apply_indexes(&state, &tenant_id, &entity, user_id_opt.as_deref()).await?;
    Ok(success_one_ok(value))
}

// ── Package-scoped handlers ──────────────────────────────────────────────────

pub async fn get_registry_package(
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    State(state): State<AppState>,
    Path((package_id, path_segment)): Path<(String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let (tenant_id, entity) =
        resolve_package(&state, tenant_id_opt.as_deref(), &package_id, &path_segment).await?;
    let value = do_get_registry(&state, &tenant_id, &entity, user_id_opt.as_deref()).await?;
    Ok(success_one_ok(value))
}

pub async fn put_registry_package(
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    State(state): State<AppState>,
    Path((package_id, path_segment)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let (tenant_id, entity) =
        resolve_package(&state, tenant_id_opt.as_deref(), &package_id, &path_segment).await?;
    let value = do_put_registry(&state, &tenant_id, &entity, user_id_opt.as_deref(), body).await?;
    Ok(success_one_ok(value))
}

pub async fn delete_registry_package(
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    State(state): State<AppState>,
    Path((package_id, path_segment)): Path<(String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let (tenant_id, entity) =
        resolve_package(&state, tenant_id_opt.as_deref(), &package_id, &path_segment).await?;
    let removed = do_delete_registry(&state, &tenant_id, &entity, user_id_opt.as_deref()).await?;
    not_found_if_absent(removed, &entity)?;
    Ok((axum::http::StatusCode::NO_CONTENT, ()))
}

pub async fn get_indexes_package(
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    State(state): State<AppState>,
    Path((package_id, path_segment)): Path<(String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let (tenant_id, entity) =
        resolve_package(&state, tenant_id_opt.as_deref(), &package_id, &path_segment).await?;
    let value = do_get_indexes(&state, &tenant_id, &entity, user_id_opt.as_deref()).await?;
    Ok(success_one_ok(value))
}

pub async fn apply_indexes_package(
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    State(state): State<AppState>,
    Path((package_id, path_segment)): Path<(String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let (tenant_id, entity) =
        resolve_package(&state, tenant_id_opt.as_deref(), &package_id, &path_segment).await?;
    let value = do_apply_indexes(&state, &tenant_id, &entity, user_id_opt.as_deref()).await?;
    Ok(success_one_ok(value))
}

fn not_found_if_absent(removed: bool, entity: &ResolvedEntity) -> Result<(), AppError> {
    if !removed {
        return Err(AppError::NotFound(format!(
            "no extensible-field registry defined for '{}'",
            entity.path_segment
        )));
    }
    Ok(())
}
