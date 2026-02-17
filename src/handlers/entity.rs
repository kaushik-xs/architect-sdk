//! Entity CRUD handlers: create, read, update, delete, list, bulk.
//! Request bodies and query param keys are accepted in camelCase and converted to snake_case for DB; response row keys are converted to camelCase.

use crate::case::{hashmap_keys_to_snake_case, to_snake_case, value_keys_to_camel_case};
use crate::config::{load_from_pool, resolve, IncludeDirection, PkType, ResolvedModel, ResolvedEntity};
use crate::error::AppError;
use crate::extractors::tenant::TenantId;
use crate::service::{CrudService, RequestValidator, TenantExecutor};
use crate::sql::IncludeSelect;
use crate::state::AppState;
use crate::store::DEFAULT_PACKAGE_ID;
use crate::tenant::TenantStrategy;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde_json::Value;
use sqlx::pool::PoolConnection;
use sqlx::Postgres;
use std::collections::{HashMap, HashSet};

/// Remove sensitive column keys from a row object. No-op if sensitive_columns is empty.
fn strip_sensitive_columns(row: &mut Value, sensitive_columns: &HashSet<String>) {
    if sensitive_columns.is_empty() {
        return;
    }
    if let Value::Object(map) = row {
        map.retain(|k, _| !sensitive_columns.contains(k));
    }
}

fn parse_id(id_str: &str, pk_type: &PkType) -> Result<Value, AppError> {
    Ok(match pk_type {
        PkType::Uuid => {
            let u = uuid::Uuid::parse_str(id_str).map_err(|_| AppError::BadRequest("invalid uuid".into()))?;
            Value::String(u.to_string())
        }
        PkType::BigInt | PkType::Int => {
            let n: i64 = id_str.parse().map_err(|_| AppError::BadRequest("invalid id".into()))?;
            Value::Number(n.into())
        }
        PkType::Text => Value::String(id_str.to_string()),
    })
}

fn body_to_map(value: Value) -> Result<HashMap<String, Value>, AppError> {
    match value {
        Value::Object(m) => Ok(m.into_iter().collect()),
        _ => Err(AppError::BadRequest("body must be a JSON object".into())),
    }
}

fn query_value_for_column(entity: &ResolvedEntity, col: &str, s: &str) -> Value {
    let col_info = entity.columns.iter().find(|c| c.name == col);
    let is_uuid = col_info
        .and_then(|c| c.pk_type.as_ref())
        .map(|t| matches!(t, PkType::Uuid))
        .unwrap_or(false)
        || col_info
            .and_then(|c| c.pg_type.as_deref())
            .map(|t| t.to_lowercase().contains("uuid"))
            .unwrap_or(false);
    let is_int = col_info
        .and_then(|c| c.pk_type.as_ref())
        .map(|t| matches!(t, PkType::BigInt | PkType::Int))
        .unwrap_or(false)
        || col_info
            .and_then(|c| c.pg_type.as_deref())
            .map(|t| {
                let l = t.to_lowercase();
                l.contains("int") || l.contains("serial")
            })
            .unwrap_or(false);
    let is_bool = col_info
        .and_then(|c| c.pg_type.as_deref())
        .map(|t| t.to_lowercase().starts_with("bool"))
        .unwrap_or(false);

    if is_uuid {
        if let Ok(u) = uuid::Uuid::parse_str(s) {
            return Value::String(u.to_string());
        }
    }
    if is_int {
        if let Ok(n) = s.parse::<i64>() {
            return Value::Number(n.into());
        }
    }
    if is_bool {
        if s.eq_ignore_ascii_case("true") {
            return Value::Bool(true);
        }
        if s.eq_ignore_ascii_case("false") {
            return Value::Bool(false);
        }
    }
    Value::String(s.to_string())
}

/// Resolve include names to (name, spec, related_entity). Call with model read lock held.
fn resolve_includes(
    model: &ResolvedModel,
    entity: &ResolvedEntity,
    include_names: &[String],
) -> Result<Vec<(String, crate::config::IncludeSpec, ResolvedEntity)>, AppError> {
    let mut out = Vec::new();
    for name in include_names {
        let spec = entity
            .includes
            .iter()
            .find(|i| i.name.as_str() == name.as_str())
            .ok_or_else(|| AppError::BadRequest(format!("unknown include: {}", name)))?
            .clone();
        let related = model
            .entity_by_path(&spec.related_path_segment)
            .cloned()
            .ok_or_else(|| AppError::BadRequest(format!("related entity not found: {}", spec.related_path_segment)))?;
        out.push((name.clone(), spec, related));
    }
    Ok(out)
}

/// Resolved tenant context: pool (or pool to acquire from for RLS), schema override, and for RLS the tenant_id to set.
pub enum TenantContext {
    Pool {
        pool: sqlx::PgPool,
        schema_override: Option<String>,
        config_pool: sqlx::PgPool,
        package_cache_key: String,
    },
    Rls {
        tenant_id: String,
        pool: sqlx::PgPool,
        config_pool: sqlx::PgPool,
        package_cache_key: String,
    },
}

impl TenantContext {
    pub fn config_pool(&self) -> &sqlx::PgPool {
        match self {
            TenantContext::Pool { config_pool, .. } | TenantContext::Rls { config_pool, .. } => config_pool,
        }
    }
    pub fn package_cache_key(&self) -> &str {
        match self {
            TenantContext::Pool { package_cache_key, .. } | TenantContext::Rls { package_cache_key, .. } => package_cache_key,
        }
    }
}

/// Resolve execution context from tenant id. X-Tenant-ID is required; returns 400 if missing, 404 if tenant unknown.
/// For package_id_opt: when None (default routes), package_cache_key is DEFAULT_PACKAGE_ID.
pub async fn resolve_tenant_context(
    state: &AppState,
    tenant_id_opt: Option<&str>,
    package_id_opt: Option<&str>,
) -> Result<TenantContext, AppError> {
    let tenant_id = tenant_id_opt
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("X-Tenant-ID header is required".into()))?;

    let package_id = package_id_opt.unwrap_or(DEFAULT_PACKAGE_ID);
    let package_cache_key = package_id.to_string();

    let entry = state.tenant_registry.get(tenant_id).ok_or_else(|| AppError::NotFound(format!("tenant not found: {}", tenant_id)))?;

    // Architect DB (from .env DATABASE_URL) is where config lives for schema/rls. If tenant has database_url, use that DB for app data only.
    let architect_pool = state.pool.clone();

    match &entry.strategy {
        TenantStrategy::Database => {
            let database_url = entry.database_url.as_deref().ok_or_else(|| AppError::BadRequest(format!("tenant {}: strategy database requires database_url", tenant_id)))?;
            let pool = get_or_create_tenant_pool(state, tenant_id, database_url).await?;
            Ok(TenantContext::Pool {
                pool: pool.clone(),
                schema_override: None,
                config_pool: pool,
                package_cache_key: format!("{}:{}", package_id, tenant_id),
            })
        }
        TenantStrategy::Schema => {
            let schema_name = entry.schema_name.clone().ok_or_else(|| AppError::BadRequest(format!("tenant {}: strategy schema requires schema_name", tenant_id)))?;
            let pool = match entry.database_url.as_deref() {
                Some(url) => get_or_create_tenant_pool(state, tenant_id, url).await?,
                None => architect_pool.clone(),
            };
            Ok(TenantContext::Pool {
                pool,
                schema_override: Some(schema_name),
                config_pool: architect_pool,
                package_cache_key,
            })
        }
        TenantStrategy::Rls => {
            let pool = match entry.database_url.as_deref() {
                Some(url) => get_or_create_tenant_pool(state, tenant_id, url).await?,
                None => architect_pool.clone(),
            };
            Ok(TenantContext::Rls {
                tenant_id: tenant_id.to_string(),
                pool,
                config_pool: architect_pool,
                package_cache_key,
            })
        }
    }
}

/// Get or create a pool for the given tenant_id and database_url. Config lives in architect DB; this pool is for app data when tenant uses a different DB.
async fn get_or_create_tenant_pool(
    state: &AppState,
    tenant_id: &str,
    database_url: &str,
) -> Result<sqlx::PgPool, AppError> {
    let existing = {
        let guard = state.tenant_pools.read().map_err(|_| AppError::BadRequest("state lock".into()))?;
        guard.get(tenant_id).cloned()
    };
    if let Some(p) = existing {
        return Ok(p);
    }
    let new_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;
    {
        let mut guard = state.tenant_pools.write().map_err(|_| AppError::BadRequest("state lock".into()))?;
        guard.entry(tenant_id.to_string()).or_insert_with(|| new_pool.clone());
    }
    Ok(new_pool)
}

/// Get resolved model for a package from cache, or load from config_pool and cache it under cache_key.
/// package_id is used for load_from_pool (config table package_id); cache_key is for the in-memory cache (e.g. "pkg" or "pkg:tenant_id").
pub(crate) async fn get_or_load_package_model(
    state: &AppState,
    config_pool: &sqlx::PgPool,
    cache_key: &str,
    package_id: &str,
) -> Result<ResolvedModel, AppError> {
    {
        let guard = state.package_models.read().map_err(|_| AppError::BadRequest("state lock".into()))?;
        if let Some(m) = guard.get(cache_key) {
            return Ok(m.clone());
        }
    }
    let config = load_from_pool(config_pool, package_id).await.map_err(AppError::Config)?;
    let model = resolve(&config).map_err(AppError::Config)?;
    state
        .package_models
        .write()
        .map_err(|_| AppError::BadRequest("state lock".into()))?
        .insert(cache_key.to_string(), model.clone());
    Ok(model)
}

/// Post-process rows from single-query list_with_includes: parse JSON include columns if string, strip sensitive and camelCase nested objects.
fn post_process_include_columns(
    rows: &mut [Value],
    resolved_includes: &[(String, crate::config::IncludeSpec, ResolvedEntity)],
) {
    for row in rows.iter_mut() {
        if let Value::Object(map) = row {
            for (name, _spec, related) in resolved_includes {
                let Some(included) = map.get_mut(name) else { continue };
                if let Value::String(s) = included {
                    if let Ok(parsed) = serde_json::from_str::<Value>(s) {
                        *included = parsed;
                    }
                }
                match included {
                    Value::Array(arr) => {
                        for item in arr.iter_mut() {
                            if let Value::Object(_) = item {
                                strip_sensitive_columns(item, &related.sensitive_columns);
                                value_keys_to_camel_case(item);
                            }
                        }
                    }
                    Value::Object(_) => {
                        strip_sensitive_columns(included, &related.sensitive_columns);
                        value_keys_to_camel_case(included);
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Attach related-entity data to rows. Modifies each row in place. resolved_includes from resolve_includes (so lock can be dropped before calling).
async fn attach_includes<'a>(
    executor: &mut TenantExecutor<'a>,
    schema_override: Option<&str>,
    _entity: &ResolvedEntity,
    rows: &mut [Value],
    resolved_includes: &[(String, crate::config::IncludeSpec, ResolvedEntity)],
) -> Result<(), AppError> {
    if resolved_includes.is_empty() || rows.is_empty() {
        return Ok(());
    }
    for (name, spec, related) in resolved_includes {
        match &spec.direction {
            IncludeDirection::ToOne => {
                let keys: Vec<Value> = rows
                    .iter()
                    .filter_map(|r| r.get(&spec.our_key_column).cloned())
                    .collect();
                let related_rows = CrudService::fetch_where_column_in(
                    executor,
                    related,
                    &spec.their_key_column,
                    &keys,
                    schema_override,
                )
                .await?;
                let mut key_to_row: HashMap<String, Value> = HashMap::new();
                for mut r in related_rows {
                    let k = r
                        .get(&spec.their_key_column)
                        .cloned()
                        .map(|v| serde_json::to_string(&v).unwrap_or_default())
                        .unwrap_or_default();
                    if !key_to_row.contains_key(&k) {
                        strip_sensitive_columns(&mut r, &related.sensitive_columns);
                        value_keys_to_camel_case(&mut r);
                        key_to_row.insert(k, r);
                    }
                }
                for row in rows.iter_mut() {
                    if let Value::Object(ref mut map) = row {
                        let key_val = map.get(&spec.our_key_column).cloned().unwrap_or(Value::Null);
                        let key = serde_json::to_string(&key_val).unwrap_or_default();
                        let included = key_to_row.get(&key).cloned().unwrap_or(Value::Null);
                        map.insert(name.clone(), included);
                    }
                }
            }
            IncludeDirection::ToMany => {
                let keys: Vec<Value> = rows
                    .iter()
                    .filter_map(|r| r.get(&spec.our_key_column).cloned())
                    .collect();
                let related_rows = CrudService::fetch_where_column_in(
                    executor,
                    related,
                    &spec.their_key_column,
                    &keys,
                    schema_override,
                )
                .await?;
                let mut grouped: HashMap<String, Vec<Value>> = HashMap::new();
                for mut r in related_rows {
                    let k = r
                        .get(&spec.their_key_column)
                        .cloned()
                        .map(|v| serde_json::to_string(&v).unwrap_or_default())
                        .unwrap_or_default();
                    strip_sensitive_columns(&mut r, &related.sensitive_columns);
                    value_keys_to_camel_case(&mut r);
                    grouped.entry(k).or_default().push(r);
                }
                for row in rows.iter_mut() {
                    if let Value::Object(ref mut map) = row {
                        let key_val = map.get(&spec.our_key_column).cloned().unwrap_or(Value::Null);
                        let key = serde_json::to_string(&key_val).unwrap_or_default();
                        let arr = grouped
                            .get(&key)
                            .cloned()
                            .unwrap_or_default();
                        map.insert(name.clone(), Value::Array(arr));
                    }
                }
            }
        }
    }
    Ok(())
}

pub async fn list(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    Path(path_segment): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), None).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<PoolConnection<Postgres>> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool { pool, schema_override, .. } => (TenantExecutor::Pool(pool), schema_override.as_deref()),
        TenantContext::Rls { tenant_id, pool, .. } => {
            let mut conn = pool.acquire().await?;
            sqlx::query("SET LOCAL app.tenant_id = $1")
                .bind(tenant_id)
                .execute(&mut *conn)
                .await?;
            rls_conn = Some(conn);
            (TenantExecutor::Conn(&mut *rls_conn.as_mut().unwrap()), None)
        }
    };

    let entity = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "read") {
        return Err(AppError::BadRequest("read not allowed".into()));
    }
    let column_names: std::collections::HashSet<&str> = entity.columns.iter().map(|c| c.name.as_str()).collect();

    let mut limit: Option<u32> = None;
    let mut offset: Option<u32> = None;
    let mut include_names: Vec<String> = Vec::new();
    let mut filters: Vec<(String, Value)> = Vec::new();

    for (k, v) in params {
        match k.as_str() {
            "limit" => limit = v.parse().ok(),
            "offset" => offset = v.parse().ok(),
            "include" => include_names = v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
            _ => {
                let col_key = to_snake_case(&k);
                if column_names.contains(col_key.as_str()) {
                    let val = query_value_for_column(&entity, &col_key, &v);
                    filters.push((col_key, val));
                }
            }
        }
    }

    let mut rows = if include_names.is_empty() {
        CrudService::list(&mut executor, &entity, &filters, limit, offset, schema_override).await?
    } else {
        let resolved = {
            let model = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?;
            resolve_includes(&model, &entity, &include_names)?
        };
        let includes: Vec<IncludeSelect> = resolved
            .iter()
            .map(|(name, spec, related)| IncludeSelect {
                name: name.as_str(),
                direction: spec.direction.clone(),
                related,
                our_key: spec.our_key_column.as_str(),
                their_key: spec.their_key_column.as_str(),
            })
            .collect();
        let mut rows = CrudService::list_with_includes(
            &mut executor,
            &entity,
            &filters,
            limit,
            offset,
            includes.as_slice(),
            schema_override,
        )
        .await?;
        post_process_include_columns(&mut rows, &resolved);
        rows
    };
    for row in &mut rows {
        strip_sensitive_columns(row, &entity.sensitive_columns);
        value_keys_to_camel_case(row);
    }
    let count = rows.len() as u64;
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessMany {
            data: rows,
            meta: crate::response::MetaCount { count },
        }),
    ))
}

pub async fn create(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    Path(path_segment): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), None).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<PoolConnection<Postgres>> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool { pool, schema_override, .. } => (TenantExecutor::Pool(pool), schema_override.as_deref()),
        TenantContext::Rls { tenant_id, pool, .. } => {
            let mut conn = pool.acquire().await?;
            sqlx::query("SET LOCAL app.tenant_id = $1").bind(tenant_id).execute(&mut *conn).await?;
            rls_conn = Some(conn);
            (TenantExecutor::Conn(&mut *rls_conn.as_mut().unwrap()), None)
        }
    };
    let entity = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "create") {
        return Err(AppError::BadRequest("create not allowed".into()));
    }
    let body = body_to_map(body)?;
    let body = hashmap_keys_to_snake_case(&body);
    RequestValidator::validate(&body, &entity.validation)?;
    let mut row = CrudService::create(&mut executor, &entity, &body, schema_override).await?;
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    Ok((axum::http::StatusCode::CREATED, Json(crate::response::SuccessOne { data: row, meta: None })))
}

pub async fn read(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    Path((path_segment, id_str)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), None).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<PoolConnection<Postgres>> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool { pool, schema_override, .. } => (TenantExecutor::Pool(pool), schema_override.as_deref()),
        TenantContext::Rls { tenant_id, pool, .. } => {
            let mut conn = pool.acquire().await?;
            sqlx::query("SET LOCAL app.tenant_id = $1").bind(tenant_id).execute(&mut *conn).await?;
            rls_conn = Some(conn);
            (TenantExecutor::Conn(&mut *rls_conn.as_mut().unwrap()), None)
        }
    };
    let entity = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "read") {
        return Err(AppError::BadRequest("read not allowed".into()));
    }
    let id = parse_id(&id_str, &entity.pk_type)?;
    let mut row = CrudService::read(&mut executor, &entity, &id, schema_override).await?
        .ok_or_else(|| AppError::NotFound(id_str))?;
    let include_names: Vec<String> = params
        .get("include")
        .map(|s| s.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
        .unwrap_or_default();
    if !include_names.is_empty() {
        let resolved = {
            let model = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?;
            resolve_includes(&model, &entity, &include_names)?
        };
        let mut rows = [row];
        attach_includes(&mut executor, schema_override, &entity, &mut rows, &resolved).await?;
        row = rows[0].clone();
    }
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    Ok((axum::http::StatusCode::OK, Json(crate::response::SuccessOne { data: row, meta: None })))
}

pub async fn update(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    Path((path_segment, id_str)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), None).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<PoolConnection<Postgres>> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool { pool, schema_override, .. } => (TenantExecutor::Pool(pool), schema_override.as_deref()),
        TenantContext::Rls { tenant_id, pool, .. } => {
            let mut conn = pool.acquire().await?;
            sqlx::query("SET LOCAL app.tenant_id = $1").bind(tenant_id).execute(&mut *conn).await?;
            rls_conn = Some(conn);
            (TenantExecutor::Conn(&mut *rls_conn.as_mut().unwrap()), None)
        }
    };
    let entity = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "update") {
        return Err(AppError::BadRequest("update not allowed".into()));
    }
    let id = parse_id(&id_str, &entity.pk_type)?;
    let body = body_to_map(body)?;
    let body = hashmap_keys_to_snake_case(&body);
    RequestValidator::validate(&body, &entity.validation)?;
    let mut row = CrudService::update(&mut executor, &entity, &id, &body, schema_override).await?
        .ok_or_else(|| AppError::NotFound(id_str))?;
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    Ok((axum::http::StatusCode::OK, Json(crate::response::SuccessOne { data: row, meta: None })))
}

pub async fn delete(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    Path((path_segment, id_str)): Path<(String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), None).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<PoolConnection<Postgres>> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool { pool, schema_override, .. } => (TenantExecutor::Pool(pool), schema_override.as_deref()),
        TenantContext::Rls { tenant_id, pool, .. } => {
            let mut conn = pool.acquire().await?;
            sqlx::query("SET LOCAL app.tenant_id = $1").bind(tenant_id).execute(&mut *conn).await?;
            rls_conn = Some(conn);
            (TenantExecutor::Conn(&mut *rls_conn.as_mut().unwrap()), None)
        }
    };
    let entity = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "delete") {
        return Err(AppError::BadRequest("delete not allowed".into()));
    }
    let id = parse_id(&id_str, &entity.pk_type)?;
    CrudService::delete(&mut executor, &entity, &id, schema_override).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

pub async fn bulk_create(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    Path(path_segment): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), None).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<PoolConnection<Postgres>> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool { pool, schema_override, .. } => (TenantExecutor::Pool(pool), schema_override.as_deref()),
        TenantContext::Rls { tenant_id, pool, .. } => {
            let mut conn = pool.acquire().await?;
            sqlx::query("SET LOCAL app.tenant_id = $1").bind(tenant_id).execute(&mut *conn).await?;
            rls_conn = Some(conn);
            (TenantExecutor::Conn(&mut *rls_conn.as_mut().unwrap()), None)
        }
    };
    let entity = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "bulk_create") {
        return Err(AppError::BadRequest("bulk_create not allowed".into()));
    }
    let items: Vec<HashMap<String, Value>> = match body {
        Value::Array(arr) => {
            let mut out = Vec::new();
            for v in arr {
                out.push(hashmap_keys_to_snake_case(&body_to_map(v)?));
            }
            out
        }
        _ => return Err(AppError::BadRequest("body must be a JSON array".into())),
    };
    for item in &items {
        RequestValidator::validate(item, &entity.validation)?;
    }
    let mut rows = CrudService::bulk_create(&mut executor, &entity, &items, schema_override).await?;
    for row in &mut rows {
        strip_sensitive_columns(row, &entity.sensitive_columns);
        value_keys_to_camel_case(row);
    }
    let count = rows.len() as u64;
    Ok((
        axum::http::StatusCode::CREATED,
        Json(crate::response::SuccessMany {
            data: rows,
            meta: crate::response::MetaCount { count },
        }),
    ))
}

pub async fn bulk_update(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    Path(path_segment): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), None).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<PoolConnection<Postgres>> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool { pool, schema_override, .. } => (TenantExecutor::Pool(pool), schema_override.as_deref()),
        TenantContext::Rls { tenant_id, pool, .. } => {
            let mut conn = pool.acquire().await?;
            sqlx::query("SET LOCAL app.tenant_id = $1").bind(tenant_id).execute(&mut *conn).await?;
            rls_conn = Some(conn);
            (TenantExecutor::Conn(&mut *rls_conn.as_mut().unwrap()), None)
        }
    };
    let entity = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "bulk_update") {
        return Err(AppError::BadRequest("bulk_update not allowed".into()));
    }
    let items: Vec<HashMap<String, Value>> = match body {
        Value::Array(arr) => {
            let mut out = Vec::new();
            for v in arr {
                out.push(hashmap_keys_to_snake_case(&body_to_map(v)?));
            }
            out
        }
        _ => return Err(AppError::BadRequest("body must be a JSON array".into())),
    };
    for item in &items {
        RequestValidator::validate(item, &entity.validation)?;
    }
    let mut rows = CrudService::bulk_update(&mut executor, &entity, &items, schema_override).await?;
    for row in &mut rows {
        strip_sensitive_columns(row, &entity.sensitive_columns);
        value_keys_to_camel_case(row);
    }
    let count = rows.len() as u64;
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessMany {
            data: rows,
            meta: crate::response::MetaCount { count },
        }),
    ))
}

// ---- Package-scoped handlers: /api/v1/package/:package_id/:path_segment ----

pub async fn list_package(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    Path((package_id, path_segment)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), Some(&package_id)).await?;
    let model = get_or_load_package_model(&state, ctx.config_pool(), ctx.package_cache_key(), &package_id).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<PoolConnection<Postgres>> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool { pool, schema_override, .. } => (TenantExecutor::Pool(pool), schema_override.as_deref()),
        TenantContext::Rls { tenant_id, pool, .. } => {
            let mut conn = pool.acquire().await?;
            sqlx::query("SET LOCAL app.tenant_id = $1").bind(tenant_id).execute(&mut *conn).await?;
            rls_conn = Some(conn);
            (TenantExecutor::Conn(&mut *rls_conn.as_mut().unwrap()), None)
        }
    };
    let entity = model.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "read") {
        return Err(AppError::BadRequest("read not allowed".into()));
    }
    let column_names: std::collections::HashSet<&str> = entity.columns.iter().map(|c| c.name.as_str()).collect();
    let mut limit: Option<u32> = None;
    let mut offset: Option<u32> = None;
    let mut include_names: Vec<String> = Vec::new();
    let mut filters: Vec<(String, Value)> = Vec::new();
    for (k, v) in params {
        match k.as_str() {
            "limit" => limit = v.parse().ok(),
            "offset" => offset = v.parse().ok(),
            "include" => include_names = v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
            _ => {
                let col_key = to_snake_case(&k);
                if column_names.contains(col_key.as_str()) {
                    filters.push((col_key.clone(), query_value_for_column(&entity, &col_key, &v)));
                }
            }
        }
    }
    let mut rows = if include_names.is_empty() {
        CrudService::list(&mut executor, &entity, &filters, limit, offset, schema_override).await?
    } else {
        let resolved = resolve_includes(&model, &entity, &include_names)?;
        let includes: Vec<IncludeSelect> = resolved.iter().map(|(name, spec, related)| IncludeSelect { name: name.as_str(), direction: spec.direction.clone(), related, our_key: spec.our_key_column.as_str(), their_key: spec.their_key_column.as_str() }).collect();
        let mut rows = CrudService::list_with_includes(&mut executor, &entity, &filters, limit, offset, includes.as_slice(), schema_override).await?;
        post_process_include_columns(&mut rows, &resolved);
        rows
    };
    for row in &mut rows {
        strip_sensitive_columns(row, &entity.sensitive_columns);
        value_keys_to_camel_case(row);
    }
    let count = rows.len() as u64;
    Ok((axum::http::StatusCode::OK, Json(crate::response::SuccessMany { data: rows, meta: crate::response::MetaCount { count } })))
}

pub async fn create_package(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    Path((package_id, path_segment)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), Some(&package_id)).await?;
    let model = get_or_load_package_model(&state, ctx.config_pool(), ctx.package_cache_key(), &package_id).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<PoolConnection<Postgres>> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool { pool, schema_override, .. } => (TenantExecutor::Pool(pool), schema_override.as_deref()),
        TenantContext::Rls { tenant_id, pool, .. } => {
            let mut conn = pool.acquire().await?;
            sqlx::query("SET LOCAL app.tenant_id = $1").bind(tenant_id).execute(&mut *conn).await?;
            rls_conn = Some(conn);
            (TenantExecutor::Conn(&mut *rls_conn.as_mut().unwrap()), None)
        }
    };
    let entity = model.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "create") {
        return Err(AppError::BadRequest("create not allowed".into()));
    }
    let body = body_to_map(body)?;
    let body = hashmap_keys_to_snake_case(&body);
    RequestValidator::validate(&body, &entity.validation)?;
    let mut row = CrudService::create(&mut executor, &entity, &body, schema_override).await?;
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    Ok((axum::http::StatusCode::CREATED, Json(crate::response::SuccessOne { data: row, meta: None })))
}

pub async fn read_package(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    Path((package_id, path_segment, id_str)): Path<(String, String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), Some(&package_id)).await?;
    let model = get_or_load_package_model(&state, ctx.config_pool(), ctx.package_cache_key(), &package_id).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<PoolConnection<Postgres>> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool { pool, schema_override, .. } => (TenantExecutor::Pool(pool), schema_override.as_deref()),
        TenantContext::Rls { tenant_id, pool, .. } => {
            let mut conn = pool.acquire().await?;
            sqlx::query("SET LOCAL app.tenant_id = $1").bind(tenant_id).execute(&mut *conn).await?;
            rls_conn = Some(conn);
            (TenantExecutor::Conn(&mut *rls_conn.as_mut().unwrap()), None)
        }
    };
    let entity = model.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "read") {
        return Err(AppError::BadRequest("read not allowed".into()));
    }
    let id = parse_id(&id_str, &entity.pk_type)?;
    let mut row = CrudService::read(&mut executor, &entity, &id, schema_override).await?.ok_or_else(|| AppError::NotFound(id_str.clone()))?;
    let include_names: Vec<String> = params.get("include").map(|s| s.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()).unwrap_or_default();
    if !include_names.is_empty() {
        let resolved = resolve_includes(&model, &entity, &include_names)?;
        let mut rows = [row];
        attach_includes(&mut executor, schema_override, &entity, &mut rows, &resolved).await?;
        row = rows[0].clone();
    }
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    Ok((axum::http::StatusCode::OK, Json(crate::response::SuccessOne { data: row, meta: None })))
}

pub async fn update_package(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    Path((package_id, path_segment, id_str)): Path<(String, String, String)>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), Some(&package_id)).await?;
    let model = get_or_load_package_model(&state, ctx.config_pool(), ctx.package_cache_key(), &package_id).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<PoolConnection<Postgres>> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool { pool, schema_override, .. } => (TenantExecutor::Pool(pool), schema_override.as_deref()),
        TenantContext::Rls { tenant_id, pool, .. } => {
            let mut conn = pool.acquire().await?;
            sqlx::query("SET LOCAL app.tenant_id = $1").bind(tenant_id).execute(&mut *conn).await?;
            rls_conn = Some(conn);
            (TenantExecutor::Conn(&mut *rls_conn.as_mut().unwrap()), None)
        }
    };
    let entity = model.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "update") {
        return Err(AppError::BadRequest("update not allowed".into()));
    }
    let id = parse_id(&id_str, &entity.pk_type)?;
    let body = body_to_map(body)?;
    let body = hashmap_keys_to_snake_case(&body);
    RequestValidator::validate(&body, &entity.validation)?;
    let mut row = CrudService::update(&mut executor, &entity, &id, &body, schema_override).await?.ok_or_else(|| AppError::NotFound(id_str))?;
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    Ok((axum::http::StatusCode::OK, Json(crate::response::SuccessOne { data: row, meta: None })))
}

pub async fn delete_package(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    Path((package_id, path_segment, id_str)): Path<(String, String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), Some(&package_id)).await?;
    let model = get_or_load_package_model(&state, ctx.config_pool(), ctx.package_cache_key(), &package_id).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<PoolConnection<Postgres>> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool { pool, schema_override, .. } => (TenantExecutor::Pool(pool), schema_override.as_deref()),
        TenantContext::Rls { tenant_id, pool, .. } => {
            let mut conn = pool.acquire().await?;
            sqlx::query("SET LOCAL app.tenant_id = $1").bind(tenant_id).execute(&mut *conn).await?;
            rls_conn = Some(conn);
            (TenantExecutor::Conn(&mut *rls_conn.as_mut().unwrap()), None)
        }
    };
    let entity = model.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "delete") {
        return Err(AppError::BadRequest("delete not allowed".into()));
    }
    let id = parse_id(&id_str, &entity.pk_type)?;
    CrudService::delete(&mut executor, &entity, &id, schema_override).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

pub async fn bulk_create_package(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    Path((package_id, path_segment)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), Some(&package_id)).await?;
    let model = get_or_load_package_model(&state, ctx.config_pool(), ctx.package_cache_key(), &package_id).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<PoolConnection<Postgres>> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool { pool, schema_override, .. } => (TenantExecutor::Pool(pool), schema_override.as_deref()),
        TenantContext::Rls { tenant_id, pool, .. } => {
            let mut conn = pool.acquire().await?;
            sqlx::query("SET LOCAL app.tenant_id = $1").bind(tenant_id).execute(&mut *conn).await?;
            rls_conn = Some(conn);
            (TenantExecutor::Conn(&mut *rls_conn.as_mut().unwrap()), None)
        }
    };
    let entity = model.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "bulk_create") {
        return Err(AppError::BadRequest("bulk_create not allowed".into()));
    }
    let items: Vec<HashMap<String, Value>> = match body {
        Value::Array(arr) => {
            let mut out = Vec::new();
            for v in arr {
                out.push(hashmap_keys_to_snake_case(&body_to_map(v)?));
            }
            out
        }
        _ => return Err(AppError::BadRequest("body must be a JSON array".into())),
    };
    for item in &items {
        RequestValidator::validate(item, &entity.validation)?;
    }
    let mut rows = CrudService::bulk_create(&mut executor, &entity, &items, schema_override).await?;
    for row in &mut rows {
        strip_sensitive_columns(row, &entity.sensitive_columns);
        value_keys_to_camel_case(row);
    }
    let count = rows.len() as u64;
    Ok((axum::http::StatusCode::CREATED, Json(crate::response::SuccessMany { data: rows, meta: crate::response::MetaCount { count } })))
}

pub async fn bulk_update_package(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    Path((package_id, path_segment)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), Some(&package_id)).await?;
    let model = get_or_load_package_model(&state, ctx.config_pool(), ctx.package_cache_key(), &package_id).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<PoolConnection<Postgres>> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool { pool, schema_override, .. } => (TenantExecutor::Pool(pool), schema_override.as_deref()),
        TenantContext::Rls { tenant_id, pool, .. } => {
            let mut conn = pool.acquire().await?;
            sqlx::query("SET LOCAL app.tenant_id = $1").bind(tenant_id).execute(&mut *conn).await?;
            rls_conn = Some(conn);
            (TenantExecutor::Conn(&mut *rls_conn.as_mut().unwrap()), None)
        }
    };
    let entity = model.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "bulk_update") {
        return Err(AppError::BadRequest("bulk_update not allowed".into()));
    }
    let items: Vec<HashMap<String, Value>> = match body {
        Value::Array(arr) => {
            let mut out = Vec::new();
            for v in arr {
                out.push(hashmap_keys_to_snake_case(&body_to_map(v)?));
            }
            out
        }
        _ => return Err(AppError::BadRequest("body must be a JSON array".into())),
    };
    for item in &items {
        RequestValidator::validate(item, &entity.validation)?;
    }
    let mut rows = CrudService::bulk_update(&mut executor, &entity, &items, schema_override).await?;
    for row in &mut rows {
        strip_sensitive_columns(row, &entity.sensitive_columns);
        value_keys_to_camel_case(row);
    }
    let count = rows.len() as u64;
    Ok((axum::http::StatusCode::OK, Json(crate::response::SuccessMany { data: rows, meta: crate::response::MetaCount { count } })))
}
