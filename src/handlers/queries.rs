//! Read-only saved-query handlers.
//!
//! A *query* is a config kind (`_sys_queries`) holding trusted, parameterized SQL. Registration
//! is restricted to the Platform Admin tenant; execution is authorized per-run via authrs (same
//! model as generated entity APIs) and runs inside a sandboxed, read-only transaction:
//! `SET TRANSACTION READ ONLY` + `statement_timeout` + an SDK-enforced row cap, plus RLS
//! `app.tenant_id` for RLS tenants and an optional `SET LOCAL ROLE` to a dedicated read-only role.
//!
//! Registration/CRUD of query definitions lives here too (`POST /config/queries`,
//! `PUT`/`DELETE /config/queries/:id`); queries carry no DDL so these are pure metadata writes.

use crate::case::value_keys_to_camel_case;
use crate::config::{compile_query, QueryConfig, ResolvedQuery};
use crate::error::AppError;
use crate::extractors::tenant::{ActAsTenant, TenantId};
use crate::extractors::user::UserId;
use crate::handlers::config::{get_config, reload_model, replace_config};
use crate::handlers::entity::{resolve_tenant_context, TenantContext};
use crate::service::{CrudService, RequestValidator, TenantExecutor};
use crate::state::AppState;
use crate::store::DEFAULT_PACKAGE_ID;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;

/// `statement_timeout` (ms) applied to saved queries. Env `ARCHITECT_QUERY_TIMEOUT_MS`, default 30000.
fn query_timeout_ms() -> u64 {
    const DEFAULT: u64 = 30_000;
    std::env::var("ARCHITECT_QUERY_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT)
}

/// SDK-enforced maximum rows returned by a query. Env `ARCHITECT_QUERY_MAX_ROWS`, default 10000.
fn query_max_rows() -> usize {
    const DEFAULT: usize = 10_000;
    std::env::var("ARCHITECT_QUERY_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT)
}

/// Optional dedicated read-only DB role for query execution. Env `ARCHITECT_QUERY_ROLE`.
fn query_role() -> Option<String> {
    std::env::var("ARCHITECT_QUERY_ROLE")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Whether result caching is enabled globally. Env `ARCHITECT_QUERY_CACHE` in {1,true,yes,on}.
fn query_cache_enabled() -> bool {
    std::env::var("ARCHITECT_QUERY_CACHE")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Default cache TTL (seconds) when a query does not specify its own. Env
/// `ARCHITECT_QUERY_CACHE_TTL_SECS`, default 300.
fn query_cache_default_ttl() -> i64 {
    const DEFAULT: i64 = 300;
    std::env::var("ARCHITECT_QUERY_CACHE_TTL_SECS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|&n| n >= 0)
        .unwrap_or(DEFAULT)
}

/// Effective TTL for a query: its own `cache_ttl_secs` override, else the global default.
/// Returns `None` when caching is disabled globally or the effective TTL is 0 (never cache).
fn effective_cache_ttl(query: &ResolvedQuery) -> Option<i64> {
    if !query_cache_enabled() {
        return None;
    }
    let ttl = query.cache_ttl_secs.unwrap_or_else(query_cache_default_ttl);
    if ttl > 0 {
        Some(ttl)
    } else {
        None
    }
}

/// 64-bit FNV-1a hash rendered as 16 hex chars — a bounded, deterministic cache key. Collisions are
/// astronomically unlikely and, because the cached envelope records the exact query/tenant/params,
/// a collision fails verification and is treated as a miss (never a wrong hit).
fn fnv1a_hex(s: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{:016x}", hash)
}

/// Deterministic string over the merged params, with keys sorted, for the cache key.
fn canonical_params(params: &HashMap<String, Value>) -> String {
    let sorted: std::collections::BTreeMap<&String, &Value> = params.iter().collect();
    serde_json::to_string(&sorted).unwrap_or_default()
}

/// Build the cache key from query identity, effective tenant, SQL (so a definition change
/// invalidates), and the merged params.
fn cache_key_for(
    query: &ResolvedQuery,
    tenant_id: &str,
    params: &HashMap<String, Value>,
) -> String {
    let material = format!(
        "{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}",
        query.package_id,
        tenant_id,
        query.id,
        query.sql,
        canonical_params(params),
    );
    fnv1a_hex(&material)
}

#[derive(Deserialize, Default)]
pub struct RunQueryRequest {
    #[serde(default)]
    pub params: HashMap<String, Value>,
}

/// Registration/mutation of query definitions is restricted to the Platform Admin tenant, since
/// the query body is trusted SQL executed verbatim.
fn require_platform_admin(
    state: &AppState,
    tenant_id_opt: &Option<String>,
) -> Result<(), AppError> {
    let tenant_id = tenant_id_opt
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("X-Tenant-ID header is required".into()))?;
    state
        .tenant_registry
        .get(tenant_id)
        .ok_or_else(|| AppError::NotFound(format!("tenant not found: {}", tenant_id)))?;
    if tenant_id != crate::tenant::platform_tenant_id() {
        return Err(AppError::Forbidden(
            "query registration is restricted to the Platform Admin tenant".into(),
        ));
    }
    Ok(())
}

/// Begin a read-only, sandboxed transaction on the tenant's data pool. Works for both tenant
/// strategies (unlike `begin_rls_tx`, which only opens a tx for RLS). Applies, in order:
/// `SET TRANSACTION READ ONLY`, `statement_timeout`, an optional read-only `ROLE`, and — for RLS
/// tenants — `SET LOCAL app.tenant_id` so row-level policies enforce isolation regardless of SQL.
async fn begin_readonly_tx(
    state: &AppState,
    ctx: &TenantContext,
) -> Result<crate::db::pool::DbTransaction, AppError> {
    let pool = ctx.migration_pool();
    let mut tx = pool.begin().await?;
    if let Some(sql) = state.dialect.set_read_only_sql() {
        sqlx::query(&sql).execute(&mut *tx).await?;
    }
    if let Some(sql) = state.dialect.set_statement_timeout_sql(query_timeout_ms()) {
        sqlx::query(&sql).execute(&mut *tx).await?;
    }
    if let Some(role) = query_role() {
        if let Some(sql) = state.dialect.set_role_sql(&role) {
            sqlx::query(&sql).execute(&mut *tx).await?;
        }
    }
    if let TenantContext::Rls { tenant_id, .. } = ctx {
        if let Some(sql) = state.dialect.set_tenant_session_sql(tenant_id) {
            sqlx::query(&sql).execute(&mut *tx).await?;
        }
    }
    Ok(tx)
}

/// Clone the resolved query out of the active model (dropping the read lock before any await).
fn lookup_query(state: &AppState, query_id: &str) -> Result<ResolvedQuery, AppError> {
    let guard = state
        .model
        .read()
        .map_err(|_| AppError::BadRequest("state lock".into()))?;
    guard
        .query(query_id)
        .cloned()
        .ok_or_else(|| AppError::NotFound(format!("query not found: {}", query_id)))
}

/// Serialize a query's public metadata (never the SQL) for list/get responses.
fn query_metadata(r: &ResolvedQuery) -> Value {
    let params: Vec<Value> = r
        .param_order
        .iter()
        .map(|name| {
            let rule = r.rules.get(name);
            json!({
                "name": name,
                "required": rule.and_then(|x| x.required).unwrap_or(false),
                "default": r.defaults.get(name).cloned().unwrap_or(Value::Null),
                "db_type": r.casts.get(name).cloned(),
            })
        })
        .collect();
    json!({
        "id": r.id,
        "name": r.name,
        "description": r.description,
        "schemas": r.schemas,
        "params": params,
        "cache_ttl_secs": r.cache_ttl_secs,
    })
}

/// Validate a cached envelope against the current request and, if fresh and matching, build the
/// run response. Returns `None` (a miss) when the envelope is expired, malformed, or does not match
/// the exact query/tenant/params (guards against the astronomically unlikely key collision).
fn cache_hit_response(
    envelope: &Value,
    query: &ResolvedQuery,
    effective_tenant: &str,
    params: &HashMap<String, Value>,
) -> Option<Value> {
    let expires_at = envelope.get("expires_at").and_then(Value::as_str)?;
    let expires = chrono::DateTime::parse_from_rfc3339(expires_at).ok()?;
    if expires <= chrono::Utc::now() {
        return None;
    }
    if envelope.get("query_id").and_then(Value::as_str) != Some(query.id.as_str()) {
        return None;
    }
    if envelope.get("tenant_id").and_then(Value::as_str) != Some(effective_tenant) {
        return None;
    }
    if envelope.get("params").and_then(Value::as_str) != Some(canonical_params(params).as_str()) {
        return None;
    }
    let result = envelope
        .get("result")
        .cloned()
        .unwrap_or(Value::Array(vec![]));
    let count = envelope
        .get("row_count")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let truncated = envelope
        .get("truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Some(json!({
        "data": result,
        "meta": { "count": count, "query": query.id, "truncated": truncated, "cached": true },
    }))
}

/// POST /api/v1/queries/:query_id/run — execute a query and return its rows.
pub async fn run_query(
    Path(query_id): Path<String>,
    TenantId(tenant_id_opt): TenantId,
    ActAsTenant(act_as_opt): ActAsTenant,
    UserId(user_id_opt): UserId,
    State(state): State<AppState>,
    body: Option<Json<RunQueryRequest>>,
) -> Result<impl IntoResponse, AppError> {
    let req = body.map(|Json(b)| b).unwrap_or_default();

    let ctx = resolve_tenant_context(
        &state,
        tenant_id_opt.as_deref(),
        act_as_opt.as_deref(),
        None,
    )
    .await?;

    let query = lookup_query(&state, &query_id)?;

    crate::authrs::check_query_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &query,
        "run",
    )
    .await?;

    // Merge declared defaults for absent params, then validate the merged set.
    let mut params = req.params;
    for (name, default) in &query.defaults {
        params
            .entry(name.clone())
            .or_insert_with(|| default.clone());
    }
    RequestValidator::validate(&params, &query.rules)?;

    // Effective tenant (an act-as target when impersonating, else the caller) scopes the cache.
    let effective_tenant = act_as_opt
        .as_deref()
        .filter(|s| !s.is_empty())
        .or(tenant_id_opt.as_deref())
        .unwrap_or("")
        .to_string();

    // Cache lookup (when enabled): a fresh, verified envelope short-circuits execution.
    let ttl = effective_cache_ttl(&query);
    let cache_key = ttl.map(|_| cache_key_for(&query, &effective_tenant, &params));
    if let Some(key) = &cache_key {
        if let Some(env) = crate::store::query_cache_get(
            ctx.config_pool(),
            &effective_tenant,
            &query.package_id,
            key,
        )
        .await?
        {
            if let Some(resp) = cache_hit_response(&env, &query, &effective_tenant, &params) {
                return Ok((StatusCode::OK, Json(resp)));
            }
        }
    }

    // Bind values in positional order; a referenced-but-absent param binds NULL.
    let binds: Vec<Value> = query
        .param_order
        .iter()
        .map(|name| params.get(name).cloned().unwrap_or(Value::Null))
        .collect();

    // Wrap the trusted SQL in an outer LIMIT so the SDK caps result size; fetch one extra row to
    // detect truncation.
    let max_rows = query_max_rows();
    let inner = query.sql.trim().trim_end_matches(';');
    let wrapped = format!("SELECT * FROM ({}) AS _query LIMIT {}", inner, max_rows + 1);

    let mut tx = begin_readonly_tx(&state, &ctx).await?;
    let mut rows = {
        let mut exec = TenantExecutor::conn(&mut tx, state.dialect.as_ref());
        CrudService::run_readonly_query(&mut exec, &wrapped, &binds).await?
    };
    // Read-only tx: drop without commit (rolls back cleanly).
    drop(tx);

    let truncated = rows.len() > max_rows;
    if truncated {
        rows.truncate(max_rows);
    }
    for row in &mut rows {
        value_keys_to_camel_case(row);
    }
    let count = rows.len();

    // Populate the cache on a miss (best-effort: a cache write failure must not fail the request).
    if let (Some(key), Some(ttl)) = (&cache_key, ttl) {
        let expires_at = chrono::Utc::now() + chrono::Duration::seconds(ttl);
        let envelope = json!({
            "expires_at": expires_at.to_rfc3339(),
            "query_id": query.id,
            "tenant_id": effective_tenant,
            "params": canonical_params(&params),
            "result": rows,
            "row_count": count,
            "truncated": truncated,
        });
        if let Err(e) = crate::store::query_cache_put(
            ctx.config_pool(),
            state.dialect.as_ref(),
            &effective_tenant,
            &query.package_id,
            key,
            &envelope,
        )
        .await
        {
            tracing::warn!(query = %query.id, error = %e, "query cache write failed");
        }
    }

    Ok((
        StatusCode::OK,
        Json(json!({
            "data": rows,
            "meta": { "count": count, "query": query.id, "truncated": truncated, "cached": false },
        })),
    ))
}

/// GET /api/v1/queries — list query metadata (never the SQL).
pub async fn list_queries(
    TenantId(tenant_id_opt): TenantId,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, AppError> {
    // Validate the tenant (same requirement as other config/data routes).
    resolve_tenant_context(&state, tenant_id_opt.as_deref(), None, None).await?;
    let guard = state
        .model
        .read()
        .map_err(|_| AppError::BadRequest("state lock".into()))?;
    let mut data: Vec<Value> = guard.queries.values().map(query_metadata).collect();
    data.sort_by(|a, b| {
        a["id"]
            .as_str()
            .unwrap_or("")
            .cmp(b["id"].as_str().unwrap_or(""))
    });
    let count = data.len();
    Ok((
        StatusCode::OK,
        Json(json!({ "data": data, "meta": { "count": count } })),
    ))
}

/// GET /api/v1/queries/:query_id — metadata for one query.
pub async fn get_query(
    Path(query_id): Path<String>,
    TenantId(tenant_id_opt): TenantId,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, AppError> {
    resolve_tenant_context(&state, tenant_id_opt.as_deref(), None, None).await?;
    let query = lookup_query(&state, &query_id)?;
    Ok((
        StatusCode::OK,
        Json(json!({ "data": query_metadata(&query) })),
    ))
}

/// GET /api/v1/config/queries — raw stored query definitions (Platform Admin only, includes SQL).
pub async fn get_queries_config(
    TenantId(tenant_id_opt): TenantId,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, AppError> {
    require_platform_admin(&state, &tenant_id_opt)?;
    let out = get_config(&state.pool, "queries", DEFAULT_PACKAGE_ID).await?;
    let count = out.len();
    Ok((
        StatusCode::OK,
        Json(json!({ "data": out, "meta": { "count": count } })),
    ))
}

/// Compile + optionally EXPLAIN-validate a set of query definitions before persisting.
async fn validate_queries(
    state: &AppState,
    tenant_id_opt: &Option<String>,
    bodies: &[Value],
) -> Result<Vec<ResolvedQuery>, AppError> {
    let cfgs: Vec<QueryConfig> = serde_json::from_value(Value::Array(bodies.to_vec()))
        .map_err(|e| AppError::BadRequest(format!("invalid queries: {}", e)))?;
    let compiled: Vec<ResolvedQuery> = cfgs
        .iter()
        .map(compile_query)
        .collect::<Result<_, _>>()
        .map_err(AppError::Config)?;

    let needs_explain = compiled.iter().any(|r| r.validate_on_register);
    if needs_explain {
        let ctx = resolve_tenant_context(state, tenant_id_opt.as_deref(), None, None).await?;
        for r in &compiled {
            if !r.validate_on_register {
                continue;
            }
            let mut tx = begin_readonly_tx(state, &ctx).await?;
            let explain = format!("EXPLAIN {}", r.sql.trim().trim_end_matches(';'));
            let nulls = vec![Value::Null; r.param_order.len()];
            let mut exec = TenantExecutor::conn(&mut tx, state.dialect.as_ref());
            CrudService::run_readonly_query(&mut exec, &explain, &nulls)
                .await
                .map_err(|e| {
                    AppError::Validation(format!("query '{}' failed validation: {}", r.id, e))
                })?;
        }
    }
    Ok(compiled)
}

/// POST /api/v1/config/queries — replace the whole standalone (`_default`) query set.
pub async fn post_queries(
    TenantId(tenant_id_opt): TenantId,
    State(state): State<AppState>,
    Json(body): Json<Vec<Value>>,
) -> Result<impl IntoResponse, AppError> {
    require_platform_admin(&state, &tenant_id_opt)?;
    validate_queries(&state, &tenant_id_opt, &body).await?;

    let (out, num) = replace_config(
        &state.pool,
        "queries",
        body,
        false,
        DEFAULT_PACKAGE_ID,
        None,
    )
    .await?;
    if num > 0 {
        reload_model(&state).await?;
    }
    let count = out.len();
    Ok((
        StatusCode::OK,
        Json(json!({ "data": out, "meta": { "count": count } })),
    ))
}

/// PUT /api/v1/config/queries/:query_id — upsert a single standalone query by id
/// (read-merge-write, preserving the rest of the set).
pub async fn put_query_by_id(
    Path(query_id): Path<String>,
    TenantId(tenant_id_opt): TenantId,
    State(state): State<AppState>,
    Json(mut body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    require_platform_admin(&state, &tenant_id_opt)?;

    let obj = body
        .as_object_mut()
        .ok_or_else(|| AppError::BadRequest("query body must be a JSON object".into()))?;
    // Path id is authoritative.
    obj.insert("id".into(), Value::String(query_id.clone()));

    // Compile + EXPLAIN-validate this one query.
    validate_queries(&state, &tenant_id_opt, std::slice::from_ref(&body)).await?;

    // Merge into the current _default set.
    let current = get_config(&state.pool, "queries", DEFAULT_PACKAGE_ID).await?;
    let mut merged: Vec<Value> = current
        .into_iter()
        .filter(|r| r.get("id").and_then(Value::as_str) != Some(query_id.as_str()))
        .collect();
    merged.push(body.clone());

    let (_out, num) = replace_config(
        &state.pool,
        "queries",
        merged,
        false,
        DEFAULT_PACKAGE_ID,
        None,
    )
    .await?;
    if num > 0 {
        reload_model(&state).await?;
    }
    Ok((StatusCode::OK, Json(json!({ "data": body }))))
}

/// DELETE /api/v1/config/queries/:query_id — remove a single standalone query by id.
pub async fn delete_query_by_id(
    Path(query_id): Path<String>,
    TenantId(tenant_id_opt): TenantId,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, AppError> {
    require_platform_admin(&state, &tenant_id_opt)?;

    let current = get_config(&state.pool, "queries", DEFAULT_PACKAGE_ID).await?;
    let existed = current
        .iter()
        .any(|r| r.get("id").and_then(Value::as_str) == Some(query_id.as_str()));
    if !existed {
        return Err(AppError::NotFound(format!("query not found: {}", query_id)));
    }
    let merged: Vec<Value> = current
        .into_iter()
        .filter(|r| r.get("id").and_then(Value::as_str) != Some(query_id.as_str()))
        .collect();

    let (_out, num) = replace_config(
        &state.pool,
        "queries",
        merged,
        false,
        DEFAULT_PACKAGE_ID,
        None,
    )
    .await?;
    if num > 0 {
        reload_model(&state).await?;
    }
    Ok((
        StatusCode::OK,
        Json(json!({ "data": { "id": query_id, "deleted": true } })),
    ))
}
