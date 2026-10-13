//! View status and materialized-view refresh.
//!
//! A materialized view stores its result until refreshed. A refresh recomputes it in the
//! caller's database: a shared (RLS) database holds every tenant's rows, so refreshing it updates
//! them all; a Database-strategy tenant refreshes its own. The last refresh per database is kept
//! in `_sys_kv_data` under [`VIEW_REFRESH_NAMESPACE`].
//!
//!   GET  /views/:view_id                              status (default package)
//!   POST /views/:view_id/refresh                      refresh (default package)
//!   GET  /package/:package_id/views/:view_id          status
//!   POST /package/:package_id/views/:view_id/refresh  refresh
//!   POST /views/refresh-stale                         refresh every stale view everywhere
//!
//! Single views are authorized via authrs: resource `service:{svc}/package:{pkg}/view:{id}`,
//! actions `get` and `refresh`. `refresh-stale` is restricted to the Platform Admin tenant; it is
//! meant to be called by one scheduler job (e.g. a workflow-engine cron schedule every minute),
//! and refreshes each materialized view whose `refresh.max_age_secs` has passed, in every
//! installed package and every database.

use crate::config::{load_from_pool, resolve, ResolvedModel, ResolvedView};
use crate::db::pool::Pool;
use crate::error::AppError;
use crate::extractors::tenant::{ActAsTenant, TenantId};
use crate::extractors::user::UserId;
use crate::handlers::entity::{get_or_load_package_model, resolve_tenant_context, TenantContext};
use crate::handlers::package::ddl_targets;
use crate::handlers::queries::fnv1a_hex;
use crate::migration::refresh_materialized_view;
use crate::state::AppState;
use crate::store::{list_package_ids, reserved_kv_get, reserved_kv_put, VIEW_REFRESH_NAMESPACE};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};

/// Key for the refresh status of one database: a tenant's own database (`tenant:<id>`), the
/// central shared database (`shared`), or a dedicated shared database (`shared:<hash of its
/// URL>`, so credentials are never stored).
pub(crate) fn refresh_scope_for(rls: bool, tenant_id: &str, database_url: Option<&str>) -> String {
    match (rls, database_url) {
        (false, _) => format!("tenant:{}", tenant_id),
        (true, Some(url)) => format!("shared:{}", fnv1a_hex(url)),
        (true, None) => "shared".to_string(),
    }
}

fn refresh_scope(state: &AppState, ctx: &TenantContext, tenant_id: &str) -> String {
    match ctx {
        TenantContext::Pool { .. } => refresh_scope_for(false, tenant_id, None),
        TenantContext::Rls { tenant_id, .. } => refresh_scope_for(
            true,
            tenant_id,
            state
                .tenant_registry
                .get(tenant_id)
                .and_then(|e| e.database_url.as_deref()),
        ),
    }
}

/// Refresh `view` in the database `pool` points at and record the outcome under `scope`.
/// The attempt is recorded before it runs, so a concurrent stale-refresh run skips the view. A
/// failed refresh keeps the previous successful one on record next to the error. Returns the
/// stored record, or the refresh error after recording it.
async fn refresh_and_record(
    state: &AppState,
    pool: &Pool,
    config_pool: &Pool,
    view: &ResolvedView,
    rls: bool,
    scope: &str,
) -> Result<Value, AppError> {
    let previous = reserved_kv_get(
        config_pool,
        VIEW_REFRESH_NAMESPACE,
        scope,
        &view.package_id,
        &view.id,
    )
    .await?;
    let kept = |field: &str| previous.as_ref().and_then(|p| p.get(field)).cloned();
    let started = Utc::now();
    let put = |record: Value| async move {
        reserved_kv_put(
            config_pool,
            state.dialect.as_ref(),
            VIEW_REFRESH_NAMESPACE,
            scope,
            &view.package_id,
            &view.id,
            &record,
        )
        .await
    };
    put(json!({
        "refreshedAt": kept("refreshedAt"),
        "durationMs": kept("durationMs"),
        "lastAttemptAt": started,
        "lastError": kept("lastError"),
    }))
    .await?;

    let clock = std::time::Instant::now();
    let outcome = refresh_materialized_view(pool, view, rls, state.dialect.as_ref()).await;
    let duration_ms = clock.elapsed().as_millis() as u64;
    let record = match &outcome {
        Ok(()) => json!({
            "refreshedAt": started,
            "durationMs": duration_ms,
            "lastAttemptAt": started,
            "lastError": Value::Null,
        }),
        Err(e) => json!({
            "refreshedAt": kept("refreshedAt"),
            "durationMs": kept("durationMs"),
            "lastAttemptAt": started,
            "lastError": e.to_string(),
        }),
    };
    put(record.clone()).await?;
    outcome.map(|()| record)
}

/// After a failed or in-progress attempt, a stale view is not retried for this long, so a broken
/// view is not hammered every minute and overlapping stale-refresh runs do not double up.
const RETRY_AFTER_SECS: i64 = 60;

/// Whether a view with refresh status `record` is due for a stale refresh at `now`: its last
/// successful refresh is missing or older than `max_age_secs`, and no attempt that has not
/// (yet) succeeded — in progress or failed — started within [`RETRY_AFTER_SECS`].
fn is_stale(record: Option<&Value>, max_age_secs: u64, now: DateTime<Utc>) -> bool {
    let time = |field: &str| {
        record
            .and_then(|r| r.get(field))
            .and_then(Value::as_str)
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|t| t.with_timezone(&Utc))
    };
    let refreshed = time("refreshedAt");
    let attempted = time("lastAttemptAt");
    let max_age = chrono::Duration::seconds(i64::try_from(max_age_secs).unwrap_or(i64::MAX));
    let expired = refreshed.map_or(true, |t| now - t >= max_age);
    // A successful refresh records the same instant for both fields.
    let unfinished_attempt = attempted.filter(|a| Some(*a) != refreshed);
    let backing_off =
        unfinished_attempt.is_some_and(|t| now - t < chrono::Duration::seconds(RETRY_AFTER_SECS));
    expired && !backing_off
}

async fn view_context(
    state: &AppState,
    tenant_id: Option<&str>,
    act_as: Option<&str>,
    package_id: Option<&str>,
    view_id: &str,
) -> Result<(TenantContext, ResolvedView), AppError> {
    let ctx = resolve_tenant_context(state, tenant_id, act_as, package_id).await?;
    let model: ResolvedModel = match package_id {
        Some(pkg) => {
            get_or_load_package_model(state, ctx.config_pool(), ctx.package_cache_key(), pkg)
                .await?
        }
        None => state
            .model
            .read()
            .map_err(|_| AppError::BadRequest("state lock".into()))?
            .clone(),
    };
    let view = model
        .view(view_id)
        .cloned()
        .ok_or_else(|| AppError::NotFound(format!("view not found: {}", view_id)))?;
    Ok((ctx, view))
}

async fn status(
    state: AppState,
    tenant_id: Option<String>,
    act_as: Option<String>,
    user_id: Option<String>,
    package_id: Option<String>,
    view_id: String,
) -> Result<impl IntoResponse, AppError> {
    let (ctx, view) = view_context(
        &state,
        tenant_id.as_deref(),
        act_as.as_deref(),
        package_id.as_deref(),
        &view_id,
    )
    .await?;
    crate::authrs::check_view_permission_opt(
        &state.authrs_client,
        tenant_id.as_deref(),
        user_id.as_deref(),
        &view,
        "get",
    )
    .await?;
    let last_refresh = if view.materialized {
        let scope = refresh_scope(&state, &ctx, tenant_id.as_deref().unwrap_or(""));
        reserved_kv_get(
            ctx.config_pool(),
            VIEW_REFRESH_NAMESPACE,
            &scope,
            &view.package_id,
            &view.id,
        )
        .await?
    } else {
        None
    };
    Ok((
        StatusCode::OK,
        Json(json!({
            "data": {
                "id": view.id,
                "name": view.name,
                "schema": view.schema_name,
                "packageId": view.package_id,
                "materialized": view.materialized,
                "lastRefresh": last_refresh,
            }
        })),
    ))
}

async fn refresh(
    state: AppState,
    tenant_id: Option<String>,
    act_as: Option<String>,
    user_id: Option<String>,
    package_id: Option<String>,
    view_id: String,
) -> Result<impl IntoResponse, AppError> {
    let (ctx, view) = view_context(
        &state,
        tenant_id.as_deref(),
        act_as.as_deref(),
        package_id.as_deref(),
        &view_id,
    )
    .await?;
    crate::authrs::check_view_permission_opt(
        &state.authrs_client,
        tenant_id.as_deref(),
        user_id.as_deref(),
        &view,
        "refresh",
    )
    .await?;
    if !view.materialized {
        return Err(AppError::BadRequest(format!(
            "view '{}' is not materialized; it always reads live data",
            view.id
        )));
    }

    let scope = refresh_scope(&state, &ctx, tenant_id.as_deref().unwrap_or(""));
    let record = refresh_and_record(
        &state,
        ctx.migration_pool(),
        ctx.config_pool(),
        &view,
        ctx.rls_tenant_id().is_some(),
        &scope,
    )
    .await?;

    Ok((
        StatusCode::OK,
        Json(json!({
            "data": {
                "viewId": view.id,
                "refreshedAt": record["refreshedAt"],
                "durationMs": record["durationMs"],
            }
        })),
    ))
}

/// POST /views/refresh-stale — Platform Admin only. Refreshes every materialized view whose
/// `refresh.max_age_secs` has passed, in every installed package and every database (the shared
/// databases and each Database-strategy tenant's). Views without `refresh` are left to on-demand
/// refreshes. One failure does not stop the run; it is reported and recorded on the view.
pub async fn refresh_stale_views(
    TenantId(tenant_id): TenantId,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, AppError> {
    let tenant_id = tenant_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("X-Tenant-ID header is required".into()))?;
    if tenant_id != crate::tenant::platform_tenant_id() {
        return Err(AppError::Forbidden(
            "refreshing stale views across all databases is restricted to the Platform Admin tenant"
                .into(),
        ));
    }

    let mut scheduled: Vec<ResolvedView> = Vec::new();
    for package_id in list_package_ids(&state.pool).await? {
        let config = load_from_pool(&state.pool, &package_id)
            .await
            .map_err(AppError::Config)?;
        let model = resolve(&config)
            .map_err(AppError::Config)?
            .with_package_id(&package_id);
        let mut views: Vec<ResolvedView> = model
            .views
            .into_values()
            .filter(|v| v.materialized && v.max_age_secs.is_some())
            .collect();
        views.sort_by(|a, b| a.id.cmp(&b.id));
        scheduled.extend(views);
    }

    let mut refreshed: Vec<Value> = Vec::new();
    let mut failed: Vec<Value> = Vec::new();
    // Fresh, or backing off after an unfinished attempt.
    let mut skipped = 0usize;
    if !scheduled.is_empty() {
        for target in ddl_targets(&state).await {
            let pool = match &target.pool {
                Ok(p) => p,
                Err(e) => {
                    failed.push(json!({ "target": target.name, "error": e }));
                    continue;
                }
            };
            for view in &scheduled {
                let entry = |extra: Value| {
                    let mut v = json!({
                        "packageId": view.package_id,
                        "viewId": view.id,
                        "target": target.name,
                    });
                    if let (Some(obj), Value::Object(more)) = (v.as_object_mut(), extra) {
                        obj.extend(more);
                    }
                    v
                };
                let record = reserved_kv_get(
                    &state.pool,
                    VIEW_REFRESH_NAMESPACE,
                    &target.refresh_scope,
                    &view.package_id,
                    &view.id,
                )
                .await?;
                if !is_stale(
                    record.as_ref(),
                    view.max_age_secs.unwrap_or(u64::MAX),
                    Utc::now(),
                ) {
                    skipped += 1;
                    continue;
                }
                match refresh_and_record(
                    &state,
                    pool,
                    &state.pool,
                    view,
                    target.rls_column.is_some(),
                    &target.refresh_scope,
                )
                .await
                {
                    Ok(r) => refreshed.push(entry(json!({ "durationMs": r["durationMs"] }))),
                    Err(e) => {
                        tracing::warn!(view = %view.id, target = %target.name, error = %e, "stale view refresh failed");
                        failed.push(entry(json!({ "error": e.to_string() })))
                    }
                }
            }
        }
    }

    Ok((
        StatusCode::OK,
        Json(json!({
            "data": { "refreshed": refreshed, "failed": failed },
            "meta": {
                "refreshed": refreshed.len(),
                "failed": failed.len(),
                "skipped": skipped,
            }
        })),
    ))
}

/// GET /views/:view_id
pub async fn get_view_status(
    TenantId(tenant_id): TenantId,
    ActAsTenant(act_as): ActAsTenant,
    UserId(user_id): UserId,
    State(state): State<AppState>,
    Path(view_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    status(state, tenant_id, act_as, user_id, None, view_id).await
}

/// POST /views/:view_id/refresh
pub async fn refresh_view(
    TenantId(tenant_id): TenantId,
    ActAsTenant(act_as): ActAsTenant,
    UserId(user_id): UserId,
    State(state): State<AppState>,
    Path(view_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    refresh(state, tenant_id, act_as, user_id, None, view_id).await
}

/// GET /package/:package_id/views/:view_id
pub async fn get_package_view_status(
    TenantId(tenant_id): TenantId,
    ActAsTenant(act_as): ActAsTenant,
    UserId(user_id): UserId,
    State(state): State<AppState>,
    Path((package_id, view_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    status(state, tenant_id, act_as, user_id, Some(package_id), view_id).await
}

/// POST /package/:package_id/views/:view_id/refresh
pub async fn refresh_package_view(
    TenantId(tenant_id): TenantId,
    ActAsTenant(act_as): ActAsTenant,
    UserId(user_id): UserId,
    State(state): State<AppState>,
    Path((package_id, view_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    refresh(state, tenant_id, act_as, user_id, Some(package_id), view_id).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs_ago: i64) -> String {
        (Utc::now() - chrono::Duration::seconds(secs_ago)).to_rfc3339()
    }

    #[test]
    fn never_refreshed_is_stale() {
        assert!(is_stale(None, 3600, Utc::now()));
    }

    #[test]
    fn staleness_follows_the_last_successful_refresh() {
        let fresh = json!({ "refreshedAt": at(100), "lastAttemptAt": at(100) });
        let old = json!({ "refreshedAt": at(4000), "lastAttemptAt": at(4000) });
        assert!(!is_stale(Some(&fresh), 3600, Utc::now()));
        assert!(is_stale(Some(&old), 3600, Utc::now()));
    }

    #[test]
    fn a_recent_unfinished_attempt_backs_off() {
        // Failed (or still running) 10s ago: not retried yet.
        let recent = json!({ "refreshedAt": at(4000), "lastAttemptAt": at(10), "lastError": "x" });
        assert!(!is_stale(Some(&recent), 3600, Utc::now()));
        // The same failure two minutes ago: retried.
        let older = json!({ "refreshedAt": at(4000), "lastAttemptAt": at(120), "lastError": "x" });
        assert!(is_stale(Some(&older), 3600, Utc::now()));
        // Never succeeded and just tried: backs off too.
        let first = json!({ "refreshedAt": null, "lastAttemptAt": at(5) });
        assert!(!is_stale(Some(&first), 3600, Utc::now()));
    }

    #[test]
    fn scopes_never_contain_the_database_url() {
        assert_eq!(refresh_scope_for(false, "acme", None), "tenant:acme");
        assert_eq!(refresh_scope_for(true, "acme", None), "shared");
        let scope = refresh_scope_for(true, "acme", Some("postgres://u:secret@h/db"));
        assert!(scope.starts_with("shared:") && !scope.contains("secret"));
    }
}
