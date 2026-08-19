//! Decision-hub event publishing. Active only when DECISION_HUB_URL env var is set.
//!
//! After a successful CRUD operation the handler calls `spawn_events()` which
//! evaluates configured triggers against the saved row and fires matching events
//! to the decision-hub `/evaluate` endpoint inside a detached tokio task — the
//! HTTP response is already on the wire before the publish begins.
//!
//! Event type format: `{package_id}.{table_name}:{event_name}`
//! Example: `manufacturing_core.materials:published`

use crate::config::resolved::ResolvedEntity;
use crate::config::types::{EntityEventTrigger, EventCondition};
use serde_json::Value;
use std::sync::Arc;

pub struct DecisionHubClient {
    base_url: String,
    client: reqwest::Client,
}

impl DecisionHubClient {
    pub fn from_env() -> Option<Arc<Self>> {
        let base_url = std::env::var("DECISION_HUB_URL").ok()?;
        let timeout_secs: u64 = std::env::var("DECISION_HUB_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .build()
            .ok()?;
        tracing::info!(url = %base_url, "decision-hub event publishing enabled");
        Some(Arc::new(Self { base_url, client }))
    }

    async fn publish(&self, tenant_id: &str, event_type: &str, context: Value) {
        let payload = serde_json::json!({
            "tenant_id": tenant_id,
            "event_type": event_type,
            "context": context,
        });
        let url = format!("{}/evaluate", self.base_url);
        log_curl(&url, &payload);
        match self.client.post(&url).json(&payload).send().await {
            Ok(resp) if !resp.status().is_success() => {
                let status = resp.status().as_u16();
                let body = resp.text().await.unwrap_or_default();
                tracing::warn!(
                    event_type = %event_type,
                    status = %status,
                    body = %body,
                    "decision-hub rejected event"
                );
            }
            Err(e) => {
                tracing::warn!(event_type = %event_type, error = %e, "decision-hub publish failed");
            }
            Ok(resp) => {
                // /evaluate answers 200 with {request_id, executions, matched} even when nothing
                // matched — log the body so a silent no-op is visible without a DB query.
                let body = resp.text().await.unwrap_or_default();
                tracing::info!(
                    event_type = %event_type,
                    response = %body,
                    "decision-hub event accepted"
                );
            }
        }
    }
}

/// Emit the outbound request as a replayable curl. Off by default: set
/// `DECISION_HUB_LOG_CURL=1` to log it at info, or enable
/// `RUST_LOG=architect_sdk::events=debug` to get it at debug.
///
/// The payload has already had `sensitive_columns` stripped, but it still carries full row data —
/// keep this off in production unless you are actively debugging.
fn log_curl(url: &str, payload: &Value) {
    let force = std::env::var("DECISION_HUB_LOG_CURL")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE"))
        .unwrap_or(false);
    if !force && !tracing::enabled!(tracing::Level::DEBUG) {
        return;
    }
    let body = serde_json::to_string(payload).unwrap_or_default();
    // Single-quoted shell literal: the only byte needing care is `'` itself.
    let curl = format!(
        "curl -sS -X POST '{}' -H 'Content-Type: application/json' --data-raw '{}'",
        url,
        body.replace('\'', r#"'\''"#),
    );
    if force {
        tracing::info!(curl = %curl, "decision-hub request");
    } else {
        tracing::debug!(curl = %curl, "decision-hub request");
    }
}

/// Returns true when the trigger's condition is satisfied.
///
/// `row` is the post-operation snake_case row (new state).
/// `pre_update_row` is the row fetched from DB *before* the update — only supplied for the
/// "update" lifecycle when the entity has `changed_to` conditions. When present, `changed_to`
/// requires a genuine transition: the field must have been a different value before the update.
fn evaluate_condition(
    condition: &EventCondition,
    row: &Value,
    pre_update_row: Option<&Value>,
) -> bool {
    let new_val = row.get(&condition.field);
    if let Some(target) = &condition.changed_to {
        let now_matches = new_val == Some(target);
        return match pre_update_row {
            // With old state: require old ≠ target AND new == target (real transition).
            Some(old_row) => now_matches && old_row.get(&condition.field) != Some(target),
            // Without old state: fall back to checking the new value only.
            None => now_matches,
        };
    }
    if let Some(target) = &condition.equals {
        return new_val == Some(target);
    }
    if let Some(not_null) = condition.not_null {
        let is_not_null = matches!(new_val, Some(v) if !v.is_null());
        return is_not_null == not_null;
    }
    true
}

fn default_event_name(on: &str) -> &str {
    match on {
        "create" => "created",
        "update" => "updated",
        "delete" => "deleted",
        "archive" => "archived",
        other => other,
    }
}

/// Check whether a trigger matches the current lifecycle + row state.
fn trigger_matches(
    trigger: &EntityEventTrigger,
    lifecycle: &str,
    raw_row: &Value,
    archive_field: Option<&str>,
    pre_update_row: Option<&Value>,
) -> bool {
    match trigger.on.as_str() {
        on if on == lifecycle => {
            if let Some(cond) = &trigger.condition {
                evaluate_condition(cond, raw_row, pre_update_row)
            } else {
                true
            }
        }
        // "archive" triggers fire during an update when archive_field transitions to non-null.
        "archive" if lifecycle == "update" => archive_field
            .and_then(|f| raw_row.get(f))
            .map(|v| !v.is_null())
            .unwrap_or(false),
        _ => false,
    }
}

/// Spawn a background task that publishes matching event triggers to decision-hub.
///
/// - `lifecycle`: `"create"` | `"update"` | `"delete"`
/// - `raw_row`: snake_case row used for condition evaluation (post-operation state)
/// - `api_row`: camelCase row sent as the event context (sensitive columns already stripped)
/// - `pre_update_row`: snake_case row fetched from DB *before* the update; pass `Some` for the
///   "update" lifecycle when `changed_to` conditions are present so transitions are detected
///   accurately. `None` for create/delete or when no `changed_to` conditions exist.
///
/// Returns immediately; the HTTP publish happens after the response is sent.
pub fn spawn_events(
    client: Arc<DecisionHubClient>,
    entity: &ResolvedEntity,
    lifecycle: &'static str,
    raw_row: Value,
    api_row: Value,
    tenant_id: String,
    pre_update_row: Option<Value>,
) {
    spawn_events_with(
        client,
        entity,
        lifecycle,
        raw_row,
        api_row,
        tenant_id,
        pre_update_row,
        None,
    );
}

/// Everything the publish task needs to re-read the row with its related entities expanded.
///
/// Owned rather than borrowed: the fetch happens inside the detached task, after the handler's
/// executor (and any RLS transaction) is gone.
pub struct EventIncludeCtx {
    pub pool: crate::db::pool::Pool,
    /// `Some(tenant)` for RLS-strategy tenants — a fresh transaction with `SET LOCAL` is opened
    /// for the fetch. `None` for database-strategy tenants.
    pub rls_tenant: Option<String>,
    pub schema_override: Option<String>,
    pub dialect: Arc<dyn crate::db::Dialect>,
    pub entity: ResolvedEntity,
    /// Every include named by any trigger on this entity, already resolved. Each trigger picks its
    /// own subset by name.
    pub resolved: Vec<(String, crate::config::IncludeSpec, ResolvedEntity)>,
    pub pk_column: String,
    /// Primary-key value of the affected row, rendered for an RSQL `==` leaf.
    pub pk_value: String,
}

/// As [`spawn_events`], plus the context needed to honour each trigger's `include` list.
#[allow(clippy::too_many_arguments)]
pub fn spawn_events_with(
    client: Arc<DecisionHubClient>,
    entity: &ResolvedEntity,
    lifecycle: &'static str,
    raw_row: Value,
    api_row: Value,
    tenant_id: String,
    pre_update_row: Option<Value>,
    include_ctx: Option<EventIncludeCtx>,
) {
    if entity.events.is_empty() {
        return;
    }

    let triggers: Vec<EntityEventTrigger> = entity
        .events
        .iter()
        .filter(|t| {
            trigger_matches(
                t,
                lifecycle,
                &raw_row,
                entity.archive_field.as_deref(),
                pre_update_row.as_ref(),
            )
        })
        .cloned()
        .collect();

    if triggers.is_empty() {
        return;
    }

    let package_id = entity.package_id.clone();
    let table_name = entity.table_name.clone();

    tokio::spawn(async move {
        // Cache expansions across triggers: several triggers on one entity usually name the same
        // includes, and a delete has no row left to read.
        let mut expanded: std::collections::HashMap<String, Value> =
            std::collections::HashMap::new();

        for trigger in &triggers {
            let suffix = trigger
                .event_name
                .as_deref()
                .unwrap_or_else(|| default_event_name(trigger.on.as_str()));
            let event_type = format!("{}.{}:{}", package_id, table_name, suffix);
            tracing::info!(
                tenant_id = %tenant_id,
                event_type = %event_type,
                lifecycle = %lifecycle,
                "publishing decision-hub event"
            );

            let entity_value = match (&include_ctx, trigger.include.is_empty(), lifecycle) {
                // Nothing requested, no context wired up, or the row is already gone.
                (_, true, _) | (None, _, _) | (_, _, "delete") => api_row.clone(),
                (Some(ctx), false, _) => {
                    let mut names = trigger.include.clone();
                    names.sort();
                    names.dedup();
                    let key = names.join(",");
                    match expanded.get(&key) {
                        Some(v) => v.clone(),
                        None => {
                            let v = fetch_with_includes(ctx, &names)
                                .await
                                .unwrap_or_else(|| api_row.clone());
                            expanded.insert(key, v.clone());
                            v
                        }
                    }
                }
            };

            let context = serde_json::json!({
                "entity": entity_value,
                "operation": lifecycle,
            });
            client.publish(&tenant_id, &event_type, context).await;
        }
    });
}

/// Re-read the affected row with `names` expanded. Returns `None` on any failure — the caller
/// falls back to the flat row, so a broken include never costs the event itself.
async fn fetch_with_includes(ctx: &EventIncludeCtx, names: &[String]) -> Option<Value> {
    use crate::service::CrudService;
    use crate::sql::{FilterNode, IncludeSelect, RsqlOp};

    let selected: Vec<&(String, crate::config::IncludeSpec, ResolvedEntity)> = ctx
        .resolved
        .iter()
        .filter(|(name, _, _)| names.iter().any(|n| n == name))
        .collect();

    for want in names {
        if !selected.iter().any(|(name, _, _)| name == want) {
            tracing::warn!(
                entity = %ctx.entity.path_segment,
                include = %want,
                "event include is not a configured relationship — skipped"
            );
        }
    }
    if selected.is_empty() {
        return None;
    }

    let include_selects: Vec<IncludeSelect> = selected
        .iter()
        .map(|(name, spec, related)| IncludeSelect {
            name: name.as_str(),
            direction: spec.direction.clone(),
            related,
            our_key: spec.our_key_column.as_str(),
            their_key: spec.their_key_column.as_str(),
        })
        .collect();

    let filter = FilterNode::Leaf {
        field: ctx.pk_column.clone(),
        op: RsqlOp::Eq,
        values: vec![ctx.pk_value.clone()],
    };

    // RLS tenants need their own transaction here: the handler's has already been committed.
    let mut rls_tx = match &ctx.rls_tenant {
        Some(tenant) => {
            let mut tx = ctx.pool.begin().await.ok()?;
            if let Some(sql) = ctx.dialect.set_tenant_session_sql(tenant) {
                sqlx::query(&sql).execute(&mut *tx).await.ok()?;
            }
            Some(tx)
        }
        None => None,
    };
    let mut executor = match rls_tx.as_mut() {
        Some(tx) => crate::service::TenantExecutor::conn(tx, ctx.dialect.as_ref()),
        None => crate::service::TenantExecutor::pool(&ctx.pool, ctx.dialect.as_ref()),
    };

    let rows = CrudService::list_with_includes(
        &mut executor,
        &ctx.entity,
        Some(&filter),
        &[],
        Some(1),
        None,
        include_selects.as_slice(),
        &[],
        ctx.schema_override.as_deref(),
        ctx.dialect.as_ref(),
        None,
    )
    .await;

    let mut rows = match rows {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                entity = %ctx.entity.path_segment,
                error = %e,
                "event include fetch failed — publishing the flat row"
            );
            return None;
        }
    };

    let owned: Vec<(String, crate::config::IncludeSpec, ResolvedEntity)> =
        selected.into_iter().cloned().collect();
    crate::handlers::entity::post_process_include_columns(&mut rows, &owned);

    let mut row = rows.into_iter().next()?;
    crate::handlers::entity::strip_sensitive_columns(&mut row, &ctx.entity.sensitive_columns);
    for (name, _, related) in &owned {
        // Related rows carry their own sensitive-column list; they are keyed by include name.
        if let Some(nested) = row.get_mut(name) {
            strip_nested_sensitive(nested, &related.sensitive_columns);
        }
    }
    crate::case::value_keys_to_camel_case(&mut row);
    Some(row)
}

fn strip_nested_sensitive(v: &mut Value, sensitive: &std::collections::HashSet<String>) {
    match v {
        Value::Array(items) => {
            for item in items {
                crate::handlers::entity::strip_sensitive_columns(item, sensitive);
            }
        }
        Value::Object(_) => crate::handlers::entity::strip_sensitive_columns(v, sensitive),
        _ => {}
    }
}
