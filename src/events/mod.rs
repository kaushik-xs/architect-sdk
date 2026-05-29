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
        match self.client.post(&url).json(&payload).send().await {
            Ok(resp) if !resp.status().is_success() => {
                tracing::warn!(
                    event_type = %event_type,
                    status = %resp.status().as_u16(),
                    "decision-hub rejected event"
                );
            }
            Err(e) => {
                tracing::warn!(event_type = %event_type, error = %e, "decision-hub publish failed");
            }
            Ok(_) => {
                tracing::info!(event_type = %event_type, "decision-hub event accepted");
            }
        }
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
            let context = serde_json::json!({
                "entity": api_row,
                "operation": lifecycle,
            });
            client.publish(&tenant_id, &event_type, context).await;
        }
    });
}
