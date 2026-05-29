//! Authrs permission-check client. Active only when AUTHRS_URL and SERVICE_NAME env vars are set.
//!
//! Before each entity operation the handler calls `check_entity_permission_opt()`, which
//! posts to authrs `/admin/permissions/check` and returns Unauthorized if the user lacks the action.
//!
//! Resource format: `service:{SERVICE_NAME}/package:{package_id}/table:{table_name}`
//! Action format:   `{httpVerb}{PascalCaseTableName}` e.g. `getMaterials`, `postMaterials`

use crate::case::to_camel_case;
use crate::config::ResolvedEntity;
use crate::error::AppError;
use serde::Deserialize;
use std::sync::Arc;

pub struct AuthrsClient {
    base_url: String,
    service_name: String,
    client: reqwest::Client,
}

#[derive(Deserialize)]
struct CheckResponse {
    allowed: Option<bool>,
}

impl AuthrsClient {
    pub fn from_env() -> Option<Arc<Self>> {
        let base_url = std::env::var("AUTHRS_URL").ok()?;
        let service_name = std::env::var("SERVICE_NAME").ok()?;
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .ok()?;
        tracing::info!(url = %base_url, service = %service_name, "authrs permission checks enabled");
        Some(Arc::new(Self {
            base_url,
            service_name,
            client,
        }))
    }

    async fn check(
        &self,
        tenant_id: &str,
        user_id: &str,
        resource: &str,
        action: &str,
    ) -> Result<bool, AppError> {
        let url = format!("{}/admin/permissions/check", self.base_url);
        let body = serde_json::json!({
            "userId": user_id,
            "resource": resource,
            "action": action,
        });
        let resp = self
            .client
            .post(&url)
            .header("X-Tenant-ID", tenant_id)
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "authrs request failed");
                AppError::Unauthorized(format!("permission service unavailable: {}", e))
            })?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            tracing::error!(status, "authrs returned non-success status");
            return Err(AppError::Unauthorized(format!(
                "permission check failed with status {}",
                status
            )));
        }

        let check_resp: CheckResponse = resp.json().await.map_err(|e| {
            tracing::error!(error = %e, "authrs response parse failed");
            AppError::Unauthorized(format!("permission check response invalid: {}", e))
        })?;

        Ok(check_resp.allowed.unwrap_or(false))
    }
}

fn pascal_case(s: &str) -> String {
    let camel = to_camel_case(s);
    let mut chars = camel.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

/// Check entity permission against authrs. No-op when authrs is not configured (client_opt is None).
///
/// Requires `X-User-ID` header when authrs is configured; returns Unauthorized if missing.
/// Returns Unauthorized when the user lacks the required action on the derived resource.
pub async fn check_entity_permission_opt(
    client_opt: &Option<Arc<AuthrsClient>>,
    tenant_id: Option<&str>,
    user_id: Option<&str>,
    entity: &ResolvedEntity,
    http_verb: &str,
) -> Result<(), AppError> {
    let client = match client_opt {
        Some(c) => c,
        None => return Ok(()),
    };

    let user_id =
        user_id.ok_or_else(|| AppError::Unauthorized("X-User-ID header is required".into()))?;
    let tenant_id = tenant_id.unwrap_or("");

    let action = format!("{}{}", http_verb, pascal_case(&entity.table_name));
    let resource = format!(
        "service:{}/package:{}/table:{}",
        client.service_name, entity.package_id, entity.table_name
    );

    tracing::debug!(
        user_id = %user_id,
        resource = %resource,
        action = %action,
        "checking authrs permission"
    );

    let allowed = client.check(tenant_id, user_id, &resource, &action).await?;

    if allowed {
        tracing::info!(
            user_id = %user_id,
            tenant_id = %tenant_id,
            resource = %resource,
            action = %action,
            "permission granted"
        );
    } else {
        tracing::warn!(
            user_id = %user_id,
            tenant_id = %tenant_id,
            resource = %resource,
            action = %action,
            "permission denied"
        );
        return Err(AppError::Unauthorized(format!(
            "action '{}' not permitted on '{}'",
            action, resource
        )));
    }

    Ok(())
}
