//! Asset handlers: standalone presigned URL generation.

use crate::error::AppError;
use crate::extractors::tenant::TenantId;
use crate::state::AppState;
use axum::{
    extract::{Query, State},
    Json,
};
use std::collections::HashMap;

pub async fn sign_asset(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    // Require X-Tenant-ID for auth parity with entity routes.
    let _tenant_id = tenant_id_opt
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("X-Tenant-ID header is required".into()))?;

    let path = params
        .get("path")
        .ok_or_else(|| AppError::BadRequest("query param 'path' is required".into()))?;

    let expires: u64 = params
        .get("expires")
        .and_then(|s| s.parse().ok())
        .unwrap_or(900);

    let storage = state
        .storage
        .as_ref()
        .ok_or_else(|| AppError::BadRequest("storage is not configured (set STORAGE_PROVIDER env var)".into()))?;

    let result = storage.presign_url(path, expires).await?;

    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessOne {
            data: serde_json::json!({
                "url": result.url,
                "expires_at": result.expires_at.to_rfc3339(),
                "expires_in": result.expires_in,
            }),
            meta: None,
        }),
    ))
}
