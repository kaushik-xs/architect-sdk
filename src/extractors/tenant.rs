//! Extract tenant id from request (e.g. X-Tenant-ID header).

use async_trait::async_trait;
use axum::{
    extract::FromRequestParts,
    http::request::Parts,
};

/// Header name for tenant id. Default: `X-Tenant-ID`.
pub const TENANT_ID_HEADER: &str = "X-Tenant-ID";

/// Extractor for optional tenant id from `X-Tenant-ID` header.
#[derive(Clone, Debug)]
pub struct TenantId(pub Option<String>);

#[async_trait]
impl<S> FromRequestParts<S> for TenantId
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let value = parts
            .headers
            .get(TENANT_ID_HEADER)
            .and_then(|v: &axum::http::HeaderValue| v.to_str().ok())
            .map(|s: &str| s.trim().to_string())
            .filter(|s: &String| !s.is_empty());
        Ok(TenantId(value))
    }
}
