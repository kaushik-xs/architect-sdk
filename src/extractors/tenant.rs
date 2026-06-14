//! Extract tenant id from request (e.g. X-Tenant-ID header).

use async_trait::async_trait;
use axum::{extract::FromRequestParts, http::request::Parts};

/// Header name for tenant id. Default: `X-Tenant-ID`.
pub const TENANT_ID_HEADER: &str = "X-Tenant-ID";

/// Header name for Platform-Admin tenant impersonation. Default: `X-Act-As-Tenant`.
pub const ACT_AS_TENANT_HEADER: &str = "X-Act-As-Tenant";

/// Read, trim, and non-empty-filter a header value.
fn header_value(parts: &Parts, name: &str) -> Option<String> {
    parts
        .headers
        .get(name)
        .and_then(|v: &axum::http::HeaderValue| v.to_str().ok())
        .map(|s: &str| s.trim().to_string())
        .filter(|s: &String| !s.is_empty())
}

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
        Ok(TenantId(header_value(parts, TENANT_ID_HEADER)))
    }
}

/// Extractor for the optional Platform-Admin impersonation header `X-Act-As-Tenant`. When set by the
/// Platform Admin, the request runs as the named tenant (see `resolve_tenant_context`). Ignored for
/// non-admin callers (rejected there with 403).
#[derive(Clone, Debug)]
pub struct ActAsTenant(pub Option<String>);

#[async_trait]
impl<S> FromRequestParts<S> for ActAsTenant
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(ActAsTenant(header_value(parts, ACT_AS_TENANT_HEADER)))
    }
}
