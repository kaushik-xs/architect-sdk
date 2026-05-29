//! Extract user id from request (X-User-ID header).

use async_trait::async_trait;
use axum::{extract::FromRequestParts, http::request::Parts};

pub const USER_ID_HEADER: &str = "X-User-ID";

#[derive(Clone, Debug)]
pub struct UserId(pub Option<String>);

#[async_trait]
impl<S> FromRequestParts<S> for UserId
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let value = parts
            .headers
            .get(USER_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        Ok(UserId(value))
    }
}
