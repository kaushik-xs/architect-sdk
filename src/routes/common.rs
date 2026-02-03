//! Common routes: health, readiness, version.

use crate::state::AppState;
use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;

#[derive(Serialize)]
struct HealthBody {
    status: &'static str,
}

#[derive(Serialize)]
struct ReadyBody {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    database: Option<&'static str>,
}

async fn health() -> Json<HealthBody> {
    Json(HealthBody { status: "ok" })
}

async fn ready(State(state): State<AppState>) -> Result<Json<ReadyBody>, (axum::http::StatusCode, Json<ReadyBody>)> {
    if sqlx::query("SELECT 1").fetch_optional(&state.pool).await.is_err() {
        return Err((
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadyBody {
                status: "degraded",
                database: Some("unavailable"),
            }),
        ));
    }
    Ok(Json(ReadyBody {
        status: "ok",
        database: Some("ok"),
    }))
}

async fn version() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "name": env!("CARGO_PKG_NAME"),
        "version": env!("CARGO_PKG_VERSION")
    }))
}

/// Common routes (no state): GET /health, GET /version, GET /info.
pub fn common_routes() -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/info", get(version))
}

/// Common routes including readiness with DB check. Requires AppState.
pub fn common_routes_with_ready(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/version", get(version))
        .route("/info", get(version))
        .with_state(state)
}
