//! Config ingestion handlers: POST/GET for each config type.

use crate::config::{load_from_pool, resolve};
use crate::error::AppError;
use crate::migration::apply_migrations;
use crate::state::AppState;
use crate::store::{qualified_sys_table, replace_config_rows, sys_table_for_kind};
use axum::extract::State;
use axum::Json;
use serde_json::Value;
use sqlx::PgPool;

/// Replace config for one kind. Returns (body, num_rows_written).
/// When run_migrations is true and num_rows_written > 0, loads full config and runs migrations (validates config; requires all dependent configs already in DB).
/// When run_migrations is false (e.g. plugin install), only writes to _sys_*; caller must run migrations once after all kinds are applied.
pub(crate) async fn replace_config(
    pool: &PgPool,
    kind: &str,
    body: Vec<Value>,
    run_migrations: bool,
) -> Result<(Vec<Value>, u64), AppError> {
    let table = sys_table_for_kind(kind).ok_or_else(|| AppError::BadRequest(format!("unknown config kind: {}", kind)))?;
    let mut tx = pool.begin().await?;
    let (count, _version) = replace_config_rows(&mut tx, table, &body).await?;
    tx.commit().await?;
    if run_migrations && count > 0 {
        let config = load_from_pool(pool).await.map_err(AppError::Config)?;
        apply_migrations(pool, &config).await?;
    }
    Ok((body, count))
}

/// Reload in-memory model from DB so new/updated entities are available without restart.
pub(crate) async fn reload_model(state: &AppState) -> Result<(), AppError> {
    let config = load_from_pool(&state.pool).await.map_err(AppError::Config)?;
    let new_model = resolve(&config).map_err(AppError::Config)?;
    let mut guard = state.model.write().map_err(|_| AppError::BadRequest("state lock".into()))?;
    *guard = new_model;
    Ok(())
}

async fn get_config(pool: &PgPool, kind: &str) -> Result<Vec<Value>, AppError> {
    let table = sys_table_for_kind(kind).ok_or_else(|| AppError::BadRequest(format!("unknown config kind: {}", kind)))?;
    let q_table = qualified_sys_table(table);
    let rows = sqlx::query_scalar::<_, Value>(&format!("SELECT payload FROM {} ORDER BY id", q_table))
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

macro_rules! config_handler {
    ($method:ident, $kind:expr) => {
        pub async fn $method(
            State(state): State<AppState>,
            Json(body): Json<Vec<Value>>,
        ) -> Result<impl axum::response::IntoResponse, AppError> {
            let (out, num_written) = replace_config(&state.pool, $kind, body, true).await?;
            if num_written > 0 {
                reload_model(&state).await?;
            }
            let count = out.len() as u64;
            Ok((
                axum::http::StatusCode::OK,
                Json(crate::response::SuccessMany {
                    data: out,
                    meta: crate::response::MetaCount { count },
                }),
            ))
        }
    };
}

macro_rules! get_config_handler {
    ($method:ident, $kind:expr) => {
        pub async fn $method(
            State(state): State<AppState>,
        ) -> Result<impl axum::response::IntoResponse, AppError> {
            let out = get_config(&state.pool, $kind).await?;
            Ok((
                axum::http::StatusCode::OK,
                Json(crate::response::SuccessMany {
                    data: out.clone(),
                    meta: crate::response::MetaCount {
                        count: out.len() as u64,
                    },
                }),
            ))
        }
    };
}

config_handler!(post_schemas, "schemas");
config_handler!(post_enums, "enums");
config_handler!(post_tables, "tables");
config_handler!(post_columns, "columns");
config_handler!(post_indexes, "indexes");
config_handler!(post_relationships, "relationships");
config_handler!(post_api_entities, "api_entities");

get_config_handler!(get_schemas, "schemas");
get_config_handler!(get_enums, "enums");
get_config_handler!(get_tables, "tables");
get_config_handler!(get_columns, "columns");
get_config_handler!(get_indexes, "indexes");
get_config_handler!(get_relationships, "relationships");
get_config_handler!(get_api_entities, "api_entities");
