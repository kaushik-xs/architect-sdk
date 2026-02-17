//! Config ingestion routes: POST and GET per config kind, plus package install.

use crate::handlers::config::{
    get_api_entities, get_columns, get_enums, get_indexes, get_kv_stores, get_relationships, get_schemas, get_tables,
    post_api_entities, post_columns, post_enums, post_indexes, post_kv_stores, post_relationships, post_schemas, post_tables,
};
use crate::handlers::package::install_package;
use crate::state::AppState;
use axum::{routing::post, Router};

pub fn config_routes(state: AppState) -> Router {
    Router::new()
        .route("/config/package", post(install_package))
        .route("/config/schemas", post(post_schemas).get(get_schemas))
        .route("/config/enums", post(post_enums).get(get_enums))
        .route("/config/tables", post(post_tables).get(get_tables))
        .route("/config/columns", post(post_columns).get(get_columns))
        .route("/config/indexes", post(post_indexes).get(get_indexes))
        .route("/config/relationships", post(post_relationships).get(get_relationships))
        .route("/config/api_entities", post(post_api_entities).get(get_api_entities))
        .route("/config/kv_stores", post(post_kv_stores).get(get_kv_stores))
        .with_state(state)
}
