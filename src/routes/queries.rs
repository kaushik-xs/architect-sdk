//! Query routes: read-only query execution plus query-definition CRUD.
//!
//! Data plane (any valid tenant, authorized per-run via authrs):
//!   GET  /queries                     list query metadata
//!   GET  /queries/:query_id          one query's metadata
//!   POST /queries/:query_id/run      execute a query
//!
//! Config plane (Platform Admin tenant only):
//!   GET    /config/queries            raw stored definitions (includes SQL)
//!   POST   /config/queries            replace the whole standalone query set
//!   PUT    /config/queries/:query_id upsert one query by id
//!   DELETE /config/queries/:query_id remove one query by id

use crate::handlers::queries::{
    delete_query_by_id, get_queries_config, get_query, list_queries, post_queries, put_query_by_id,
    run_query,
};
use crate::state::AppState;
use axum::{
    routing::{get, post, put},
    Router,
};

pub fn query_routes(state: AppState) -> Router {
    Router::new()
        .route("/queries", get(list_queries))
        .route("/queries/:query_id", get(get_query))
        .route("/queries/:query_id/run", post(run_query))
        .route(
            "/config/queries",
            get(get_queries_config).post(post_queries),
        )
        .route(
            "/config/queries/:query_id",
            put(put_query_by_id).delete(delete_query_by_id),
        )
        .with_state(state)
}
