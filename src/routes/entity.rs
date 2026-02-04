//! Entity CRUD routes built from resolved model.
//! Uses parameterized paths so Path extractors receive the segment and id; handlers resolve the entity by path.
//! Unprefixed routes use the default/active model; /module/:module_id/... use that module's model (same entity names, different modules).

use crate::handlers::entity::{
    bulk_create, bulk_create_module, bulk_update, bulk_update_module, create, create_module,
    delete as delete_handler, delete_module, list, list_module, read, read_module, update,
    update_module,
};
use crate::state::AppState;
use axum::{routing::get, routing::post, Router};

pub fn entity_routes(state: AppState) -> Router {
    Router::new()
        .route("/:path_segment", get(list).post(create))
        .route("/:path_segment/bulk", post(bulk_create).patch(bulk_update))
        .route(
            "/:path_segment/:id",
            get(read).patch(update).delete(delete_handler),
        )
        .route("/module/:module_id/:path_segment", get(list_module).post(create_module))
        .route(
            "/module/:module_id/:path_segment/bulk",
            post(bulk_create_module).patch(bulk_update_module),
        )
        .route(
            "/module/:module_id/:path_segment/:id",
            get(read_module).patch(update_module).delete(delete_module),
        )
        .with_state(state)
}
