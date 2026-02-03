//! Entity CRUD routes built from resolved model.
//! Uses parameterized paths so Path extractors receive the segment and id; handlers resolve the entity by path.

use crate::handlers::entity::{
    bulk_create, bulk_update, create, delete as delete_handler, list, read, update,
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
        .with_state(state)
}
