//! Entity CRUD routes built from resolved model.

use crate::handlers::entity::{
    bulk_create, bulk_update, create, delete as delete_handler, read, update,
};
use crate::state::AppState;
use axum::{routing::get, routing::post, Router};

pub fn entity_routes(state: AppState) -> Router {
    let entities = state.model.entities.clone();
    let mut router = Router::new();
    for entity in &entities {
        let path_segment = entity.path_segment.clone();
        router = router
            .route(&format!("/{}", path_segment), post(create))
            .route(&format!("/{}/bulk", path_segment), post(bulk_create).patch(bulk_update))
            .route(
                &format!("/{}/:id", path_segment),
                get(read).patch(update).delete(delete_handler),
            );
    }
    router.with_state(state)
}
