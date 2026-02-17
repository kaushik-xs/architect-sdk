//! Entity CRUD routes built from resolved model.
//! Uses parameterized paths so Path extractors receive the segment and id; handlers resolve the entity by path.
//! Unprefixed routes use the default/active model; /package/:package_id/... use that package's model (same entity names, different packages).

use crate::handlers::entity::{
    bulk_create, bulk_create_package, bulk_update, bulk_update_package, create, create_package,
    delete as delete_handler, delete_package, list, list_package, read, read_package, update,
    update_package,
};
use crate::handlers::kv::{kv_delete, kv_get, kv_list_keys, kv_put};
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
        .route("/package/:package_id/kv/:namespace", get(kv_list_keys))
        .route(
            "/package/:package_id/kv/:namespace/:key",
            get(kv_get).put(kv_put).delete(kv_delete),
        )
        .route("/package/:package_id/:path_segment", get(list_package).post(create_package))
        .route(
            "/package/:package_id/:path_segment/bulk",
            post(bulk_create_package).patch(bulk_update_package),
        )
        .route(
            "/package/:package_id/:path_segment/:id",
            get(read_package).patch(update_package).delete(delete_package),
        )
        .with_state(state)
}
