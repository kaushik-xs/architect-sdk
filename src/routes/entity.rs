//! Entity CRUD routes built from resolved model.
//! Uses parameterized paths so Path extractors receive the segment and id; handlers resolve the entity by path.
//! Unprefixed routes use the default/active model; /package/:package_id/... use that package's model (same entity names, different packages).

use crate::handlers::asset::sign_asset;
use crate::handlers::entity::{
    archive, archive_package, bulk_create, bulk_create_package, bulk_update, bulk_update_package,
    create, create_graph, create_graph_package, create_package, delete as delete_handler,
    delete_package, list, list_history, list_package, read, read_history_version, read_package,
    unarchive, unarchive_package, update, update_package,
};
use crate::handlers::extensible_fields::{
    apply_indexes_handler, apply_indexes_package, delete_registry_handler, delete_registry_package,
    get_indexes, get_indexes_package, get_registry, get_registry_package, put_registry,
    put_registry_package,
};
use crate::handlers::kv::{kv_delete, kv_get, kv_list_keys, kv_put};
use crate::state::AppState;
use axum::{routing::get, routing::post, Router};

pub fn entity_routes(state: AppState) -> Router {
    Router::new()
        // /assets/sign must be declared before /:path_segment to avoid being captured.
        .route("/assets/sign", get(sign_asset))
        .route("/:path_segment", get(list).post(create))
        .route("/:path_segment/bulk", post(bulk_create).patch(bulk_update))
        // Static second segment — takes precedence over /:path_segment/:id (like /bulk).
        .route("/:path_segment/graph", post(create_graph))
        .route(
            "/:path_segment/extensible-fields",
            get(get_registry)
                .put(put_registry)
                .delete(delete_registry_handler),
        )
        .route(
            "/:path_segment/extensible-fields/indexes",
            get(get_indexes).post(apply_indexes_handler),
        )
        .route(
            "/:path_segment/:id",
            get(read).patch(update).delete(delete_handler),
        )
        .route("/:path_segment/:id/archive", post(archive))
        .route("/:path_segment/:id/unarchive", post(unarchive))
        .route("/:path_segment/:id/history", get(list_history))
        .route(
            "/:path_segment/:id/history/:version",
            get(read_history_version),
        )
        .route("/package/:package_id/kv/:namespace", get(kv_list_keys))
        .route(
            "/package/:package_id/kv/:namespace/:key",
            get(kv_get).put(kv_put).delete(kv_delete),
        )
        .route(
            "/package/:package_id/:path_segment",
            get(list_package).post(create_package),
        )
        .route(
            "/package/:package_id/:path_segment/bulk",
            post(bulk_create_package).patch(bulk_update_package),
        )
        .route(
            "/package/:package_id/:path_segment/graph",
            post(create_graph_package),
        )
        .route(
            "/package/:package_id/:path_segment/extensible-fields",
            get(get_registry_package)
                .put(put_registry_package)
                .delete(delete_registry_package),
        )
        .route(
            "/package/:package_id/:path_segment/extensible-fields/indexes",
            get(get_indexes_package).post(apply_indexes_package),
        )
        .route(
            "/package/:package_id/:path_segment/:id",
            get(read_package)
                .patch(update_package)
                .delete(delete_package),
        )
        .route(
            "/package/:package_id/:path_segment/:id/archive",
            post(archive_package),
        )
        .route(
            "/package/:package_id/:path_segment/:id/unarchive",
            post(unarchive_package),
        )
        .with_state(state)
}

#[cfg(test)]
mod route_tests {
    use axum::{routing::get, Router};

    async fn noop() -> &'static str {
        ""
    }

    /// matchit panics at build time on conflicting routes. This proves the static
    /// `extensible-fields` segment coexists with the `:id` param segment (same pattern as `bulk`).
    #[test]
    fn extensible_fields_route_coexists_with_id_route() {
        let _router: Router = Router::new()
            .route("/:path_segment", get(noop))
            .route("/:path_segment/bulk", get(noop))
            .route("/:path_segment/extensible-fields", get(noop))
            .route("/:path_segment/extensible-fields/indexes", get(noop))
            .route("/:path_segment/:id", get(noop))
            .route("/:path_segment/:id/archive", get(noop))
            .route("/package/:package_id/:path_segment", get(noop))
            .route("/package/:package_id/:path_segment/bulk", get(noop))
            .route(
                "/package/:package_id/:path_segment/extensible-fields",
                get(noop),
            )
            .route(
                "/package/:package_id/:path_segment/extensible-fields/indexes",
                get(noop),
            )
            .route("/package/:package_id/:path_segment/:id", get(noop));
    }
}
