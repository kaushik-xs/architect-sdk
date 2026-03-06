//! Build OpenAPI spec from resolved model (api_entities + columns). Exposed at GET /spec.
//! Entity paths are generated dynamically from what exists in _sys_* tables: default model from state
//! and package-scoped paths by listing _sys_packages and loading each package's config from _sys_*.

use crate::case::to_camel_case;
use crate::config::{
    load_from_pool, resolve, KvStoreConfig, ResolvedEntity, ResolvedModel,
};
use crate::state::AppState;
use crate::store::list_package_ids;
use std::collections::HashMap;
use axum::extract::State;
use axum::Json;
use utoipa::openapi::path::{
    HttpMethod, Operation, OperationBuilder, Parameter, ParameterBuilder, ParameterIn,
    PathItemBuilder, PathsBuilder,
};
use utoipa::openapi::request_body::RequestBodyBuilder;
use utoipa::openapi::response::{Response, ResponsesBuilder};
use utoipa::openapi::schema::{ObjectBuilder, Schema, SchemaType, Type};
use utoipa::openapi::server::{ServerBuilder, ServerVariableBuilder};
use utoipa::openapi::{Content, Info, OpenApi, OpenApiBuilder, RefOr, Required};

/// Build server with URL `http://{host}:{port}` and variable defaults.
fn build_server() -> utoipa::openapi::server::Server {
    ServerBuilder::new()
        .url("http://{host}:{port}")
        .parameter(
            "host",
            ServerVariableBuilder::new()
                .default_value("localhost")
                .description(Some("API host")),
        )
        .parameter(
            "port",
            ServerVariableBuilder::new()
                .default_value("3000")
                .description(Some("API port")),
        )
        .build()
}

fn json_object_schema() -> Schema {
    Schema::Object(
        ObjectBuilder::new()
            .schema_type(SchemaType::new(Type::Object))
            .description(Some("JSON object; keys may be in camelCase (e.g. entity fields)."))
            .into(),
    )
}

fn json_array_of_objects_schema() -> Schema {
    Schema::Array(
        utoipa::openapi::schema::ArrayBuilder::new()
            .items(RefOr::T(json_object_schema()))
            .build()
            .into(),
    )
}

fn default_responses() -> ResponsesBuilder {
    ResponsesBuilder::new()
        .response("200", Response::new("OK"))
        .response("201", Response::new("Created"))
        .response("204", Response::new("No Content"))
        .response("400", Response::new("Bad Request"))
        .response("404", Response::new("Not Found"))
}

/// X-Tenant-ID header required for all config and entity APIs.
fn x_tenant_id_header() -> Parameter {
    ParameterBuilder::new()
        .name("X-Tenant-ID")
        .parameter_in(ParameterIn::Header)
        .required(Required::True)
        .description(Some(
            "Tenant id; must match a tenant in architect._sys_tenants (e.g. default-mode-1, default-mode-3).",
        ))
        .schema(Some(RefOr::T(Schema::Object(
            utoipa::openapi::schema::ObjectBuilder::new()
                .schema_type(SchemaType::new(Type::String))
                .into(),
        ))))
        .build()
}

fn list_operation(entity: &ResolvedEntity, op_suffix: &str) -> Operation {
    let mut params = vec![x_tenant_id_header(),
        ParameterBuilder::new()
            .name("limit")
            .parameter_in(ParameterIn::Query)
            .required(Required::False)
            .description(Some("Max number of items to return"))
            .schema(Some(RefOr::T(Schema::Object(
                utoipa::openapi::schema::ObjectBuilder::new()
                    .schema_type(SchemaType::new(Type::Integer))
                    .into(),
            ))))
            .build(),
        ParameterBuilder::new()
            .name("offset")
            .parameter_in(ParameterIn::Query)
            .required(Required::False)
            .description(Some("Number of items to skip"))
            .schema(Some(RefOr::T(Schema::Object(
                utoipa::openapi::schema::ObjectBuilder::new()
                    .schema_type(SchemaType::new(Type::Integer))
                    .into(),
            ))))
            .build(),
        ParameterBuilder::new()
            .name("include")
            .parameter_in(ParameterIn::Query)
            .required(Required::False)
            .description(Some("Comma-separated related entity path segments to include"))
            .schema(Some(RefOr::T(Schema::Object(
                utoipa::openapi::schema::ObjectBuilder::new()
                    .schema_type(SchemaType::new(Type::String))
                    .into(),
            ))))
            .build(),
    ];
    for col in &entity.columns {
        if entity.sensitive_columns.contains(&col.name) {
            continue;
        }
        let camel = to_camel_case(&col.name);
        let schema = match col.pg_type.as_deref().unwrap_or("").to_lowercase() {
            t if t.contains("int") || t.contains("serial") => Schema::Object(
                utoipa::openapi::schema::ObjectBuilder::new()
                    .schema_type(SchemaType::new(Type::Integer))
                    .into(),
            ),
            t if t.contains("bool") => Schema::Object(
                utoipa::openapi::schema::ObjectBuilder::new()
                    .schema_type(SchemaType::new(Type::Boolean))
                    .into(),
            ),
            t if t.contains("uuid") => Schema::Object(
                utoipa::openapi::schema::ObjectBuilder::new()
                    .schema_type(SchemaType::new(Type::String))
                    .format(Some(utoipa::openapi::schema::SchemaFormat::KnownFormat(
                        utoipa::openapi::schema::KnownFormat::Uuid,
                    )))
                    .into(),
            ),
            _ => Schema::Object(
                utoipa::openapi::schema::ObjectBuilder::new()
                    .schema_type(SchemaType::new(Type::String))
                    .into(),
            ),
        };
        params.push(
            ParameterBuilder::new()
                .name(camel)
                .parameter_in(ParameterIn::Query)
                .required(Required::False)
                .description(Some(format!("Filter by {}", col.name)))
                .schema(Some(RefOr::T(schema)))
                .build(),
        );
    }
    OperationBuilder::new()
        .summary(Some(format!("List {}", entity.path_segment)))
        .description(Some(format!(
            "List {} with optional filters, pagination (limit, offset), and includes.",
            entity.path_segment
        )))
        .operation_id(Some(format!("list_{}{}", entity.path_segment, op_suffix)))
        .parameters(Some(params))
        .responses(default_responses().build())
        .build()
}

fn create_operation(entity: &ResolvedEntity, op_suffix: &str) -> Operation {
    let body = RequestBodyBuilder::new()
        .description(Some(format!(
            "JSON object with {} fields (camelCase). PK may be omitted if DB default exists.",
            entity.path_segment
        )))
        .content("application/json", Content::new(Some(RefOr::T(json_object_schema()))))
        .required(Some(Required::True))
        .build();
    OperationBuilder::new()
        .summary(Some(format!("Create {}", entity.path_segment)))
        .description(Some(format!("Create a single {}", entity.path_segment)))
        .operation_id(Some(format!("create_{}{}", entity.path_segment, op_suffix)))
        .parameters(Some(vec![x_tenant_id_header()]))
        .request_body(Some(body))
        .responses(
            ResponsesBuilder::new()
                .response("201", Response::new("Created"))
                .response("400", Response::new("Bad Request"))
                .build(),
        )
        .build()
}

fn read_operation(entity: &ResolvedEntity, op_suffix: &str) -> Operation {
    let id_param = ParameterBuilder::new()
        .name("id")
        .parameter_in(ParameterIn::Path)
        .required(Required::True)
        .description(Some("Entity ID (UUID, integer, or text depending on table PK)"))
        .schema(Some(RefOr::T(Schema::Object(
            utoipa::openapi::schema::ObjectBuilder::new()
                .schema_type(SchemaType::new(Type::String))
                .into(),
        ))))
        .build();
    let include_param = ParameterBuilder::new()
        .name("include")
        .parameter_in(ParameterIn::Query)
        .required(Required::False)
        .description(Some("Comma-separated related entity path segments to include"))
        .schema(Some(RefOr::T(Schema::Object(
            utoipa::openapi::schema::ObjectBuilder::new()
                .schema_type(SchemaType::new(Type::String))
                .into(),
        ))))
        .build();
    OperationBuilder::new()
        .summary(Some(format!("Get {} by id", entity.path_segment)))
        .description(Some(format!("Get a single {} by id.", entity.path_segment)))
        .operation_id(Some(format!("read_{}{}", entity.path_segment, op_suffix)))
        .parameters(Some(vec![x_tenant_id_header(), id_param, include_param]))
        .responses(default_responses().build())
        .build()
}

fn update_operation(entity: &ResolvedEntity, op_suffix: &str) -> Operation {
    let id_param = ParameterBuilder::new()
        .name("id")
        .parameter_in(ParameterIn::Path)
        .required(Required::True)
        .description(Some("Entity ID"))
        .schema(Some(RefOr::T(Schema::Object(
            utoipa::openapi::schema::ObjectBuilder::new()
                .schema_type(SchemaType::new(Type::String))
                .into(),
        ))))
        .build();
    let body = RequestBodyBuilder::new()
        .description(Some("JSON object with fields to update (camelCase, partial)."))
        .content("application/json", Content::new(Some(RefOr::T(json_object_schema()))))
        .required(Some(Required::True))
        .build();
    OperationBuilder::new()
        .summary(Some(format!("Update {} by id", entity.path_segment)))
        .description(Some(format!("Update a single {} by id.", entity.path_segment)))
        .operation_id(Some(format!("update_{}{}", entity.path_segment, op_suffix)))
        .parameters(Some(vec![x_tenant_id_header(), id_param]))
        .request_body(Some(body))
        .responses(default_responses().build())
        .build()
}

fn delete_operation(entity: &ResolvedEntity, op_suffix: &str) -> Operation {
    let id_param = ParameterBuilder::new()
        .name("id")
        .parameter_in(ParameterIn::Path)
        .required(Required::True)
        .description(Some("Entity ID"))
        .schema(Some(RefOr::T(Schema::Object(
            utoipa::openapi::schema::ObjectBuilder::new()
                .schema_type(SchemaType::new(Type::String))
                .into(),
        ))))
        .build();
    OperationBuilder::new()
        .summary(Some(format!("Delete {} by id", entity.path_segment)))
        .description(Some(format!("Delete a single {} by id.", entity.path_segment)))
        .operation_id(Some(format!("delete_{}{}", entity.path_segment, op_suffix)))
        .parameters(Some(vec![x_tenant_id_header(), id_param]))
        .responses(
            ResponsesBuilder::new()
                .response("204", Response::new("No Content"))
                .response("400", Response::new("Bad Request"))
                .response("404", Response::new("Not Found"))
                .build(),
        )
        .build()
}

fn bulk_create_operation(entity: &ResolvedEntity, op_suffix: &str) -> Operation {
    let body = RequestBodyBuilder::new()
        .description(Some("JSON array of objects; each object has same shape as create body."))
        .content(
            "application/json",
            Content::new(Some(RefOr::T(json_array_of_objects_schema()))),
        )
        .required(Some(Required::True))
        .build();
    OperationBuilder::new()
        .summary(Some(format!("Bulk create {}", entity.path_segment)))
        .description(Some(format!("Create multiple {}.", entity.path_segment)))
        .operation_id(Some(format!("bulk_create_{}{}", entity.path_segment, op_suffix)))
        .parameters(Some(vec![x_tenant_id_header()]))
        .request_body(Some(body))
        .responses(
            ResponsesBuilder::new()
                .response("201", Response::new("Created"))
                .response("400", Response::new("Bad Request"))
                .build(),
        )
        .build()
}

fn bulk_update_operation(entity: &ResolvedEntity, op_suffix: &str) -> Operation {
    let body = RequestBodyBuilder::new()
        .description(Some(
            "JSON array of objects; each must include id and fields to update.",
        ))
        .content(
            "application/json",
            Content::new(Some(RefOr::T(json_array_of_objects_schema()))),
        )
        .required(Some(Required::True))
        .build();
    OperationBuilder::new()
        .summary(Some(format!("Bulk update {}", entity.path_segment)))
        .description(Some(format!("Update multiple {}.", entity.path_segment)))
        .operation_id(Some(format!("bulk_update_{}{}", entity.path_segment, op_suffix)))
        .parameters(Some(vec![x_tenant_id_header()]))
        .request_body(Some(body))
        .responses(default_responses().build())
        .build()
}

/// Add entity paths for one model to the builder. When `package_id` is None, paths are
/// `{base}/{path_segment}` (default model). When `Some(id)`, paths are `{base}/package/{id}/{path_segment}`.
fn add_entity_paths(
    mut builder: PathsBuilder,
    base: &str,
    model: &ResolvedModel,
    package_id: Option<&str>,
) -> PathsBuilder {
    let path_prefix = match package_id {
        None => base.to_string(),
        Some(pid) => format!("{}/package/{}", base, pid),
    };
    let op_suffix = package_id
        .map(|pid| format!("_package_{}", pid.replace('-', "_")))
        .unwrap_or_default();

    for entity in &model.entities {
        let seg = &entity.path_segment;
        let list_path = format!("{}/{}", path_prefix, seg);
        let by_id_path = format!("{}/{}/{{id}}", path_prefix, seg);
        let bulk_path = format!("{}/{}/bulk", path_prefix, seg);

        let has_list = entity.operations.iter().any(|o| o == "read");
        let has_create = entity.operations.iter().any(|o| o == "create");
        if has_list || has_create {
            let mut list_item = PathItemBuilder::new();
            if has_list {
                list_item = list_item.operation(HttpMethod::Get, list_operation(entity, &op_suffix));
            }
            if has_create {
                list_item =
                    list_item.operation(HttpMethod::Post, create_operation(entity, &op_suffix));
            }
            builder = builder.path(list_path, list_item.build());
        }

        let has_read = entity.operations.iter().any(|o| o == "read");
        let has_update = entity.operations.iter().any(|o| o == "update");
        let has_delete = entity.operations.iter().any(|o| o == "delete");
        if has_read || has_update || has_delete {
            let mut by_id_item = PathItemBuilder::new();
            if has_read {
                by_id_item =
                    by_id_item.operation(HttpMethod::Get, read_operation(entity, &op_suffix));
            }
            if has_update {
                by_id_item =
                    by_id_item.operation(HttpMethod::Patch, update_operation(entity, &op_suffix));
            }
            if has_delete {
                by_id_item =
                    by_id_item.operation(HttpMethod::Delete, delete_operation(entity, &op_suffix));
            }
            builder = builder.path(by_id_path, by_id_item.build());
        }

        let has_bulk_create = entity.operations.iter().any(|o| o == "bulk_create");
        let has_bulk_update = entity.operations.iter().any(|o| o == "bulk_update");
        if has_bulk_create || has_bulk_update {
            let mut bulk_item = PathItemBuilder::new();
            if has_bulk_create {
                bulk_item = bulk_item
                    .operation(HttpMethod::Post, bulk_create_operation(entity, &op_suffix));
            }
            if has_bulk_update {
                bulk_item = bulk_item
                    .operation(HttpMethod::Patch, bulk_update_operation(entity, &op_suffix));
            }
            builder = builder.path(bulk_path, bulk_item.build());
        }
    }
    builder
}

fn kv_list_keys_operation(package_id: &str, namespace: &str) -> Operation {
    OperationBuilder::new()
        .summary(Some(format!("List KV keys in namespace {}", namespace)))
        .description(Some(format!(
            "List all keys and values in package {} namespace {}.",
            package_id, namespace
        )))
        .operation_id(Some(format!(
            "kv_list_keys_package_{}_ns_{}",
            package_id.replace('-', "_"),
            namespace.replace('-', "_")
        )))
        .parameters(Some(vec![x_tenant_id_header()]))
        .responses(default_responses().build())
        .build()
}

fn kv_key_operations(package_id: &str, namespace: &str) -> (Operation, Operation, Operation) {
    let get_op = OperationBuilder::new()
        .summary(Some("Get KV value by key"))
        .description(Some(format!(
            "Get value for key in package {} namespace {}.",
            package_id, namespace
        )))
        .operation_id(Some(format!(
            "kv_get_package_{}_ns_{}",
            package_id.replace('-', "_"),
            namespace.replace('-', "_")
        )))
        .parameters(Some(vec![
            x_tenant_id_header(),
            ParameterBuilder::new()
                .name("key")
                .parameter_in(ParameterIn::Path)
                .required(Required::True)
                .description(Some("KV key"))
                .schema(Some(RefOr::T(Schema::Object(
                    utoipa::openapi::schema::ObjectBuilder::new()
                        .schema_type(SchemaType::new(Type::String))
                        .into(),
                ))))
                .build(),
        ]))
        .responses(default_responses().build())
        .build();

    let put_op = OperationBuilder::new()
        .summary(Some("Set KV value (upsert)"))
        .description(Some("Set or overwrite value for key. Body is arbitrary JSON."))
        .operation_id(Some(format!(
            "kv_put_package_{}_ns_{}",
            package_id.replace('-', "_"),
            namespace.replace('-', "_")
        )))
        .parameters(Some(vec![
            x_tenant_id_header(),
            ParameterBuilder::new()
                .name("key")
                .parameter_in(ParameterIn::Path)
                .required(Required::True)
                .schema(Some(RefOr::T(Schema::Object(
                    utoipa::openapi::schema::ObjectBuilder::new()
                        .schema_type(SchemaType::new(Type::String))
                        .into(),
                ))))
                .build(),
        ]))
        .request_body(Some(
            RequestBodyBuilder::new()
                .description(Some("JSON value (string, number, object, or array)"))
                .content("application/json", Content::new(Some(RefOr::T(json_object_schema()))))
                .required(Some(Required::True))
                .build(),
        ))
        .responses(
            ResponsesBuilder::new()
                .response("200", Response::new("OK"))
                .response("400", Response::new("Bad Request"))
                .build(),
        )
        .build();

    let delete_op = OperationBuilder::new()
        .summary(Some("Delete KV key"))
        .description(Some("Delete key. Returns 204 No Content."))
        .operation_id(Some(format!(
            "kv_delete_package_{}_ns_{}",
            package_id.replace('-', "_"),
            namespace.replace('-', "_")
        )))
        .parameters(Some(vec![
            x_tenant_id_header(),
            ParameterBuilder::new()
                .name("key")
                .parameter_in(ParameterIn::Path)
                .required(Required::True)
                .schema(Some(RefOr::T(Schema::Object(
                    utoipa::openapi::schema::ObjectBuilder::new()
                        .schema_type(SchemaType::new(Type::String))
                        .into(),
                ))))
                .build(),
        ]))
        .responses(
            ResponsesBuilder::new()
                .response("204", Response::new("No Content"))
                .response("404", Response::new("Not Found"))
                .build(),
        )
        .build();

    (get_op, put_op, delete_op)
}

/// Add KV store paths for each package's namespaces: list keys and get/put/delete by key.
fn add_kv_paths(
    mut builder: PathsBuilder,
    base: &str,
    package_kv_stores: &HashMap<String, Vec<KvStoreConfig>>,
) -> PathsBuilder {
    for (package_id, stores) in package_kv_stores {
        for store in stores {
            let namespace = &store.namespace;
            let list_path = format!("{}/package/{}/kv/{}", base, package_id, namespace);
            let key_path = format!("{}/package/{}/kv/{}/{{key}}", base, package_id, namespace);

            let list_item = PathItemBuilder::new()
                .operation(HttpMethod::Get, kv_list_keys_operation(package_id, namespace));
            builder = builder.path(list_path, list_item.build());

            let (get_op, put_op, delete_op) = kv_key_operations(package_id, namespace);
            let key_item = PathItemBuilder::new()
                .operation(HttpMethod::Get, get_op)
                .operation(HttpMethod::Put, put_op)
                .operation(HttpMethod::Delete, delete_op);
            builder = builder.path(key_path, key_item.build());
        }
    }
    builder
}

/// Build full OpenAPI spec for entity APIs: default model paths plus package-scoped paths for each package,
/// plus KV store paths per package namespace.
pub fn build_spec(
    default_model: &ResolvedModel,
    base_path: &str,
    package_models: &HashMap<String, ResolvedModel>,
    package_kv_stores: &HashMap<String, Vec<KvStoreConfig>>,
) -> OpenApi {
    let server = build_server();
    let mut builder = PathsBuilder::new();
    builder = add_entity_paths(builder, base_path, default_model, None);
    for (package_id, model) in package_models {
        builder = add_entity_paths(builder, base_path, model, Some(package_id.as_str()));
    }
    builder = add_kv_paths(builder, base_path, package_kv_stores);
    let paths = builder.build();
    OpenApiBuilder::new()
        .info(Info::new("Architect Entity API", env!("CARGO_PKG_VERSION")))
        .servers(Some(vec![server]))
        .paths(paths)
        .build()
}

/// GET /spec — return OpenAPI JSON for entity APIs. Default (unprefixed) routes come from
/// state.model; package-scoped routes are built by listing _sys_packages and loading each
/// package's config from _sys_* tables (same source of truth as runtime routes).
pub async fn spec_handler(State(state): State<AppState>) -> Json<OpenApi> {
    let default_model = state.model.read().expect("model read lock").clone();
    let base_path = "/api/v1";

    let package_ids = list_package_ids(&state.pool).await.unwrap_or_default();
    let mut package_models: HashMap<String, ResolvedModel> = HashMap::new();
    let mut package_kv_stores: HashMap<String, Vec<KvStoreConfig>> = HashMap::new();
    for package_id in package_ids {
        if let Ok(config) = load_from_pool(&state.pool, &package_id).await {
            if let Ok(model) = resolve(&config) {
                package_models.insert(package_id.clone(), model);
            }
            package_kv_stores.insert(package_id, config.kv_stores);
        }
    }

    let spec = build_spec(
        &default_model,
        base_path,
        &package_models,
        &package_kv_stores,
    );
    Json(spec)
}
