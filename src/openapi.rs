//! Build OpenAPI spec from resolved model (api_entities + columns). Exposed at GET /spec.

use crate::case::to_camel_case;
use crate::config::{ResolvedEntity, ResolvedModel};
use crate::state::AppState;
use axum::extract::State;
use axum::Json;
use utoipa::openapi::path::{
    HttpMethod, Operation, OperationBuilder, ParameterBuilder, ParameterIn, PathItemBuilder,
    PathsBuilder,
};
use utoipa::openapi::request_body::RequestBodyBuilder;
use utoipa::openapi::response::{Response, ResponsesBuilder};
use utoipa::openapi::schema::{ObjectBuilder, Schema, SchemaType, Type};
use utoipa::openapi::server::{ServerBuilder, ServerVariableBuilder};
use utoipa::openapi::{Content, Info, OpenApi, OpenApiBuilder, Paths, RefOr, Required};

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

fn list_operation(entity: &ResolvedEntity) -> Operation {
    let mut params = vec![
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
        .operation_id(Some(format!("list_{}", entity.path_segment)))
        .parameters(Some(params))
        .responses(default_responses().build())
        .build()
}

fn create_operation(entity: &ResolvedEntity) -> Operation {
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
        .operation_id(Some(format!("create_{}", entity.path_segment)))
        .request_body(Some(body))
        .responses(
            ResponsesBuilder::new()
                .response("201", Response::new("Created"))
                .response("400", Response::new("Bad Request"))
                .build(),
        )
        .build()
}

fn read_operation(entity: &ResolvedEntity) -> Operation {
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
        .operation_id(Some(format!("read_{}", entity.path_segment)))
        .parameters(Some(vec![id_param, include_param]))
        .responses(default_responses().build())
        .build()
}

fn update_operation(entity: &ResolvedEntity) -> Operation {
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
        .operation_id(Some(format!("update_{}", entity.path_segment)))
        .parameters(Some(vec![id_param]))
        .request_body(Some(body))
        .responses(default_responses().build())
        .build()
}

fn delete_operation(entity: &ResolvedEntity) -> Operation {
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
        .operation_id(Some(format!("delete_{}", entity.path_segment)))
        .parameters(Some(vec![id_param]))
        .responses(
            ResponsesBuilder::new()
                .response("204", Response::new("No Content"))
                .response("400", Response::new("Bad Request"))
                .response("404", Response::new("Not Found"))
                .build(),
        )
        .build()
}

fn bulk_create_operation(entity: &ResolvedEntity) -> Operation {
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
        .operation_id(Some(format!("bulk_create_{}", entity.path_segment)))
        .request_body(Some(body))
        .responses(
            ResponsesBuilder::new()
                .response("201", Response::new("Created"))
                .response("400", Response::new("Bad Request"))
                .build(),
        )
        .build()
}

fn bulk_update_operation(entity: &ResolvedEntity) -> Operation {
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
        .operation_id(Some(format!("bulk_update_{}", entity.path_segment)))
        .request_body(Some(body))
        .responses(default_responses().build())
        .build()
}

/// Build OpenAPI paths for default (unprefixed) entity APIs: /api/v1/{path_segment} and /api/v1/{path_segment}/{id}, bulk.
fn entity_paths(base: &str, model: &ResolvedModel) -> Paths {
    let mut builder = PathsBuilder::new();
    for entity in &model.entities {
        let seg = &entity.path_segment;
        let list_path = format!("{}/{}", base, seg);
        let by_id_path = format!("{}/{}/{{id}}", base, seg);
        let bulk_path = format!("{}/{}/bulk", base, seg);

        let has_list = entity.operations.iter().any(|o| o == "read");
        let has_create = entity.operations.iter().any(|o| o == "create");
        if has_list || has_create {
            let mut list_item = PathItemBuilder::new();
            if has_list {
                list_item = list_item.operation(HttpMethod::Get, list_operation(entity));
            }
            if has_create {
                list_item = list_item.operation(HttpMethod::Post, create_operation(entity));
            }
            builder = builder.path(list_path, list_item.build());
        }

        let has_read = entity.operations.iter().any(|o| o == "read");
        let has_update = entity.operations.iter().any(|o| o == "update");
        let has_delete = entity.operations.iter().any(|o| o == "delete");
        if has_read || has_update || has_delete {
            let mut by_id_item = PathItemBuilder::new();
            if has_read {
                by_id_item = by_id_item.operation(HttpMethod::Get, read_operation(entity));
            }
            if has_update {
                by_id_item = by_id_item.operation(HttpMethod::Patch, update_operation(entity));
            }
            if has_delete {
                by_id_item = by_id_item.operation(HttpMethod::Delete, delete_operation(entity));
            }
            builder = builder.path(by_id_path, by_id_item.build());
        }

        let has_bulk_create = entity.operations.iter().any(|o| o == "bulk_create");
        let has_bulk_update = entity.operations.iter().any(|o| o == "bulk_update");
        if has_bulk_create || has_bulk_update {
            let mut bulk_item = PathItemBuilder::new();
            if has_bulk_create {
                bulk_item = bulk_item.operation(HttpMethod::Post, bulk_create_operation(entity));
            }
            if has_bulk_update {
                bulk_item = bulk_item.operation(HttpMethod::Patch, bulk_update_operation(entity));
            }
            builder = builder.path(bulk_path, bulk_item.build());
        }
    }
    builder.build()
}

/// Build full OpenAPI spec for entity APIs with server URL `http://{host}:{port}`.
pub fn build_spec(model: &ResolvedModel, base_path: &str) -> OpenApi {
    let server = build_server();
    let paths = entity_paths(base_path, model);
    OpenApiBuilder::new()
        .info(Info::new("Architect Entity API", env!("CARGO_PKG_VERSION")))
        .servers(Some(vec![server]))
        .paths(paths)
        .build()
}

/// GET /spec — return OpenAPI JSON for entity APIs (from current resolved model).
pub async fn spec_handler(State(state): State<AppState>) -> Json<OpenApi> {
    let model = state.model.read().expect("model read lock");
    let spec = build_spec(&model, "/api/v1");
    Json(spec)
}
