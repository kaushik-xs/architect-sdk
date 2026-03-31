//! Build OpenAPI spec from architect._sys_* tables. Exposed at GET /spec.
//! APIs and paths come from _sys_api_entities per package; parameters and request/response body
//! schemas are built from _sys_columns (column names, types, nullable, default). Entity and KV
//! paths are generated dynamically by listing _sys_packages and loading each package's config.

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

/// Map PostgreSQL type (from _sys_columns) to OpenAPI schema type for parameters and body properties.
fn column_schema_from_pg_type(pg_type: Option<&str>) -> Schema {
    let t = pg_type.unwrap_or("").to_lowercase();
    // Handle PostgreSQL array types (e.g. uuid[], text[], _int4, _uuid) by mapping
    // them to OpenAPI arrays whose item schema is derived from the element type.
    if t.ends_with("[]") || t.starts_with('_') {
        let element_type = t.trim_end_matches("[]").trim_start_matches('_');
        let item_schema = column_schema_from_pg_type(Some(element_type));
        return Schema::Array(
            utoipa::openapi::schema::ArrayBuilder::new()
                .items(RefOr::T(item_schema))
                .build()
                .into(),
        );
    }
    if t.contains("int") || t.contains("serial") {
        return Schema::Object(
            utoipa::openapi::schema::ObjectBuilder::new()
                .schema_type(SchemaType::new(Type::Integer))
                .into(),
        );
    }
    if t.contains("bool") {
        return Schema::Object(
            utoipa::openapi::schema::ObjectBuilder::new()
                .schema_type(SchemaType::new(Type::Boolean))
                .into(),
        );
    }
    if t.contains("uuid") {
        return Schema::Object(
            utoipa::openapi::schema::ObjectBuilder::new()
                .schema_type(SchemaType::new(Type::String))
                .format(Some(utoipa::openapi::schema::SchemaFormat::KnownFormat(
                    utoipa::openapi::schema::KnownFormat::Uuid,
                )))
                .into(),
        );
    }
    if t.contains("numeric") || t.contains("decimal") || t.contains("real") || t.contains("double") || t.contains("float") {
        return Schema::Object(
            utoipa::openapi::schema::ObjectBuilder::new()
                .schema_type(SchemaType::new(Type::Number))
                .into(),
        );
    }
    if t.contains("timestamp") || t.contains("date") {
        return Schema::Object(
            utoipa::openapi::schema::ObjectBuilder::new()
                .schema_type(SchemaType::new(Type::String))
                .format(Some(utoipa::openapi::schema::SchemaFormat::KnownFormat(
                    utoipa::openapi::schema::KnownFormat::DateTime,
                )))
                .into(),
        );
    }
    Schema::Object(
        utoipa::openapi::schema::ObjectBuilder::new()
            .schema_type(SchemaType::new(Type::String))
            .into(),
    )
}

/// Build OpenAPI object schema from entity columns (_sys_columns). Properties use camelCase.
/// For create: required = !nullable && !has_default. For update: all optional (partial).
fn entity_body_schema(entity: &ResolvedEntity, for_create: bool) -> Schema {
    let mut builder = utoipa::openapi::schema::ObjectBuilder::new()
        .schema_type(SchemaType::new(Type::Object))
        .description(Some(format!(
            "Fields from architect._sys_columns for table {} (API uses camelCase).",
            entity.table_id
        )));
    let mut required = Vec::new();
    for col in &entity.columns {
        if entity.sensitive_columns.contains(&col.name) {
            continue;
        }
        let camel = to_camel_case(&col.name);
        let prop_schema = column_schema_from_pg_type(col.pg_type.as_deref());
        builder = builder.property(camel.clone(), RefOr::T(prop_schema));
        if for_create && !col.nullable && !col.has_default {
            required.push(camel);
        }
    }
    for r in &required {
        builder = builder.required(r.clone());
    }
    Schema::Object(builder.into())
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

/// Path parameter for package-scoped routes: packageId (from architect._sys_packages). No literal package ids in the spec.
fn package_id_param() -> Parameter {
    ParameterBuilder::new()
        .name("packageId")
        .parameter_in(ParameterIn::Path)
        .required(Required::True)
        .description(Some("Package id from architect._sys_packages."))
        .schema(Some(RefOr::T(Schema::Object(
            utoipa::openapi::schema::ObjectBuilder::new()
                .schema_type(SchemaType::new(Type::String))
                .into(),
        ))))
        .build()
}

fn list_operation(entity: &ResolvedEntity, op_suffix: &str, include_package_id_param: bool) -> Operation {
    let mut params = vec![x_tenant_id_header()];
    if include_package_id_param {
        params.push(package_id_param());
    }
    params.extend(vec![
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
    ]);
    for col in &entity.columns {
        if entity.sensitive_columns.contains(&col.name) {
            continue;
        }
        let camel = to_camel_case(&col.name);
        let schema = column_schema_from_pg_type(col.pg_type.as_deref());
        params.push(
            ParameterBuilder::new()
                .name(camel)
                .parameter_in(ParameterIn::Query)
                .required(Required::False)
                .description(Some(format!(
                    "Filter by {} (from _sys_columns)",
                    col.name
                )))
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

fn create_operation(entity: &ResolvedEntity, op_suffix: &str, include_package_id_param: bool) -> Operation {
    let mut params = vec![x_tenant_id_header()];
    if include_package_id_param {
        params.push(package_id_param());
    }
    let body = RequestBodyBuilder::new()
        .description(Some(format!(
            "JSON object with {} fields from _sys_columns (camelCase). PK may be omitted if DB default exists.",
            entity.path_segment
        )))
        .content(
            "application/json",
            Content::new(Some(RefOr::T(entity_body_schema(entity, true)))),
        )
        .required(Some(Required::True))
        .build();
    OperationBuilder::new()
        .summary(Some(format!("Create {}", entity.path_segment)))
        .description(Some(format!("Create a single {}", entity.path_segment)))
        .operation_id(Some(format!("create_{}{}", entity.path_segment, op_suffix)))
        .parameters(Some(params))
        .request_body(Some(body))
        .responses(
            ResponsesBuilder::new()
                .response("201", Response::new("Created"))
                .response("400", Response::new("Bad Request"))
                .build(),
        )
        .build()
}

fn read_operation(entity: &ResolvedEntity, op_suffix: &str, include_package_id_param: bool) -> Operation {
    let mut params = vec![x_tenant_id_header()];
    if include_package_id_param {
        params.push(package_id_param());
    }
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
    params.push(id_param);
    params.push(include_param);
    OperationBuilder::new()
        .summary(Some(format!("Get {} by id", entity.path_segment)))
        .description(Some(format!("Get a single {} by id.", entity.path_segment)))
        .operation_id(Some(format!("read_{}{}", entity.path_segment, op_suffix)))
        .parameters(Some(params))
        .responses(default_responses().build())
        .build()
}

fn update_operation(entity: &ResolvedEntity, op_suffix: &str, include_package_id_param: bool) -> Operation {
    let mut params = vec![x_tenant_id_header()];
    if include_package_id_param {
        params.push(package_id_param());
    }
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
    params.push(id_param);
    let body = RequestBodyBuilder::new()
        .description(Some(
            "JSON object with fields from _sys_columns to update (camelCase, partial).",
        ))
        .content(
            "application/json",
            Content::new(Some(RefOr::T(entity_body_schema(entity, false)))),
        )
        .required(Some(Required::True))
        .build();
    OperationBuilder::new()
        .summary(Some(format!("Update {} by id", entity.path_segment)))
        .description(Some(format!("Update a single {} by id.", entity.path_segment)))
        .operation_id(Some(format!("update_{}{}", entity.path_segment, op_suffix)))
        .parameters(Some(params))
        .request_body(Some(body))
        .responses(default_responses().build())
        .build()
}

fn delete_operation(entity: &ResolvedEntity, op_suffix: &str, include_package_id_param: bool) -> Operation {
    let mut params = vec![x_tenant_id_header()];
    if include_package_id_param {
        params.push(package_id_param());
    }
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
    params.push(id_param);
    OperationBuilder::new()
        .summary(Some(format!("Delete {} by id", entity.path_segment)))
        .description(Some(format!("Delete a single {} by id.", entity.path_segment)))
        .operation_id(Some(format!("delete_{}{}", entity.path_segment, op_suffix)))
        .parameters(Some(params))
        .responses(
            ResponsesBuilder::new()
                .response("204", Response::new("No Content"))
                .response("400", Response::new("Bad Request"))
                .response("404", Response::new("Not Found"))
                .build(),
        )
        .build()
}

fn bulk_create_operation(entity: &ResolvedEntity, op_suffix: &str, include_package_id_param: bool) -> Operation {
    let mut params = vec![x_tenant_id_header()];
    if include_package_id_param {
        params.push(package_id_param());
    }
    let item_schema = entity_body_schema(entity, true);
    let body = RequestBodyBuilder::new()
        .description(Some(
            "JSON array of objects; each has shape from _sys_columns (same as create body).",
        ))
        .content(
            "application/json",
            Content::new(Some(RefOr::T(Schema::Array(
                utoipa::openapi::schema::ArrayBuilder::new()
                    .items(RefOr::T(item_schema))
                    .build()
                    .into(),
            )))),
        )
        .required(Some(Required::True))
        .build();
    OperationBuilder::new()
        .summary(Some(format!("Bulk create {}", entity.path_segment)))
        .description(Some(format!("Create multiple {}.", entity.path_segment)))
        .operation_id(Some(format!("bulk_create_{}{}", entity.path_segment, op_suffix)))
        .parameters(Some(params))
        .request_body(Some(body))
        .responses(
            ResponsesBuilder::new()
                .response("201", Response::new("Created"))
                .response("400", Response::new("Bad Request"))
                .build(),
        )
        .build()
}

fn bulk_update_operation(entity: &ResolvedEntity, op_suffix: &str, include_package_id_param: bool) -> Operation {
    let mut params = vec![x_tenant_id_header()];
    if include_package_id_param {
        params.push(package_id_param());
    }
    let item_schema = entity_body_schema(entity, false);
    let body = RequestBodyBuilder::new()
        .description(Some(
            "JSON array of objects; each must include id and fields from _sys_columns to update (camelCase, partial).",
        ))
        .content(
            "application/json",
            Content::new(Some(RefOr::T(Schema::Array(
                utoipa::openapi::schema::ArrayBuilder::new()
                    .items(RefOr::T(item_schema))
                    .build()
                    .into(),
            )))),
        )
        .required(Some(Required::True))
        .build();
    OperationBuilder::new()
        .summary(Some(format!("Bulk update {}", entity.path_segment)))
        .description(Some(format!("Update multiple {}.", entity.path_segment)))
        .operation_id(Some(format!("bulk_update_{}{}", entity.path_segment, op_suffix)))
        .parameters(Some(params))
        .request_body(Some(body))
        .responses(default_responses().build())
        .build()
}

/// Add entity paths for one model.
/// - For default model: paths are `{base}/{path_segment}` (no package segment).
/// - For package models: paths are `{base}/package/{package_id}/{path_segment}` with the concrete package id.
fn add_entity_paths(
    mut builder: PathsBuilder,
    base: &str,
    model: &ResolvedModel,
    use_package_param: bool,
    package_id_literal: Option<&str>,
) -> PathsBuilder {
    let path_prefix = if use_package_param {
        match package_id_literal {
            Some(pkg) => format!("{}/package/{}", base, pkg),
            None => format!("{}/package/{{packageId}}", base),
        }
    } else {
        base.to_string()
    };
    let op_suffix = if use_package_param { "_package" } else { "" };

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
                list_item = list_item.operation(
                    HttpMethod::Get,
                    list_operation(entity, op_suffix, use_package_param),
                );
            }
            if has_create {
                list_item = list_item.operation(
                    HttpMethod::Post,
                    create_operation(entity, op_suffix, use_package_param),
                );
            }
            builder = builder.path(list_path, list_item.build());
        }

        let has_read = entity.operations.iter().any(|o| o == "read");
        let has_update = entity.operations.iter().any(|o| o == "update");
        let has_delete = entity.operations.iter().any(|o| o == "delete");
        if has_read || has_update || has_delete {
            let mut by_id_item = PathItemBuilder::new();
            if has_read {
                by_id_item = by_id_item.operation(
                    HttpMethod::Get,
                    read_operation(entity, op_suffix, use_package_param),
                );
            }
            if has_update {
                by_id_item = by_id_item.operation(
                    HttpMethod::Patch,
                    update_operation(entity, op_suffix, use_package_param),
                );
            }
            if has_delete {
                by_id_item = by_id_item.operation(
                    HttpMethod::Delete,
                    delete_operation(entity, op_suffix, use_package_param),
                );
            }
            builder = builder.path(by_id_path, by_id_item.build());
        }

        let has_bulk_create = entity.operations.iter().any(|o| o == "bulk_create");
        let has_bulk_update = entity.operations.iter().any(|o| o == "bulk_update");
        if has_bulk_create || has_bulk_update {
            let mut bulk_item = PathItemBuilder::new();
            if has_bulk_create {
                bulk_item = bulk_item.operation(
                    HttpMethod::Post,
                    bulk_create_operation(entity, op_suffix, use_package_param),
                );
            }
            if has_bulk_update {
                bulk_item = bulk_item.operation(
                    HttpMethod::Patch,
                    bulk_update_operation(entity, op_suffix, use_package_param),
                );
            }
            builder = builder.path(bulk_path, bulk_item.build());
        }
    }
    builder
}

fn kv_namespace_param() -> Parameter {
    ParameterBuilder::new()
        .name("namespace")
        .parameter_in(ParameterIn::Path)
        .required(Required::True)
        .description(Some("KV store namespace (from _sys_kv_stores)."))
        .schema(Some(RefOr::T(Schema::Object(
            utoipa::openapi::schema::ObjectBuilder::new()
                .schema_type(SchemaType::new(Type::String))
                .into(),
        ))))
        .build()
}

fn kv_list_keys_operation() -> Operation {
    OperationBuilder::new()
        .summary(Some("List KV keys in namespace"))
        .description(Some(
            "List all keys and values in the given package and namespace.",
        ))
        .operation_id(Some("kv_list_keys"))
        .parameters(Some(vec![
            x_tenant_id_header(),
            package_id_param(),
            kv_namespace_param(),
        ]))
        .responses(default_responses().build())
        .build()
}

fn kv_key_param() -> Parameter {
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
        .build()
}

fn kv_key_operations() -> (Operation, Operation, Operation) {
    let get_op = OperationBuilder::new()
        .summary(Some("Get KV value by key"))
        .description(Some("Get value for key in package and namespace."))
        .operation_id(Some("kv_get"))
        .parameters(Some(vec![
            x_tenant_id_header(),
            package_id_param(),
            kv_namespace_param(),
            kv_key_param(),
        ]))
        .responses(default_responses().build())
        .build();

    let put_op = OperationBuilder::new()
        .summary(Some("Set KV value (upsert)"))
        .description(Some("Set or overwrite value for key. Body is arbitrary JSON."))
        .operation_id(Some("kv_put"))
        .parameters(Some(vec![
            x_tenant_id_header(),
            package_id_param(),
            kv_namespace_param(),
            kv_key_param(),
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
        .operation_id(Some("kv_delete"))
        .parameters(Some(vec![
            x_tenant_id_header(),
            package_id_param(),
            kv_namespace_param(),
            kv_key_param(),
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

/// Add KV store paths with concrete package ids and {namespace}/{key}.
fn add_kv_paths(
    mut builder: PathsBuilder,
    base: &str,
    package_kv_stores: &HashMap<String, Vec<KvStoreConfig>>,
) -> PathsBuilder {
    for (package_id, stores) in package_kv_stores {
        if stores.is_empty() {
            continue;
        }
        let list_path = format!("{}/package/{}/kv/{{namespace}}", base, package_id);
        let key_path = format!("{}/package/{}/kv/{{namespace}}/{{key}}", base, package_id);

        let list_item = PathItemBuilder::new()
            .operation(HttpMethod::Get, kv_list_keys_operation());
        builder = builder.path(list_path, list_item.build());

        let (get_op, put_op, delete_op) = kv_key_operations();
        let key_item = PathItemBuilder::new()
            .operation(HttpMethod::Get, get_op)
            .operation(HttpMethod::Put, put_op)
            .operation(HttpMethod::Delete, delete_op);
        builder = builder.path(key_path, key_item.build());
    }
    builder
}

/// Add config API paths: install/uninstall package and GET/POST per config kind.
fn add_config_paths(mut builder: PathsBuilder, base: &str) -> PathsBuilder {
    let install_path = format!("{}/config/package", base);
    let install_op = OperationBuilder::new()
        .summary(Some("Install package"))
        .description(Some(
            "Upload a package zip. Zip must contain manifest.json (id, name, version, schema) at root and config JSON files. Use multipart/form-data with field 'file' or 'package' (ZIP file).",
        ))
        .operation_id(Some("config_install_package"))
        .parameters(Some(vec![x_tenant_id_header()]))
        .request_body(Some(
            RequestBodyBuilder::new()
                .description(Some("Multipart form with 'file' or 'package' field containing the ZIP."))
                .content(
                    "multipart/form-data",
                    Content::new(Some(RefOr::T(Schema::Object(
                        ObjectBuilder::new()
                            .schema_type(SchemaType::new(Type::Object))
                            .property(
                                "file",
                                Schema::Object(
                                    ObjectBuilder::new()
                                        .schema_type(SchemaType::new(Type::String))
                                        .format(Some(utoipa::openapi::schema::SchemaFormat::KnownFormat(
                                            utoipa::openapi::schema::KnownFormat::Binary,
                                        )))
                                        .description(Some("ZIP file (manifest.json + config JSONs)"))
                                        .into(),
                                ),
                            )
                            .into(),
                    )))),
                )
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
    let install_item = PathItemBuilder::new().operation(HttpMethod::Post, install_op);
    builder = builder.path(install_path, install_item.build());

    let uninstall_path = format!("{}/config/package/{{packageId}}", base);
    let uninstall_op = OperationBuilder::new()
        .summary(Some("Uninstall package"))
        .description(Some(
            "Revert migrations for the package, delete all _sys_* config and KV data, remove package record.",
        ))
        .operation_id(Some("config_uninstall_package"))
        .parameters(Some(vec![x_tenant_id_header(), package_id_param()]))
        .responses(
            ResponsesBuilder::new()
                .response("200", Response::new("OK"))
                .response("404", Response::new("Not Found"))
                .build(),
        )
        .build();
    let uninstall_item = PathItemBuilder::new().operation(HttpMethod::Delete, uninstall_op);
    builder = builder.path(uninstall_path, uninstall_item.build());

    let config_kinds = [
        ("schemas", "Schema definitions"),
        ("enums", "Enum types"),
        ("tables", "Table definitions"),
        ("columns", "Column definitions"),
        ("indexes", "Index definitions"),
        ("relationships", "Relationship definitions"),
        ("api_entities", "API entity definitions"),
        ("kv_stores", "KV store definitions"),
    ];
    for (kind, description) in config_kinds {
        let path = format!("{}/config/{}", base, kind);
        let get_op = OperationBuilder::new()
            .summary(Some(format!("Get {}", kind)))
            .description(Some(format!("Get {} (from _sys_{}). {}", description, kind, "X-Tenant-ID required.")))
            .operation_id(Some(format!("config_get_{}", kind)))
            .parameters(Some(vec![x_tenant_id_header()]))
            .responses(default_responses().build())
            .build();
        let post_body = RequestBodyBuilder::new()
            .description(Some(format!("JSON array of {} records.", description)))
            .content(
                "application/json",
                Content::new(Some(RefOr::T(Schema::Array(
                    utoipa::openapi::schema::ArrayBuilder::new()
                        .items(RefOr::T(json_object_schema()))
                        .into(),
                )))),
            )
            .required(Some(Required::True))
            .build();
        let post_op = OperationBuilder::new()
            .summary(Some(format!("Replace {}", kind)))
            .description(Some(format!("Replace {} for the default package. Runs migrations when rows change.", kind)))
            .operation_id(Some(format!("config_post_{}", kind)))
            .parameters(Some(vec![x_tenant_id_header()]))
            .request_body(Some(post_body))
            .responses(default_responses().build())
            .build();
        let item = PathItemBuilder::new()
            .operation(HttpMethod::Get, get_op)
            .operation(HttpMethod::Post, post_op);
        builder = builder.path(path, item.build());
    }
    builder
}

/// Build full OpenAPI spec for entity APIs: default model paths plus package-scoped paths
/// with concrete package ids, plus KV paths with {namespace}/{key} per package.
pub fn build_spec(
    default_model: &ResolvedModel,
    base_path: &str,
    package_models: &HashMap<String, ResolvedModel>,
    package_kv_stores: &HashMap<String, Vec<KvStoreConfig>>,
) -> OpenApi {
    let server = build_server();
    let mut builder = PathsBuilder::new();
    builder = add_config_paths(builder, base_path);
    builder = add_entity_paths(builder, base_path, default_model, false, None);
    for (package_id, model) in package_models {
        if !model.entities.is_empty() {
            builder = add_entity_paths(builder, base_path, model, true, Some(package_id.as_str()));
        }
    }
    builder = add_kv_paths(builder, base_path, package_kv_stores);
    let paths = builder.build();
    OpenApiBuilder::new()
        .info(
            Info::builder()
                .title("Architect API")
                .version(env!("CARGO_PKG_VERSION"))
                .description(Some("Config APIs (package install/uninstall, schemas, enums, tables, etc.) and entity CRUD + package-scoped entity and KV APIs."))
                .build(),
        )
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
