//! Generates MCP `Tool` definitions from `ResolvedModel`.

use rmcp::model::{JsonObject, Tool, ToolAnnotations};
use serde_json::{json, Value};

use crate::config::resolved::ResolvedEntity;
use crate::config::{types::McpEntityConfig, ResolvedModel};

/// All MCP-exposed operations this entity supports.
pub struct McpToolSpec {
    pub tool: Tool,
    pub path_segment: String,
    pub operation: String,
}

/// Build the list of MCP tools for all entities that have `mcp.enabled = true`.
pub fn build_tool_list(model: &ResolvedModel) -> Vec<McpToolSpec> {
    let mut specs = Vec::new();

    for entity in &model.entities {
        let Some(mcp_cfg) = &entity.mcp else { continue };
        if !mcp_cfg.enabled {
            continue;
        }

        let exposed_ops = effective_operations(entity, mcp_cfg);
        let prefix = mcp_cfg
            .tool_prefix
            .as_deref()
            .unwrap_or(&entity.path_segment);
        let entity_desc = mcp_cfg
            .description
            .as_deref()
            .unwrap_or(&entity.path_segment);

        for op in &exposed_ops {
            let (name, description, schema, annotations) =
                build_tool_for_op(op, prefix, entity_desc, entity);
            let tool = Tool::new(name, description, schema).with_annotations(annotations);
            specs.push(McpToolSpec {
                tool,
                path_segment: entity.path_segment.clone(),
                operation: op.clone(),
            });
        }
    }

    specs
}

fn effective_operations(entity: &ResolvedEntity, cfg: &McpEntityConfig) -> Vec<String> {
    if cfg.operations.is_empty() {
        entity.operations.clone()
    } else {
        cfg.operations
            .iter()
            .filter(|op| entity.operations.contains(op))
            .cloned()
            .collect()
    }
}

fn build_tool_for_op(
    op: &str,
    prefix: &str,
    entity_desc: &str,
    entity: &ResolvedEntity,
) -> (String, String, JsonObject, ToolAnnotations) {
    match op {
        "list" => {
            let name = format!("{prefix}_list");
            let desc = format!(
                "List {entity_desc} records with optional filters, sorting, and pagination."
            );
            let schema = list_schema();
            let annotations = ToolAnnotations::new().read_only(true).destructive(false);
            (name, desc, schema, annotations)
        }
        "read" => {
            let name = format!("{prefix}_get");
            let desc = format!("Retrieve a single {entity_desc} record by ID.");
            let schema = id_schema();
            let annotations = ToolAnnotations::new().read_only(true).destructive(false);
            (name, desc, schema, annotations)
        }
        "create" => {
            let name = format!("{prefix}_create");
            let desc = format!("Create a new {entity_desc} record.");
            let schema = create_schema(entity);
            let annotations = ToolAnnotations::new()
                .read_only(false)
                .destructive(false)
                .idempotent(false);
            (name, desc, schema, annotations)
        }
        "update" => {
            let name = format!("{prefix}_update");
            let desc = format!("Update an existing {entity_desc} record by ID.");
            let schema = update_schema(entity);
            let annotations = ToolAnnotations::new()
                .read_only(false)
                .destructive(false)
                .idempotent(true);
            (name, desc, schema, annotations)
        }
        "delete" => {
            let name = format!("{prefix}_delete");
            let desc = format!("Delete a {entity_desc} record by ID.");
            let schema = id_schema();
            let annotations = ToolAnnotations::new()
                .read_only(false)
                .destructive(true)
                .idempotent(true);
            (name, desc, schema, annotations)
        }
        other => {
            let name = format!("{prefix}_{other}");
            let desc = format!("{other} on {entity_desc}.");
            let schema = id_schema();
            let annotations = ToolAnnotations::new();
            (name, desc, schema, annotations)
        }
    }
}

fn base_properties() -> serde_json::Map<String, Value> {
    let mut props = serde_json::Map::new();
    props.insert(
        "tenant_id".into(),
        json!({
            "type": "string",
            "description": "Tenant ID (overrides MCP_TENANT_ID env var when provided)"
        }),
    );
    props
}

fn list_schema() -> JsonObject {
    let mut props = base_properties();
    props.insert(
        "filter".into(),
        json!({ "type": "string", "description": "RSQL filter expression (e.g. name==Alice)" }),
    );
    props.insert(
        "sort".into(),
        json!({ "type": "string", "description": "Sort spec (e.g. name:asc,created_at:desc)" }),
    );
    props.insert(
        "limit".into(),
        json!({ "type": "integer", "minimum": 1, "maximum": 1000, "description": "Max records to return (default 100)" }),
    );
    props.insert(
        "offset".into(),
        json!({ "type": "integer", "minimum": 0, "description": "Number of records to skip" }),
    );
    props.insert(
        "include".into(),
        json!({ "type": "string", "description": "Comma-separated related entities to include (e.g. orders,profile)" }),
    );
    JsonObject::from_iter([
        ("type".into(), json!("object")),
        ("properties".into(), Value::Object(props)),
    ])
}

fn id_schema() -> JsonObject {
    let mut props = base_properties();
    props.insert(
        "id".into(),
        json!({ "type": "string", "description": "Record ID" }),
    );
    JsonObject::from_iter([
        ("type".into(), json!("object")),
        ("properties".into(), Value::Object(props)),
        ("required".into(), json!(["id"])),
    ])
}

fn create_schema(entity: &ResolvedEntity) -> JsonObject {
    let (mut props, required) = column_properties(entity, false);
    props.extend(base_properties());
    let mut schema = JsonObject::from_iter([
        ("type".into(), json!("object")),
        ("properties".into(), Value::Object(props)),
    ]);
    if !required.is_empty() {
        schema.insert("required".into(), json!(required));
    }
    schema
}

fn update_schema(entity: &ResolvedEntity) -> JsonObject {
    let (mut props, _) = column_properties(entity, true);
    props.extend(base_properties());
    props.insert(
        "id".into(),
        json!({ "type": "string", "description": "Record ID to update" }),
    );
    JsonObject::from_iter([
        ("type".into(), json!("object")),
        ("properties".into(), Value::Object(props)),
        ("required".into(), json!(["id"])),
    ])
}

/// Build JSON Schema properties from the entity's non-PK, non-sensitive columns.
/// Returns (properties map, required field names).
fn column_properties(
    entity: &ResolvedEntity,
    all_optional: bool,
) -> (serde_json::Map<String, Value>, Vec<String>) {
    let mut props = serde_json::Map::new();
    let mut required = Vec::new();
    let pk_set: std::collections::HashSet<_> = entity.pk_columns.iter().collect();

    for col in &entity.columns {
        if pk_set.contains(&col.name) {
            continue;
        }
        if entity.sensitive_columns.contains(&col.name) {
            continue;
        }
        if col.has_default && !col.nullable {
            // DB-generated columns (e.g. created_at with NOW() default) — omit from create schema
            continue;
        }

        let json_type = pg_type_to_json_type(col.pg_type.as_deref());
        props.insert(col.name.clone(), json!({ "type": json_type }));

        let validation = entity.validation.get(&col.name);
        let is_required = validation
            .map(|v| v.required.unwrap_or(false))
            .unwrap_or(false);

        if is_required && !col.nullable && !all_optional {
            required.push(col.name.clone());
        }
    }

    (props, required)
}

fn pg_type_to_json_type(pg_type: Option<&str>) -> &'static str {
    match pg_type {
        Some("int4") | Some("int8") | Some("int2") | Some("bigint") | Some("integer")
        | Some("smallint") | Some("serial") | Some("bigserial") => "integer",
        Some("float4")
        | Some("float8")
        | Some("numeric")
        | Some("decimal")
        | Some("real")
        | Some("double precision") => "number",
        Some("bool") | Some("boolean") => "boolean",
        Some("json") | Some("jsonb") => "object",
        _ => "string",
    }
}
