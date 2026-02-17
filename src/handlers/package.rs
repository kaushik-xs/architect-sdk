//! Package install handler: accept zip upload, extract manifest + configs, apply configs in dependency order (most atomic first), store manifest, and reload model. X-Tenant-ID is required.

use crate::config::{load_from_pool, resolve};
use crate::error::AppError;
use crate::extractors::tenant::TenantId;
use crate::handlers::config::replace_config;
use crate::migration::apply_migrations;
use crate::state::AppState;
use crate::store::upsert_package;
use axum::extract::{Multipart, State};
use axum::Json;
use serde_json::Value;
use std::collections::HashSet;
use std::io::Cursor;
use zip::ZipArchive;

/// All config kinds that may appear in a package zip (excluding schemas, which are derived from manifest).
const CONFIG_KINDS: &[&str] = &[
    "schemas",
    "enums",
    "tables",
    "columns",
    "indexes",
    "relationships",
    "api_entities",
];

/// Dependencies for each config kind: these must be applied before this kind.
/// Order: most atomic and independent first (schemas, then enums/tables, then columns, etc.).
fn dependencies(kind: &str) -> &'static [&'static str] {
    match kind {
        "schemas" => &[],
        "enums" => &["schemas"],
        "tables" => &["schemas"],
        "columns" => &["tables"],
        "indexes" => &["schemas", "tables"],
        "relationships" => &["schemas", "tables", "columns"],
        "api_entities" => &["tables"],
        _ => &[],
    }
}

/// Topological sort of config kinds so that dependencies are applied first.
/// Returns order to apply: most atomic and independent first.
fn config_apply_order() -> Vec<&'static str> {
    let mut order = Vec::with_capacity(CONFIG_KINDS.len());
    let mut done: HashSet<&'static str> = HashSet::new();
    while order.len() < CONFIG_KINDS.len() {
        let mut made_progress = false;
        for &kind in CONFIG_KINDS {
            if done.contains(kind) {
                continue;
            }
            let deps = dependencies(kind);
            if deps.iter().all(|d| done.contains(d)) {
                order.push(kind);
                done.insert(kind);
                made_progress = true;
            }
        }
        if !made_progress {
            break;
        }
    }
    order
}

/// Schema id used when manifest provides the schema name (no separate schemas.json).
const DEFAULT_SCHEMA_ID: &str = "default";

const DEFAULT_EMPTY_JSON: &str = "[]";

fn inject_schema_id(body: &mut [Value], schema_id: &str) {
    for rec in body.iter_mut() {
        if let Some(obj) = rec.as_object_mut() {
            if !obj.contains_key("schema_id") {
                obj.insert("schema_id".into(), Value::String(schema_id.to_string()));
            }
        }
    }
}

fn inject_relationship_schema_ids(body: &mut [Value], schema_id: &str) {
    for rec in body.iter_mut() {
        if let Some(obj) = rec.as_object_mut() {
            if !obj.contains_key("from_schema_id") {
                obj.insert("from_schema_id".into(), Value::String(schema_id.to_string()));
            }
            if !obj.contains_key("to_schema_id") {
                obj.insert("to_schema_id".into(), Value::String(schema_id.to_string()));
            }
        }
    }
}

fn read_zip_entry_to_string<R: std::io::Read + std::io::Seek>(
    archive: &mut ZipArchive<R>,
    name: &str,
) -> Result<String, AppError> {
    let mut f = archive.by_name(name).map_err(|e| AppError::BadRequest(e.to_string()))?;
    let mut s = String::new();
    std::io::Read::read_to_string(&mut f, &mut s).map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(s)
}

/// POST /api/v1/config/package: multipart form with file field containing a zip (manifest.json + config JSONs). X-Tenant-ID required.
pub async fn install_package(
    TenantId(tenant_id_opt): TenantId,
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let tenant_id = tenant_id_opt
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("X-Tenant-ID header is required".into()))?;
    state
        .tenant_registry
        .get(tenant_id)
        .ok_or_else(|| AppError::NotFound(format!("tenant not found: {}", tenant_id)))?;

    let mut zip_bytes: Option<Vec<u8>> = None;
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or("").to_string();
        if name == "file" || name == "package" {
            let data = field.bytes().await.map_err(|e| AppError::BadRequest(e.to_string()))?;
            zip_bytes = Some(data.to_vec());
            break;
        }
    }
    let zip_bytes = zip_bytes.ok_or_else(|| AppError::BadRequest("missing 'file' or 'package' field in multipart body".into()))?;

    let mut archive = ZipArchive::new(Cursor::new(zip_bytes))
        .map_err(|e| AppError::BadRequest(format!("invalid zip: {}", e)))?;

    let manifest_name = archive
        .file_names()
        .find(|n| *n == "manifest.json" || n.ends_with("/manifest.json"))
        .map(String::from)
        .ok_or_else(|| AppError::BadRequest("zip must contain manifest.json at root".into()))?;

    let manifest_value: Value = {
        let mut file = archive.by_name(&manifest_name).map_err(|e| AppError::BadRequest(e.to_string()))?;
        let mut buf = String::new();
        std::io::Read::read_to_string(&mut file, &mut buf).map_err(|e| AppError::BadRequest(e.to_string()))?;
        serde_json::from_str(&buf).map_err(|e| AppError::BadRequest(format!("invalid manifest.json: {}", e)))?
    };

    let manifest_obj = manifest_value.as_object().ok_or_else(|| AppError::BadRequest("manifest.json must be an object".into()))?;
    let id = manifest_obj
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::BadRequest("manifest must have 'id' (string)".into()))?;
    let _name = manifest_obj
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::BadRequest("manifest must have 'name' (string)".into()))?;
    let _version = manifest_obj
        .get("version")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::BadRequest("manifest must have 'version' (string)".into()))?;
    let schema_name = manifest_obj
        .get("schema")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::BadRequest("manifest must have 'schema' (string) - the schema name for all configs".into()))?;

    let schemas_body = vec![serde_json::json!({
        "id": DEFAULT_SCHEMA_ID,
        "name": schema_name
    })];

    let apply_order = config_apply_order();
    let mut applied = Vec::with_capacity(apply_order.len());
    for kind in &apply_order {
        let body: Vec<Value> = if *kind == "schemas" {
            serde_json::from_value(Value::Array(schemas_body.clone()))
                .map_err(|e| AppError::BadRequest(format!("schemas body: {}", e)))?
        } else {
            let file_name = format!("{}.json", kind);
            let file_name_with_slash = format!("{}/{}", kind, file_name);
            let content = read_zip_entry_to_string(&mut archive, &file_name)
                .or_else(|_| read_zip_entry_to_string(&mut archive, &file_name_with_slash))
                .unwrap_or_else(|_| DEFAULT_EMPTY_JSON.to_string());

            let mut body: Vec<Value> = serde_json::from_str(&content)
                .map_err(|e| AppError::BadRequest(format!("invalid {}: {}", file_name, e)))?;
            match *kind {
                "enums" | "tables" | "indexes" => inject_schema_id(&mut body, DEFAULT_SCHEMA_ID),
                "relationships" => inject_relationship_schema_ids(&mut body, DEFAULT_SCHEMA_ID),
                _ => {}
            }
            body
        };
        replace_config(&state.pool, kind, body, false, id).await?;
        applied.push((*kind).to_string());
    }

    upsert_package(&state.pool, id, &manifest_value).await?;

    let config = load_from_pool(&state.pool, id).await.map_err(AppError::Config)?;
    apply_migrations(&state.pool, &config, None).await?;
    let new_model = resolve(&config).map_err(AppError::Config)?;
    {
        let mut guard = state.model.write().map_err(|_| AppError::BadRequest("state lock".into()))?;
        *guard = new_model.clone();
        state
            .package_models
            .write()
            .map_err(|_| AppError::BadRequest("state lock".into()))?
            .insert(id.to_string(), new_model);
    }

    #[derive(serde::Serialize)]
    struct PackageInstallResponse {
        package: Value,
        applied: Vec<String>,
    }
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessOne {
            data: PackageInstallResponse {
                package: manifest_value,
                applied,
            },
            meta: None,
        }),
    ))
}
