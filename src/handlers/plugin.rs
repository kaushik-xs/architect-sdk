//! Plugin install handler: accept zip upload, extract manifest + configs, apply configs and store manifest.

use crate::error::AppError;
use crate::handlers::config::replace_config;
use crate::state::AppState;
use crate::store::upsert_plugin;
use axum::extract::{Multipart, State};
use axum::Json;
use serde_json::Value;
use std::io::Cursor;
use zip::ZipArchive;

const CONFIG_ORDER: &[&str] = &[
    "schemas",
    "enums",
    "tables",
    "columns",
    "indexes",
    "relationships",
    "api_entities",
];

const DEFAULT_EMPTY_JSON: &str = "[]";

fn read_zip_entry_to_string<R: std::io::Read + std::io::Seek>(
    archive: &mut ZipArchive<R>,
    name: &str,
) -> Result<String, AppError> {
    let mut f = archive.by_name(name).map_err(|e| AppError::BadRequest(e.to_string()))?;
    let mut s = String::new();
    std::io::Read::read_to_string(&mut f, &mut s).map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(s)
}

/// POST /api/v1/config/plugin: multipart form with file field containing a zip (manifest.json + config JSONs).
pub async fn install_plugin(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let mut zip_bytes: Option<Vec<u8>> = None;
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or("").to_string();
        if name == "file" || name == "plugin" {
            let data = field.bytes().await.map_err(|e| AppError::BadRequest(e.to_string()))?;
            zip_bytes = Some(data.to_vec());
            break;
        }
    }
    let zip_bytes = zip_bytes.ok_or_else(|| AppError::BadRequest("missing 'file' or 'plugin' field in multipart body".into()))?;

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

    let mut applied = Vec::with_capacity(CONFIG_ORDER.len());
    for kind in CONFIG_ORDER {
        let file_name = format!("{}.json", kind);
        let file_name_with_slash = format!("{}/{}", kind, file_name);
        let content = read_zip_entry_to_string(&mut archive, &file_name)
            .or_else(|_| read_zip_entry_to_string(&mut archive, &file_name_with_slash))
            .unwrap_or_else(|_| DEFAULT_EMPTY_JSON.to_string());

        let body: Vec<Value> = serde_json::from_str(&content)
            .map_err(|e| AppError::BadRequest(format!("invalid {}: {}", file_name, e)))?;
        replace_config(&state.pool, kind, body).await?;
        applied.push(kind.to_string());
    }

    upsert_plugin(&state.pool, id, &manifest_value).await?;

    #[derive(serde::Serialize)]
    struct PluginInstallResponse {
        plugin: Value,
        applied: Vec<String>,
    }
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessOne {
            data: PluginInstallResponse {
                plugin: manifest_value,
                applied,
            },
            meta: None,
        }),
    ))
}
