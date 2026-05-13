//! Package install/uninstall handlers. Install: zip upload, extract manifest + configs, apply configs, store manifest, reload model. Uninstall: revert migrations, delete _sys_* rows and package record. X-Tenant-ID is required.
//! Config is stored in the architect DB (DATABASE_URL). Schemas/tables are created in the tenant's target DB (for database/RLS with database_url).

use crate::config::{load_from_pool, resolve};
use crate::error::AppError;
use crate::extractors::tenant::TenantId;
use crate::handlers::config::{reload_model, replace_config};
use crate::handlers::entity::resolve_tenant_context;
use crate::migration::{apply_migrations, revert_migrations};
use crate::state::AppState;
use crate::store::{count_package_kind, delete_package_and_config, get_package, list_package_ids, list_packages, upsert_package};
use axum::extract::{Multipart, Path, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
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
    "kv_stores",
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
        "kv_stores" => &[],
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

/// Read all records for a config kind from a zip archive.
/// Tries `{kind}.json` first (flat file), then scans `{kind}/*.json` (subdirectory),
/// merging all arrays in alphabetical order. Returns an empty vec if neither exists.
fn read_kind_from_zip<R: std::io::Read + std::io::Seek>(
    archive: &mut ZipArchive<R>,
    kind: &str,
) -> Result<Vec<Value>, AppError> {
    let flat = format!("{}.json", kind);
    if let Ok(content) = read_zip_entry_to_string(archive, &flat) {
        return serde_json::from_str(&content)
            .map_err(|e| AppError::BadRequest(format!("invalid {}: {}", flat, e)));
    }

    let prefix = format!("{}/", kind);
    let mut names: Vec<String> = archive
        .file_names()
        .filter(|n| n.starts_with(&prefix) && n.ends_with(".json"))
        .map(String::from)
        .collect();
    names.sort();

    let mut merged: Vec<Value> = Vec::new();
    for name in names {
        let content = read_zip_entry_to_string(archive, &name)?;
        let mut items: Vec<Value> = serde_json::from_str(&content)
            .map_err(|e| AppError::BadRequest(format!("invalid {}: {}", name, e)))?;
        merged.append(&mut items);
    }
    Ok(merged)
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

    let ctx = resolve_tenant_context(&state, Some(tenant_id), Some(id)).await?;
    let config_pool = ctx.config_pool();
    let migration_pool = ctx.migration_pool();
    let schema_override = ctx.schema_override();
    let package_cache_key = ctx.package_cache_key().to_string();

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
            let mut body = read_kind_from_zip(&mut archive, kind)?;
            match *kind {
                "enums" | "tables" | "indexes" => inject_schema_id(&mut body, DEFAULT_SCHEMA_ID),
                "relationships" => inject_relationship_schema_ids(&mut body, DEFAULT_SCHEMA_ID),
                _ => {}
            }
            body
        };
        replace_config(config_pool, kind, body, false, id).await?;
        applied.push((*kind).to_string());
    }

    upsert_package(config_pool, id, &manifest_value).await?;

    let config = load_from_pool(config_pool, id).await.map_err(AppError::Config)?;
    apply_migrations(migration_pool, &config, schema_override, ctx.rls_tenant_column()).await?;
    let new_model = resolve(&config).map_err(AppError::Config)?;
    {
        let mut guard = state.model.write().map_err(|_| AppError::BadRequest("state lock".into()))?;
        *guard = new_model.clone();
        state
            .package_models
            .write()
            .map_err(|_| AppError::BadRequest("state lock".into()))?
            .insert(package_cache_key, new_model);
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

#[derive(Deserialize)]
pub struct UninstallPath {
    pub package_id: String,
}

/// DELETE /api/v1/config/package/:package_id — uninstall package: revert migrations in tenant DB, delete all _sys_* config and KV data, remove package record. X-Tenant-ID required.
pub async fn uninstall_package(
    TenantId(tenant_id_opt): TenantId,
    State(state): State<AppState>,
    Path(UninstallPath { package_id }): Path<UninstallPath>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let tenant_id = tenant_id_opt
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("X-Tenant-ID header is required".into()))?;

    let ctx = resolve_tenant_context(&state, Some(tenant_id), Some(&package_id)).await?;
    let config_pool = ctx.config_pool();
    let migration_pool = ctx.migration_pool();
    let schema_override = ctx.schema_override();
    let package_cache_key = ctx.package_cache_key().to_string();

    let installed = list_package_ids(config_pool).await?;
    if !installed.contains(&package_id) {
        return Err(AppError::NotFound(format!("package not found: {}", package_id)));
    }

    let config = load_from_pool(config_pool, &package_id).await.map_err(AppError::Config)?;
    revert_migrations(migration_pool, &config, schema_override).await?;
    delete_package_and_config(config_pool, &package_id).await?;

    {
        state
            .package_models
            .write()
            .map_err(|_| AppError::BadRequest("state lock".into()))?
            .remove(&package_cache_key);
    }

    // Reload default model when uninstall was on the central DB so in-memory state stays in sync (no process restart needed).
    if std::ptr::eq(&state.pool as *const _, config_pool as *const _) {
        let _ = reload_model(&state).await;
    }

    #[derive(serde::Serialize)]
    struct UninstallResponse {
        package_id: String,
    }
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessOne {
            data: UninstallResponse { package_id },
            meta: None,
        }),
    ))
}

/// Build the stats + full config payload for a package by fetching all 8 config kinds in parallel.
async fn package_detail_data(pool: &sqlx::PgPool, package_id: &str) -> Result<Value, crate::error::AppError> {
    use crate::handlers::config::get_config;

    let (schemas, enums, tables, columns, indexes, relationships, api_entities, kv_stores) = tokio::try_join!(
        get_config(pool, "schemas", package_id),
        get_config(pool, "enums", package_id),
        get_config(pool, "tables", package_id),
        get_config(pool, "columns", package_id),
        get_config(pool, "indexes", package_id),
        get_config(pool, "relationships", package_id),
        get_config(pool, "api_entities", package_id),
        get_config(pool, "kv_stores", package_id),
    )?;

    Ok(json!({
        "stats": {
            "schemas": schemas.len(),
            "enums": enums.len(),
            "tables": tables.len(),
            "columns": columns.len(),
            "indexes": indexes.len(),
            "relationships": relationships.len(),
            "apiEntities": api_entities.len(),
            "kvStores": kv_stores.len(),
        },
        "schemas": schemas,
        "enums": enums,
        "tables": tables,
        "columns": columns,
        "indexes": indexes,
        "relationships": relationships,
        "apiEntities": api_entities,
        "kvStores": kv_stores,
    }))
}

/// GET /api/v1/config/packages — list all installed packages with manifest info and per-kind counts.
pub async fn list_packages_handler(
    State(state): State<AppState>,
) -> Result<impl axum::response::IntoResponse, crate::error::AppError> {
    let packages = list_packages(&state.pool).await?;

    let mut items: Vec<Value> = Vec::with_capacity(packages.len());
    for pkg in packages {
        let (schemas, enums, tables, columns, indexes, relationships, api_entities, kv_stores) = tokio::try_join!(
            count_package_kind(&state.pool, "schemas", &pkg.id),
            count_package_kind(&state.pool, "enums", &pkg.id),
            count_package_kind(&state.pool, "tables", &pkg.id),
            count_package_kind(&state.pool, "columns", &pkg.id),
            count_package_kind(&state.pool, "indexes", &pkg.id),
            count_package_kind(&state.pool, "relationships", &pkg.id),
            count_package_kind(&state.pool, "api_entities", &pkg.id),
            count_package_kind(&state.pool, "kv_stores", &pkg.id),
        )?;

        let name = pkg.payload.get("name").and_then(Value::as_str).map(String::from);
        let version = pkg.payload.get("version").and_then(Value::as_str).map(String::from);
        let schema = pkg.payload.get("schema").and_then(Value::as_str).map(String::from);

        items.push(json!({
            "id": pkg.id,
            "name": name,
            "version": version,
            "schema": schema,
            "installedVersion": pkg.version,
            "updatedAt": pkg.updated_at,
            "stats": {
                "schemas": schemas,
                "enums": enums,
                "tables": tables,
                "columns": columns,
                "indexes": indexes,
                "relationships": relationships,
                "apiEntities": api_entities,
                "kvStores": kv_stores,
            },
        }));
    }

    let count = items.len() as u64;
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessMany {
            data: items,
            meta: crate::response::MetaCount { count },
        }),
    ))
}

#[derive(Deserialize)]
pub struct PackageIdPath {
    pub package_id: String,
}

/// GET /api/v1/config/packages/:package_id — full details of one installed package including all config objects.
pub async fn get_package_handler(
    State(state): State<AppState>,
    Path(PackageIdPath { package_id }): Path<PackageIdPath>,
) -> Result<impl axum::response::IntoResponse, crate::error::AppError> {
    let pkg = get_package(&state.pool, &package_id)
        .await?
        .ok_or_else(|| crate::error::AppError::NotFound(format!("package not found: {}", package_id)))?;

    let name = pkg.payload.get("name").and_then(Value::as_str).map(String::from);
    let version = pkg.payload.get("version").and_then(Value::as_str).map(String::from);
    let schema = pkg.payload.get("schema").and_then(Value::as_str).map(String::from);

    let mut detail = package_detail_data(&state.pool, &package_id).await?;
    let obj = detail.as_object_mut().unwrap();
    obj.insert("id".into(), json!(pkg.id));
    obj.insert("name".into(), json!(name));
    obj.insert("version".into(), json!(version));
    obj.insert("schema".into(), json!(schema));
    obj.insert("installedVersion".into(), json!(pkg.version));
    obj.insert("updatedAt".into(), json!(pkg.updated_at));
    obj.insert("manifest".into(), pkg.payload);

    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessOne {
            data: detail,
            meta: None,
        }),
    ))
}
