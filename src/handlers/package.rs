//! Package install/uninstall handlers. Install: zip upload, extract manifest + configs, apply configs, store manifest, reload model. Uninstall: revert migrations, delete _sys_* rows and package record. X-Tenant-ID is required.
//! Config is stored in the architect DB (DATABASE_URL). Schemas/tables are created in the tenant's target DB (for database/RLS with database_url).

use crate::config::{load_from_pool, resolve};
use crate::error::AppError;
use crate::extractors::tenant::TenantId;
use crate::handlers::config::{reload_model, replace_config};
use crate::handlers::entity::resolve_tenant_context;
use crate::migration::{apply_migrations, compute_migration_plan, execute_migration_plan, revert_migrations, MigrationPlan};
use crate::state::AppState;
use crate::store::{
    count_package_kind, delete_package_and_config, get_migration_plan, get_package,
    list_package_ids, list_packages, mark_migration_plan_applied, save_migration_plan, upsert_package,
};
use axum::extract::{Multipart, Path, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::io::Cursor;
use uuid::Uuid;
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

    let incoming_version = manifest_obj
        .get("version")
        .and_then(Value::as_str)
        .unwrap_or("");

    // For upgrades: load old config BEFORE replacing so we can diff
    let is_upgrade = if let Some(existing) = get_package(config_pool, id).await? {
        if existing.semantic_version.as_deref() == Some(incoming_version) {
            return Err(AppError::Conflict(format!(
                "package '{}' version '{}' is already installed",
                id, incoming_version
            )));
        }
        true
    } else {
        false
    };

    let old_config = if is_upgrade {
        Some(load_from_pool(config_pool, id).await.map_err(AppError::Config)?)
    } else {
        None
    };

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

    // Fresh install: full apply_migrations. Upgrade: compute diff and execute only changed DDL.
    let migration_warnings: Vec<String> = if let Some(ref old) = old_config {
        let plan = compute_migration_plan(old, &config, schema_override, ctx.rls_tenant_column())
            .map_err(|e| AppError::BadRequest(format!("migration plan error: {}", e)))?;
        let migration_id = Uuid::new_v4().to_string();
        let result = execute_migration_plan(
            migration_pool, config_pool, &plan,
            &migration_id, id, tenant_id, old_config.as_ref().and_then(|_| {
                manifest_value.get("version").and_then(Value::as_str)
            }),
            incoming_version,
        ).await?;
        result.warnings
    } else {
        apply_migrations(migration_pool, &config, schema_override, ctx.rls_tenant_column()).await?;
        Vec::new()
    };

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
        warnings: Vec<String>,
    }
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessOne {
            data: PackageInstallResponse {
                package: manifest_value,
                applied,
                warnings: migration_warnings,
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

// ─── Migration preview / apply ───────────────────────────────────────────────

/// POST /api/v1/config/package/migration/preview
/// Upload a package zip to preview the migration plan without applying any changes.
/// The returned `migration_id` can be passed to the apply endpoint after review.
/// X-Tenant-ID required. Only valid for upgrades (package must already be installed).
pub async fn preview_migration_handler(
    TenantId(tenant_id_opt): TenantId,
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let tenant_id = tenant_id_opt
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("X-Tenant-ID header is required".into()))?;

    let mut zip_bytes_raw: Option<Vec<u8>> = None;
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or("").to_string();
        if name == "file" || name == "package" {
            let data = field.bytes().await.map_err(|e| AppError::BadRequest(e.to_string()))?;
            zip_bytes_raw = Some(data.to_vec());
            break;
        }
    }
    let zip_bytes = zip_bytes_raw.ok_or_else(|| AppError::BadRequest("missing 'file' or 'package' field".into()))?;

    let mut archive = ZipArchive::new(Cursor::new(zip_bytes.clone()))
        .map_err(|e| AppError::BadRequest(format!("invalid zip: {}", e)))?;

    let manifest_name = archive
        .file_names()
        .find(|n| *n == "manifest.json" || n.ends_with("/manifest.json"))
        .map(String::from)
        .ok_or_else(|| AppError::BadRequest("zip must contain manifest.json".into()))?;

    let manifest_value: Value = {
        let mut file = archive.by_name(&manifest_name).map_err(|e| AppError::BadRequest(e.to_string()))?;
        let mut buf = String::new();
        std::io::Read::read_to_string(&mut file, &mut buf).map_err(|e| AppError::BadRequest(e.to_string()))?;
        serde_json::from_str(&buf).map_err(|e| AppError::BadRequest(format!("invalid manifest.json: {}", e)))?
    };
    let manifest_obj = manifest_value.as_object().ok_or_else(|| AppError::BadRequest("manifest.json must be an object".into()))?;

    let id = manifest_obj.get("id").and_then(Value::as_str)
        .ok_or_else(|| AppError::BadRequest("manifest must have 'id'".into()))?;
    let incoming_version = manifest_obj.get("version").and_then(Value::as_str).unwrap_or("");
    let schema_name = manifest_obj.get("schema").and_then(Value::as_str)
        .ok_or_else(|| AppError::BadRequest("manifest must have 'schema'".into()))?;

    let existing = get_package(&state.pool, id).await?
        .ok_or_else(|| AppError::NotFound(format!("package '{}' is not installed — preview is only for upgrades", id)))?;

    if existing.semantic_version.as_deref() == Some(incoming_version) {
        return Err(AppError::Conflict(format!("package '{}' version '{}' is already installed", id, incoming_version)));
    }

    let from_version = existing.semantic_version.clone();
    let ctx = resolve_tenant_context(&state, Some(tenant_id), Some(id)).await?;
    let config_pool = ctx.config_pool();

    let old_config = load_from_pool(config_pool, id).await.map_err(AppError::Config)?;

    // Build new FullConfig from the zip (same logic as install_package, without writing to DB)
    let schemas_body = vec![serde_json::json!({ "id": DEFAULT_SCHEMA_ID, "name": schema_name })];
    let config_kinds = ["schemas", "enums", "tables", "columns", "indexes", "relationships", "api_entities", "kv_stores"];
    let mut all_values: std::collections::HashMap<String, Vec<Value>> = std::collections::HashMap::new();
    for kind in &config_kinds {
        let body: Vec<Value> = if *kind == "schemas" {
            serde_json::from_value(Value::Array(schemas_body.clone())).unwrap_or_default()
        } else {
            let mut body = read_kind_from_zip(&mut archive, kind).unwrap_or_default();
            match *kind {
                "enums" | "tables" | "indexes" => inject_schema_id(&mut body, DEFAULT_SCHEMA_ID),
                "relationships" => inject_relationship_schema_ids(&mut body, DEFAULT_SCHEMA_ID),
                _ => {}
            }
            body
        };
        all_values.insert(kind.to_string(), body);
    }

    // Deserialize into FullConfig manually using the same logic as load_from_pool
    let new_config = build_full_config_from_values(&all_values)?;

    let plan = compute_migration_plan(&old_config, &new_config, ctx.schema_override(), ctx.rls_tenant_column())
        .map_err(|e| AppError::BadRequest(format!("migration plan error: {}", e)))?;

    let summary = plan.summary();
    let plan_json = serde_json::to_value(&plan).map_err(|e| AppError::BadRequest(e.to_string()))?;
    let migration_id = Uuid::new_v4().to_string();

    save_migration_plan(
        config_pool, &migration_id, id, tenant_id,
        from_version.as_deref(), incoming_version,
        &plan_json, &zip_bytes,
    ).await?;

    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessOne {
            data: json!({
                "migration_id": migration_id,
                "package_id": id,
                "from_version": from_version,
                "to_version": incoming_version,
                "expires_in_hours": 24,
                "summary": {
                    "total": summary.total,
                    "safe": summary.safe,
                    "best_effort": summary.best_effort,
                    "warn_only": summary.warn_only,
                },
                "steps": plan.steps,
            }),
            meta: None,
        }),
    ))
}

#[derive(Deserialize)]
pub struct MigrationIdPath {
    pub migration_id: String,
}

/// POST /api/v1/config/package/migration/apply/:migration_id
/// Apply a previously previewed migration plan. Idempotent: calling twice returns 409.
/// Applies config changes to _sys_* tables, executes DDL against the tenant DB, and writes audit records.
/// X-Tenant-ID required.
pub async fn apply_migration_handler(
    TenantId(tenant_id_opt): TenantId,
    State(state): State<AppState>,
    Path(MigrationIdPath { migration_id }): Path<MigrationIdPath>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let tenant_id = tenant_id_opt
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("X-Tenant-ID header is required".into()))?;

    let row = get_migration_plan(&state.pool, &migration_id).await?
        .ok_or_else(|| AppError::NotFound(format!("migration plan '{}' not found", migration_id)))?;

    if row.status == "applied" {
        return Err(AppError::Conflict(format!("migration plan '{}' has already been applied", migration_id)));
    }
    if row.status != "pending" {
        return Err(AppError::BadRequest(format!("migration plan '{}' has status '{}' and cannot be applied", migration_id, row.status)));
    }

    let now = chrono::Utc::now();
    if now > row.expires_at {
        return Err(AppError::BadRequest(format!("migration plan '{}' expired at {} — re-run preview to generate a new plan", migration_id, row.expires_at)));
    }

    if row.tenant_id != tenant_id {
        return Err(AppError::BadRequest(format!("migration plan '{}' was created for tenant '{}', not '{}'", migration_id, row.tenant_id, tenant_id)));
    }

    let plan: MigrationPlan = serde_json::from_value(row.plan_json.clone())
        .map_err(|e| AppError::BadRequest(format!("corrupted migration plan: {}", e)))?;

    let ctx = resolve_tenant_context(&state, Some(tenant_id), Some(&row.package_id)).await?;
    let config_pool = ctx.config_pool();
    let migration_pool = ctx.migration_pool();
    let package_cache_key = ctx.package_cache_key().to_string();

    // Re-apply configs from the stored zip bytes
    let mut archive = ZipArchive::new(Cursor::new(row.zip_bytes.clone()))
        .map_err(|e| AppError::BadRequest(format!("stored zip corrupted: {}", e)))?;

    let manifest_name = archive
        .file_names()
        .find(|n| *n == "manifest.json" || n.ends_with("/manifest.json"))
        .map(String::from)
        .ok_or_else(|| AppError::BadRequest("stored zip missing manifest.json".into()))?;

    let manifest_value: Value = {
        let mut file = archive.by_name(&manifest_name).map_err(|e| AppError::BadRequest(e.to_string()))?;
        let mut buf = String::new();
        std::io::Read::read_to_string(&mut file, &mut buf).map_err(|e| AppError::BadRequest(e.to_string()))?;
        serde_json::from_str(&buf).map_err(|e| AppError::BadRequest(format!("invalid manifest: {}", e)))?
    };
    let schema_name = manifest_value.get("schema").and_then(Value::as_str)
        .ok_or_else(|| AppError::BadRequest("manifest missing 'schema'".into()))?;

    let schemas_body = vec![serde_json::json!({ "id": DEFAULT_SCHEMA_ID, "name": schema_name })];
    let apply_order = config_apply_order();
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
        replace_config(config_pool, kind, body, false, &row.package_id).await?;
    }
    upsert_package(config_pool, &row.package_id, &manifest_value).await?;

    // Atomically mark plan as applied (prevents double-apply under concurrent requests)
    let claimed = mark_migration_plan_applied(config_pool, &migration_id).await?;
    if !claimed {
        return Err(AppError::Conflict(format!("migration plan '{}' was applied by a concurrent request", migration_id)));
    }

    // Execute the DDL plan with audit
    let result = execute_migration_plan(
        migration_pool, config_pool, &plan,
        &migration_id, &row.package_id, tenant_id,
        row.from_version.as_deref(), &row.to_version,
    ).await?;

    // Reload in-memory model
    let new_config = load_from_pool(config_pool, &row.package_id).await.map_err(AppError::Config)?;
    let new_model = resolve(&new_config).map_err(AppError::Config)?;
    {
        let mut guard = state.model.write().map_err(|_| AppError::BadRequest("state lock".into()))?;
        *guard = new_model.clone();
        state.package_models
            .write()
            .map_err(|_| AppError::BadRequest("state lock".into()))?
            .insert(package_cache_key, new_model);
    }

    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessOne {
            data: json!({
                "migration_id": migration_id,
                "package_id": row.package_id,
                "from_version": row.from_version,
                "to_version": row.to_version,
                "steps_applied": result.applied,
                "steps_warned": result.warned,
                "warnings": result.warnings,
            }),
            meta: None,
        }),
    ))
}

/// Build a FullConfig from pre-parsed per-kind value maps (used in preview, without touching the DB).
fn build_full_config_from_values(
    values: &std::collections::HashMap<String, Vec<Value>>,
) -> Result<crate::config::FullConfig, AppError> {
    fn parse_kind<T: serde::de::DeserializeOwned>(values: &std::collections::HashMap<String, Vec<Value>>, key: &str) -> Result<Vec<T>, AppError> {
        let arr = values.get(key).cloned().unwrap_or_default();
        arr.into_iter()
            .map(|v| serde_json::from_value(v).map_err(|e| AppError::BadRequest(format!("{} parse error: {}", key, e))))
            .collect()
    }

    Ok(crate::config::FullConfig {
        schemas: parse_kind(values, "schemas")?,
        enums: parse_kind(values, "enums")?,
        tables: parse_kind(values, "tables")?,
        columns: parse_kind(values, "columns")?,
        indexes: parse_kind(values, "indexes")?,
        relationships: parse_kind(values, "relationships")?,
        api_entities: parse_kind(values, "api_entities")?,
        kv_stores: parse_kind(values, "kv_stores")?,
    })
}
