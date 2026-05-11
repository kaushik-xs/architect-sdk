//! Example server: ensures _sys_* tables exist, then loads config from PACKAGE_PATH (package directory with manifest.json) or from DB (config APIs).
//! If PACKAGE_PATH is set, config is loaded from that directory (must contain manifest.json + config JSONs) and migrations applied; otherwise config is loaded from _sys_* tables (empty until fed via config APIs or package install).

use architect_sdk::{
    apply_migrations,
    common_routes_with_ready,
    config_routes,
    entity_routes,
    ensure_database_exists,
    ensure_sys_tables,
    load_from_pool,
    load_registry_from_pool,
    resolve,
    AppState,
    DEFAULT_PACKAGE_ID,
    FullConfig,
};
use axum::Router;
use sqlx::PgPool;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("architect_sdk=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .init();

    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/architect".into());
    ensure_database_exists(&database_url).await?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    ensure_sys_tables(&pool).await?;

    let tenant_registry = load_registry_from_pool(&pool).await.map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
    tracing::info!("loaded tenant registry (X-Tenant-ID required for config and entity APIs)");

    let (config, package_id) = match std::env::var("PACKAGE_PATH") {
        Ok(package_path) => {
            tracing::info!("loading config from package path: {}", package_path);
            let (cfg, id) = load_config_from_package_path(&package_path).await?;
            (cfg, id)
        }
        Err(_) => {
            tracing::info!("PACKAGE_PATH not set; loading config from _sys_* tables (use config APIs or POST /api/v1/config/package to insert)");
            let cfg = load_from_pool(&pool, DEFAULT_PACKAGE_ID).await.map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
            (cfg, DEFAULT_PACKAGE_ID.to_string())
        }
    };
    apply_migrations(&pool, &config, None, None).await?;
    let model = resolve(&config)?;
    let mut package_models = HashMap::new();
    package_models.insert(package_id.clone(), model.clone());
    let storage = architect_sdk::init_storage_provider().await;
    let state = AppState {
        pool: pool.clone(),
        model: Arc::new(RwLock::new(model)),
        package_models: Arc::new(RwLock::new(package_models)),
        tenant_pools: Arc::new(RwLock::new(HashMap::new())),
        tenant_registry: Arc::new(tenant_registry),
        storage,
    };

    let api = Router::new()
        .merge(common_routes_with_ready(state.clone()))
        .nest("/api/v1", config_routes(state.clone()))
        .nest("/api/v1", entity_routes(state));

    let app = Router::new()
        .nest("/", api);

    let listener = TcpListener::bind("0.0.0.0:3000").await?;
    tracing::info!("listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

/// Read all JSON records for a config kind from a package directory.
/// Tries `{kind}.json` first (flat file), then scans `{kind}/*.json` (subdirectory),
/// merging all arrays in alphabetical order. Returns an empty vec if neither exists.
async fn read_kind_from_dir(dir: &Path, kind: &str) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let flat = dir.join(format!("{}.json", kind));
    if flat.exists() {
        let content = tokio::fs::read_to_string(&flat).await?;
        return Ok(serde_json::from_str(&content)?);
    }

    let subdir = dir.join(kind);
    if subdir.is_dir() {
        let mut read_dir = tokio::fs::read_dir(&subdir).await?;
        let mut files: Vec<PathBuf> = Vec::new();
        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                files.push(path);
            }
        }
        files.sort();
        let mut merged: Vec<serde_json::Value> = Vec::new();
        for path in files {
            let content = tokio::fs::read_to_string(&path).await?;
            let mut items: Vec<serde_json::Value> = serde_json::from_str(&content)?;
            merged.append(&mut items);
        }
        return Ok(merged);
    }

    Ok(vec![])
}

async fn load_config_from_package_path(dir: &str) -> Result<(FullConfig, String), Box<dyn std::error::Error>> {
    let dir = PathBuf::from(dir);
    let manifest_path = dir.join("manifest.json");
    let manifest_json = tokio::fs::read_to_string(&manifest_path).await.map_err(|e| {
        format!("package path must contain manifest.json: {}", e)
    })?;
    let manifest: serde_json::Value = serde_json::from_str(&manifest_json)?;
    let manifest_obj = manifest.as_object().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "manifest.json must be an object"))?;
    let package_id = manifest_obj.get("id").and_then(|v| v.as_str()).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "manifest must have 'id' (string)"))?.to_string();
    let _name = manifest_obj.get("name").and_then(|v| v.as_str()).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "manifest must have 'name' (string)"))?;
    let _version = manifest_obj.get("version").and_then(|v| v.as_str()).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "manifest must have 'version' (string)"))?;
    let schema_name = manifest_obj.get("schema").and_then(|v| v.as_str()).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "manifest must have 'schema' (string)"))?;
    tracing::info!("package manifest: id={:?} name={:?} version={:?} schema={:?}", package_id, _name, _version, schema_name);

    let schemas = vec![serde_json::json!({ "id": "default", "name": schema_name })];
    let schemas: Vec<architect_sdk::config::SchemaConfig> = serde_json::from_value(serde_json::Value::Array(schemas))?;

    let mut enums = read_kind_from_dir(&dir, "enums").await?;
    for o in enums.iter_mut() {
        if let Some(obj) = o.as_object_mut() {
            obj.entry("schema_id").or_insert_with(|| serde_json::Value::String("default".into()));
        }
    }
    let enums: Vec<architect_sdk::config::EnumConfig> = serde_json::from_value(serde_json::Value::Array(enums))?;

    let mut tables = read_kind_from_dir(&dir, "tables").await?;
    for o in tables.iter_mut() {
        if let Some(obj) = o.as_object_mut() {
            obj.entry("schema_id").or_insert_with(|| serde_json::Value::String("default".into()));
        }
    }
    let tables: Vec<architect_sdk::config::TableConfig> = serde_json::from_value(serde_json::Value::Array(tables))?;

    let columns_raw = read_kind_from_dir(&dir, "columns").await?;
    let columns: Vec<architect_sdk::config::ColumnConfig> = serde_json::from_value(serde_json::Value::Array(columns_raw))?;

    let mut indexes = read_kind_from_dir(&dir, "indexes").await?;
    for o in indexes.iter_mut() {
        if let Some(obj) = o.as_object_mut() {
            obj.entry("schema_id").or_insert_with(|| serde_json::Value::String("default".into()));
        }
    }
    let indexes: Vec<architect_sdk::config::IndexConfig> = serde_json::from_value(serde_json::Value::Array(indexes))?;

    let mut relationships = read_kind_from_dir(&dir, "relationships").await?;
    for o in relationships.iter_mut() {
        if let Some(obj) = o.as_object_mut() {
            obj.entry("from_schema_id").or_insert_with(|| serde_json::Value::String("default".into()));
            obj.entry("to_schema_id").or_insert_with(|| serde_json::Value::String("default".into()));
        }
    }
    let relationships: Vec<architect_sdk::config::RelationshipConfig> = serde_json::from_value(serde_json::Value::Array(relationships))?;

    let api_entities_raw = read_kind_from_dir(&dir, "api_entities").await?;
    let api_entities: Vec<architect_sdk::config::ApiEntityConfig> = serde_json::from_value(serde_json::Value::Array(api_entities_raw))?;

    let kv_stores_raw = read_kind_from_dir(&dir, "kv_stores").await?;
    let kv_stores: Vec<architect_sdk::config::KvStoreConfig> = serde_json::from_value(serde_json::Value::Array(kv_stores_raw))?;

    Ok((
        FullConfig {
            schemas,
            enums,
            tables,
            columns,
            indexes,
            relationships,
            api_entities,
            kv_stores,
        },
        package_id,
    ))
}
