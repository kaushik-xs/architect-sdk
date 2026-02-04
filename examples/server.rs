//! Example server: ensures _sys_* tables exist, then loads config from MODULE_PATH (module directory with manifest.json) or from DB (config APIs).
//! If MODULE_PATH is set, config is loaded from that directory (must contain manifest.json + config JSONs) and migrations applied; otherwise config is loaded from _sys_* tables (empty until fed via config APIs or module install).

use architect_sdk::{
    apply_migrations,
    common_routes_with_ready,
    config_routes,
    entity_routes,
    ensure_database_exists,
    ensure_sys_tables,
    load_from_pool,
    resolve,
    AppState,
    DEFAULT_MODULE_ID,
    FullConfig,
};
use axum::Router;
use std::collections::HashMap;
use std::path::PathBuf;
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

    let (config, module_id) = match std::env::var("MODULE_PATH") {
        Ok(module_path) => {
            tracing::info!("loading config from module path: {}", module_path);
            let (cfg, id) = load_config_from_module_path(&module_path).await?;
            (cfg, id)
        }
        Err(_) => {
            tracing::info!("MODULE_PATH not set; loading config from _sys_* tables (use config APIs or POST /api/v1/config/module to insert)");
            let cfg = load_from_pool(&pool, DEFAULT_MODULE_ID).await.map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
            (cfg, DEFAULT_MODULE_ID.to_string())
        }
    };
    apply_migrations(&pool, &config).await?;
    let model = resolve(&config)?;
    let mut module_models = HashMap::new();
    module_models.insert(module_id.clone(), model.clone());
    let state = AppState {
        pool: pool.clone(),
        model: Arc::new(RwLock::new(model)),
        module_models: Arc::new(RwLock::new(module_models)),
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

async fn load_config_from_module_path(dir: &str) -> Result<(FullConfig, String), Box<dyn std::error::Error>> {
    let dir = PathBuf::from(dir);
    let manifest_path = dir.join("manifest.json");
    let manifest_json = tokio::fs::read_to_string(&manifest_path).await.map_err(|e| {
        format!("module path must contain manifest.json: {}", e)
    })?;
    let manifest: serde_json::Value = serde_json::from_str(&manifest_json)?;
    let manifest_obj = manifest.as_object().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "manifest.json must be an object"))?;
    let module_id = manifest_obj.get("id").and_then(|v| v.as_str()).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "manifest must have 'id' (string)"))?.to_string();
    let _name = manifest_obj.get("name").and_then(|v| v.as_str()).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "manifest must have 'name' (string)"))?;
    let _version = manifest_obj.get("version").and_then(|v| v.as_str()).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "manifest must have 'version' (string)"))?;
    let schema_name = manifest_obj.get("schema").and_then(|v| v.as_str()).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "manifest must have 'schema' (string)"))?;
    tracing::info!("module manifest: id={:?} name={:?} version={:?} schema={:?}", module_id, _name, _version, schema_name);

    let schemas = vec![serde_json::json!({ "id": "default", "name": schema_name })];
    let schemas: Vec<architect_sdk::config::SchemaConfig> = serde_json::from_value(serde_json::Value::Array(schemas))?;

    let mut enums: Vec<serde_json::Value> = serde_json::from_str(&tokio::fs::read_to_string(dir.join("enums.json")).await?)?;
    for o in enums.iter_mut() {
        if let Some(obj) = o.as_object_mut() {
            obj.entry("schema_id").or_insert_with(|| serde_json::Value::String("default".into()));
        }
    }
    let enums: Vec<architect_sdk::config::EnumConfig> = serde_json::from_value(serde_json::Value::Array(enums))?;

    let mut tables: Vec<serde_json::Value> = serde_json::from_str(&tokio::fs::read_to_string(dir.join("tables.json")).await?)?;
    for o in tables.iter_mut() {
        if let Some(obj) = o.as_object_mut() {
            obj.entry("schema_id").or_insert_with(|| serde_json::Value::String("default".into()));
        }
    }
    let tables: Vec<architect_sdk::config::TableConfig> = serde_json::from_value(serde_json::Value::Array(tables))?;

    let columns: Vec<architect_sdk::config::ColumnConfig> = serde_json::from_str(&tokio::fs::read_to_string(dir.join("columns.json")).await?)?;

    let mut indexes: Vec<serde_json::Value> = serde_json::from_str(&tokio::fs::read_to_string(dir.join("indexes.json")).await?)?;
    for o in indexes.iter_mut() {
        if let Some(obj) = o.as_object_mut() {
            obj.entry("schema_id").or_insert_with(|| serde_json::Value::String("default".into()));
        }
    }
    let indexes: Vec<architect_sdk::config::IndexConfig> = serde_json::from_value(serde_json::Value::Array(indexes))?;

    let mut relationships: Vec<serde_json::Value> = serde_json::from_str(&tokio::fs::read_to_string(dir.join("relationships.json")).await?)?;
    for o in relationships.iter_mut() {
        if let Some(obj) = o.as_object_mut() {
            obj.entry("from_schema_id").or_insert_with(|| serde_json::Value::String("default".into()));
            obj.entry("to_schema_id").or_insert_with(|| serde_json::Value::String("default".into()));
        }
    }
    let relationships: Vec<architect_sdk::config::RelationshipConfig> = serde_json::from_value(serde_json::Value::Array(relationships))?;

    let api_entities: Vec<architect_sdk::config::ApiEntityConfig> = serde_json::from_str(
        &tokio::fs::read_to_string(dir.join("api_entities.json")).await.unwrap_or_else(|_| "[]".into()),
    )?;

    Ok((
        FullConfig {
            schemas,
            enums,
            tables,
            columns,
            indexes,
            relationships,
            api_entities,
        },
        module_id,
    ))
}
