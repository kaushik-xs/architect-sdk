//! Example server: ensures _private_* tables exist, then loads config from PLUGIN_PATH (plugin directory with manifest.json) or from DB (config APIs).
//! If PLUGIN_PATH is set, config is loaded from that directory (must contain manifest.json + config JSONs) and migrations applied; otherwise config is loaded from _private_* tables (empty until fed via config APIs or plugin install).

use architect_sdk::{
    apply_migrations,
    common_routes_with_ready,
    config_routes,
    entity_routes,
    ensure_database_exists,
    ensure_private_tables,
    load_from_pool,
    resolve,
    AppState,
    FullConfig,
};
use axum::Router;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("architect_sdk=info".parse()?))
        .init();

    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/architect".into());
    ensure_database_exists(&database_url).await?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    ensure_private_tables(&pool).await?;

    let config = match std::env::var("PLUGIN_PATH") {
        Ok(plugin_path) => {
            tracing::info!("loading config from plugin path: {}", plugin_path);
            load_config_from_plugin_path(&plugin_path).await?
        }
        Err(_) => {
            tracing::info!("PLUGIN_PATH not set; loading config from _private_* tables (use config APIs or POST /api/v1/config/plugin to insert)");
            load_from_pool(&pool).await.map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?
        }
    };
    apply_migrations(&pool, &config).await?;
    let model = resolve(&config)?;
    let state = AppState {
        pool: pool.clone(),
        model: Arc::new(model),
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

async fn load_config_from_plugin_path(dir: &str) -> Result<FullConfig, Box<dyn std::error::Error>> {
    let dir = PathBuf::from(dir);
    let manifest_path = dir.join("manifest.json");
    let manifest_json = tokio::fs::read_to_string(&manifest_path).await.map_err(|e| {
        format!("plugin path must contain manifest.json: {}", e)
    })?;
    let manifest: serde_json::Value = serde_json::from_str(&manifest_json)?;
    let manifest_obj = manifest.as_object().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "manifest.json must be an object"))?;
    let _id = manifest_obj.get("id").and_then(|v| v.as_str()).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "manifest must have 'id' (string)"))?;
    let _name = manifest_obj.get("name").and_then(|v| v.as_str()).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "manifest must have 'name' (string)"))?;
    let _version = manifest_obj.get("version").and_then(|v| v.as_str()).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "manifest must have 'version' (string)"))?;
    tracing::info!("plugin manifest: id={:?} name={:?} version={:?}", _id, _name, _version);

    let schemas: Vec<_> = serde_json::from_str(&tokio::fs::read_to_string(dir.join("schemas.json")).await?)?;
    let enums: Vec<_> = serde_json::from_str(&tokio::fs::read_to_string(dir.join("enums.json")).await?)?;
    let tables: Vec<_> = serde_json::from_str(&tokio::fs::read_to_string(dir.join("tables.json")).await?)?;
    let columns: Vec<_> = serde_json::from_str(&tokio::fs::read_to_string(dir.join("columns.json")).await?)?;
    let indexes: Vec<_> = serde_json::from_str(&tokio::fs::read_to_string(dir.join("indexes.json")).await?)?;
    let relationships: Vec<_> = serde_json::from_str(&tokio::fs::read_to_string(dir.join("relationships.json")).await?)?;
    let api_entities: Vec<_> = serde_json::from_str(
        &tokio::fs::read_to_string(dir.join("api_entities.json")).await.unwrap_or_else(|_| "[]".into()),
    )?;

    Ok(FullConfig {
        schemas,
        enums,
        tables,
        columns,
        indexes,
        relationships,
        api_entities,
    })
}
