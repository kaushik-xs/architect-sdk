//! Example server: loads config from _private_* tables or from env (config path), ensures _private_* tables exist, mounts common, config, and entity routes.

use architect_sdk::{
    apply_migrations,
    common_routes_with_ready,
    config_routes,
    entity_routes,
    ensure_database_exists,
    ensure_private_tables,
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

    let config_source = std::env::var("CONFIG_PATH").unwrap_or_else(|_| "sample".into());
    let config = load_config_from_path(&config_source).await?;
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

async fn load_config_from_path(dir: &str) -> Result<FullConfig, Box<dyn std::error::Error>> {
    let dir = PathBuf::from(dir);
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
