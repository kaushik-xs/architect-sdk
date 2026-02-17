//! Example consumer: a separate Rust project that uses architect-sdk as a dependency.
//!
//! Run from repo root: `cargo run -p example-consumer`
//! Or from this directory: `cargo run`

use architect_sdk::{
    common_routes_with_ready,
    ensure_database_exists,
    ensure_sys_tables,
    load_from_pool,
    load_registry_from_pool,
    resolve,
    AppState,
    DEFAULT_PACKAGE_ID,
};
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("architect_sdk=info")),
        )
        .init();

    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/architect".into());
    ensure_database_exists(&database_url).await?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    ensure_sys_tables(&pool).await?;
    let _registry = load_registry_from_pool(&pool).await?;
    let config = load_from_pool(&pool, DEFAULT_PACKAGE_ID).await?;
    let model = resolve(&config)?;
    let state = AppState {
        pool: pool.clone(),
        model: Arc::new(std::sync::RwLock::new(model)),
        package_models: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        tenant_pools: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        tenant_registry: Arc::new(_registry),
    };

    let app = common_routes_with_ready(state);
    let listener = TcpListener::bind("127.0.0.1:3000").await?;
    let port = listener.local_addr()?.port();
    tracing::info!("Example consumer listening on http://127.0.0.1:{}", port);
    axum::serve(listener, app).await?;
    Ok(())
}
