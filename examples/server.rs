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
    seed_default_tenants(&pool, &database_url).await?;
    tracing::info!("seeded default tenants: default-mode-1 (database), default-mode-3 (rls)");

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
    let state = AppState {
        pool: pool.clone(),
        model: Arc::new(RwLock::new(model)),
        package_models: Arc::new(RwLock::new(package_models)),
        tenant_pools: Arc::new(RwLock::new(HashMap::new())),
        tenant_registry: Arc::new(tenant_registry),
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

    let kv_stores: Vec<architect_sdk::config::KvStoreConfig> = serde_json::from_str(
        &tokio::fs::read_to_string(dir.join("kv_stores.json")).await.unwrap_or_else(|_| "[]".into()),
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
            kv_stores,
        },
        package_id,
    ))
}

/// Seed default tenants for the example server (database and rls strategies). Isolated from the core library.
async fn seed_default_tenants(pool: &PgPool, central_database_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let schema = std::env::var("ARCHITECT_SCHEMA").unwrap_or_else(|_| "architect".into());
    let q_table = format!("{}.{}", schema, "_sys_tenants");

    let (_, central_db) = parse_db_name_from_url(central_database_url)
        .map_err(|e| format!("DATABASE_URL: {}", e))?;
    let tenant_db_name = if central_db.is_empty() || central_db == "postgres" {
        "architect_tenant_default_mode_1".to_string()
    } else {
        format!("{}_tenant_default_mode_1", central_db)
    };
    let database_url = database_url_with_name(central_database_url, &tenant_db_name)?;
    ensure_database_exists(&database_url).await
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;

    let database_url_mode3 = database_url_with_name(central_database_url, "temp_2")?;
    ensure_database_exists(&database_url_mode3).await
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;

    let insert_sql = format!(
        r#"
        INSERT INTO {} (id, strategy, database_url, updated_at, comment)
        VALUES
            ($1, $2, $3, NOW(), $4),
            ($5, $6, $7, NOW(), $8)
        ON CONFLICT (id) DO NOTHING
        "#,
        q_table
    );
    sqlx::query(&insert_sql)
        .bind("default-mode-1")
        .bind("database")
        .bind(&database_url)
        .bind("Tenant with own database (seed)")
        .bind("default-mode-3")
        .bind("rls")
        .bind(&database_url_mode3)
        .bind("Tenant with RLS in shared DB (seed)")
        .execute(pool)
        .await?;
    Ok(())
}

fn parse_db_name_from_url(url: &str) -> Result<(String, String), String> {
    let path_start = url.rfind('/').ok_or("no path")? + 1;
    let path_and_query = url.get(path_start..).unwrap_or("");
    let db_name = path_and_query.split('?').next().unwrap_or("").trim();
    let base = url.get(..path_start).unwrap_or(url);
    Ok((format!("{}postgres", base), db_name.to_string()))
}

fn database_url_with_name(base_url: &str, new_db_name: &str) -> Result<String, String> {
    let path_start = base_url.rfind('/').ok_or("no path")? + 1;
    let base = base_url.get(..path_start).unwrap_or(base_url);
    let query = base_url.get(path_start..).and_then(|s| s.find('?').map(|i| &s[i..])).unwrap_or("");
    Ok(format!("{}{}{}", base, new_db_name, query))
}
