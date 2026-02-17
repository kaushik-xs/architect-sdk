//! _sys_* table DDL and config persistence. All _sys_* tables live in a schema named from `ARCHITECT_SCHEMA` env (default `architect`).

use crate::error::AppError;
use sqlx::ConnectOptions;
use sqlx::PgPool;
use std::collections::HashMap;
use std::str::FromStr;

/// Schema name for _sys_* tables. From env `ARCHITECT_SCHEMA`, default `architect`. Must be a valid PostgreSQL identifier.
pub fn architect_schema() -> String {
    std::env::var("ARCHITECT_SCHEMA").unwrap_or_else(|_| "architect".into())
}

/// Returns schema-qualified table name for _sys_* tables (e.g. "architect._sys_schemas").
pub fn qualified_sys_table(table: &str) -> String {
    format!("{}.{}", architect_schema(), table)
}

/// Config tables (each row is keyed by id + package_id). Excludes _sys_packages.
const CONFIG_TABLES: &[&str] = &[
    "_sys_schemas",
    "_sys_enums",
    "_sys_tables",
    "_sys_columns",
    "_sys_indexes",
    "_sys_relationships",
    "_sys_api_entities",
];

/// Package id used when config is posted directly (no package install). Ensures (id, package_id) is unique per package.
pub const DEFAULT_PACKAGE_ID: &str = "_default";

/// Create schema from `ARCHITECT_SCHEMA` env if not exists, then _sys_* tables.
/// Config tables have (id, package_id) as composite primary key; _sys_packages has id only.
pub async fn ensure_sys_tables(pool: &PgPool) -> Result<(), AppError> {
    let schema = architect_schema();
    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {}", schema))
        .execute(pool)
        .await?;

    for table in CONFIG_TABLES {
        let q_table = qualified_sys_table(table);
        let ddl = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id TEXT NOT NULL,
                package_id TEXT NOT NULL,
                payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                version BIGINT NOT NULL DEFAULT 1,
                PRIMARY KEY (id, package_id)
            )
            "#,
            q_table
        );
        sqlx::query(&ddl).execute(pool).await?;
        let alter_version = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1",
            q_table
        );
        let _ = sqlx::query(&alter_version).execute(pool).await;
        let alter_package = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS package_id TEXT NOT NULL DEFAULT '{}'",
            q_table, DEFAULT_PACKAGE_ID
        );
        let _ = sqlx::query(&alter_package).execute(pool).await;

        let history_table = qualified_sys_table(&format!("{}_history", table));
        let history_ddl = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id TEXT NOT NULL,
                package_id TEXT NOT NULL,
                payload JSONB NOT NULL,
                version BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (id, package_id, version)
            )
            "#,
            history_table
        );
        sqlx::query(&history_ddl).execute(pool).await?;
        let alter_history_package = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS package_id TEXT NOT NULL DEFAULT '{}'",
            history_table, DEFAULT_PACKAGE_ID
        );
        let _ = sqlx::query(&alter_history_package).execute(pool).await;
    }

    let q_packages = qualified_sys_table("_sys_packages");
    let packages_ddl = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {} (
            id TEXT PRIMARY KEY,
            payload JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            version BIGINT NOT NULL DEFAULT 1
        )
        "#,
        q_packages
    );
    sqlx::query(&packages_ddl).execute(pool).await?;
    let q_packages_history = qualified_sys_table("_sys_packages_history");
    let packages_history_ddl = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {} (
            id TEXT NOT NULL,
            payload JSONB NOT NULL,
            version BIGINT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (id, version)
        )
        "#,
        q_packages_history
    );
    sqlx::query(&packages_history_ddl).execute(pool).await?;

    let q_tenants = qualified_sys_table("_sys_tenants");
    let tenants_ddl = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {} (
            id TEXT PRIMARY KEY,
            strategy TEXT NOT NULL,
            database_url TEXT,
            schema_name TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            comment TEXT
        )
        "#,
        q_tenants
    );
    sqlx::query(&tenants_ddl).execute(pool).await?;

    Ok(())
}

/// Resolve the storage id for a config record. For api_entities, entity_id is used when id is absent.
fn config_record_id(table: &str, rec: &serde_json::Value) -> Result<String, AppError> {
    let id = rec.get("id").and_then(|v| v.as_str());
    let entity_id = rec.get("entity_id").and_then(|v| v.as_str());
    match (table, id, entity_id) {
        ("_sys_api_entities", None, Some(eid)) => Ok(eid.to_string()),
        (_, Some(id), _) => Ok(id.to_string()),
        _ => Err(AppError::BadRequest(
            "each config record must have an 'id' field (or 'entity_id' for api_entities)".into(),
        )),
    }
}

/// Deep-compare incoming records with current stored payloads (by id).
/// Returns true if they are identical (same ids and same payload per id).
fn config_payloads_unchanged(
    table: &str,
    current: &HashMap<String, serde_json::Value>,
    records: &[serde_json::Value],
) -> Result<bool, AppError> {
    if current.len() != records.len() {
        return Ok(false);
    }
    for rec in records {
        let id = config_record_id(table, rec)?;
        match current.get(&id) {
            None => return Ok(false),
            Some(existing) if existing != rec => return Ok(false),
            Some(_) => {}
        }
    }
    Ok(true)
}

/// Replace all rows for a config type for one package: copy current (for this package_id) to history, delete, insert with new version.
/// If incoming payloads are deep-equal to current, no write is performed and no new version is created.
/// Returns (count inserted, version). Call within transaction for atomicity.
pub async fn replace_config_rows(
    tx: &mut sqlx::PgConnection,
    table: &str,
    package_id: &str,
    records: &[serde_json::Value],
) -> Result<(u64, i64), AppError> {
    let q_table = qualified_sys_table(table);
    let current_version: (Option<i64>,) = sqlx::query_as(&format!(
        "SELECT COALESCE(MAX(version), 0) FROM {} WHERE package_id = $1",
        q_table
    ))
    .bind(package_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(AppError::Db)?;
    let current_version = current_version.0.unwrap_or(0);

    let rows: Vec<(String, serde_json::Value)> = sqlx::query_as(&format!(
        "SELECT id, payload FROM {} WHERE package_id = $1",
        q_table
    ))
    .bind(package_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(AppError::Db)?;
    let current: HashMap<String, serde_json::Value> = rows.into_iter().collect();

    if config_payloads_unchanged(table, &current, records)? {
        return Ok((0, current_version));
    }

    let history_table = qualified_sys_table(&format!("{}_history", table));
    let new_version = current_version + 1;

    sqlx::query(&format!(
        "INSERT INTO {} (id, package_id, payload, version, created_at) SELECT id, package_id, payload, version, updated_at FROM {} WHERE package_id = $1",
        history_table, q_table
    ))
    .bind(package_id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(&format!("DELETE FROM {} WHERE package_id = $1", q_table))
        .bind(package_id)
        .execute(&mut *tx)
        .await?;

    let mut count = 0u64;
    for rec in records {
        let id = config_record_id(table, rec)?;
        sqlx::query(&format!(
            "INSERT INTO {} (id, package_id, payload, updated_at, version) VALUES ($1, $2, $3, NOW(), $4)",
            q_table
        ))
        .bind(&id)
        .bind(package_id)
        .bind(rec)
        .bind(new_version)
        .execute(&mut *tx)
        .await?;
        count += 1;
    }
    Ok((count, new_version))
}

const PACKAGES_TABLE: &str = "_sys_packages";
const PACKAGES_HISTORY_TABLE: &str = "_sys_packages_history";

/// Upsert one package row by id: copy current to history if exists, then insert or replace with new payload and incremented version.
pub async fn upsert_package(
    pool: &PgPool,
    id: &str,
    payload: &serde_json::Value,
) -> Result<i64, AppError> {
    let q_packages = qualified_sys_table(PACKAGES_TABLE);
    let q_packages_history = qualified_sys_table(PACKAGES_HISTORY_TABLE);
    let mut tx = pool.begin().await?;
    let current: Option<(serde_json::Value, i64)> = sqlx::query_as(&format!(
        "SELECT payload, version FROM {} WHERE id = $1",
        q_packages
    ))
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(AppError::Db)?;

    let new_version = match &current {
        Some((_, v)) => v + 1,
        None => 1,
    };

    if let Some((old_payload, old_version)) = current {
        sqlx::query(&format!(
            "INSERT INTO {} (id, payload, version, created_at) VALUES ($1, $2, $3, NOW())",
            q_packages_history
        ))
        .bind(id)
        .bind(old_payload)
        .bind(old_version)
        .execute(&mut *tx)
        .await?;
    }

    sqlx::query(&format!(
        "DELETE FROM {} WHERE id = $1",
        q_packages
    ))
    .bind(id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(&format!(
        "INSERT INTO {} (id, payload, updated_at, version) VALUES ($1, $2, NOW(), $3)",
        q_packages
    ))
    .bind(id)
    .bind(payload)
    .bind(new_version)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(new_version)
}

/// Ensure the database in `database_url` exists; create it if not. Connects to the
/// default `postgres` database to run CREATE DATABASE. Call before creating the main pool.
pub async fn ensure_database_exists(database_url: &str) -> Result<(), AppError> {
    let (admin_url, db_name) = parse_db_name_from_url(database_url)?;
    if db_name.is_empty() || db_name == "postgres" {
        return Ok(());
    }
    let opts = sqlx::postgres::PgConnectOptions::from_str(&admin_url)
        .map_err(|e| AppError::BadRequest(format!("invalid DATABASE_URL: {}", e)))?;
    let mut conn: sqlx::PgConnection = opts.connect().await.map_err(AppError::Db)?;
    let exists: (bool,) = sqlx::query_as("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
        .bind(&db_name)
        .fetch_one(&mut conn)
        .await
        .map_err(AppError::Db)?;
    if !exists.0 {
        let quoted = quote_ident(&db_name);
        sqlx::query(&format!("CREATE DATABASE {}", quoted))
            .execute(&mut conn)
            .await
            .map_err(AppError::Db)?;
    }
    Ok(())
}

fn parse_db_name_from_url(url: &str) -> Result<(String, String), AppError> {
    let path_start = url.rfind('/').ok_or_else(|| AppError::BadRequest("DATABASE_URL: no path".into()))? + 1;
    let path_and_query = url.get(path_start..).unwrap_or("");
    let db_name = path_and_query.split('?').next().unwrap_or("").trim();
    let base = url.get(..path_start).unwrap_or(url);
    let admin_url = format!("{}postgres", base);
    Ok((admin_url, db_name.to_string()))
}

/// Build a new database URL with the same host/credentials but a different database name.
fn database_url_with_name(base_url: &str, new_db_name: &str) -> Result<String, AppError> {
    let path_start = base_url.rfind('/').ok_or_else(|| AppError::BadRequest("DATABASE_URL: no path".into()))? + 1;
    let base = base_url.get(..path_start).unwrap_or(base_url);
    let query = base_url.get(path_start..).and_then(|s| s.find('?').map(|i| &s[i..])).unwrap_or("");
    Ok(format!("{}{}{}", base, new_db_name, query))
}

/// Seed default tenants for the three strategies (idempotent). Inserts default-mode-1 (database),
/// default-mode-2 (schema), default-mode-3 (rls). For database tenant, creates the DB if missing
/// and uses a URL derived from `central_database_url` with database name `{central_db}_tenant_default_mode_1`.
pub async fn seed_default_tenants(pool: &PgPool, central_database_url: &str) -> Result<(), AppError> {
    let q_table = qualified_sys_table("_sys_tenants");

    // default-mode-1: database strategy — derive tenant DB URL and ensure DB exists
    let (_, central_db) = parse_db_name_from_url(central_database_url)?;
    let tenant_db_name = if central_db.is_empty() || central_db == "postgres" {
        "architect_tenant_default_mode_1".to_string()
    } else {
        format!("{}_tenant_default_mode_1", central_db)
    };
    let database_url = database_url_with_name(central_database_url, &tenant_db_name)?;
    ensure_database_exists(&database_url).await?;

    // default-mode-2 (schema): use temp_1 DB
    let database_url_mode2 = database_url_with_name(central_database_url, "temp_1")?;
    ensure_database_exists(&database_url_mode2).await?;

    // default-mode-3 (rls): use temp_2 DB
    let database_url_mode3 = database_url_with_name(central_database_url, "temp_2")?;
    ensure_database_exists(&database_url_mode3).await?;

    let insert_sql = format!(
        r#"
        INSERT INTO {} (id, strategy, database_url, schema_name, updated_at, comment)
        VALUES
            ($1, $2, $3, $4, NOW(), $5),
            ($6, $7, $8, $9, NOW(), $10),
            ($11, $12, $13, $14, NOW(), $15)
        ON CONFLICT (id) DO NOTHING
        "#,
        q_table
    );
    sqlx::query(&insert_sql)
        .bind("default-mode-1")
        .bind("database")
        .bind(&database_url)
        .bind::<Option<String>>(None)
        .bind("Tenant with own database (seed)")
        .bind("default-mode-2")
        .bind("schema")
        .bind(&database_url_mode2)
        .bind("tenant_default_mode_2")
        .bind("Tenant with own schema in shared DB (seed)")
        .bind("default-mode-3")
        .bind("rls")
        .bind(&database_url_mode3)
        .bind::<Option<String>>(None)
        .bind("Tenant with RLS in shared DB (seed)")
        .execute(pool)
        .await?;

    Ok(())
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('\\', "\\\\").replace('"', "\\\""))
}

pub fn sys_table_for_kind(kind: &str) -> Option<&'static str> {
    match kind {
        "schemas" => Some("_sys_schemas"),
        "enums" => Some("_sys_enums"),
        "tables" => Some("_sys_tables"),
        "columns" => Some("_sys_columns"),
        "indexes" => Some("_sys_indexes"),
        "relationships" => Some("_sys_relationships"),
        "api_entities" => Some("_sys_api_entities"),
        _ => None,
    }
}
