//! Apply config to the database: DDL for schemas, enums, tables, indexes, and foreign keys.
//! Order follows PostgreSQL dependencies (see docs/postgres-config-schema.md § 3.5).

use crate::config::types::*;
use crate::config::{validate, FullConfig};
use crate::db::parse_canonical;
use crate::db::pool::Pool;
use crate::db::{ColumnFacts, DbSnapshot, Dialect};
use crate::error::AppError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

/// Name of the column added to app tables when RLS is enabled. Used by migration and CRUD.
pub const RLS_TENANT_COLUMN: &str = "tenant_id";

/// Ensure every table in `config` has the RLS tenant column, row-level security enabled, and the
/// four tenant-isolation policies. Tables flagged `global` instead get asymmetric policies — every
/// tenant may read (`SELECT USING (true)`) but only the Platform Admin tenant may write — so they
/// hold data shared across all RLS tenants. Fully idempotent (`ADD COLUMN IF NOT EXISTS`,
/// `DROP POLICY IF EXISTS` then `CREATE POLICY`), so it is safe to run on both fresh installs and
/// upgrades. On upgrades this is the path that backfills the tenant column + policies for newly
/// created tables, which the diff-based migration plan does not touch.
pub async fn apply_rls_to_tables(
    pool: &Pool,
    config: &FullConfig,
    schema_override: Option<&str>,
    rls_tenant_column: &str,
    dialect: &dyn Dialect,
) -> Result<(), AppError> {
    if !dialect.supports_rls() {
        tracing::warn!(dialect = %dialect.name(), "RLS requested but not supported by this dialect; skipping");
        return Ok(());
    }
    let default_sid = config.schemas.first().map(|s| s.id.as_str()).unwrap_or("");
    let platform_id = crate::tenant::platform_tenant_id();
    let schemas_by_id: HashMap<_, _> = config.schemas.iter().map(|s| (s.id.as_str(), s)).collect();
    let columns_by_table: HashMap<_, Vec<&ColumnConfig>> =
        config.columns.iter().fold(HashMap::new(), |mut m, c| {
            m.entry(c.table_id.as_str()).or_default().push(c);
            m
        });
    let col = rls_tenant_column;

    for t in &config.tables {
        let sid = t.schema_id.as_deref().unwrap_or(default_sid);
        let schema = match schemas_by_id.get(sid) {
            Some(s) => s,
            None => continue,
        };
        let schema_name = quote(schema_override.unwrap_or(&schema.name));
        let full_name = format!("{}.{}", schema_name, quote(&t.name));
        let config_col_names: HashSet<&str> = columns_by_table
            .get(t.id.as_str())
            .map(|v| v.iter().map(|c| c.name.as_str()).collect())
            .unwrap_or_default();

        if !config_col_names.contains(col) {
            let add_col = format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} TEXT",
                full_name,
                quote(col)
            );
            sqlx::query(&add_col).execute(pool).await?;
        }
        let enable_rls = format!("ALTER TABLE {} ENABLE ROW LEVEL SECURITY", full_name);
        sqlx::query(&enable_rls).execute(pool).await?;
        let q_col = quote(col);
        let setting = "current_setting('app.tenant_id', true)";
        // Tenant-isolated condition: a row belongs to the caller's tenant.
        let tenant_cond = format!("{} = {}", q_col, setting);
        // Platform-admin condition: the caller's session is the Platform Admin tenant. Keys off the
        // session setting, not a row column, so it gates writes regardless of the row's tenant_id.
        let admin_cond = format!("{} = '{}'", setting, platform_id.replace('\'', "''"));
        let read_all = "true".to_string();
        let policy_prefix = format!("rls_tenant_{}", t.name);
        // Global tables: everyone reads (USING true), only the Platform Admin writes.
        // Tenant tables: every operation is scoped to the caller's tenant_id.
        let policies: Vec<(&str, &str, Option<&str>, Option<&str>)> = if t.global {
            vec![
                ("select", "SELECT", Some(read_all.as_str()), None),
                ("insert", "INSERT", None, Some(admin_cond.as_str())),
                (
                    "update",
                    "UPDATE",
                    Some(admin_cond.as_str()),
                    Some(admin_cond.as_str()),
                ),
                ("delete", "DELETE", Some(admin_cond.as_str()), None),
            ]
        } else {
            vec![
                ("select", "SELECT", Some(tenant_cond.as_str()), None),
                ("insert", "INSERT", None, Some(tenant_cond.as_str())),
                (
                    "update",
                    "UPDATE",
                    Some(tenant_cond.as_str()),
                    Some(tenant_cond.as_str()),
                ),
                ("delete", "DELETE", Some(tenant_cond.as_str()), None),
            ]
        };
        for (suffix, cmd, using_cond, with_check) in policies.iter() {
            let policy_name = format!("{}_{}", policy_prefix, suffix);
            let drop_sql = format!(
                "DROP POLICY IF EXISTS {} ON {}",
                quote(&policy_name),
                full_name
            );
            let _ = sqlx::query(&drop_sql).execute(pool).await;
            let create_sql = match (using_cond, with_check) {
                (Some(u), Some(w)) => format!(
                    "CREATE POLICY {} ON {} FOR {} USING ( {} ) WITH CHECK ( {} )",
                    quote(&policy_name),
                    full_name,
                    cmd,
                    u,
                    w
                ),
                (Some(u), None) => format!(
                    "CREATE POLICY {} ON {} FOR {} USING ( {} )",
                    quote(&policy_name),
                    full_name,
                    cmd,
                    u
                ),
                (None, Some(w)) => format!(
                    "CREATE POLICY {} ON {} FOR {} WITH CHECK ( {} )",
                    quote(&policy_name),
                    full_name,
                    cmd,
                    w
                ),
                (None, None) => continue,
            };
            sqlx::query(&create_sql).execute(pool).await?;
        }
    }
    Ok(())
}

/// Add any of `desired` columns that the table does not have yet.
///
/// `CREATE TABLE IF NOT EXISTS` is a no-op on a table that already exists, so a table created by
/// an earlier version of a package never picks up columns added since. This closes that gap and
/// makes install/bootstrap idempotent in the same way an upgrade plan is.
///
/// Best-effort by design: a column that cannot be added (most often `NOT NULL` with no default on
/// a table that already has rows) is retried as nullable and then logged, never returned as an
/// error — refusing to install because of pre-existing drift helps nobody.
async fn add_missing_columns(
    pool: &Pool,
    snapshot: &DbSnapshot,
    dialect: &dyn Dialect,
    schema: &str,
    table: &str,
    desired: &[(String, String)],
) {
    if !snapshot.introspected {
        return;
    }
    let full = format!("{}.{}", quote(schema), quote(table));
    for (name, def) in desired {
        if snapshot.has_column(schema, table, name) {
            continue;
        }
        let col_def = format!("{} {}", quote(name), def);
        let sql = add_column_ddl(dialect, &full, &col_def);
        match sqlx::query(&sql).execute(pool).await {
            Ok(_) => {
                tracing::info!(schema, table, column = %name, "added missing column to pre-existing table");
            }
            Err(e) => {
                // NOT NULL without a default cannot be added to a populated table; fall back to a
                // nullable column so reads and writes of that field at least work.
                let upper = def.to_uppercase();
                if upper.contains("NOT NULL") && !upper.contains("DEFAULT") {
                    let relaxed_def = def.replace("NOT NULL", "").trim().to_string();
                    let retry =
                        add_column_ddl(dialect, &full, &format!("{} {}", quote(name), relaxed_def));
                    if sqlx::query(&retry).execute(pool).await.is_ok() {
                        tracing::warn!(schema, table, column = %name, "added missing column as NULLABLE — NOT NULL could not be applied to the existing table");
                        continue;
                    }
                }
                tracing::warn!(schema, table, column = %name, error = %e, "could not add missing column to pre-existing table");
            }
        }
    }
}

/// Source columns as a companion (`_audit` / `_history`) table replicates them: same type,
/// always nullable, no constraints. Mirrors `audit_table_ddl` / `history_table_ddl`.
fn companion_source_columns(
    source_cols: &[&ColumnConfig],
    dialect: &dyn Dialect,
) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = source_cols
        .iter()
        .map(|c| (c.name.clone(), dialect.ddl_type(&parse_canonical(&c.type_))))
        .collect();
    let config_col_names: HashSet<&str> = source_cols.iter().map(|c| c.name.as_str()).collect();
    let audit_ts = dialect.audit_timestamp_type();
    for (name, typ) in [
        ("created_at", audit_ts),
        ("updated_at", audit_ts),
        ("archived_at", audit_ts),
        ("created_by", "TEXT"),
        ("updated_by", "TEXT"),
    ] {
        if !config_col_names.contains(name) {
            out.push((name.to_string(), typ.to_string()));
        }
    }
    out
}

/// Apply full config to the database: CREATE SCHEMA, CREATE TYPE, CREATE TABLE, CREATE INDEX, ADD FK.
/// Validates config first. Idempotent for schemas and types (IF NOT EXISTS); tables are CREATE TABLE only (fails if exists).
/// When `schema_override` is `Some(s)`, app tables/indexes/FKs are created in schema `s` instead of config schema names (e.g. for schema-strategy tenants).
/// When `rls_tenant_column` is `Some(col)`, each table gets that column (if missing), RLS enabled, and policies using `current_setting('app.tenant_id', true)`.
pub async fn apply_migrations(
    pool: &Pool,
    config: &FullConfig,
    schema_override: Option<&str>,
    rls_tenant_column: Option<&str>,
    dialect: &dyn Dialect,
    cross_package_configs: &HashMap<String, FullConfig>,
) -> Result<(), AppError> {
    validate(config)?;
    let default_sid = config
        .schemas
        .first()
        .map(|s| s.id.as_str())
        .ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::Validation(
                "at least one schema required".into(),
            ))
        })?;

    if dialect.supports_schemas() {
        if let Some(s) = schema_override {
            let name = quote(s);
            sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {}", name))
                .execute(pool)
                .await?;
        }
    }

    let schemas_by_id: HashMap<_, _> = config.schemas.iter().map(|s| (s.id.as_str(), s)).collect();
    let tables_by_id: HashMap<_, _> = config.tables.iter().map(|t| (t.id.as_str(), t)).collect();
    let columns_by_table: HashMap<_, Vec<&ColumnConfig>> =
        config.columns.iter().fold(HashMap::new(), |mut m, c| {
            m.entry(c.table_id.as_str()).or_default().push(c);
            m
        });

    // When schema_override is set, we only create the override schema; otherwise create config schemas.
    if schema_override.is_none() && dialect.supports_schemas() {
        for s in &config.schemas {
            let name = quote(&s.name);
            let comment = s
                .comment
                .as_ref()
                .map(|c| format!("COMMENT ON SCHEMA {} IS '{}'", name, c.replace('\'', "''")));
            sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {}", name))
                .execute(pool)
                .await?;
            if let Some(sql) = comment {
                let _ = sqlx::query(&sql).execute(pool).await;
            }
        }
    }

    for e in &config.enums {
        let sid = e.schema_id.as_deref().unwrap_or(default_sid);
        let schema = schemas_by_id.get(sid).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "schema",
                id: sid.to_string(),
            })
        })?;
        let schema_name = quote(schema_override.unwrap_or(&schema.name));
        let type_name = quote(&e.name);
        if dialect.supports_named_enum_types() {
            let values: Vec<String> = e
                .values
                .iter()
                .map(|v| format!("'{}'", v.replace('\'', "''")))
                .collect();
            let sql = format!(
                "CREATE TYPE {}.{} AS ENUM ({})",
                schema_name,
                type_name,
                values.join(", ")
            );
            let _ = sqlx::query(&sql).execute(pool).await;
        }
    }

    // Which tables already exist, and with which columns? `CREATE TABLE IF NOT EXISTS` silently
    // leaves a pre-existing table alone, so without this a table created by an older version of
    // the package would never gain the columns added since. Taken before any DDL runs, so
    // `has_table` means "existed before this call".
    let target_schemas: Vec<String> = match schema_override {
        Some(o) => vec![o.to_string()],
        None => config.schemas.iter().map(|s| s.name.clone()).collect(),
    };
    let pre_snapshot = crate::db::introspect(pool, dialect, &target_schemas).await;

    for t in &config.tables {
        let sid = t.schema_id.as_deref().unwrap_or(default_sid);
        let schema = schemas_by_id.get(sid).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "schema",
                id: sid.to_string(),
            })
        })?;
        let schema_raw = schema_override.unwrap_or(&schema.name);
        let schema_name = quote(schema_raw);
        let table_name = quote(&t.name);
        let full_name = format!("{}.{}", schema_name, table_name);
        let table_pre_existed = pre_snapshot.has_table(schema_raw, &t.name);

        let cols = columns_by_table
            .get(t.id.as_str())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        // (column name, DDL suffix) pairs — the `add_columns` twin is reused below to add
        // columns a pre-existing table is missing, so CREATE TABLE and ALTER TABLE can never
        // disagree about a definition (beyond what ADD COLUMN forces, e.g. SQLite VIRTUAL).
        let mut columns: Vec<(String, String)> = Vec::new();
        let mut add_columns: Vec<(String, String)> = Vec::new();
        for c in cols {
            columns.push((c.name.clone(), column_def_suffix(c, dialect, false)));
            add_columns.push((c.name.clone(), column_def_suffix(c, dialect, true)));
        }

        let config_col_names: HashSet<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        let ts_default = format!(
            "{} NOT NULL DEFAULT {}",
            dialect.sys_timestamp_type(),
            dialect.now_fn()
        );
        let ts_nullable = dialect.sys_timestamp_type().to_string();
        for (name, def_suffix) in [
            ("created_at", ts_default.as_str()),
            ("updated_at", ts_default.as_str()),
            ("archived_at", ts_nullable.as_str()),
            ("created_by", "TEXT"),
            ("updated_by", "TEXT"),
        ] {
            if !config_col_names.contains(name) {
                columns.push((name.to_string(), def_suffix.to_string()));
                add_columns.push((name.to_string(), def_suffix.to_string()));
            }
        }

        let mut col_defs: Vec<String> = columns
            .iter()
            .map(|(name, def)| format!("{} {}", quote(name), def))
            .collect();

        let pk_cols = match &t.primary_key {
            PrimaryKeyConfig::Single(s) => vec![quote(s)],
            PrimaryKeyConfig::Composite(v) => v.iter().map(|s| quote(s)).collect::<Vec<_>>(),
        };
        let pk_def = format!("PRIMARY KEY ({})", pk_cols.join(", "));
        col_defs.push(pk_def);

        for u in &t.unique {
            let cols: Vec<String> = u.iter().map(|s| quote(s)).collect();
            col_defs.push(format!("UNIQUE ({})", cols.join(", ")));
        }
        for ch in &t.check {
            col_defs.push(format!(
                "CONSTRAINT {} CHECK ({})",
                quote(&ch.name),
                ch.expression
            ));
        }

        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (\n  {}\n)",
            full_name,
            col_defs.join(",\n  ")
        );
        sqlx::query(&sql).execute(pool).await?;

        if table_pre_existed {
            add_missing_columns(
                pool,
                &pre_snapshot,
                dialect,
                schema_raw,
                &t.name,
                &add_columns,
            )
            .await;
        }

        if t.audit_log {
            let audit_sql = audit_table_ddl(schema_raw, &t.name, cols, dialect);
            sqlx::query(&audit_sql).execute(pool).await?;
            let pk_col = match &t.primary_key {
                PrimaryKeyConfig::Single(s) => s.clone(),
                PrimaryKeyConfig::Composite(v) => v[0].clone(),
            };
            let audit_full = format!(
                "{}.{}",
                quote(schema_raw),
                quote(&format!("{}_audit", t.name))
            );
            let idx_sql = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {} ({}, {})",
                quote(&format!("{}_audit_record_idx", t.name)),
                audit_full,
                quote(&pk_col),
                quote("audit_at")
            );
            let _ = sqlx::query(&idx_sql).execute(pool).await;

            let audit_table = format!("{}_audit", t.name);
            if pre_snapshot.has_table(schema_raw, &audit_table) {
                add_missing_columns(
                    pool,
                    &pre_snapshot,
                    dialect,
                    schema_raw,
                    &audit_table,
                    &companion_source_columns(cols, dialect),
                )
                .await;
            }
        }

        if t.versioning.as_ref().is_some_and(|v| v.enabled) {
            let pk_col = match &t.primary_key {
                PrimaryKeyConfig::Single(s) => s.clone(),
                PrimaryKeyConfig::Composite(v) => v[0].clone(),
            };
            let history_ddl = history_table_ddl(schema_raw, &t.name, &pk_col, cols, dialect);
            // history_table_ddl embeds a comment line for the index; execute CREATE TABLE only
            let create_only = history_ddl
                .lines()
                .take_while(|l| !l.trim_start().starts_with("-- index:"))
                .collect::<Vec<_>>()
                .join("\n");
            sqlx::query(create_only.trim()).execute(pool).await?;
            let idx_sql = history_index_ddl(schema_raw, &t.name, &pk_col);
            let _ = sqlx::query(&idx_sql).execute(pool).await;

            let history_table = format!("{}_history", t.name);
            if pre_snapshot.has_table(schema_raw, &history_table) {
                add_missing_columns(
                    pool,
                    &pre_snapshot,
                    dialect,
                    schema_raw,
                    &history_table,
                    &companion_source_columns(cols, dialect),
                )
                .await;
            }
        }
    }

    if let Some(col) = rls_tenant_column {
        apply_rls_to_tables(pool, config, schema_override, col, dialect).await?;
    }

    for idx in &config.indexes {
        let sid = idx.schema_id.as_deref().unwrap_or(default_sid);
        let schema = schemas_by_id.get(sid).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "schema",
                id: sid.to_string(),
            })
        })?;
        let table = tables_by_id.get(idx.table_id.as_str()).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "table",
                id: idx.table_id.clone(),
            })
        })?;
        let schema_name = quote(schema_override.unwrap_or(&schema.name));
        let table_name = quote(&table.name);
        let full_table = format!("{}.{}", schema_name, table_name);
        let index_name = quote(&idx.name);

        let mut col_parts: Vec<String> = Vec::new();
        for col in &idx.columns {
            match col {
                IndexColumnEntry::Name(n) => col_parts.push(quote(n)),
                IndexColumnEntry::Spec {
                    name, direction, ..
                } => {
                    let dir = direction
                        .as_deref()
                        .map(|d| format!(" {}", d.to_uppercase()))
                        .unwrap_or_default();
                    col_parts.push(format!("{}{}", quote(name), dir));
                }
                IndexColumnEntry::Expression { expression } => col_parts.push(expression.clone()),
            }
        }
        let method = idx.method.as_deref().unwrap_or("btree");
        let unique = if idx.unique { "UNIQUE " } else { "" };
        let include: String = if idx.include.is_empty() {
            String::new()
        } else {
            let inc: Vec<String> = idx.include.iter().map(|s| quote(s)).collect();
            format!(" INCLUDE ({})", inc.join(", "))
        };
        let where_clause: String = idx
            .where_
            .as_ref()
            .map(|w| format!(" WHERE {}", w))
            .unwrap_or_default();

        let sql = format!(
            "CREATE {}INDEX IF NOT EXISTS {} ON {} USING {} ({}){}{}",
            unique,
            index_name,
            full_table,
            method,
            col_parts.join(", "),
            include,
            where_clause
        );
        let _ = sqlx::query(&sql).execute(pool).await;
    }

    for rel in &config.relationships {
        let from_sid = rel.from_schema_id.as_deref().unwrap_or(default_sid);
        let from_schema = schemas_by_id.get(from_sid).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "schema",
                id: from_sid.to_string(),
            })
        })?;
        let from_table = tables_by_id
            .get(rel.from_table_id.as_str())
            .ok_or_else(|| {
                AppError::Config(crate::error::ConfigError::MissingReference {
                    kind: "table",
                    id: rel.from_table_id.clone(),
                })
            })?;

        // Resolve the target schema and table — either from a cross-package config or this config.
        let (to_schema_name_owned, to_table_name, to_col_name) = if let Some(pkg_id) =
            rel.to_package_id.as_deref()
        {
            let foreign = cross_package_configs.get(pkg_id).ok_or_else(|| {
                AppError::Config(crate::error::ConfigError::MissingReference {
                    kind: "cross_package",
                    id: pkg_id.to_string(),
                })
            })?;
            let foreign_tables: HashMap<_, _> =
                foreign.tables.iter().map(|t| (t.id.as_str(), t)).collect();
            let foreign_schemas: HashMap<_, _> =
                foreign.schemas.iter().map(|s| (s.id.as_str(), s)).collect();
            let to_tbl = foreign_tables
                .get(rel.to_table_id.as_str())
                .ok_or_else(|| {
                    AppError::Config(crate::error::ConfigError::MissingReference {
                        kind: "table",
                        id: rel.to_table_id.clone(),
                    })
                })?;
            let foreign_default_sid = foreign.schemas.first().map(|s| s.id.as_str()).unwrap_or("");
            let to_sid = rel.to_schema_id.as_deref().unwrap_or(foreign_default_sid);
            let to_schema = foreign_schemas.get(to_sid).ok_or_else(|| {
                AppError::Config(crate::error::ConfigError::MissingReference {
                    kind: "schema",
                    id: to_sid.to_string(),
                })
            })?;
            let col_name = foreign
                .columns
                .iter()
                .find(|c| c.id == rel.to_column_id)
                .map(|c| c.name.clone())
                .ok_or_else(|| {
                    AppError::Config(crate::error::ConfigError::MissingReference {
                        kind: "column",
                        id: rel.to_column_id.clone(),
                    })
                })?;
            // Cross-package FKs always use the real schema name (no schema_override — the
            // target package lives in its own schema, not the tenant override).
            (to_schema.name.clone(), to_tbl.name.clone(), col_name)
        } else {
            let to_sid = rel.to_schema_id.as_deref().unwrap_or(default_sid);
            let to_schema = schemas_by_id.get(to_sid).ok_or_else(|| {
                AppError::Config(crate::error::ConfigError::MissingReference {
                    kind: "schema",
                    id: to_sid.to_string(),
                })
            })?;
            let to_table = tables_by_id.get(rel.to_table_id.as_str()).ok_or_else(|| {
                AppError::Config(crate::error::ConfigError::MissingReference {
                    kind: "table",
                    id: rel.to_table_id.clone(),
                })
            })?;
            let col_name = config
                .columns
                .iter()
                .find(|c| c.id == rel.to_column_id)
                .map(|c| c.name.clone())
                .ok_or_else(|| {
                    AppError::Config(crate::error::ConfigError::MissingReference {
                        kind: "column",
                        id: rel.to_column_id.clone(),
                    })
                })?;
            (
                schema_override.unwrap_or(&to_schema.name).to_string(),
                to_table.name.clone(),
                col_name,
            )
        };

        let from_schema_name = schema_override.unwrap_or(&from_schema.name);
        let from_col = config
            .columns
            .iter()
            .find(|c| c.id == rel.from_column_id)
            .map(|c| c.name.as_str())
            .ok_or_else(|| {
                AppError::Config(crate::error::ConfigError::MissingReference {
                    kind: "column",
                    id: rel.from_column_id.clone(),
                })
            })?;

        let from_full = format!("{}.{}", quote(from_schema_name), quote(&from_table.name));
        let to_full = format!("{}.{}", quote(&to_schema_name_owned), quote(&to_table_name));
        let constraint_name = rel.name.as_deref().unwrap_or(&rel.id);
        let on_update = rel.on_update.as_deref().unwrap_or("NO ACTION");
        let on_delete = rel.on_delete.as_deref().unwrap_or("NO ACTION");

        let sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}) ON UPDATE {} ON DELETE {}",
            from_full,
            quote(constraint_name),
            quote(from_col),
            to_full,
            quote(&to_col_name),
            on_update,
            on_delete
        );
        let _ = sqlx::query(&sql).execute(pool).await;
    }

    // Views last: they read the tables (and FK-joined data) created above.
    create_views(
        pool,
        config,
        schema_override,
        dialect,
        rls_tenant_column.is_some(),
    )
    .await?;

    Ok(())
}

// ─── Views ───────────────────────────────────────────────────────────────────

/// In SDK-generated DDL (the materialized view's unique index): the tenant column followed by a
/// comma in shared (RLS) databases, nothing elsewhere.
const TENANT_KEY_TOKEN: &str = "{{tenant_key}}";

/// Expand the tenant placeholders in view SQL for one target database.
///
/// `{{tenant_id:o}}` (or `{{tenant_id}}` without a table alias) is the row's tenant id: the RLS
/// tenant column (`o."tenant_id"`) in a shared (RLS) database, and a typed NULL in a tenant's own
/// database, where tables have no tenant column. Use it in SELECT lists and GROUP BY, not in
/// joins (NULL never equals NULL). Materialized views must return it as `tenant_id`.
pub fn render_tenant_tokens(sql: &str, rls: bool, dialect: &dyn Dialect) -> String {
    use std::sync::OnceLock;
    static TOKEN: OnceLock<regex::Regex> = OnceLock::new();
    let token = TOKEN.get_or_init(|| {
        regex::Regex::new(r"\{\{\s*tenant_id\s*(?::\s*([A-Za-z_][A-Za-z0-9_]*)\s*)?\}\}")
            .expect("valid tenant token regex")
    });
    let col = quote(RLS_TENANT_COLUMN);
    let rendered = token.replace_all(sql, |caps: &regex::Captures<'_>| {
        if !rls {
            return dialect.null_text_expr().to_string();
        }
        match caps.get(1) {
            Some(alias) => format!("{}.{}", alias.as_str(), col),
            None => col.clone(),
        }
    });
    let key = if rls {
        format!("{}, ", col)
    } else {
        String::new()
    };
    rendered.replace(TENANT_KEY_TOKEN, &key)
}

/// Name of a materialized view's backing relation, which holds every tenant's rows. Callers
/// read the wrapper view under the view's own name instead.
pub fn materialized_backing_name(view_name: &str) -> String {
    format!("{}__mv", view_name)
}

/// Recompute a materialized view's stored rows in the database `pool` points at. Several
/// statements (dialects without native materialized views) run in one transaction, so readers
/// see the old rows until the new ones commit. `rls` expands tenant tokens for a shared database.
pub async fn refresh_materialized_view(
    pool: &Pool,
    view: &crate::config::ResolvedView,
    rls: bool,
    dialect: &dyn Dialect,
) -> Result<(), AppError> {
    if !view.materialized {
        return Err(AppError::BadRequest(format!(
            "view '{}' is not materialized; it always reads live data",
            view.id
        )));
    }
    let backing = format!(
        "{}.{}",
        quote(&view.schema_name),
        quote(&materialized_backing_name(&view.name))
    );
    let body = render_tenant_tokens(&view.sql, rls, dialect);
    let statements = dialect.refresh_materialized_view_sql(&backing, &body);
    if let [only] = statements.as_slice() {
        sqlx::query(only).execute(pool).await?;
        return Ok(());
    }
    let mut tx = pool.begin().await?;
    for sql in &statements {
        sqlx::query(sql).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// A package view prepared for DDL with the active dialect.
struct ViewDdl<'a> {
    view: &'a ViewConfig,
    schema: String,
    /// Quoted `schema.name`: the view callers read (a materialized view's wrapper).
    qualified: String,
    /// The view body for the active dialect, tenant tokens not yet expanded.
    body: &'a str,
}

impl ViewDdl<'_> {
    /// Lowercased `schema.name`, the form `reads` entries are matched against.
    fn key(&self) -> String {
        format!("{}.{}", self.schema, self.view.name).to_lowercase()
    }

    /// Lowercased `schema.name` of every relation this view creates: the view itself and, for a
    /// materialized view, its backing relation.
    fn relation_keys(&self) -> Vec<String> {
        let mut keys = vec![self.key()];
        if self.view.materialized {
            keys.push(format!("{}.{}", self.schema, self.backing_name()).to_lowercase());
        }
        keys
    }

    fn backing_name(&self) -> String {
        materialized_backing_name(&self.view.name)
    }

    fn backing_qualified(&self) -> String {
        format!("{}.{}", quote(&self.schema), quote(&self.backing_name()))
    }

    /// Statements that create the view, as `(object_type, sql)`, tenant tokens unexpanded.
    ///
    /// A materialized view is a backing relation (native materialized view, or a table where the
    /// dialect has none), a unique index where the dialect refreshes concurrently, and a plain
    /// wrapper view under the view's own name returning the declared columns of the caller's
    /// tenant only.
    fn create_statements(&self, dialect: &dyn Dialect) -> Vec<(&'static str, String)> {
        if !self.view.materialized {
            return vec![("view", dialect.create_view_sql(&self.qualified, self.body))];
        }
        let backing = self.backing_qualified();
        let mut out = vec![(
            "materialized_view",
            dialect.create_materialized_view_sql(&backing, self.body),
        )];
        let key_columns = self
            .view
            .primary_key_columns()
            .iter()
            .map(|c| quote(c))
            .collect::<Vec<_>>()
            .join(", ");
        if let Some(sql) = dialect.materialized_view_key_sql(
            &quote(&format!("{}_key", self.backing_name())),
            &backing,
            &format!("{}{}", TENANT_KEY_TOKEN, key_columns),
        ) {
            out.push(("index", sql));
        }
        let columns = self
            .view
            .columns
            .iter()
            .map(|c| quote(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        let filter = dialect
            .tenant_session_filter_sql(RLS_TENANT_COLUMN)
            .map(|f| format!(" WHERE {}", f))
            .unwrap_or_default();
        let wrapper = format!("SELECT {} FROM {}{}", columns, backing, filter);
        out.push(("view", dialect.create_view_sql(&self.qualified, &wrapper)));
        out
    }

    /// Statements that drop the view, as `(object_type, sql)`.
    fn drop_statements(&self, dialect: &dyn Dialect) -> Vec<(&'static str, String)> {
        let mut out = vec![("view", dialect.drop_view_sql(&self.qualified))];
        if self.view.materialized {
            out.push((
                "materialized_view",
                dialect.drop_materialized_view_sql(&self.backing_qualified()),
            ));
        }
        out
    }
}

/// The package's views in creation order: a view that `reads` another view of the package comes
/// after it; otherwise config order is kept. A cycle is a config error.
fn ordered_views<'a>(
    config: &'a FullConfig,
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
) -> Result<Vec<ViewDdl<'a>>, AppError> {
    let default_sid = config.schemas.first().map(|s| s.id.as_str()).unwrap_or("");
    let schemas_by_id: HashMap<_, _> = config.schemas.iter().map(|s| (s.id.as_str(), s)).collect();
    let mut views = Vec::with_capacity(config.views.len());
    for v in &config.views {
        let sid = v.schema_id.as_deref().unwrap_or(default_sid);
        let schema = schemas_by_id.get(sid).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "schema",
                id: sid.to_string(),
            })
        })?;
        let schema = schema_override.unwrap_or(&schema.name).to_string();
        views.push(ViewDdl {
            qualified: format!("{}.{}", quote(&schema), quote(&v.name)),
            schema,
            body: v.sql_for(dialect.name()),
            view: v,
        });
    }

    let keys: Vec<String> = views.iter().map(ViewDdl::key).collect();
    // deps[i] = indexes of package views that view i reads.
    let deps: Vec<Vec<usize>> = views
        .iter()
        .map(|v| {
            v.view
                .reads
                .iter()
                .filter_map(|r| keys.iter().position(|k| *k == r.to_lowercase()))
                .collect()
        })
        .collect();

    let mut order: Vec<usize> = Vec::with_capacity(views.len());
    while order.len() < views.len() {
        let next = (0..views.len())
            .find(|i| !order.contains(i) && deps[*i].iter().all(|d| order.contains(d)));
        match next {
            Some(i) => order.push(i),
            None => {
                let stuck: Vec<&str> = (0..views.len())
                    .filter(|i| !order.contains(i))
                    .map(|i| views[i].view.id.as_str())
                    .collect();
                return Err(AppError::Config(crate::error::ConfigError::Validation(
                    format!("views read each other in a cycle: [{}]", stuck.join(", ")),
                )));
            }
        }
    }
    let mut slots: Vec<Option<ViewDdl<'a>>> = views.into_iter().map(Some).collect();
    Ok(order.into_iter().filter_map(|i| slots[i].take()).collect())
}

/// Check that a view body (tenant tokens expanded) compiles against the database and returns
/// exactly the declared columns — plus `tenant_id` for a materialized view — before the view is
/// created.
async fn check_view_columns(pool: &Pool, v: &ViewDdl<'_>, body: &str) -> Result<(), AppError> {
    use sqlx::{Column, Executor};
    let probe = format!("SELECT * FROM ({}) AS architect_view_check", body);
    let described = pool.describe(&probe).await.map_err(|e| {
        AppError::BadRequest(format!("view '{}': sql does not compile: {}", v.view.id, e))
    })?;
    let actual: Vec<String> = described
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();
    let mut declared: Vec<&str> = v.view.columns.iter().map(|c| c.name.as_str()).collect();
    if v.view.materialized {
        declared.push(RLS_TENANT_COLUMN);
    }
    let missing: Vec<&str> = declared
        .iter()
        .filter(|d| !actual.iter().any(|a| a.eq_ignore_ascii_case(d)))
        .copied()
        .collect();
    let extra: Vec<&str> = actual
        .iter()
        .filter(|a| !declared.iter().any(|d| a.eq_ignore_ascii_case(d)))
        .map(String::as_str)
        .collect();
    if missing.is_empty() && extra.is_empty() {
        return Ok(());
    }
    let hint = if v.view.materialized && missing.contains(&RLS_TENANT_COLUMN) {
        format!(
            "; a materialized view must also return {{{{tenant_id:<alias>}}}} AS {}",
            RLS_TENANT_COLUMN
        )
    } else {
        String::new()
    };
    Err(AppError::BadRequest(format!(
        "view '{}': declared columns do not match its sql (missing from sql: [{}]; not declared: [{}]){}",
        v.view.id,
        missing.join(", "),
        extra.join(", "),
        hint
    )))
}

/// Create every view of the package, in dependency order, after checking each one's columns.
/// `rls` selects how tenant tokens expand (see [`render_tenant_tokens`]).
async fn create_views(
    pool: &Pool,
    config: &FullConfig,
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
    rls: bool,
) -> Result<(), AppError> {
    for v in ordered_views(config, schema_override, dialect)? {
        check_view_columns(pool, &v, &render_tenant_tokens(v.body, rls, dialect)).await?;
        if !dialect.create_view_replaces() {
            exec_revert_ddl(pool, &dialect.drop_view_sql(&v.qualified)).await;
        }
        for (_, sql) in v.create_statements(dialect) {
            sqlx::query(&render_tenant_tokens(&sql, rls, dialect))
                .execute(pool)
                .await
                .map_err(|e| view_ddl_error(&v, "create", &e))?;
        }
    }
    Ok(())
}

/// Drop every view of the package in reverse dependency order. Best-effort, like the rest of
/// the revert: failures are logged.
async fn drop_views(
    pool: &Pool,
    config: &FullConfig,
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
) -> Result<(), AppError> {
    for v in ordered_views(config, schema_override, dialect)?
        .iter()
        .rev()
    {
        for (_, sql) in v.drop_statements(dialect) {
            exec_revert_ddl(pool, &sql).await;
        }
    }
    Ok(())
}

fn view_ddl_error(v: &ViewDdl<'_>, action: &str, e: &sqlx::Error) -> AppError {
    let msg = e.to_string();
    let hint = if msg.contains("security_invoker") {
        " (views need PostgreSQL 15 or later)"
    } else {
        ""
    };
    AppError::BadRequest(format!(
        "could not {} view '{}': {}{}",
        action, v.view.id, msg, hint
    ))
}

/// Upgrade steps for views, as `(drops, creates)`: drops run before every table step and
/// creates after them, so table changes never trip over a view reading the table.
///
/// A view is rebuilt (dropped and recreated) when it is new, its body or columns changed, it
/// reads a relation the table steps touch (or declares no `reads` while any table step exists),
/// or it reads another rebuilt view. Removed views are dropped. Unaffected views are left alone,
/// so views in other packages that read them keep working.
fn view_plan_steps(
    old: &FullConfig,
    new: &FullConfig,
    table_steps: &[MigrationStep],
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
) -> Result<(Vec<MigrationStep>, Vec<MigrationStep>), AppError> {
    let old_views = ordered_views(old, schema_override, dialect)?;
    let new_views = ordered_views(new, schema_override, dialect)?;
    let touched: HashSet<String> = table_steps
        .iter()
        .filter_map(|s| {
            s.table
                .as_ref()
                .map(|t| format!("{}.{}", s.schema, t).to_lowercase())
        })
        .collect();
    let has_table_steps = !table_steps.is_empty();

    let changed = |nv: &ViewDdl<'_>, ov: &ViewDdl<'_>| {
        nv.body != ov.body
            || nv.qualified != ov.qualified
            || nv.view.materialized != ov.view.materialized
            || nv.view.primary_key_columns() != ov.view.primary_key_columns()
            || nv.view.columns.len() != ov.view.columns.len()
            || nv
                .view
                .columns
                .iter()
                .zip(&ov.view.columns)
                .any(|(a, b)| a.name != b.name || a.type_ != b.type_)
    };

    // New views to (re)create, in creation order; their keys feed the "reads a rebuilt view" rule.
    let mut rebuilt_keys: HashSet<String> = HashSet::new();
    let mut creates: Vec<&ViewDdl<'_>> = Vec::new();
    for nv in &new_views {
        let old_match = old_views.iter().find(|ov| ov.view.id == nv.view.id);
        let reads: Vec<String> = nv.view.reads.iter().map(|r| r.to_lowercase()).collect();
        let rebuild = match old_match {
            None => true,
            Some(ov) => {
                changed(nv, ov)
                    || (has_table_steps && reads.is_empty())
                    || reads
                        .iter()
                        .any(|r| touched.contains(r) || rebuilt_keys.contains(r))
            }
        };
        if rebuild {
            rebuilt_keys.insert(nv.key());
            creates.push(nv);
        }
    }

    // Old views to drop: removed ones and existing ones being rebuilt, in reverse creation order.
    let drops: Vec<&ViewDdl<'_>> = old_views
        .iter()
        .rev()
        .filter(
            |ov| match new_views.iter().find(|nv| nv.view.id == ov.view.id) {
                None => true,
                Some(nv) => creates.iter().any(|c| c.view.id == nv.view.id),
            },
        )
        .collect();

    // One step per statement. Safe: a failed view step aborts the upgrade. A drop fails when
    // objects outside the package still read the view, which must stop the upgrade before tables
    // change. Index steps use CreateView too: the pre-flight snapshot predates the drop of the
    // relation the index belongs to, so CreateIndex reconciliation would wrongly skip it.
    let steps = |op: MigrationOperation, v: &ViewDdl<'_>, statements: Vec<(&str, String)>| {
        let verb = match op {
            MigrationOperation::DropView => "Drop",
            _ => "Create",
        };
        statements
            .into_iter()
            .map(|(object_type, ddl)| MigrationStep {
                step: 0,
                operation: op.clone(),
                schema: v.schema.clone(),
                table: None,
                object: v.view.name.clone(),
                from_object: None,
                object_type: object_type.into(),
                description: format!(
                    "{} {} of view \"{}\".\"{}\"",
                    verb,
                    object_type.replace('_', " "),
                    v.schema,
                    v.view.name
                ),
                ddl: Some(ddl),
                safety: MigrationSafety::Safe,
                risk: MigrationRisk::None,
                risk_detail: None,
            })
            .collect::<Vec<_>>()
    };
    let drop_steps = drops
        .into_iter()
        .flat_map(|v| steps(MigrationOperation::DropView, v, v.drop_statements(dialect)))
        .collect();
    let create_steps = creates
        .into_iter()
        .flat_map(|v| {
            steps(
                MigrationOperation::CreateView,
                v,
                v.create_statements(dialect),
            )
        })
        .collect();
    Ok((drop_steps, create_steps))
}

/// Physical tables `revert_migrations` drops for a package, as `(schema, table)` pairs: each
/// table's `_audit` / `_history` companions (when enabled) followed by the table itself.
fn package_physical_tables(
    config: &FullConfig,
    schema_override: Option<&str>,
) -> Result<Vec<(String, String)>, AppError> {
    let default_sid = config
        .schemas
        .first()
        .map(|s| s.id.as_str())
        .ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::Validation(
                "at least one schema required".into(),
            ))
        })?;

    let schemas_by_id: HashMap<_, _> = config.schemas.iter().map(|s| (s.id.as_str(), s)).collect();

    let mut out = Vec::new();
    for t in &config.tables {
        let sid = t.schema_id.as_deref().unwrap_or(default_sid);
        let schema = schemas_by_id.get(sid).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "schema",
                id: sid.to_string(),
            })
        })?;
        let schema_raw = schema_override.unwrap_or(&schema.name).to_string();
        if t.audit_log {
            out.push((schema_raw.clone(), format!("{}_audit", t.name)));
        }
        if t.versioning.as_ref().is_some_and(|v| v.enabled) {
            out.push((schema_raw.clone(), format!("{}_history", t.name)));
        }
        out.push((schema_raw, t.name.clone()));
    }
    Ok(out)
}

/// Schemas `revert_migrations` drops whole: the package's own schemas, except `public` and
/// except when a schema override is in effect (the override schema is shared).
fn package_dropped_schemas(config: &FullConfig, schema_override: Option<&str>) -> Vec<String> {
    if schema_override.is_some() {
        return Vec::new();
    }
    config
        .schemas
        .iter()
        .filter(|s| !s.name.eq_ignore_ascii_case("public"))
        .map(|s| s.name.clone())
        .collect()
}

/// A view outside a package that reads one of the package's tables.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct DependentView {
    pub view_schema: String,
    pub view_name: String,
    pub table_schema: String,
    pub table_name: String,
    /// The column read, or `None` for a dependency on the table as a whole.
    pub column: Option<String>,
}

impl std::fmt::Display for DependentView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{} (reads {}.{}",
            self.view_schema, self.view_name, self.table_schema, self.table_name
        )?;
        if let Some(c) = &self.column {
            write!(f, ".{}", c)?;
        }
        write!(f, ")")
    }
}

/// Every view reading `tables` (directly or through other views), one entry per column read.
/// Empty when the dialect cannot report view dependencies.
async fn find_dependent_views(
    pool: &Pool,
    dialect: &dyn Dialect,
    tables: &[(String, String)],
) -> Result<Vec<DependentView>, AppError> {
    if tables.is_empty() {
        return Ok(Vec::new());
    }
    let Some(sql) = dialect.dependent_views_sql(tables) else {
        return Ok(Vec::new());
    };
    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            use sqlx::Row;
            Some(DependentView {
                view_schema: row.try_get::<String, _>(0).ok()?,
                view_name: row.try_get::<String, _>(1).ok()?,
                table_schema: row.try_get::<String, _>(2).ok()?,
                table_name: row.try_get::<String, _>(3).ok()?,
                column: row.try_get::<String, _>(4).ok().filter(|c| !c.is_empty()),
            })
        })
        .collect())
}

/// Lowercased `schema.name` of every view the package owns.
fn own_view_keys(
    config: &FullConfig,
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
) -> Result<Vec<String>, AppError> {
    Ok(ordered_views(config, schema_override, dialect)?
        .iter()
        .flat_map(ViewDdl::relation_keys)
        .collect())
}

/// Views outside the package that would block an upgrade `plan`: views reading a column the plan
/// retypes or drops. Postgres refuses such an `ALTER`, which would otherwise leave the stored
/// config ahead of the database. The package's own views are rebuilt by the plan, so they do not
/// block. Run against every target database before anything executes.
pub async fn find_views_blocking_upgrade(
    pool: &Pool,
    dialect: &dyn Dialect,
    config: &FullConfig,
    plan: &MigrationPlan,
) -> Result<Vec<DependentView>, AppError> {
    let changed: Vec<(String, String, String)> = plan
        .steps
        .iter()
        .filter(|s| {
            matches!(
                s.operation,
                MigrationOperation::AlterColumnType | MigrationOperation::DropColumn
            )
        })
        .filter_map(|s| {
            s.table
                .as_ref()
                .map(|t| (s.schema.clone(), t.clone(), s.object.clone()))
        })
        .collect();
    let mut tables: Vec<(String, String)> = changed
        .iter()
        .map(|(s, t, _)| (s.clone(), t.clone()))
        .collect();
    tables.sort();
    tables.dedup();
    let found = find_dependent_views(pool, dialect, &tables).await?;
    Ok(views_reading_changed_columns(
        found,
        &changed,
        &own_view_keys(config, None, dialect)?,
    ))
}

/// Of `found`, the views outside `own_views` that read one of the `changed` `(schema, table,
/// column)` columns, sorted and de-duplicated. Table-level dependencies do not block a column
/// change.
fn views_reading_changed_columns(
    found: Vec<DependentView>,
    changed: &[(String, String, String)],
    own_views: &[String],
) -> Vec<DependentView> {
    let mut out: Vec<DependentView> = found
        .into_iter()
        .filter(|v| {
            let key = format!("{}.{}", v.view_schema, v.view_name).to_lowercase();
            !own_views.contains(&key)
        })
        .filter(|v| {
            v.column.as_deref().is_some_and(|col| {
                changed.iter().any(|(s, t, c)| {
                    s.eq_ignore_ascii_case(&v.table_schema)
                        && t.eq_ignore_ascii_case(&v.table_name)
                        && c.eq_ignore_ascii_case(col)
                })
            })
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Views that `revert_migrations` would break: views reading one of the package's tables that
/// live outside the schemas the package drops. On Postgres `DROP TABLE … CASCADE` would drop them
/// silently; on MySQL they would be left invalid. Views inside the package's own schemas belong
/// to the package and go with it, so they are not reported.
///
/// Returns an empty list when the dialect cannot report view dependencies (SQLite). A failing
/// catalog query is an error rather than an empty list, so the guard never passes by accident.
pub async fn find_external_dependent_views(
    pool: &Pool,
    dialect: &dyn Dialect,
    config: &FullConfig,
    schema_override: Option<&str>,
) -> Result<Vec<DependentView>, AppError> {
    let tables = package_physical_tables(config, schema_override)?;
    // Uninstall drops whole tables, so report each view once per table, not per column.
    let found = find_dependent_views(pool, dialect, &tables)
        .await?
        .into_iter()
        .map(|v| DependentView { column: None, ..v })
        .collect();
    Ok(external_dependent_views(
        found,
        &package_dropped_schemas(config, schema_override),
        &own_view_keys(config, schema_override, dialect)?,
    ))
}

/// Keep only views outside `dropped_schemas` that are not the package's own views (lowercased
/// `schema.name` in `own_views`), sorted and de-duplicated.
fn external_dependent_views(
    found: Vec<DependentView>,
    dropped_schemas: &[String],
    own_views: &[String],
) -> Vec<DependentView> {
    let mut out: Vec<DependentView> = found
        .into_iter()
        .filter(|v| {
            !dropped_schemas
                .iter()
                .any(|s| s.eq_ignore_ascii_case(&v.view_schema))
        })
        .filter(|v| {
            let key = format!("{}.{}", v.view_schema, v.view_name).to_lowercase();
            !own_views.contains(&key)
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Run one revert DDL statement. Revert stays best-effort (one failed drop must not leave the
/// rest of the package behind), but failures are logged instead of discarded.
async fn exec_revert_ddl(pool: &Pool, sql: &str) {
    if let Err(e) = sqlx::query(sql).execute(pool).await {
        tracing::warn!(sql, error = %e, "package revert statement failed");
    }
}

/// Revert migrations for a package: drop tables, enum types, and schema (if not public) in reverse order of apply.
/// Uses the same schema_override as apply_migrations (tables/enums live in that schema).
/// Callers should first check [`find_external_dependent_views`]: the drops use `CASCADE`.
pub async fn revert_migrations(
    pool: &Pool,
    config: &FullConfig,
    schema_override: Option<&str>,
) -> Result<(), AppError> {
    let dialect = crate::db::active_dialect();

    // 0. Drop the package's views before the tables they read
    drop_views(pool, config, schema_override, dialect.as_ref()).await?;

    // 1. Drop tables (CASCADE drops FKs and dependent objects)
    for (schema, table) in package_physical_tables(config, schema_override)? {
        let drop_sql = format!(
            "DROP TABLE IF EXISTS {}.{} CASCADE",
            quote(&schema),
            quote(&table)
        );
        exec_revert_ddl(pool, &drop_sql).await;
    }

    // 2. Drop enum types
    let enums: &[EnumConfig] = if dialect.supports_named_enum_types() {
        &config.enums
    } else {
        &[]
    };
    let default_sid = config.schemas.first().map(|s| s.id.as_str()).unwrap_or("");
    let schemas_by_id: HashMap<_, _> = config.schemas.iter().map(|s| (s.id.as_str(), s)).collect();
    for e in enums {
        let sid = e.schema_id.as_deref().unwrap_or(default_sid);
        let schema = schemas_by_id.get(sid).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "schema",
                id: sid.to_string(),
            })
        })?;
        let schema_name = quote(schema_override.unwrap_or(&schema.name));
        let type_name = quote(&e.name);
        let drop_sql = format!("DROP TYPE IF EXISTS {}.{} CASCADE", schema_name, type_name);
        exec_revert_ddl(pool, &drop_sql).await;
    }

    // 3. Drop schema only if not public (shared schema)
    if !dialect.supports_schemas() {
        return Ok(());
    }
    for s in package_dropped_schemas(config, schema_override) {
        let drop_sql = format!("DROP SCHEMA IF EXISTS {} CASCADE", quote(&s));
        exec_revert_ddl(pool, &drop_sql).await;
    }

    Ok(())
}

/// `ALTER TABLE … ADD COLUMN` with an `IF NOT EXISTS` guard where the dialect supports it.
///
/// The guard makes the step a no-op when the column is already present — the common case when a
/// plan is replayed after a partial failure, or broadcast to a tenant database that already has
/// the column. Dialects without the syntax (MySQL, SQLite) rely on the executor's pre-flight
/// introspection instead (see `db::introspect`).
fn add_column_ddl(dialect: &dyn Dialect, full_table: &str, col_def: &str) -> String {
    if dialect.supports_add_column_if_not_exists() {
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {}",
            full_table, col_def
        )
    } else {
        format!("ALTER TABLE {} ADD COLUMN {}", full_table, col_def)
    }
}

/// Column definition after the name: `TYPE [GENERATED ALWAYS AS (…) STORED] [NOT NULL]
/// [DEFAULT …]`. The generated clause sits directly after the type because MySQL requires it
/// there. `adding_column` is true inside `ALTER TABLE … ADD COLUMN`.
fn column_def_suffix(c: &ColumnConfig, dialect: &dyn Dialect, adding_column: bool) -> String {
    let mut def = dialect.ddl_type(&parse_canonical(&c.type_));
    if let Some(g) = &c.generated {
        def.push(' ');
        def.push_str(
            &dialect.generated_column_clause(g.expression_for(dialect.name()), adding_column),
        );
    }
    if !c.nullable {
        def.push_str(" NOT NULL");
    }
    if let Some(d) = &c.default {
        def.push_str(" DEFAULT ");
        def.push_str(&default_str(d));
    }
    def
}

// ─── Migration plan types ────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationOperation {
    CreateSchema,
    CreateEnum,
    DropEnum,
    AddEnumValue,
    RemoveEnumValue,
    CreateTable,
    DropTable,
    AddColumn,
    DropColumn,
    RenameColumn,
    AlterColumnType,
    /// Change the expression of a generated column in place.
    SetGeneratedExpression,
    /// Turn a generated column into a regular one, keeping its current values.
    DropGeneratedExpression,
    BackfillNulls,
    SetNotNull,
    DropNotNull,
    SetDefault,
    DropDefault,
    CreateIndex,
    DropIndex,
    AddForeignKey,
    DropForeignKey,
    CreateView,
    DropView,
}

impl std::fmt::Display for MigrationOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = serde_json::to_value(self)
            .ok()
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(|| format!("{:?}", self));
        write!(f, "{}", s)
    }
}

/// How safely a migration step can be executed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationSafety {
    /// Guaranteed to succeed, no data impact.
    Safe,
    /// Attempted; execution failure is captured as a warning instead of aborting.
    BestEffort,
    /// No DDL generated — config change noted as a warning only (e.g. removed tables/columns).
    WarnOnly,
    /// Irreversible data loss. DDL *is* generated but is only executed when the caller explicitly
    /// confirms (`confirm_destructive`). Without confirmation the step behaves like `WarnOnly`:
    /// a warning is recorded and the object is retained. Used for dropping columns.
    Destructive,
}

/// Risk category associated with a migration step.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationRisk {
    None,
    /// Cast may fail for incompatible values (e.g. TEXT → INTEGER).
    MayFail,
    /// SET NOT NULL will fail if any existing row has NULL in this column.
    ExistingNullsMustBeAbsent,
    /// Existing NULL rows will be overwritten with the column default.
    DataWillBeModified,
    /// Cannot be automated — requires a manual database action.
    ManualActionRequired,
    /// The object and all data it holds will be permanently dropped. Irreversible.
    /// Only executed when the apply request sets `confirm_destructive`.
    DataWillBeDropped,
}

/// One step in a migration plan: a DDL statement with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationStep {
    pub step: usize,
    pub operation: MigrationOperation,
    pub schema: String,
    pub table: Option<String>,
    /// Column name, index name, FK constraint name, enum name, etc.
    pub object: String,
    /// Previous name of `object` for rename operations. `None` for every other operation.
    /// Optional in the serialized form so migration plans saved before this field existed
    /// still deserialize.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_object: Option<String>,
    /// "column" | "table" | "index" | "foreign_key" | "enum" | "enum_value" | "schema"
    pub object_type: String,
    pub description: String,
    /// The SQL to execute. None for WarnOnly steps. A `Destructive` step carries its DDL here but
    /// runs only when the apply request confirms it.
    pub ddl: Option<String>,
    pub safety: MigrationSafety,
    pub risk: MigrationRisk,
    pub risk_detail: Option<String>,
}

/// Computed diff between two package versions expressed as ordered migration steps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationPlan {
    pub steps: Vec<MigrationStep>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MigrationSummary {
    pub total: usize,
    pub safe: usize,
    pub best_effort: usize,
    pub warn_only: usize,
    /// Steps that will permanently drop data and only run when `confirm_destructive` is set.
    pub destructive: usize,
}

impl MigrationPlan {
    /// The plan for one target database, with tenant tokens in view DDL expanded for a shared
    /// (RLS) database when `rls` is true. Plans are computed once and broadcast, so expansion
    /// happens per target. [`execute_migration_plan`] expands any token still left as for a
    /// tenant's own database, which in a shared database makes materialized views show no rows
    /// rather than other tenants' rows.
    pub fn for_target(&self, rls: bool, dialect: &dyn Dialect) -> MigrationPlan {
        let mut plan = self.clone();
        for s in &mut plan.steps {
            if let Some(ddl) = &s.ddl {
                s.ddl = Some(render_tenant_tokens(ddl, rls, dialect));
            }
        }
        plan
    }

    /// Split off the trailing view-creation steps, as `(before, view_creates)`. RLS targets run
    /// the tenant-column reconciliation between the two, so views can read the tenant column of
    /// tables the upgrade creates.
    pub fn split_view_creates(&self) -> (MigrationPlan, MigrationPlan) {
        let at = self
            .steps
            .iter()
            .rposition(|s| !matches!(s.operation, MigrationOperation::CreateView))
            .map_or(0, |i| i + 1);
        (
            MigrationPlan {
                steps: self.steps[..at].to_vec(),
            },
            MigrationPlan {
                steps: self.steps[at..].to_vec(),
            },
        )
    }

    pub fn summary(&self) -> MigrationSummary {
        let (mut safe, mut best_effort, mut warn_only, mut destructive) = (0, 0, 0, 0);
        for s in &self.steps {
            match s.safety {
                MigrationSafety::Safe => safe += 1,
                MigrationSafety::BestEffort => best_effort += 1,
                MigrationSafety::WarnOnly => warn_only += 1,
                MigrationSafety::Destructive => destructive += 1,
            }
        }
        MigrationSummary {
            total: self.steps.len(),
            safe,
            best_effort,
            warn_only,
            destructive,
        }
    }
}

/// Result returned by `execute_migration_plan`.
pub struct MigrationExecutionResult {
    pub applied: usize,
    pub warned: usize,
    /// Steps whose effect was already present in the database and were not re-run.
    pub skipped: usize,
    pub warnings: Vec<String>,
    /// One human-readable line per skipped step.
    pub skips: Vec<String>,
}

fn default_str(d: &ColumnDefaultConfig) -> String {
    match d {
        ColumnDefaultConfig::Literal(s) => s.clone(),
        ColumnDefaultConfig::Expression { expression } => expression.clone(),
    }
}

/// Steps for a column whose `generated` definition changed between versions. Generated values
/// are derived data, so in-place statements are used where the dialect has them (keeping the
/// column's indexes); everything else is surfaced for manual action rather than guessed at.
fn generated_change_steps(
    old_col: &ColumnConfig,
    new_col: &ColumnConfig,
    schema: &str,
    table: &str,
    full: &str,
    dialect: &dyn Dialect,
) -> Vec<MigrationStep> {
    let old_expr = old_col
        .generated
        .as_ref()
        .map(|g| g.expression_for(dialect.name()));
    let new_expr = new_col
        .generated
        .as_ref()
        .map(|g| g.expression_for(dialect.name()));
    let col = quote(&new_col.name);
    let col_def = format!("{} {}", col, column_def_suffix(new_col, dialect, false));
    let step = |operation, description: String, ddl: Option<String>, safety, risk, risk_detail| {
        MigrationStep {
            step: 0,
            operation,
            schema: schema.to_string(),
            table: Some(table.to_string()),
            object: new_col.name.clone(),
            object_type: "column".into(),
            from_object: None,
            description,
            ddl,
            safety,
            risk,
            risk_detail,
        }
    };
    let manual = |description: String, detail: String| {
        step(
            MigrationOperation::AlterColumnType,
            description,
            None,
            MigrationSafety::WarnOnly,
            MigrationRisk::ManualActionRequired,
            Some(detail),
        )
    };
    match (old_expr, new_expr) {
        (Some(old), Some(new)) if old != new => {
            let description = format!(
                "Change generated expression of \"{}\".\"{}\".\"{}\": ({}) → ({})",
                schema, table, new_col.name, old, new
            );
            match dialect.alter_generated_expression_sql(full, &col, new, &col_def) {
                Some(ddl) => {
                    let detail = match dialect.name() {
                        "postgres" => "Requires PostgreSQL 17+ (SET EXPRESSION); rewrites the table. On older versions drop and re-add the column manually.",
                        _ => "Rewrites the table to recompute every row.",
                    };
                    vec![step(
                        MigrationOperation::SetGeneratedExpression,
                        description,
                        Some(ddl),
                        MigrationSafety::BestEffort,
                        MigrationRisk::MayFail,
                        Some(detail.into()),
                    )]
                }
                None => vec![manual(
                    description,
                    format!(
                        "{} cannot change a generated column's expression in place. Drop and re-add the column manually (recreate any index on it).",
                        dialect.name()
                    ),
                )],
            }
        }
        (Some(old), None) => {
            let plain_def = format!(
                "{} {}",
                col,
                column_def_suffix(
                    &ColumnConfig {
                        generated: None,
                        ..new_col.clone()
                    },
                    dialect,
                    false
                )
            );
            let description = format!(
                "Make \"{}\".\"{}\".\"{}\" a regular column (was generated as ({})); current values are kept",
                schema, table, new_col.name, old
            );
            match dialect.drop_generated_expression_sql(full, &col, &plain_def) {
                Some(ddl) => vec![step(
                    MigrationOperation::DropGeneratedExpression,
                    description,
                    Some(ddl),
                    MigrationSafety::BestEffort,
                    MigrationRisk::MayFail,
                    Some(match dialect.name() {
                        "postgres" => "Requires PostgreSQL 13+ (DROP EXPRESSION).".into(),
                        _ => "Only STORED generated columns can become regular columns.".into(),
                    }),
                )],
                None => vec![manual(
                    description,
                    format!(
                        "{} cannot drop a column's generation expression. Recreate the column manually.",
                        dialect.name()
                    ),
                )],
            }
        }
        (None, Some(new)) => vec![manual(
            format!(
                "Make \"{}\".\"{}\".\"{}\" generated as ({})",
                schema, table, new_col.name, new
            ),
            "A regular column cannot become a generated column in place. Drop and re-add it manually; existing values will be replaced by computed ones.".into(),
        )],
        _ => Vec::new(),
    }
}

/// A column whose type is (an array of) a given enum — i.e. one that must be recast when the
/// enum type is rebuilt.
struct EnumColumnRef {
    schema: String,
    table: String,
    column: String,
    default: Option<String>,
    is_array: bool,
}

/// If `t` is a custom enum reference (`schema.name`, bare `name`, or an array of one), return
/// `(enum_name, is_array)` where `enum_name` is the unqualified type name. None for built-in types.
fn enum_type_name(t: &crate::db::CanonicalType) -> Option<(String, bool)> {
    use crate::db::CanonicalType;
    let unqualified = |s: &str| s.rsplit('.').next().unwrap_or(s).to_string();
    match t {
        CanonicalType::Custom(s) => Some((unqualified(s), false)),
        CanonicalType::Array(inner) => match inner.as_ref() {
            CanonicalType::Custom(s) => Some((unqualified(s), true)),
            _ => None,
        },
        _ => None,
    }
}

/// Find every column in `new` whose type is `new_enum` (or an array of it), resolved to its live
/// table/schema name. Used to drive the recast steps of an enum rebuild.
fn enum_dependent_columns(
    new_enum: &EnumConfig,
    new: &FullConfig,
    new_tables: &HashMap<&str, &TableConfig>,
    new_schemas: &HashMap<&str, &SchemaConfig>,
    schema_override: Option<&str>,
) -> Vec<EnumColumnRef> {
    let default_sid = new.schemas.first().map(|s| s.id.as_str()).unwrap_or("");
    let mut out = Vec::new();
    for c in &new.columns {
        let Some((tyname, is_array)) = enum_type_name(&parse_canonical(&c.type_)) else {
            continue;
        };
        if tyname != new_enum.name {
            continue;
        }
        let Some(table) = new_tables.get(c.table_id.as_str()) else {
            continue;
        };
        let tsid = table.schema_id.as_deref().unwrap_or(default_sid);
        let schema = schema_override.map(String::from).unwrap_or_else(|| {
            new_schemas
                .get(tsid)
                .map(|s| s.name.clone())
                .unwrap_or_else(|| tsid.to_string())
        });
        out.push(EnumColumnRef {
            schema,
            table: table.name.clone(),
            column: c.name.clone(),
            default: c.default.as_ref().map(default_str),
            is_array,
        });
    }
    out
}

/// PostgreSQL cannot drop a value from an enum in place, so when one is removed the type must be
/// rebuilt: rename the live type aside, create it afresh with the reduced value set, recast every
/// dependent column through a text cast, then drop the old type. Steps are emitted as `BestEffort`
/// — a recast that fails because a row still holds a removed value is surfaced as a warning rather
/// than aborting the whole upgrade. Each statement is a separate step because `execute_migration_plan`
/// runs them individually (the extended protocol forbids multiple statements per query).
fn recreate_enum_steps(
    steps: &mut Vec<MigrationStep>,
    schema: &str,
    new_enum: &EnumConfig,
    removed: &[&str],
    dependents: &[EnumColumnRef],
) {
    let type_q = format!("{}.{}", quote(schema), quote(&new_enum.name));
    let tmp_name = format!("{}__arch_old", new_enum.name);
    let values: Vec<String> = new_enum
        .values
        .iter()
        .map(|v| format!("'{}'", v.replace('\'', "''")))
        .collect();

    // 0. Informational summary of the destructive rebuild (no DDL).
    steps.push(MigrationStep {
        step: 0,
        operation: MigrationOperation::RemoveEnumValue,
        schema: schema.to_string(),
        table: None,
        object: format!("{}:{}", new_enum.name, removed.join(",")),
        object_type: "enum".into(),
        from_object: None,
        description: format!(
            "Rebuild enum \"{}\".\"{}\" to remove value(s): {}",
            schema,
            new_enum.name,
            removed.join(", ")
        ),
        ddl: None,
        safety: MigrationSafety::WarnOnly,
        risk: MigrationRisk::ManualActionRequired,
        risk_detail: Some(format!(
            "PostgreSQL cannot drop enum values in place. The type is rebuilt and {} dependent \
             column(s) are recast via a text cast. Any existing row holding a removed value ({}) \
             will make its recast fail — reassign those rows first.",
            dependents.len(),
            removed.join(", ")
        )),
    });

    // 1. Rename the live type aside.
    steps.push(MigrationStep {
        step: 0,
        operation: MigrationOperation::DropEnum,
        schema: schema.to_string(),
        table: None,
        object: new_enum.name.clone(),
        object_type: "enum".into(),
        from_object: None,
        description: format!(
            "Rename enum \"{}\".\"{}\" to \"{}\" before rebuild",
            schema, new_enum.name, tmp_name
        ),
        ddl: Some(format!(
            "ALTER TYPE {} RENAME TO {}",
            type_q,
            quote(&tmp_name)
        )),
        safety: MigrationSafety::BestEffort,
        risk: MigrationRisk::None,
        risk_detail: None,
    });

    // 2. Create the type afresh with the reduced value set (folds in any added values too).
    steps.push(MigrationStep {
        step: 0,
        operation: MigrationOperation::CreateEnum,
        schema: schema.to_string(),
        table: None,
        object: new_enum.name.clone(),
        object_type: "enum".into(),
        from_object: None,
        description: format!(
            "Recreate enum \"{}\".\"{}\" with {} value(s)",
            schema,
            new_enum.name,
            new_enum.values.len()
        ),
        ddl: Some(format!(
            "CREATE TYPE {} AS ENUM ({})",
            type_q,
            values.join(", ")
        )),
        safety: MigrationSafety::BestEffort,
        risk: MigrationRisk::None,
        risk_detail: None,
    });

    // 3. Recast every dependent column from the renamed type onto the rebuilt one. A column with a
    //    default must drop it first (the default still references the renamed type) and restore it after.
    for dep in dependents {
        let table_q = format!("{}.{}", quote(&dep.schema), quote(&dep.table));
        let col_q = quote(&dep.column);

        if dep.default.is_some() {
            steps.push(MigrationStep {
                step: 0,
                operation: MigrationOperation::DropDefault,
                schema: dep.schema.clone(),
                table: Some(dep.table.clone()),
                object: dep.column.clone(),
                object_type: "column".into(),
                from_object: None,
                description: format!(
                    "Drop default on {}.{} before enum recast",
                    dep.table, dep.column
                ),
                ddl: Some(format!(
                    "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                    table_q, col_q
                )),
                safety: MigrationSafety::BestEffort,
                risk: MigrationRisk::None,
                risk_detail: None,
            });
        }

        let (col_type, using) = if dep.is_array {
            (
                format!("{}[]", type_q),
                format!("{}::text[]::{}[]", col_q, type_q),
            )
        } else {
            (type_q.clone(), format!("{}::text::{}", col_q, type_q))
        };
        steps.push(MigrationStep {
            step: 0,
            operation: MigrationOperation::AlterColumnType,
            schema: dep.schema.clone(),
            table: Some(dep.table.clone()),
            object: dep.column.clone(),
            object_type: "column".into(),
            from_object: None,
            description: format!(
                "Recast {}.{} onto rebuilt enum \"{}\"",
                dep.table, dep.column, new_enum.name
            ),
            ddl: Some(format!(
                "ALTER TABLE {} ALTER COLUMN {} TYPE {} USING {}",
                table_q, col_q, col_type, using
            )),
            safety: MigrationSafety::BestEffort,
            risk: MigrationRisk::MayFail,
            risk_detail: Some(format!(
                "Cast fails if any row holds a removed value ({}).",
                removed.join(", ")
            )),
        });

        if let Some(def) = &dep.default {
            steps.push(MigrationStep {
                step: 0,
                operation: MigrationOperation::SetDefault,
                schema: dep.schema.clone(),
                table: Some(dep.table.clone()),
                object: dep.column.clone(),
                object_type: "column".into(),
                from_object: None,
                description: format!(
                    "Restore default on {}.{} after enum recast",
                    dep.table, dep.column
                ),
                ddl: Some(format!(
                    "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                    table_q, col_q, def
                )),
                safety: MigrationSafety::BestEffort,
                risk: MigrationRisk::None,
                risk_detail: None,
            });
        }
    }

    // 4. Drop the renamed original type.
    steps.push(MigrationStep {
        step: 0,
        operation: MigrationOperation::DropEnum,
        schema: schema.to_string(),
        table: None,
        object: tmp_name.clone(),
        object_type: "enum".into(),
        from_object: None,
        description: format!("Drop superseded enum \"{}\".\"{}\"", schema, tmp_name),
        ddl: Some(format!(
            "DROP TYPE IF EXISTS {}.{}",
            quote(schema),
            quote(&tmp_name)
        )),
        safety: MigrationSafety::BestEffort,
        risk: MigrationRisk::None,
        risk_detail: None,
    });
}

// ─── compute_migration_plan ──────────────────────────────────────────────────

/// Diff two package configs and produce an ordered list of migration steps.
/// This is a pure function — it does not touch the database.
/// Pass the result to `execute_migration_plan` after user confirmation.
pub fn compute_migration_plan(
    old: &FullConfig,
    new: &FullConfig,
    schema_override: Option<&str>,
    _rls_tenant_column: Option<&str>,
    dialect: &dyn Dialect,
    cross_package_configs: &HashMap<String, FullConfig>,
) -> Result<MigrationPlan, AppError> {
    validate(new)?;

    let default_old_sid = old.schemas.first().map(|s| s.id.as_str()).unwrap_or("");
    let default_new_sid = new.schemas.first().map(|s| s.id.as_str()).unwrap_or("");

    let old_schemas: HashMap<&str, &SchemaConfig> =
        old.schemas.iter().map(|s| (s.id.as_str(), s)).collect();
    let new_schemas: HashMap<&str, &SchemaConfig> =
        new.schemas.iter().map(|s| (s.id.as_str(), s)).collect();
    let old_tables: HashMap<&str, &TableConfig> =
        old.tables.iter().map(|t| (t.id.as_str(), t)).collect();
    let new_tables: HashMap<&str, &TableConfig> =
        new.tables.iter().map(|t| (t.id.as_str(), t)).collect();
    let old_columns: HashMap<&str, &ColumnConfig> =
        old.columns.iter().map(|c| (c.id.as_str(), c)).collect();
    let old_enums: HashMap<&str, &EnumConfig> =
        old.enums.iter().map(|e| (e.id.as_str(), e)).collect();
    let new_enums: HashMap<&str, &EnumConfig> =
        new.enums.iter().map(|e| (e.id.as_str(), e)).collect();
    let old_indexes: HashMap<&str, &IndexConfig> =
        old.indexes.iter().map(|i| (i.id.as_str(), i)).collect();
    let new_indexes: HashMap<&str, &IndexConfig> =
        new.indexes.iter().map(|i| (i.id.as_str(), i)).collect();
    let old_rels: HashMap<&str, &RelationshipConfig> = old
        .relationships
        .iter()
        .map(|r| (r.id.as_str(), r))
        .collect();
    let new_rels: HashMap<&str, &RelationshipConfig> = new
        .relationships
        .iter()
        .map(|r| (r.id.as_str(), r))
        .collect();

    let mut steps: Vec<MigrationStep> = Vec::new();

    let schema_name_for = |sid: &str, schemas: &HashMap<&str, &SchemaConfig>| -> String {
        schema_override.map(String::from).unwrap_or_else(|| {
            schemas
                .get(sid)
                .map(|s| s.name.clone())
                .unwrap_or_else(|| sid.to_string())
        })
    };

    // ── 1. New schemas ───────────────────────────────────────────────────────
    if schema_override.is_none() {
        for s in &new.schemas {
            if !old_schemas.contains_key(s.id.as_str()) {
                steps.push(MigrationStep {
                    step: 0,
                    operation: MigrationOperation::CreateSchema,
                    schema: s.name.clone(),
                    table: None,
                    object: s.name.clone(),
                    object_type: "schema".into(),
                    from_object: None,
                    description: format!("Create schema \"{}\"", s.name),
                    ddl: Some(format!("CREATE SCHEMA IF NOT EXISTS {}", quote(&s.name))),
                    safety: MigrationSafety::Safe,
                    risk: MigrationRisk::None,
                    risk_detail: None,
                });
            }
        }
    }

    // ── 2. Enums ─────────────────────────────────────────────────────────────
    for new_enum in &new.enums {
        let sid = new_enum.schema_id.as_deref().unwrap_or(default_new_sid);
        let schema = schema_name_for(sid, &new_schemas);

        if let Some(old_enum) = old_enums.get(new_enum.id.as_str()) {
            let old_vals: HashSet<&str> = old_enum.values.iter().map(String::as_str).collect();
            let new_vals: HashSet<&str> = new_enum.values.iter().map(String::as_str).collect();
            let removed: Vec<&str> = old_enum
                .values
                .iter()
                .map(String::as_str)
                .filter(|v| !new_vals.contains(v))
                .collect();

            if removed.is_empty() {
                // Purely additive — append each new value in place (cheap, non-destructive).
                for val in new_enum
                    .values
                    .iter()
                    .map(String::as_str)
                    .filter(|v| !old_vals.contains(v))
                {
                    steps.push(MigrationStep {
                        step: 0,
                        operation: MigrationOperation::AddEnumValue,
                        schema: schema.clone(),
                        table: None,
                        object: format!("{}:{}", new_enum.name, val),
                        object_type: "enum_value".into(),
                        from_object: None,
                        description: format!(
                            "Add value '{}' to enum \"{}\".\"{}\"",
                            val, schema, new_enum.name
                        ),
                        ddl: Some(format!(
                            "ALTER TYPE {}.{} ADD VALUE IF NOT EXISTS '{}'",
                            quote(&schema),
                            quote(&new_enum.name),
                            val.replace('\'', "''")
                        )),
                        safety: MigrationSafety::Safe,
                        risk: MigrationRisk::None,
                        risk_detail: None,
                    });
                }
            } else {
                // One or more values removed. PostgreSQL has no DROP VALUE, so rebuild the type and
                // recast every dependent column. Added values (if any) are folded into the rebuilt
                // value list, so no separate ADD VALUE step is needed.
                let dependents = enum_dependent_columns(
                    new_enum,
                    new,
                    &new_tables,
                    &new_schemas,
                    schema_override,
                );
                recreate_enum_steps(&mut steps, &schema, new_enum, &removed, &dependents);
            }
        } else {
            let values: Vec<String> = new_enum
                .values
                .iter()
                .map(|v| format!("'{}'", v.replace('\'', "''")))
                .collect();
            steps.push(MigrationStep {
                step: 0,
                operation: MigrationOperation::CreateEnum,
                schema: schema.clone(),
                table: None,
                object: new_enum.name.clone(),
                object_type: "enum".into(),
                from_object: None,
                description: format!("Create enum type \"{}\".\"{}\"", schema, new_enum.name),
                ddl: Some(format!("CREATE TYPE {}.{} AS ENUM ({})", quote(&schema), quote(&new_enum.name), values.join(", "))),
                safety: MigrationSafety::BestEffort,
                risk: MigrationRisk::None,
                risk_detail: Some("PostgreSQL has no CREATE TYPE IF NOT EXISTS; ignored if the type already exists.".into()),
            });
        }
    }
    for old_enum in &old.enums {
        if !new_enums.contains_key(old_enum.id.as_str()) {
            let sid = old_enum.schema_id.as_deref().unwrap_or(default_old_sid);
            let schema = schema_name_for(sid, &old_schemas);
            steps.push(MigrationStep {
                step: 0,
                operation: MigrationOperation::DropEnum,
                schema: schema.clone(),
                table: None,
                object: old_enum.name.clone(),
                object_type: "enum".into(),
                from_object: None,
                description: format!("Enum \"{}\".\"{}\" removed from config", schema, old_enum.name),
                ddl: None,
                safety: MigrationSafety::WarnOnly,
                risk: MigrationRisk::ManualActionRequired,
                risk_detail: Some("Enum type NOT dropped from database (data safety). Run DROP TYPE manually if intended.".into()),
            });
        }
    }

    // ── 3. New and removed tables ────────────────────────────────────────────
    let added_table_ids: HashSet<&str> = new
        .tables
        .iter()
        .filter(|t| !old_tables.contains_key(t.id.as_str()))
        .map(|t| t.id.as_str())
        .collect();

    let cols_by_table: HashMap<&str, Vec<&ColumnConfig>> =
        new.columns.iter().fold(HashMap::new(), |mut m, c| {
            m.entry(c.table_id.as_str()).or_default().push(c);
            m
        });

    for new_table in &new.tables {
        if !added_table_ids.contains(new_table.id.as_str()) {
            continue;
        }
        let sid = new_table.schema_id.as_deref().unwrap_or(default_new_sid);
        let schema = schema_name_for(sid, &new_schemas);
        let full = format!("{}.{}", quote(&schema), quote(&new_table.name));

        let cols = cols_by_table
            .get(new_table.id.as_str())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let mut col_defs: Vec<String> = Vec::new();
        for c in cols {
            let def = format!(
                "{} {}",
                quote(&c.name),
                column_def_suffix(c, dialect, false)
            );
            col_defs.push(def);
        }
        let cfg_col_names: HashSet<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        // Note: compute_migration_plan is a pure DDL-generation function; timestamp strings are
        // embedded in the DDL output for display/execution. We use postgres-compatible strings
        // here since the plan is always applied to a real DB via execute_migration_plan which
        // uses the dialect there. If dialect-awareness is needed here in future, pass dialect in.
        for (name, suf) in [
            ("created_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
            ("updated_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
            ("archived_at", "TIMESTAMPTZ"),
            ("created_by", "TEXT"),
            ("updated_by", "TEXT"),
        ] {
            if !cfg_col_names.contains(name) {
                col_defs.push(format!("{} {}", quote(name), suf));
            }
        }
        let pk_cols = match &new_table.primary_key {
            PrimaryKeyConfig::Single(s) => vec![quote(s)],
            PrimaryKeyConfig::Composite(v) => v.iter().map(|s| quote(s)).collect(),
        };
        col_defs.push(format!("PRIMARY KEY ({})", pk_cols.join(", ")));
        for u in &new_table.unique {
            col_defs.push(format!(
                "UNIQUE ({})",
                u.iter().map(|s| quote(s)).collect::<Vec<_>>().join(", ")
            ));
        }
        for ch in &new_table.check {
            col_defs.push(format!(
                "CONSTRAINT {} CHECK ({})",
                quote(&ch.name),
                ch.expression
            ));
        }

        steps.push(MigrationStep {
            step: 0,
            operation: MigrationOperation::CreateTable,
            schema: schema.clone(),
            table: Some(new_table.name.clone()),
            object: new_table.name.clone(),
            object_type: "table".into(),
            from_object: None,
            description: format!("Create table \"{}\".\"{}\"", schema, new_table.name),
            ddl: Some(format!(
                "CREATE TABLE IF NOT EXISTS {} (\n  {}\n)",
                full,
                col_defs.join(",\n  ")
            )),
            safety: MigrationSafety::Safe,
            risk: MigrationRisk::None,
            risk_detail: None,
        });
        if new_table.audit_log {
            let audit_ddl = audit_table_ddl(&schema, &new_table.name, cols, dialect);
            steps.push(MigrationStep {
                step: 0,
                operation: MigrationOperation::CreateTable,
                schema: schema.clone(),
                table: Some(format!("{}_audit", new_table.name)),
                object: format!("{}_audit", new_table.name),
                object_type: "table".into(),
                from_object: None,
                description: format!(
                    "Create audit table \"{}\".\"{}_audit\"",
                    schema, new_table.name
                ),
                ddl: Some(audit_ddl),
                safety: MigrationSafety::Safe,
                risk: MigrationRisk::None,
                risk_detail: None,
            });
        }
        if new_table.versioning.as_ref().is_some_and(|v| v.enabled) {
            let pk_col = match &new_table.primary_key {
                PrimaryKeyConfig::Single(s) => s.clone(),
                PrimaryKeyConfig::Composite(v) => v[0].clone(),
            };
            // Split history DDL into CREATE TABLE and index
            let history_create = format!(
                "CREATE TABLE IF NOT EXISTS {}.{} (\n  {}\n)",
                quote(&schema),
                quote(&format!("{}_history", new_table.name)),
                {
                    let full_ddl =
                        history_table_ddl(&schema, &new_table.name, &pk_col, cols, dialect);
                    full_ddl
                        .lines()
                        .skip(1) // skip CREATE TABLE line
                        .take_while(|l| !l.trim_start().starts_with("-- index:"))
                        .collect::<Vec<_>>()
                        .join("\n")
                        .trim_end_matches(['\n', ',', ')'])
                        .to_string()
                        + "\n)"
                }
            );
            steps.push(MigrationStep {
                step: 0,
                operation: MigrationOperation::CreateTable,
                schema: schema.clone(),
                table: Some(format!("{}_history", new_table.name)),
                object: format!("{}_history", new_table.name),
                object_type: "table".into(),
                from_object: None,
                description: format!(
                    "Create history table \"{}\".\"{}_history\" (versioning)",
                    schema, new_table.name
                ),
                ddl: Some(history_create),
                safety: MigrationSafety::Safe,
                risk: MigrationRisk::None,
                risk_detail: None,
            });
            steps.push(MigrationStep {
                step: 0,
                operation: MigrationOperation::CreateIndex,
                schema: schema.clone(),
                table: Some(format!("{}_history", new_table.name)),
                object: format!("{}_history_{}_idx", new_table.name, pk_col),
                object_type: "index".into(),
                from_object: None,
                description: format!(
                    "Create index on history table \"{}\".\"{}\" ({pk_col}, _version DESC)",
                    schema, new_table.name
                ),
                ddl: Some(history_index_ddl(&schema, &new_table.name, &pk_col)),
                safety: MigrationSafety::Safe,
                risk: MigrationRisk::None,
                risk_detail: None,
            });
        }
    }

    // Existing tables that gained audit_log or versioning
    for new_table in &new.tables {
        if added_table_ids.contains(new_table.id.as_str()) {
            continue;
        }
        if let Some(old_table) = old_tables.get(new_table.id.as_str()) {
            let sid = new_table.schema_id.as_deref().unwrap_or(default_new_sid);
            let schema = schema_name_for(sid, &new_schemas);
            let cols = cols_by_table
                .get(new_table.id.as_str())
                .map(|v| v.as_slice())
                .unwrap_or(&[]);

            if !old_table.audit_log && new_table.audit_log {
                let audit_ddl = audit_table_ddl(&schema, &new_table.name, cols, dialect);
                steps.push(MigrationStep {
                    step: 0,
                    operation: MigrationOperation::CreateTable,
                    schema: schema.clone(),
                    table: Some(format!("{}_audit", new_table.name)),
                    object: format!("{}_audit", new_table.name),
                    object_type: "table".into(),
                    from_object: None,
                    description: format!(
                        "Enable audit log: create \"{}\".\"{}_audit\"",
                        schema, new_table.name
                    ),
                    ddl: Some(audit_ddl),
                    safety: MigrationSafety::Safe,
                    risk: MigrationRisk::None,
                    risk_detail: None,
                });
            }

            let old_versioning_enabled = old_table.versioning.as_ref().is_some_and(|v| v.enabled);
            let new_versioning_enabled = new_table.versioning.as_ref().is_some_and(|v| v.enabled);
            if !old_versioning_enabled && new_versioning_enabled {
                let pk_col = match &new_table.primary_key {
                    PrimaryKeyConfig::Single(s) => s.clone(),
                    PrimaryKeyConfig::Composite(v) => v[0].clone(),
                };
                let history_ddl =
                    history_table_ddl(&schema, &new_table.name, &pk_col, cols, dialect);
                let create_only = history_ddl
                    .lines()
                    .take_while(|l| !l.trim_start().starts_with("-- index:"))
                    .collect::<Vec<_>>()
                    .join("\n");
                steps.push(MigrationStep {
                    step: 0,
                    operation: MigrationOperation::CreateTable,
                    schema: schema.clone(),
                    table: Some(format!("{}_history", new_table.name)),
                    object: format!("{}_history", new_table.name),
                    object_type: "table".into(),
                    from_object: None,
                    description: format!(
                        "Enable versioning: create \"{}\".\"{}_history\"",
                        schema, new_table.name
                    ),
                    ddl: Some(create_only.trim().to_string()),
                    safety: MigrationSafety::Safe,
                    risk: MigrationRisk::None,
                    risk_detail: None,
                });
                steps.push(MigrationStep {
                    step: 0,
                    operation: MigrationOperation::CreateIndex,
                    schema: schema.clone(),
                    table: Some(format!("{}_history", new_table.name)),
                    object: format!("{}_history_{}_idx", new_table.name, pk_col),
                    object_type: "index".into(),
                    from_object: None,
                    description: format!(
                        "Create history index on \"{}\".\"{}\"",
                        schema, new_table.name
                    ),
                    ddl: Some(history_index_ddl(&schema, &new_table.name, &pk_col)),
                    safety: MigrationSafety::Safe,
                    risk: MigrationRisk::None,
                    risk_detail: None,
                });
            }
        }
    }

    for old_table in &old.tables {
        if !new_tables.contains_key(old_table.id.as_str()) {
            let sid = old_table.schema_id.as_deref().unwrap_or(default_old_sid);
            let schema = schema_name_for(sid, &old_schemas);
            steps.push(MigrationStep {
                step: 0,
                operation: MigrationOperation::DropTable,
                schema: schema.clone(),
                table: Some(old_table.name.clone()),
                object: old_table.name.clone(),
                object_type: "table".into(),
                from_object: None,
                description: format!("Table \"{}\".\"{}\" removed from config", schema, old_table.name),
                ddl: None,
                safety: MigrationSafety::WarnOnly,
                risk: MigrationRisk::ManualActionRequired,
                risk_detail: Some("Table NOT dropped from database (data safety). Run DROP TABLE manually if intended.".into()),
            });
        }
    }

    // ── 4. Column changes for existing tables ────────────────────────────────
    for new_col in &new.columns {
        if added_table_ids.contains(new_col.table_id.as_str()) {
            continue;
        }
        let table = match new_tables.get(new_col.table_id.as_str()) {
            Some(t) => t,
            None => continue,
        };
        let sid = table.schema_id.as_deref().unwrap_or(default_new_sid);
        let schema = schema_name_for(sid, &new_schemas);
        let full = format!("{}.{}", quote(&schema), quote(&table.name));

        if let Some(old_col) = old_columns.get(new_col.id.as_str()) {
            if old_col.table_id != new_col.table_id {
                steps.push(MigrationStep {
                    step: 0,
                    operation: MigrationOperation::AddColumn,
                    schema: schema.clone(),
                    table: Some(table.name.clone()),
                    object: new_col.name.clone(),
                    object_type: "column".into(),
                    from_object: None,
                    description: format!("Column \"{}\" (id: {}) appears to have moved tables — manual migration required", new_col.name, new_col.id),
                    ddl: None,
                    safety: MigrationSafety::WarnOnly,
                    risk: MigrationRisk::ManualActionRequired,
                    risk_detail: Some(format!("Cannot automate column move from table {} to {}.", old_col.table_id, new_col.table_id)),
                });
                continue;
            }

            // Rename
            if old_col.name != new_col.name {
                steps.push(MigrationStep {
                    step: 0,
                    operation: MigrationOperation::RenameColumn,
                    schema: schema.clone(),
                    table: Some(table.name.clone()),
                    object: new_col.name.clone(),
                    object_type: "column".into(),
                    from_object: Some(old_col.name.clone()),
                    description: format!(
                        "Rename column \"{}\" → \"{}\" on \"{}\".\"{}\"",
                        old_col.name, new_col.name, schema, table.name
                    ),
                    ddl: Some(format!(
                        "ALTER TABLE {} RENAME COLUMN {} TO {}",
                        full,
                        quote(&old_col.name),
                        quote(&new_col.name)
                    )),
                    safety: MigrationSafety::Safe,
                    risk: MigrationRisk::None,
                    risk_detail: None,
                });
                steps.extend(companion_column_steps(
                    &schema,
                    table,
                    &CompanionColumnOp::Rename {
                        old: &old_col.name,
                        new: &new_col.name,
                    },
                    dialect,
                ));
            }

            steps.extend(generated_change_steps(
                old_col,
                new_col,
                &schema,
                &table.name,
                &full,
                dialect,
            ));

            // Type change
            let old_type = dialect.ddl_type(&parse_canonical(&old_col.type_));
            let new_type = dialect.ddl_type(&parse_canonical(&new_col.type_));
            if old_type.to_uppercase() != new_type.to_uppercase() {
                let col_name = &new_col.name;
                steps.push(MigrationStep {
                    step: 0,
                    operation: MigrationOperation::AlterColumnType,
                    schema: schema.clone(),
                    table: Some(table.name.clone()),
                    object: col_name.clone(),
                    object_type: "column".into(),
                    from_object: None,
                    description: format!("Change type of \"{}\".\"{}\".\"{}\": {} → {}", schema, table.name, col_name, old_type, new_type),
                    ddl: Some(format!("ALTER TABLE {} ALTER COLUMN {} TYPE {} USING {}::{}", full, quote(col_name), new_type, quote(col_name), new_type)),
                    safety: MigrationSafety::BestEffort,
                    risk: MigrationRisk::MayFail,
                    risk_detail: Some(format!("USING {}::{} cast may fail for incompatible values. Provide a custom USING expression if needed.", col_name, new_type)),
                });
                steps.extend(companion_column_steps(
                    &schema,
                    table,
                    &CompanionColumnOp::AlterType {
                        name: col_name.as_str(),
                        ty: &new_type,
                    },
                    dialect,
                ));
            }

            // Nullability: nullable → NOT NULL
            if old_col.nullable && !new_col.nullable {
                if let Some(ref d) = new_col.default {
                    let default_val = default_str(d);
                    // Backfill NULLs first using the configured default
                    steps.push(MigrationStep {
                        step: 0,
                        operation: MigrationOperation::BackfillNulls,
                        schema: schema.clone(),
                        table: Some(table.name.clone()),
                        object: new_col.name.clone(),
                        object_type: "column".into(),
                        from_object: None,
                        description: format!("Backfill NULLs in \"{}\".\"{}\".\"{}\": SET {} = {} WHERE {} IS NULL", schema, table.name, new_col.name, new_col.name, default_val, new_col.name),
                        ddl: Some(format!("UPDATE {} SET {} = {} WHERE {} IS NULL", full, quote(&new_col.name), default_val, quote(&new_col.name))),
                        safety: MigrationSafety::Safe,
                        risk: MigrationRisk::DataWillBeModified,
                        risk_detail: Some(format!("Existing NULLs in column \"{}\" will be set to {} before NOT NULL is enforced.", new_col.name, default_val)),
                    });
                    // Then set NOT NULL — safe because NULLs are gone
                    steps.push(MigrationStep {
                        step: 0,
                        operation: MigrationOperation::SetNotNull,
                        schema: schema.clone(),
                        table: Some(table.name.clone()),
                        object: new_col.name.clone(),
                        object_type: "column".into(),
                        from_object: None,
                        description: format!("Set NOT NULL on \"{}\".\"{}\".\"{}\": NULLs pre-filled with default ({})", schema, table.name, new_col.name, default_val),
                        ddl: Some(format!("ALTER TABLE {} ALTER COLUMN {} SET NOT NULL", full, quote(&new_col.name))),
                        safety: MigrationSafety::Safe,
                        risk: MigrationRisk::None,
                        risk_detail: None,
                    });
                } else {
                    // No default — best effort; will fail if NULLs exist
                    steps.push(MigrationStep {
                        step: 0,
                        operation: MigrationOperation::SetNotNull,
                        schema: schema.clone(),
                        table: Some(table.name.clone()),
                        object: new_col.name.clone(),
                        object_type: "column".into(),
                        from_object: None,
                        description: format!("Set NOT NULL on \"{}\".\"{}\".\"{}\": no default configured — will fail if NULLs exist", schema, table.name, new_col.name),
                        ddl: Some(format!("ALTER TABLE {} ALTER COLUMN {} SET NOT NULL", full, quote(&new_col.name))),
                        safety: MigrationSafety::BestEffort,
                        risk: MigrationRisk::ExistingNullsMustBeAbsent,
                        risk_detail: Some(format!(
                            "No default value configured for column \"{}\". Add a default to the config to enable automatic NULL backfill before enforcing NOT NULL.",
                            new_col.name
                        )),
                    });
                }
            }

            // Nullability: NOT NULL → nullable
            if !old_col.nullable && new_col.nullable {
                steps.push(MigrationStep {
                    step: 0,
                    operation: MigrationOperation::DropNotNull,
                    schema: schema.clone(),
                    table: Some(table.name.clone()),
                    object: new_col.name.clone(),
                    object_type: "column".into(),
                    from_object: None,
                    description: format!(
                        "Drop NOT NULL on \"{}\".\"{}\".\"{}\": column becomes nullable",
                        schema, table.name, new_col.name
                    ),
                    ddl: Some(format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                        full,
                        quote(&new_col.name)
                    )),
                    safety: MigrationSafety::Safe,
                    risk: MigrationRisk::None,
                    risk_detail: None,
                });
            }

            // Default change
            let old_def = old_col.default.as_ref().map(default_str);
            let new_def = new_col.default.as_ref().map(default_str);
            if old_def != new_def {
                match &new_col.default {
                    Some(d) => {
                        let val = default_str(d);
                        steps.push(MigrationStep {
                            step: 0,
                            operation: MigrationOperation::SetDefault,
                            schema: schema.clone(),
                            table: Some(table.name.clone()),
                            object: new_col.name.clone(),
                            object_type: "column".into(),
                            from_object: None,
                            description: format!(
                                "Set DEFAULT {} on \"{}\".\"{}\".\"{}\": was {}",
                                val,
                                schema,
                                table.name,
                                new_col.name,
                                old_def.as_deref().unwrap_or("none")
                            ),
                            ddl: Some(format!(
                                "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                                full,
                                quote(&new_col.name),
                                val
                            )),
                            safety: MigrationSafety::Safe,
                            risk: MigrationRisk::None,
                            risk_detail: None,
                        });
                    }
                    None => {
                        steps.push(MigrationStep {
                            step: 0,
                            operation: MigrationOperation::DropDefault,
                            schema: schema.clone(),
                            table: Some(table.name.clone()),
                            object: new_col.name.clone(),
                            object_type: "column".into(),
                            from_object: None,
                            description: format!(
                                "Drop DEFAULT on \"{}\".\"{}\".\"{}\": was {}",
                                schema,
                                table.name,
                                new_col.name,
                                old_def.as_deref().unwrap_or("none")
                            ),
                            ddl: Some(format!(
                                "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                                full,
                                quote(&new_col.name)
                            )),
                            safety: MigrationSafety::Safe,
                            risk: MigrationRisk::None,
                            risk_detail: None,
                        });
                    }
                }
            }
        } else {
            // New column: ADD COLUMN
            let new_type = dialect.ddl_type(&parse_canonical(&new_col.type_));
            let col_def = format!(
                "{} {}",
                quote(&new_col.name),
                column_def_suffix(new_col, dialect, true)
            );
            steps.push(MigrationStep {
                step: 0,
                operation: MigrationOperation::AddColumn,
                schema: schema.clone(),
                table: Some(table.name.clone()),
                object: new_col.name.clone(),
                object_type: "column".into(),
                from_object: None,
                description: format!(
                    "Add column \"{}\" {} to \"{}\".\"{}\"",
                    new_col.name, new_type, schema, table.name
                ),
                ddl: Some(add_column_ddl(dialect, &full, &col_def)),
                safety: MigrationSafety::Safe,
                risk: MigrationRisk::None,
                risk_detail: None,
            });
            steps.extend(companion_column_steps(
                &schema,
                table,
                &CompanionColumnOp::Add {
                    name: &new_col.name,
                    ty: &new_type,
                },
                dialect,
            ));
        }
    }

    // Removed columns (warn only)
    for old_col in &old.columns {
        if new.columns.iter().any(|c| c.id == old_col.id) {
            continue;
        }
        if !new_tables.contains_key(old_col.table_id.as_str()) {
            continue;
        }
        let table_name = old_tables
            .get(old_col.table_id.as_str())
            .map(|t| t.name.as_str())
            .unwrap_or(&old_col.table_id);
        let sid = old_tables
            .get(old_col.table_id.as_str())
            .and_then(|t| t.schema_id.as_deref())
            .unwrap_or(default_old_sid);
        let schema = schema_name_for(sid, &old_schemas);
        let full = format!("{}.{}", quote(&schema), quote(table_name));
        steps.push(MigrationStep {
            step: 0,
            operation: MigrationOperation::DropColumn,
            schema: schema.clone(),
            table: Some(table_name.to_string()),
            object: old_col.name.clone(),
            object_type: "column".into(),
            from_object: None,
            description: format!("Column \"{}\" removed from config on \"{}\".\"{}\"", old_col.name, schema, table_name),
            // DDL is generated but only runs when the apply request sets `confirm_destructive`.
            // Without confirmation this step warns and the column (and its data) is retained.
            ddl: Some(format!(
                "ALTER TABLE {} DROP COLUMN IF EXISTS {}",
                full,
                quote(&old_col.name)
            )),
            safety: MigrationSafety::Destructive,
            risk: MigrationRisk::DataWillBeDropped,
            risk_detail: Some(format!(
                "Column \"{}\" and all its data will be permanently dropped. Runs only when the apply \
                 request confirms the destructive change; otherwise the column is retained.",
                old_col.name
            )),
        });
    }

    // ── 5. Indexes ───────────────────────────────────────────────────────────
    for old_idx in &old.indexes {
        if !new_indexes.contains_key(old_idx.id.as_str()) {
            let sid = old_idx.schema_id.as_deref().unwrap_or(default_old_sid);
            let schema = schema_name_for(sid, &old_schemas);
            steps.push(MigrationStep {
                step: 0,
                operation: MigrationOperation::DropIndex,
                schema: schema.clone(),
                table: old_tables
                    .get(old_idx.table_id.as_str())
                    .map(|t| t.name.clone()),
                object: old_idx.name.clone(),
                object_type: "index".into(),
                from_object: None,
                description: format!("Drop index \"{}\" in schema \"{}\"", old_idx.name, schema),
                ddl: Some(format!(
                    "DROP INDEX IF EXISTS {}.{}",
                    quote(&schema),
                    quote(&old_idx.name)
                )),
                safety: MigrationSafety::Safe,
                risk: MigrationRisk::None,
                risk_detail: None,
            });
        }
    }
    for new_idx in &new.indexes {
        if old_indexes.contains_key(new_idx.id.as_str())
            || added_table_ids.contains(new_idx.table_id.as_str())
        {
            continue;
        }
        let sid = new_idx.schema_id.as_deref().unwrap_or(default_new_sid);
        let schema = match new_schemas.get(sid) {
            Some(s) => schema_override.unwrap_or(&s.name).to_string(),
            None => continue,
        };
        let table = match new_tables.get(new_idx.table_id.as_str()) {
            Some(t) => t,
            None => continue,
        };
        let full_table = format!("{}.{}", quote(&schema), quote(&table.name));
        let mut col_parts: Vec<String> = Vec::new();
        for col in &new_idx.columns {
            match col {
                IndexColumnEntry::Name(n) => col_parts.push(quote(n)),
                IndexColumnEntry::Spec {
                    name, direction, ..
                } => {
                    let dir = direction
                        .as_deref()
                        .map(|d| format!(" {}", d.to_uppercase()))
                        .unwrap_or_default();
                    col_parts.push(format!("{}{}", quote(name), dir));
                }
                IndexColumnEntry::Expression { expression } => col_parts.push(expression.clone()),
            }
        }
        let method = new_idx.method.as_deref().unwrap_or("btree");
        let unique_kw = if new_idx.unique { "UNIQUE " } else { "" };
        let include = if new_idx.include.is_empty() {
            String::new()
        } else {
            format!(
                " INCLUDE ({})",
                new_idx
                    .include
                    .iter()
                    .map(|s| quote(s))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let where_clause = new_idx
            .where_
            .as_ref()
            .map(|w| format!(" WHERE {}", w))
            .unwrap_or_default();
        steps.push(MigrationStep {
            step: 0,
            operation: MigrationOperation::CreateIndex,
            schema: schema.clone(),
            table: Some(table.name.clone()),
            object: new_idx.name.clone(),
            object_type: "index".into(),
            from_object: None,
            description: format!(
                "Create {}index \"{}\" on \"{}\".\"{}\"",
                if new_idx.unique { "unique " } else { "" },
                new_idx.name,
                schema,
                table.name
            ),
            ddl: Some(format!(
                "CREATE {}INDEX IF NOT EXISTS {} ON {} USING {} ({}){}{}",
                unique_kw,
                quote(&new_idx.name),
                full_table,
                method,
                col_parts.join(", "),
                include,
                where_clause
            )),
            safety: MigrationSafety::Safe,
            risk: MigrationRisk::None,
            risk_detail: None,
        });
    }

    // ── 6. Foreign keys ──────────────────────────────────────────────────────
    for old_rel in &old.relationships {
        if !new_rels.contains_key(old_rel.id.as_str()) {
            let from_sid_fallback = old_rel.from_schema_id.as_deref().unwrap_or(default_old_sid);
            let from_schema = old_schemas
                .get(from_sid_fallback)
                .map(|s| s.name.as_str())
                .unwrap_or(from_sid_fallback);
            let from_table = old_tables
                .get(old_rel.from_table_id.as_str())
                .map(|t| t.name.as_str())
                .unwrap_or(&old_rel.from_table_id);
            let constraint = old_rel.name.as_deref().unwrap_or(&old_rel.id);
            let schema_q = quote(schema_override.unwrap_or(from_schema));
            steps.push(MigrationStep {
                step: 0,
                operation: MigrationOperation::DropForeignKey,
                schema: schema_override.unwrap_or(from_schema).to_string(),
                table: Some(from_table.to_string()),
                object: constraint.to_string(),
                object_type: "foreign_key".into(),
                from_object: None,
                description: format!(
                    "Drop FK \"{}\" from \"{}\".\"{}\"",
                    constraint,
                    schema_override.unwrap_or(from_schema),
                    from_table
                ),
                ddl: Some(format!(
                    "ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {}",
                    schema_q,
                    quote(from_table),
                    quote(constraint)
                )),
                safety: MigrationSafety::Safe,
                risk: MigrationRisk::None,
                risk_detail: None,
            });
        }
    }
    for new_rel in &new.relationships {
        if old_rels.contains_key(new_rel.id.as_str())
            || added_table_ids.contains(new_rel.from_table_id.as_str())
            || added_table_ids.contains(new_rel.to_table_id.as_str())
        {
            continue;
        }
        let from_sid = new_rel.from_schema_id.as_deref().unwrap_or(default_new_sid);
        let from_schema = match new_schemas.get(from_sid) {
            Some(s) => s,
            None => continue,
        };
        let from_table = match new_tables.get(new_rel.from_table_id.as_str()) {
            Some(t) => t,
            None => continue,
        };
        let from_col = new
            .columns
            .iter()
            .find(|c| c.id == new_rel.from_column_id)
            .map(|c| c.name.clone())
            .unwrap_or_else(|| new_rel.from_column_id.clone());

        // Resolve the target side — cross-package or same-package.
        let (to_schema_name, to_table_name, to_col) =
            if let Some(pkg_id) = new_rel.to_package_id.as_deref() {
                match cross_package_configs.get(pkg_id) {
                    Some(foreign) => {
                        let foreign_tables: HashMap<_, _> =
                            foreign.tables.iter().map(|t| (t.id.as_str(), t)).collect();
                        let foreign_schemas: HashMap<_, _> =
                            foreign.schemas.iter().map(|s| (s.id.as_str(), s)).collect();
                        let foreign_default_sid =
                            foreign.schemas.first().map(|s| s.id.as_str()).unwrap_or("");
                        let to_sid = new_rel
                            .to_schema_id
                            .as_deref()
                            .unwrap_or(foreign_default_sid);
                        let tbl = match foreign_tables.get(new_rel.to_table_id.as_str()) {
                            Some(t) => t,
                            None => continue,
                        };
                        let schema = match foreign_schemas.get(to_sid) {
                            Some(s) => s,
                            None => continue,
                        };
                        let col = foreign
                            .columns
                            .iter()
                            .find(|c| c.id == new_rel.to_column_id)
                            .map(|c| c.name.clone())
                            .unwrap_or_else(|| new_rel.to_column_id.clone());
                        (schema.name.clone(), tbl.name.clone(), col)
                    }
                    None => continue,
                }
            } else {
                let to_sid = new_rel.to_schema_id.as_deref().unwrap_or(default_new_sid);
                let to_schema = match new_schemas.get(to_sid) {
                    Some(s) => s,
                    None => continue,
                };
                let to_table = match new_tables.get(new_rel.to_table_id.as_str()) {
                    Some(t) => t,
                    None => continue,
                };
                let col = new
                    .columns
                    .iter()
                    .find(|c| c.id == new_rel.to_column_id)
                    .map(|c| c.name.clone())
                    .unwrap_or_else(|| new_rel.to_column_id.clone());
                (
                    schema_override.unwrap_or(&to_schema.name).to_string(),
                    to_table.name.clone(),
                    col,
                )
            };

        let from_schema_str = schema_override.unwrap_or(&from_schema.name);
        let from_q = format!("{}.{}", quote(from_schema_str), quote(&from_table.name));
        let to_q = format!("{}.{}", quote(&to_schema_name), quote(&to_table_name));
        let constraint = new_rel.name.as_deref().unwrap_or(&new_rel.id);
        let on_update = new_rel.on_update.as_deref().unwrap_or("NO ACTION");
        let on_delete = new_rel.on_delete.as_deref().unwrap_or("NO ACTION");
        steps.push(MigrationStep {
            step: 0,
            operation: MigrationOperation::AddForeignKey,
            schema: from_schema_str.to_string(),
            table: Some(from_table.name.clone()),
            object: constraint.to_string(),
            object_type: "foreign_key".into(),
            from_object: None,
            description: format!(
                "Add FK \"{}\" on \"{}\".\"{}\" → \"{}\".\"{}\"",
                constraint, from_schema_str, from_table.name, to_schema_name, to_table_name
            ),
            ddl: Some(format!(
                "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}) ON UPDATE {} ON DELETE {}",
                from_q, quote(constraint), quote(&from_col), to_q, quote(&to_col), on_update, on_delete
            )),
            safety: MigrationSafety::BestEffort,
            risk: MigrationRisk::None,
            risk_detail: Some("PostgreSQL has no ADD CONSTRAINT IF NOT EXISTS; ignored if constraint already exists.".into()),
        });
    }

    let (view_drops, view_creates) = view_plan_steps(old, new, &steps, schema_override, dialect)?;
    let mut steps: Vec<MigrationStep> = view_drops
        .into_iter()
        .chain(steps)
        .chain(view_creates)
        .collect();

    // Assign sequential step numbers
    for (i, s) in steps.iter_mut().enumerate() {
        s.step = i + 1;
    }

    Ok(MigrationPlan { steps })
}

// ─── Plan reconciliation against the physical database ───────────────────────

/// Whether a plan step still has work to do against a particular database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepDecision {
    /// Run the step's DDL.
    Execute,
    /// The step's effect is already present; the payload explains why it was skipped.
    Skip(String),
}

/// Decide whether `step` needs to run against the database described by `snap`.
///
/// Migration plans are a pure config-vs-config diff, so they assume the database matches the
/// *old* config exactly. It often does not: an upgrade may have failed halfway and been retried,
/// one plan is broadcast to many tenant databases at different states, and RLS/companion DDL adds
/// columns outside the diff. Rather than aborting on `column "x" already exists`, skip the steps
/// whose outcome is already true.
///
/// Negative rules ("the column is missing, so this step cannot run") only fire when the table is
/// actually known to the snapshot — an empty or failed introspection therefore skips nothing.
pub fn reconcile_step(step: &MigrationStep, snap: &DbSnapshot) -> StepDecision {
    if step.ddl.is_none() || !snap.introspected {
        return StepDecision::Execute;
    }
    let schema = step.schema.as_str();
    let object = step.object.as_str();
    let Some(table) = step.table.as_deref() else {
        return StepDecision::Execute;
    };
    let table_known = snap.has_table(schema, table);
    let column = snap.column(schema, table, object);
    // Only meaningful for a table we could actually read: an unknown table means the snapshot
    // has nothing to say, not that the column is missing.
    let column_gone = table_known && column.is_none();
    let missing_column = || {
        format!(
            "column \"{}\" does not exist on \"{}\".\"{}\"",
            object, schema, table
        )
    };

    use MigrationOperation as Op;
    let reason: Option<String> = match step.operation {
        Op::CreateTable if table_known => {
            Some(format!("table \"{}\".\"{}\" already exists", schema, table))
        }
        Op::AddColumn if column.is_some() => Some(format!(
            "column \"{}\" already exists on \"{}\".\"{}\"",
            object, schema, table
        )),
        // Confirmed drop whose column is already gone: nothing left to drop.
        Op::DropColumn if column_gone => Some(format!(
            "column \"{}\" already dropped from \"{}\".\"{}\"",
            object, schema, table
        )),
        Op::RenameColumn => rename_skip_reason(step, snap, schema, table, table_known),

        // A column that is gone cannot be retyped, defaulted or backfilled.
        Op::AlterColumnType
        | Op::SetGeneratedExpression
        | Op::DropGeneratedExpression
        | Op::BackfillNulls
        | Op::SetDefault
            if column_gone =>
        {
            Some(missing_column())
        }

        Op::SetNotNull if column.is_some_and(|f| !f.nullable) => Some(format!(
            "column \"{}\".\"{}\".\"{}\" is already NOT NULL",
            schema, table, object
        )),
        Op::DropNotNull if column.is_some_and(|f| f.nullable) => Some(format!(
            "column \"{}\".\"{}\".\"{}\" is already nullable",
            schema, table, object
        )),
        Op::DropDefault if column.is_some_and(|f| !f.has_default) => Some(format!(
            "column \"{}\".\"{}\".\"{}\" has no DEFAULT",
            schema, table, object
        )),
        Op::SetNotNull | Op::DropNotNull | Op::DropDefault if column_gone => Some(missing_column()),

        Op::CreateIndex if snap.indexes_known && snap.has_index(schema, object) => Some(format!(
            "index \"{}\".\"{}\" already exists",
            schema, object
        )),
        Op::AddForeignKey
            if snap.constraints_known && snap.has_constraint(schema, table, object) =>
        {
            Some(format!(
                "constraint \"{}\" already exists on \"{}\".\"{}\"",
                object, schema, table
            ))
        }

        // Drops already carry IF EXISTS, and enum steps are either IF NOT EXISTS or part of a
        // rename/recreate sequence whose intermediate state a pre-flight snapshot cannot describe.
        _ => None,
    };

    match reason {
        Some(r) => StepDecision::Skip(r),
        None => StepDecision::Execute,
    }
}

/// Skip reason for a `RenameColumn` step, or `None` when the rename still has work to do.
fn rename_skip_reason(
    step: &MigrationStep,
    snap: &DbSnapshot,
    schema: &str,
    table: &str,
    table_known: bool,
) -> Option<String> {
    // Plans saved before `from_object` existed carry no previous name; nothing to check, so let
    // the statement run.
    let from = step.from_object.as_deref()?;
    if !table_known {
        return None;
    }
    let to = step.object.as_str();
    match (
        snap.has_column(schema, table, from),
        snap.has_column(schema, table, to),
    ) {
        (false, true) => Some(format!(
            "column \"{}\".\"{}\".\"{}\" is already named \"{}\"",
            schema, table, from, to
        )),
        (false, false) => Some(format!(
            "neither \"{}\" nor \"{}\" exists on \"{}\".\"{}\" — nothing to rename",
            from, to, schema, table
        )),
        _ => None,
    }
}

/// Fold a successfully executed step into `snap` so later steps in the same plan see the state
/// the earlier ones produced (e.g. a column added and then made NOT NULL).
///
/// `CreateTable` is deliberately not recorded: registering a table with no known columns would
/// make later column steps look impossible and get them wrongly skipped.
fn apply_step_to_snapshot(step: &MigrationStep, snap: &mut DbSnapshot) {
    let schema = step.schema.as_str();
    let object = step.object.as_str();
    let table = step.table.as_deref();
    // `from_object` rides in the tuple so the rename arm needs no nested conditional.
    match (step.operation.clone(), table, step.from_object.as_deref()) {
        (MigrationOperation::AddColumn, Some(t), _) => {
            let ddl = step.ddl.as_deref().unwrap_or("").to_uppercase();
            snap.add_column(
                schema,
                t,
                object,
                ColumnFacts {
                    data_type: String::new(),
                    nullable: !ddl.contains(" NOT NULL"),
                    has_default: ddl.contains(" DEFAULT "),
                },
            );
        }
        (MigrationOperation::RenameColumn, Some(t), Some(from)) => {
            snap.rename_column(schema, t, from, object)
        }
        (MigrationOperation::DropColumn, Some(t), _) => snap.remove_column(schema, t, object),
        (MigrationOperation::SetNotNull, Some(t), _) => snap.set_nullable(schema, t, object, false),
        (MigrationOperation::DropNotNull, Some(t), _) => snap.set_nullable(schema, t, object, true),
        (MigrationOperation::SetDefault, Some(t), _) => {
            snap.set_has_default(schema, t, object, true)
        }
        (MigrationOperation::DropDefault, Some(t), _) => {
            snap.set_has_default(schema, t, object, false)
        }
        (MigrationOperation::CreateIndex, _, _) => snap.add_index(schema, object),
        (MigrationOperation::DropIndex, _, _) => snap.remove_index(schema, object),
        (MigrationOperation::AddForeignKey, Some(t), _) => snap.add_constraint(schema, t, object),
        (MigrationOperation::DropForeignKey, Some(t), _) => {
            snap.remove_constraint(schema, t, object)
        }
        _ => {}
    }
}

/// Recognise "the object I tried to create already exists" errors.
///
/// Backstop for whatever pre-flight introspection could not see: a schema it had no privileges
/// on, an object created by a concurrent migration, or a dialect that reports no index and
/// constraint catalogs.
fn duplicate_object_reason(dialect: &dyn Dialect, e: &sqlx::Error) -> Option<String> {
    let dbe = e.as_database_error()?;
    let duplicate_code = dbe
        .code()
        .is_some_and(|code| dialect.is_duplicate_object_code(code.as_ref()));
    if duplicate_code {
        return Some(format!(
            "object already exists (SQLSTATE {}): {}",
            dbe.code().unwrap_or_default(),
            dbe.message()
        ));
    }
    let msg = dbe.message().to_ascii_lowercase();
    if msg.contains("already exists")
        || msg.contains("duplicate column name")
        || msg.contains("duplicate key name")
    {
        return Some(format!("object already exists: {}", dbe.message()));
    }
    None
}

// ─── execute_migration_plan ──────────────────────────────────────────────────

/// Identifiers of the migration being executed, threaded into every audit row.
struct AuditContext<'a> {
    migration_plan_id: &'a str,
    package_id: &'a str,
    tenant_id: &'a str,
    from_version: Option<&'a str>,
    to_version: &'a str,
}

async fn audit_step(
    config_pool: &Pool,
    ctx: &AuditContext<'_>,
    step: &MigrationStep,
    status: &str,
    error_message: Option<&str>,
) {
    let _ = crate::store::insert_migration_audit(
        config_pool,
        ctx.migration_plan_id,
        ctx.package_id,
        ctx.tenant_id,
        ctx.from_version,
        ctx.to_version,
        step.step as i32,
        &step.operation.to_string(),
        &step.schema,
        step.table.as_deref(),
        &step.object,
        &step.object_type,
        &step.description,
        step.ddl.as_deref(),
        &format!("{:?}", step.safety),
        &format!("{:?}", step.risk),
        status,
        error_message,
    )
    .await;
}

/// Execute a pre-computed `MigrationPlan` against the tenant database.
///
/// The plan is reconciled against the database's actual state first (see [`reconcile_step`]), so
/// steps whose effect is already present are skipped instead of failing the whole migration.
/// Writes per-step audit records to the config (architect) database.
/// Returns counts and any warning messages collected from best-effort failures.
#[allow(clippy::too_many_arguments)]
pub async fn execute_migration_plan(
    migration_pool: &Pool,
    config_pool: &Pool,
    plan: &MigrationPlan,
    migration_plan_id: &str,
    package_id: &str,
    tenant_id: &str,
    from_version: Option<&str>,
    to_version: &str,
    dialect: &dyn Dialect,
    confirm_destructive: bool,
) -> Result<MigrationExecutionResult, AppError> {
    let mut applied = 0usize;
    let mut warned = 0usize;
    let mut skipped = 0usize;
    let mut warnings: Vec<String> = Vec::new();
    let mut skips: Vec<String> = Vec::new();

    let ctx = AuditContext {
        migration_plan_id,
        package_id,
        tenant_id,
        from_version,
        to_version,
    };

    // Pre-flight: what does this database actually look like? Best-effort — on failure the
    // snapshot stays empty, nothing is skipped, and execution behaves exactly as before.
    let schemas: Vec<String> = {
        let mut seen: Vec<String> = Vec::new();
        for step in &plan.steps {
            if step.ddl.is_some() && !seen.iter().any(|s| s == &step.schema) {
                seen.push(step.schema.clone());
            }
        }
        seen
    };
    let mut snapshot = crate::db::introspect(migration_pool, dialect, &schemas).await;

    for step in &plan.steps {
        let op = step.operation.to_string();

        // A destructive step (e.g. DROP COLUMN) carries executable DDL but only runs when the
        // caller explicitly confirmed it. Unconfirmed, it degrades to a warn-only step: the object
        // and its data are retained and a warning is recorded.
        let effective_safety = match step.safety {
            MigrationSafety::Destructive if !confirm_destructive => &MigrationSafety::WarnOnly,
            ref other => other,
        };

        match effective_safety {
            MigrationSafety::WarnOnly => {
                let msg = step
                    .risk_detail
                    .clone()
                    .unwrap_or_else(|| step.description.clone());
                tracing::warn!(step = step.step, %op, "migration plan warning (no DDL)");
                warnings.push(format!("[Step {}] {}", step.step, msg));
                audit_step(config_pool, &ctx, step, "skipped", None).await;
                warned += 1;
            }
            MigrationSafety::Safe | MigrationSafety::BestEffort | MigrationSafety::Destructive => {
                let Some(ref sql) = step.ddl else {
                    continue;
                };
                let rendered = render_tenant_tokens(sql, false, dialect);
                let sql = &rendered;

                if let StepDecision::Skip(reason) = reconcile_step(step, &snapshot) {
                    tracing::info!(step = step.step, %op, %reason, "migration step already satisfied — skipping");
                    skips.push(format!(
                        "[Step {}] {} — {}",
                        step.step, step.description, reason
                    ));
                    audit_step(config_pool, &ctx, step, "skipped_exists", Some(&reason)).await;
                    skipped += 1;
                    continue;
                }

                tracing::info!(step = step.step, %op, %sql, "executing migration step");
                match sqlx::query(sql).execute(migration_pool).await {
                    Ok(_) => {
                        apply_step_to_snapshot(step, &mut snapshot);
                        audit_step(config_pool, &ctx, step, "applied", None).await;
                        applied += 1;
                    }
                    Err(e) => {
                        // The object already exists: the step's effect is in place, whatever the
                        // config diff believed. Record it and carry on.
                        if let Some(reason) = duplicate_object_reason(dialect, &e) {
                            tracing::info!(step = step.step, %op, %reason, "migration step already satisfied — skipping");
                            skips.push(format!(
                                "[Step {}] {} — {}",
                                step.step, step.description, reason
                            ));
                            audit_step(config_pool, &ctx, step, "skipped_exists", Some(&reason))
                                .await;
                            apply_step_to_snapshot(step, &mut snapshot);
                            skipped += 1;
                            continue;
                        }

                        let err_str = e.to_string();
                        if matches!(step.safety, MigrationSafety::BestEffort) {
                            tracing::warn!(step = step.step, %op, error = %e, "migration step failed (best-effort, continuing)");
                            warnings.push(format!(
                                "[Step {}] {} — Error: {}",
                                step.step, step.description, err_str
                            ));
                            audit_step(config_pool, &ctx, step, "warned", Some(&err_str)).await;
                            warned += 1;
                        } else {
                            audit_step(config_pool, &ctx, step, "failed", Some(&err_str)).await;
                            return Err(AppError::Db(e));
                        }
                    }
                }
            }
        }
    }

    Ok(MigrationExecutionResult {
        applied,
        warned,
        skipped,
        warnings,
        skips,
    })
}

/// Build CREATE TABLE DDL for the `{table}_history` companion table used by row versioning.
/// All source columns are replicated with their types but as nullable and without any constraints
/// (no NOT NULL, no UNIQUE, no FK, no CHECK). Five versioning metadata columns are prepended.
pub fn history_table_ddl(
    schema_name: &str,
    table_name: &str,
    pk_col: &str,
    source_cols: &[&ColumnConfig],
    dialect: &dyn Dialect,
) -> String {
    let history_name = format!("{}_history", table_name);
    let history_full = format!("{}.{}", quote(schema_name), quote(&history_name));

    let mut col_defs: Vec<String> = Vec::new();
    col_defs.push(format!(
        "{} {} NOT NULL DEFAULT {}",
        quote("_history_id"),
        "UUID",
        dialect.uuid_default_expr()
    ));
    col_defs.push(format!("{} BIGINT NOT NULL", quote("_version")));
    col_defs.push(format!("{} TEXT NOT NULL", quote("_operation")));
    col_defs.push(format!(
        "{} {} NOT NULL DEFAULT {}",
        quote("_recorded_at"),
        dialect.audit_timestamp_type(),
        dialect.now_fn()
    ));
    col_defs.push(format!(
        "{} {}",
        quote("_valid_from"),
        dialect.audit_timestamp_type()
    ));
    col_defs.push(format!(
        "{} {}",
        quote("_valid_to"),
        dialect.audit_timestamp_type()
    ));

    let config_col_names: HashSet<&str> = source_cols.iter().map(|c| c.name.as_str()).collect();
    for c in source_cols {
        let typ = dialect.ddl_type(&parse_canonical(&c.type_));
        col_defs.push(format!("{} {}", quote(&c.name), typ));
    }
    let audit_ts = dialect.audit_timestamp_type();
    for (name, typ) in [
        ("created_at", audit_ts),
        ("updated_at", audit_ts),
        ("archived_at", audit_ts),
        ("created_by", "TEXT"),
        ("updated_by", "TEXT"),
    ] {
        if !config_col_names.contains(name) {
            col_defs.push(format!("{} {}", quote(name), typ));
        }
    }
    col_defs.push(format!("PRIMARY KEY ({})", quote("_history_id")));

    let history_full_quoted = format!("{}.{}", quote(schema_name), quote(&history_name));
    let idx_sql = format!(
        "-- index: CREATE INDEX IF NOT EXISTS {} ON {} ({}, {})",
        quote(&format!("{}_history_{}_idx", table_name, pk_col)),
        history_full_quoted,
        quote(pk_col),
        quote("_version")
    );

    format!(
        "CREATE TABLE IF NOT EXISTS {} (\n  {}\n)\n{}",
        history_full,
        col_defs.join(",\n  "),
        idx_sql
    )
}

/// Build just the index DDL for the `{table}_history` table (separate from CREATE TABLE).
fn history_index_ddl(schema_name: &str, table_name: &str, pk_col: &str) -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS {} ON {}.{} ({}, {} DESC)",
        quote(&format!("{}_history_{}_idx", table_name, pk_col)),
        quote(schema_name),
        quote(&format!("{}_history", table_name)),
        quote(pk_col),
        quote("_version")
    )
}

/// A structural column change that must be mirrored onto companion (`_audit`/`_history`) tables.
enum CompanionColumnOp<'a> {
    /// A new column was added to the source table.
    Add { name: &'a str, ty: &'a str },
    /// A source column was renamed.
    Rename { old: &'a str, new: &'a str },
    /// A source column's type changed.
    AlterType { name: &'a str, ty: &'a str },
}

/// Companion-table suffixes that are enabled for a table (`audit`, `history`).
fn enabled_companion_suffixes(table: &TableConfig) -> Vec<&'static str> {
    let mut suffixes = Vec::new();
    if table.audit_log {
        suffixes.push("audit");
    }
    if table.versioning.as_ref().is_some_and(|v| v.enabled) {
        suffixes.push("history");
    }
    suffixes
}

/// Generate ALTER steps that keep the `{table}_audit` / `{table}_history` companion tables in
/// schema-sync with their source table when a column is added, renamed, or retyped.
///
/// Companion tables replicate source columns as **nullable with no constraints**, so only
/// structural changes propagate here. Nullability and default changes on the source column are
/// intentionally NOT mirrored — companion rows are historical snapshots that must stay nullable.
fn companion_column_steps(
    schema: &str,
    table: &TableConfig,
    op: &CompanionColumnOp<'_>,
    dialect: &dyn Dialect,
) -> Vec<MigrationStep> {
    let mut steps = Vec::new();
    for suffix in enabled_companion_suffixes(table) {
        let companion = format!("{}_{}", table.name, suffix);
        let full = format!("{}.{}", quote(schema), quote(&companion));
        let (operation, object, from_object, ddl, description, safety, risk, risk_detail) = match op
        {
            CompanionColumnOp::Add { name, ty } => (
                MigrationOperation::AddColumn,
                name.to_string(),
                None,
                // The IF NOT EXISTS guard (where supported) covers collisions with synthetic
                // columns the companion table may already carry (e.g. created_at/updated_by).
                add_column_ddl(dialect, &full, &format!("{} {}", quote(name), ty)),
                format!(
                    "Sync {} table: add column \"{}\" to \"{}\".\"{}\"",
                    suffix, name, schema, companion
                ),
                MigrationSafety::Safe,
                MigrationRisk::None,
                None,
            ),
            CompanionColumnOp::Rename { old, new } => (
                MigrationOperation::RenameColumn,
                new.to_string(),
                Some(old.to_string()),
                format!(
                    "ALTER TABLE {} RENAME COLUMN {} TO {}",
                    full,
                    quote(old),
                    quote(new)
                ),
                format!(
                    "Sync {} table: rename column \"{}\" → \"{}\" on \"{}\".\"{}\"",
                    suffix, old, new, schema, companion
                ),
                MigrationSafety::Safe,
                MigrationRisk::None,
                None,
            ),
            CompanionColumnOp::AlterType { name, ty } => (
                MigrationOperation::AlterColumnType,
                name.to_string(),
                None,
                format!(
                    "ALTER TABLE {} ALTER COLUMN {} TYPE {} USING {}::{}",
                    full,
                    quote(name),
                    ty,
                    quote(name),
                    ty
                ),
                format!(
                    "Sync {} table: change type of \"{}\".\"{}\".\"{}\" → {}",
                    suffix, schema, companion, name, ty
                ),
                MigrationSafety::BestEffort,
                MigrationRisk::MayFail,
                Some(format!(
                    "USING {}::{} cast may fail for incompatible values in the {} table.",
                    name, ty, suffix
                )),
            ),
        };
        steps.push(MigrationStep {
            step: 0,
            operation,
            schema: schema.to_string(),
            table: Some(companion.clone()),
            object,
            object_type: "column".into(),
            from_object,
            description,
            ddl: Some(ddl),
            safety,
            risk,
            risk_detail,
        });
    }
    steps
}

/// Build CREATE TABLE DDL for the `{table}_audit` companion table.
/// All source columns are replicated as nullable with no constraints, plus five audit metadata
/// columns prepended: audit_id (PK), audit_action, audit_at, audit_by, changed_fields.
fn audit_table_ddl(
    schema_name: &str,
    table_name: &str,
    source_cols: &[&ColumnConfig],
    dialect: &dyn Dialect,
) -> String {
    let audit_name = format!("{}_audit", table_name);
    let audit_full = format!("{}.{}", quote(schema_name), quote(&audit_name));

    let mut col_defs: Vec<String> = Vec::new();
    col_defs.push(format!(
        "{} {} NOT NULL DEFAULT {}",
        quote("audit_id"),
        "UUID",
        dialect.uuid_default_expr()
    ));
    col_defs.push(format!("{} TEXT NOT NULL", quote("audit_action")));
    col_defs.push(format!(
        "{} {} NOT NULL DEFAULT {}",
        quote("audit_at"),
        dialect.audit_timestamp_type(),
        dialect.now_fn()
    ));
    col_defs.push(format!("{} TEXT", quote("audit_by")));
    col_defs.push(format!(
        "{} {}",
        quote("changed_fields"),
        dialect.sys_json_type()
    ));

    let config_col_names: HashSet<&str> = source_cols.iter().map(|c| c.name.as_str()).collect();
    for c in source_cols {
        let typ = dialect.ddl_type(&parse_canonical(&c.type_));
        col_defs.push(format!("{} {}", quote(&c.name), typ));
    }
    let audit_ts = dialect.audit_timestamp_type();
    for (name, typ) in [
        ("created_at", audit_ts),
        ("updated_at", audit_ts),
        ("archived_at", audit_ts),
        ("created_by", "TEXT"),
        ("updated_by", "TEXT"),
    ] {
        if !config_col_names.contains(name) {
            col_defs.push(format!("{} {}", quote(name), typ));
        }
    }
    col_defs.push(format!("PRIMARY KEY ({})", quote("audit_id")));

    format!(
        "CREATE TABLE IF NOT EXISTS {} (\n  {}\n)",
        audit_full,
        col_defs.join(",\n  ")
    )
}

#[cfg(test)]
mod enum_recreate_tests {
    use super::*;

    fn schema(id: &str, name: &str) -> SchemaConfig {
        SchemaConfig {
            id: id.into(),
            name: name.into(),
            comment: None,
        }
    }

    fn table(id: &str, name: &str, schema_id: &str) -> TableConfig {
        TableConfig {
            id: id.into(),
            schema_id: Some(schema_id.into()),
            name: name.into(),
            comment: None,
            primary_key: PrimaryKeyConfig::Single("id".into()),
            unique: vec![],
            check: vec![],
            audit_log: false,
            versioning: None,
            global: false,
        }
    }

    fn col(id: &str, table_id: &str, name: &str, ty: &str, default: Option<&str>) -> ColumnConfig {
        ColumnConfig {
            id: id.into(),
            table_id: table_id.into(),
            name: name.into(),
            type_: ColumnTypeConfig::Simple(ty.into()),
            nullable: true,
            default: default.map(|d| ColumnDefaultConfig::Literal(d.into())),
            comment: None,
            asset: None,
            extensible: false,
            generated: None,
        }
    }

    fn enum_cfg(id: &str, name: &str, schema_id: &str, values: &[&str]) -> EnumConfig {
        EnumConfig {
            id: id.into(),
            schema_id: Some(schema_id.into()),
            name: name.into(),
            values: values.iter().map(|s| s.to_string()).collect(),
            comment: None,
        }
    }

    fn ddls(steps: &[MigrationStep]) -> Vec<String> {
        steps.iter().filter_map(|s| s.ddl.clone()).collect()
    }

    #[test]
    fn finds_scalar_and_array_dependent_columns() {
        let mut cfg = FullConfig::default();
        cfg.schemas = vec![schema("s1", "app")];
        cfg.tables = vec![table("t_orders", "orders", "s1")];
        cfg.columns = vec![
            col(
                "c1",
                "t_orders",
                "status",
                "order_status",
                Some("'pending'"),
            ),
            col("c2", "t_orders", "tags", "order_status[]", None),
            col("c3", "t_orders", "name", "text", None), // not an enum
        ];
        let e = enum_cfg("e1", "order_status", "s1", &["pending", "shipped"]);
        let new_tables: HashMap<&str, &TableConfig> =
            cfg.tables.iter().map(|t| (t.id.as_str(), t)).collect();
        let new_schemas: HashMap<&str, &SchemaConfig> =
            cfg.schemas.iter().map(|s| (s.id.as_str(), s)).collect();

        let deps = enum_dependent_columns(&e, &cfg, &new_tables, &new_schemas, None);
        assert_eq!(deps.len(), 2);
        let scalar = deps.iter().find(|d| d.column == "status").unwrap();
        assert_eq!(scalar.schema, "app");
        assert_eq!(scalar.table, "orders");
        assert!(!scalar.is_array);
        assert_eq!(scalar.default.as_deref(), Some("'pending'"));
        let arr = deps.iter().find(|d| d.column == "tags").unwrap();
        assert!(arr.is_array);
        assert!(arr.default.is_none());
    }

    #[test]
    fn recreate_sequence_emits_rename_create_recast_drop() {
        let e = enum_cfg("e1", "order_status", "s1", &["pending", "shipped"]);
        let deps = vec![
            EnumColumnRef {
                schema: "app".into(),
                table: "orders".into(),
                column: "status".into(),
                default: Some("'pending'".into()),
                is_array: false,
            },
            EnumColumnRef {
                schema: "app".into(),
                table: "orders".into(),
                column: "tags".into(),
                default: None,
                is_array: true,
            },
        ];
        let mut steps = Vec::new();
        recreate_enum_steps(&mut steps, "app", &e, &["cancelled"], &deps);
        let sql = ddls(&steps);

        // Leading informational step carries no DDL.
        assert!(matches!(steps[0].safety, MigrationSafety::WarnOnly));
        assert!(steps[0].ddl.is_none());

        // Rename → create → (drop default, recast, set default) → recast array → drop old.
        assert_eq!(
            sql[0],
            r#"ALTER TYPE "app"."order_status" RENAME TO "order_status__arch_old""#
        );
        assert_eq!(
            sql[1],
            r#"CREATE TYPE "app"."order_status" AS ENUM ('pending', 'shipped')"#
        );
        assert_eq!(
            sql[2],
            r#"ALTER TABLE "app"."orders" ALTER COLUMN "status" DROP DEFAULT"#
        );
        assert_eq!(
            sql[3],
            r#"ALTER TABLE "app"."orders" ALTER COLUMN "status" TYPE "app"."order_status" USING "status"::text::"app"."order_status""#
        );
        assert_eq!(
            sql[4],
            r#"ALTER TABLE "app"."orders" ALTER COLUMN "status" SET DEFAULT 'pending'"#
        );
        // Array column: no default, single recast with array casts.
        assert_eq!(
            sql[5],
            r#"ALTER TABLE "app"."orders" ALTER COLUMN "tags" TYPE "app"."order_status"[] USING "tags"::text[]::"app"."order_status"[]"#
        );
        assert_eq!(
            *sql.last().unwrap(),
            r#"DROP TYPE IF EXISTS "app"."order_status__arch_old""#
        );
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod companion_sync_tests {
    use super::*;
    use crate::db::sqlite::SqliteDialect;

    fn schema(id: &str, name: &str) -> SchemaConfig {
        SchemaConfig {
            id: id.into(),
            name: name.into(),
            comment: None,
        }
    }

    fn table(id: &str, name: &str, audit: bool, versioning: bool) -> TableConfig {
        TableConfig {
            id: id.into(),
            schema_id: Some("s1".into()),
            name: name.into(),
            comment: None,
            primary_key: PrimaryKeyConfig::Single("id".into()),
            unique: vec![],
            check: vec![],
            audit_log: audit,
            versioning: versioning.then(|| VersioningConfig {
                enabled: true,
                keep_versions: None,
            }),
            global: false,
        }
    }

    fn col(id: &str, name: &str, ty: &str) -> ColumnConfig {
        ColumnConfig {
            id: id.into(),
            table_id: "t1".into(),
            name: name.into(),
            type_: ColumnTypeConfig::Simple(ty.into()),
            nullable: true,
            default: None,
            comment: None,
            asset: None,
            extensible: false,
            generated: None,
        }
    }

    fn base(audit: bool, versioning: bool) -> FullConfig {
        let mut cfg = FullConfig::default();
        cfg.schemas = vec![schema("s1", "app")];
        cfg.tables = vec![table("t1", "orders", audit, versioning)];
        cfg.columns = vec![col("c0", "id", "uuid"), col("c1", "status", "text")];
        cfg
    }

    fn plan(old: &FullConfig, new: &FullConfig) -> Vec<String> {
        let dialect = SqliteDialect;
        compute_migration_plan(old, new, None, None, &dialect, &HashMap::new())
            .unwrap()
            .steps
            .into_iter()
            .filter_map(|s| s.ddl)
            .collect()
    }

    #[test]
    fn add_column_syncs_audit_and_history() {
        let old = base(true, true);
        let mut new = base(true, true);
        new.columns.push(col("c2", "note", "text"));

        // SQLite has no ADD COLUMN IF NOT EXISTS, so the guard must not appear here.
        let sql = plan(&old, &new);
        assert!(sql
            .iter()
            .any(|s| s == r#"ALTER TABLE "app"."orders" ADD COLUMN "note" TEXT"#));
        assert!(sql
            .iter()
            .any(|s| s == r#"ALTER TABLE "app"."orders_audit" ADD COLUMN "note" TEXT"#));
        assert!(sql
            .iter()
            .any(|s| s == r#"ALTER TABLE "app"."orders_history" ADD COLUMN "note" TEXT"#));
    }

    #[test]
    fn rename_column_syncs_companions() {
        let old = base(true, true);
        let mut new = base(true, true);
        new.columns[1].name = "state".into();

        let sql = plan(&old, &new);
        assert!(sql
            .iter()
            .any(|s| s == r#"ALTER TABLE "app"."orders_audit" RENAME COLUMN "status" TO "state""#));
        assert!(sql.iter().any(
            |s| s == r#"ALTER TABLE "app"."orders_history" RENAME COLUMN "status" TO "state""#
        ));
    }

    #[test]
    fn alter_type_syncs_companions() {
        let old = base(true, false);
        let mut new = base(true, false);
        new.columns[1].type_ = ColumnTypeConfig::Simple("integer".into());

        let sql = plan(&old, &new);
        assert!(sql.iter().any(|s| s
            == r#"ALTER TABLE "app"."orders_audit" ALTER COLUMN "status" TYPE INTEGER USING "status"::INTEGER"#));
        // versioning disabled → no history sync
        assert!(!sql.iter().any(|s| s.contains("orders_history")));
    }

    #[test]
    fn no_companion_steps_when_features_disabled() {
        let old = base(false, false);
        let mut new = base(false, false);
        new.columns.push(col("c2", "note", "text"));

        let sql = plan(&old, &new);
        assert!(sql
            .iter()
            .any(|s| s == r#"ALTER TABLE "app"."orders" ADD COLUMN "note" TEXT"#));
        assert!(!sql.iter().any(|s| s.contains("orders_audit")));
        assert!(!sql.iter().any(|s| s.contains("orders_history")));
    }

    #[test]
    fn nullability_change_does_not_touch_companions() {
        let old = base(true, true);
        let mut new = base(true, true);
        new.columns[1].nullable = false;

        let sql = plan(&old, &new);
        // Main table gets SET NOT NULL, companions are untouched (snapshots stay nullable).
        assert!(sql.iter().any(|s| s.contains("SET NOT NULL")));
        assert!(!sql.iter().any(|s| s.contains("orders_audit")));
        assert!(!sql.iter().any(|s| s.contains("orders_history")));
    }

    #[test]
    fn dropped_column_is_a_destructive_step_with_real_ddl() {
        // A column removed from config now emits an executable DROP COLUMN gated behind
        // `confirm_destructive`, instead of the old warn-only-with-no-DDL step.
        let old = base(false, false);
        let mut new = base(false, false);
        new.columns.retain(|c| c.name != "status"); // drop "status"

        let dialect = SqliteDialect;
        let steps = compute_migration_plan(&old, &new, None, None, &dialect, &HashMap::new())
            .unwrap()
            .steps;
        let drop = steps
            .iter()
            .find(|s| matches!(s.operation, MigrationOperation::DropColumn))
            .expect("expected a DropColumn step");

        assert!(matches!(drop.safety, MigrationSafety::Destructive));
        assert!(matches!(drop.risk, MigrationRisk::DataWillBeDropped));
        assert_eq!(
            drop.ddl.as_deref(),
            Some(r#"ALTER TABLE "app"."orders" DROP COLUMN IF EXISTS "status""#)
        );
    }
}

#[cfg(test)]
mod reconcile_tests {
    use super::*;

    fn snapshot() -> DbSnapshot {
        let mut snap = DbSnapshot::default();
        snap.introspected = true;
        snap.indexes_known = true;
        snap.constraints_known = true;
        snap
    }

    fn facts(nullable: bool, has_default: bool) -> ColumnFacts {
        ColumnFacts {
            data_type: "text".into(),
            nullable,
            has_default,
        }
    }

    fn step(op: MigrationOperation, table: Option<&str>, object: &str) -> MigrationStep {
        MigrationStep {
            step: 1,
            operation: op,
            schema: "app".into(),
            table: table.map(String::from),
            object: object.into(),
            object_type: "column".into(),
            from_object: None,
            description: "test step".into(),
            ddl: Some("SELECT 1".into()),
            safety: MigrationSafety::Safe,
            risk: MigrationRisk::None,
            risk_detail: None,
        }
    }

    fn skipped(d: StepDecision) -> bool {
        matches!(d, StepDecision::Skip(_))
    }

    #[test]
    fn add_column_is_skipped_when_the_column_already_exists() {
        // The regression this whole path exists for: an upgrade plan that adds a column the
        // physical table already has must not fail the migration.
        let mut snap = snapshot();
        snap.add_column("app", "transport_units", "project_id", facts(true, false));

        let s = step(
            MigrationOperation::AddColumn,
            Some("transport_units"),
            "project_id",
        );
        assert!(skipped(reconcile_step(&s, &snap)));
    }

    #[test]
    fn add_column_runs_when_the_column_is_absent() {
        let mut snap = snapshot();
        snap.add_column("app", "transport_units", "id", facts(false, false));

        let s = step(
            MigrationOperation::AddColumn,
            Some("transport_units"),
            "project_id",
        );
        assert_eq!(reconcile_step(&s, &snap), StepDecision::Execute);
    }

    #[test]
    fn nothing_is_skipped_when_introspection_produced_nothing() {
        // A failed or empty introspection must never be read as "the object does not exist".
        let snap = DbSnapshot::default();
        for op in [
            MigrationOperation::AddColumn,
            MigrationOperation::SetNotNull,
            MigrationOperation::CreateIndex,
        ] {
            let s = step(op, Some("orders"), "note");
            assert_eq!(reconcile_step(&s, &snap), StepDecision::Execute);
        }
    }

    #[test]
    fn create_table_is_skipped_when_the_table_exists() {
        let mut snap = snapshot();
        snap.add_column("app", "orders", "id", facts(false, false));

        let s = step(MigrationOperation::CreateTable, Some("orders"), "orders");
        assert!(skipped(reconcile_step(&s, &snap)));
    }

    #[test]
    fn rename_is_skipped_once_it_has_been_applied() {
        let mut snap = snapshot();
        snap.add_column("app", "orders", "state", facts(true, false));

        let mut s = step(MigrationOperation::RenameColumn, Some("orders"), "state");
        s.from_object = Some("status".into());
        assert!(skipped(reconcile_step(&s, &snap)));
    }

    #[test]
    fn rename_runs_while_the_old_column_is_still_there() {
        let mut snap = snapshot();
        snap.add_column("app", "orders", "status", facts(true, false));

        let mut s = step(MigrationOperation::RenameColumn, Some("orders"), "state");
        s.from_object = Some("status".into());
        assert_eq!(reconcile_step(&s, &snap), StepDecision::Execute);
    }

    #[test]
    fn rename_is_skipped_when_neither_name_exists() {
        let mut snap = snapshot();
        snap.add_column("app", "orders", "id", facts(false, false));

        let mut s = step(MigrationOperation::RenameColumn, Some("orders"), "state");
        s.from_object = Some("status".into());
        assert!(skipped(reconcile_step(&s, &snap)));
    }

    #[test]
    fn nullability_and_default_steps_respect_the_current_column_state() {
        let mut snap = snapshot();
        snap.add_column("app", "orders", "not_null_col", facts(false, false));
        snap.add_column("app", "orders", "nullable_col", facts(true, true));

        assert!(skipped(reconcile_step(
            &step(
                MigrationOperation::SetNotNull,
                Some("orders"),
                "not_null_col"
            ),
            &snap
        )));
        assert_eq!(
            reconcile_step(
                &step(
                    MigrationOperation::SetNotNull,
                    Some("orders"),
                    "nullable_col"
                ),
                &snap
            ),
            StepDecision::Execute
        );
        assert!(skipped(reconcile_step(
            &step(
                MigrationOperation::DropNotNull,
                Some("orders"),
                "nullable_col"
            ),
            &snap
        )));
        assert!(skipped(reconcile_step(
            &step(
                MigrationOperation::DropDefault,
                Some("orders"),
                "not_null_col"
            ),
            &snap
        )));
        assert_eq!(
            reconcile_step(
                &step(
                    MigrationOperation::DropDefault,
                    Some("orders"),
                    "nullable_col"
                ),
                &snap
            ),
            StepDecision::Execute
        );
    }

    #[test]
    fn column_steps_are_skipped_when_the_column_is_gone_from_a_known_table() {
        let mut snap = snapshot();
        snap.add_column("app", "orders", "id", facts(false, false));

        for op in [
            MigrationOperation::AlterColumnType,
            MigrationOperation::SetDefault,
            MigrationOperation::SetNotNull,
            MigrationOperation::BackfillNulls,
        ] {
            assert!(
                skipped(reconcile_step(&step(op, Some("orders"), "dropped"), &snap)),
                "expected a skip for a column that no longer exists"
            );
        }
    }

    #[test]
    fn index_and_foreign_key_steps_are_skipped_when_the_object_exists() {
        let mut snap = snapshot();
        snap.add_column("app", "orders", "user_id", facts(true, false));
        snap.add_index("app", "orders_user_idx");
        snap.add_constraint("app", "orders", "fk_orders_user");

        let mut idx = step(
            MigrationOperation::CreateIndex,
            Some("orders"),
            "orders_user_idx",
        );
        idx.object_type = "index".into();
        assert!(skipped(reconcile_step(&idx, &snap)));

        let mut fk = step(
            MigrationOperation::AddForeignKey,
            Some("orders"),
            "fk_orders_user",
        );
        fk.object_type = "foreign_key".into();
        assert!(skipped(reconcile_step(&fk, &snap)));
    }

    #[test]
    fn index_steps_run_when_the_dialect_cannot_report_indexes() {
        let mut snap = snapshot();
        snap.indexes_known = false;
        snap.add_column("app", "orders", "user_id", facts(true, false));

        let s = step(
            MigrationOperation::CreateIndex,
            Some("orders"),
            "orders_user_idx",
        );
        assert_eq!(reconcile_step(&s, &snap), StepDecision::Execute);
    }

    #[test]
    fn executed_steps_are_folded_into_the_snapshot() {
        // Later steps must see what earlier ones did, or a plan that adds a column and then
        // constrains it would skip the second half.
        let mut snap = snapshot();
        snap.add_table("app", "orders");

        let mut add = step(MigrationOperation::AddColumn, Some("orders"), "note");
        add.ddl = Some(r#"ALTER TABLE "app"."orders" ADD COLUMN "note" TEXT NOT NULL"#.into());
        apply_step_to_snapshot(&add, &mut snap);
        assert!(snap.has_column("app", "orders", "note"));

        // Re-running the same step is now a no-op.
        assert!(skipped(reconcile_step(&add, &snap)));

        // …and the recorded nullability matches the DDL that was executed.
        assert!(skipped(reconcile_step(
            &step(MigrationOperation::SetNotNull, Some("orders"), "note"),
            &snap
        )));
    }

    #[test]
    fn drops_and_enum_steps_are_never_skipped() {
        let mut snap = snapshot();
        snap.add_column("app", "orders", "note", facts(true, false));

        for op in [
            MigrationOperation::DropIndex,
            MigrationOperation::DropForeignKey,
            MigrationOperation::CreateEnum,
            MigrationOperation::AddEnumValue,
        ] {
            assert_eq!(
                reconcile_step(&step(op, Some("orders"), "note"), &snap),
                StepDecision::Execute
            );
        }
    }

    #[test]
    fn drop_column_runs_when_present_and_is_skipped_once_gone() {
        // Column present on a known table: the drop must run.
        let mut snap = snapshot();
        snap.add_table("app", "orders");
        snap.add_column("app", "orders", "note", facts(true, false));
        let s = step(MigrationOperation::DropColumn, Some("orders"), "note");
        assert_eq!(reconcile_step(&s, &snap), StepDecision::Execute);

        // After the drop folds into the snapshot, re-running is a no-op.
        apply_step_to_snapshot(&s, &mut snap);
        assert!(!snap.has_column("app", "orders", "note"));
        assert!(skipped(reconcile_step(&s, &snap)));
    }
}

#[cfg(all(test, feature = "postgres"))]
mod postgres_idempotent_ddl_tests {
    use super::*;
    use crate::db::postgres::PostgresDialect;

    #[test]
    fn add_column_carries_an_if_not_exists_guard() {
        let ddl = add_column_ddl(&PostgresDialect, r#""app"."orders""#, r#""note" TEXT"#);
        assert_eq!(
            ddl,
            r#"ALTER TABLE "app"."orders" ADD COLUMN IF NOT EXISTS "note" TEXT"#
        );
    }

    #[test]
    fn duplicate_object_sqlstates_are_recognised() {
        let d = PostgresDialect;
        // duplicate_column — what "column already exists" reports.
        assert!(d.is_duplicate_object_code("42701"));
        // duplicate_table / duplicate_object.
        assert!(d.is_duplicate_object_code("42P07"));
        assert!(d.is_duplicate_object_code("42710"));
        // undefined_column must still be a real failure.
        assert!(!d.is_duplicate_object_code("42703"));
    }
}

#[cfg(test)]
mod uninstall_guard_tests {
    use super::*;

    fn config() -> FullConfig {
        let table =
            |id: &str, name: &str, schema_id: &str, audit: bool, versioning: bool| TableConfig {
                id: id.into(),
                schema_id: Some(schema_id.into()),
                name: name.into(),
                comment: None,
                primary_key: PrimaryKeyConfig::Single("id".into()),
                unique: vec![],
                check: vec![],
                audit_log: audit,
                versioning: versioning.then_some(VersioningConfig {
                    enabled: true,
                    keep_versions: None,
                }),
                global: false,
            };
        FullConfig {
            schemas: vec![
                SchemaConfig {
                    id: "s1".into(),
                    name: "ecommerce".into(),
                    comment: None,
                },
                SchemaConfig {
                    id: "s2".into(),
                    name: "public".into(),
                    comment: None,
                },
            ],
            tables: vec![
                table("t1", "orders", "s1", true, true),
                table("t2", "settings", "s2", false, false),
            ],
            ..Default::default()
        }
    }

    fn view(view_schema: &str, view_name: &str, table: &str) -> DependentView {
        DependentView {
            view_schema: view_schema.into(),
            view_name: view_name.into(),
            table_schema: "ecommerce".into(),
            table_name: table.into(),
            column: None,
        }
    }

    fn pair(s: &str, t: &str) -> (String, String) {
        (s.into(), t.into())
    }

    #[test]
    fn physical_tables_include_companions_in_drop_order() {
        assert_eq!(
            package_physical_tables(&config(), None).unwrap(),
            vec![
                pair("ecommerce", "orders_audit"),
                pair("ecommerce", "orders_history"),
                pair("ecommerce", "orders"),
                pair("public", "settings"),
            ]
        );
    }

    #[test]
    fn physical_tables_follow_schema_override() {
        let tables = package_physical_tables(&config(), Some("tenant_x")).unwrap();
        assert!(tables.iter().all(|(s, _)| s == "tenant_x"));
        assert_eq!(tables.len(), 4);
    }

    #[test]
    fn dropped_schemas_skip_public_and_override() {
        assert_eq!(package_dropped_schemas(&config(), None), vec!["ecommerce"]);
        assert!(package_dropped_schemas(&config(), Some("tenant_x")).is_empty());
    }

    #[test]
    fn views_inside_dropped_schemas_are_not_external() {
        let found = vec![
            view("an_sales", "sales_daily", "orders"),
            view("ECOMMERCE", "v_orders", "orders"),
            view("an_sales", "sales_daily", "orders"),
            view("an_finance", "revenue", "orders"),
        ];
        assert_eq!(
            external_dependent_views(found, &["ecommerce".into()], &[]),
            vec![
                view("an_finance", "revenue", "orders"),
                view("an_sales", "sales_daily", "orders"),
            ]
        );
    }

    #[test]
    fn views_in_shared_schemas_are_external() {
        // With a schema override nothing is dropped whole, so every dependent view counts.
        let found = vec![view("tenant_x", "v_orders", "orders")];
        assert_eq!(external_dependent_views(found.clone(), &[], &[]), found);
    }

    #[test]
    fn the_packages_own_views_are_not_external() {
        let found = vec![
            view("tenant_x", "v_orders", "orders"),
            view("tenant_x", "other", "orders"),
        ];
        assert_eq!(
            external_dependent_views(found, &[], &["tenant_x.v_orders".into()]),
            vec![view("tenant_x", "other", "orders")]
        );
    }

    #[test]
    fn only_views_reading_a_changed_column_block_an_upgrade() {
        let col = |view_name: &str, c: Option<&str>| DependentView {
            column: c.map(String::from),
            ..view("an_sales", view_name, "orders")
        };
        let found = vec![
            col("by_total", Some("TOTAL")),
            col("by_region", Some("region")),
            col("counts", None),
            col("own", Some("total")),
        ];
        let changed = vec![("ecommerce".into(), "orders".into(), "total".into())];
        assert_eq!(
            views_reading_changed_columns(found, &changed, &["an_sales.own".into()]),
            vec![col("by_total", Some("TOTAL"))]
        );
    }

    #[test]
    fn dependent_view_display_names_the_column_when_known() {
        let v = DependentView {
            column: Some("total".into()),
            ..view("an_sales", "sales_daily", "orders")
        };
        assert_eq!(
            v.to_string(),
            "an_sales.sales_daily (reads ecommerce.orders.total)"
        );
    }

    #[test]
    fn dependent_view_display_names_view_and_table() {
        assert_eq!(
            view("an_sales", "sales_daily", "orders").to_string(),
            "an_sales.sales_daily (reads ecommerce.orders)"
        );
    }
}

#[cfg(test)]
mod view_tests {
    use super::*;

    fn view(id: &str, name: &str, sql: &str, reads: &[&str]) -> ViewConfig {
        ViewConfig {
            id: id.into(),
            schema_id: Some("s1".into()),
            name: name.into(),
            comment: None,
            sql: sql.into(),
            sql_by_dialect: HashMap::new(),
            columns: vec![ViewColumnConfig {
                name: "id".into(),
                type_: ColumnTypeConfig::Simple("int".into()),
                nullable: false,
                comment: None,
            }],
            primary_key: Some(PrimaryKeyConfig::Single("id".into())),
            reads: reads.iter().map(|r| r.to_string()).collect(),
            materialized: false,
            refresh: None,
        }
    }

    fn config(views: Vec<ViewConfig>) -> FullConfig {
        FullConfig {
            schemas: vec![SchemaConfig {
                id: "s1".into(),
                name: "an".into(),
                comment: None,
            }],
            views,
            ..Default::default()
        }
    }

    fn table_step(schema: &str, table: &str) -> MigrationStep {
        MigrationStep {
            step: 0,
            operation: MigrationOperation::AddColumn,
            schema: schema.into(),
            table: Some(table.into()),
            object: "c".into(),
            from_object: None,
            object_type: "column".into(),
            description: String::new(),
            ddl: Some("ALTER TABLE …".into()),
            safety: MigrationSafety::Safe,
            risk: MigrationRisk::None,
            risk_detail: None,
        }
    }

    fn names(steps: &[MigrationStep]) -> Vec<String> {
        steps
            .iter()
            .map(|s| format!("{}:{}", s.operation, s.object))
            .collect()
    }

    fn plan(
        old: &FullConfig,
        new: &FullConfig,
        table_steps: &[MigrationStep],
    ) -> (Vec<String>, Vec<String>) {
        let dialect = crate::db::active_dialect();
        let (d, c) = view_plan_steps(old, new, table_steps, None, dialect.as_ref()).unwrap();
        (names(&d), names(&c))
    }

    #[test]
    fn views_are_ordered_by_reads() {
        let cfg = config(vec![
            view("v_top", "top", "SELECT 1", &["an.mid"]),
            view("v_mid", "mid", "SELECT 1", &["an.base"]),
            view("v_base", "base", "SELECT 1", &["shop.orders"]),
        ]);
        let dialect = crate::db::active_dialect();
        let order: Vec<&str> = ordered_views(&cfg, None, dialect.as_ref())
            .unwrap()
            .iter()
            .map(|v| v.view.id.as_str())
            .collect();
        assert_eq!(order, vec!["v_base", "v_mid", "v_top"]);
    }

    #[test]
    fn view_cycles_are_rejected() {
        let cfg = config(vec![
            view("v_a", "a", "SELECT 1", &["an.b"]),
            view("v_b", "b", "SELECT 1", &["AN.A"]),
        ]);
        let dialect = crate::db::active_dialect();
        assert!(ordered_views(&cfg, None, dialect.as_ref()).is_err());
    }

    #[test]
    fn new_views_are_created_and_removed_views_dropped() {
        let old = config(vec![view("v_old", "old", "SELECT 1", &[])]);
        let new = config(vec![view("v_new", "new", "SELECT 1", &[])]);
        assert_eq!(
            plan(&old, &new, &[]),
            (vec!["drop_view:old".into()], vec!["create_view:new".into()])
        );
    }

    #[test]
    fn unchanged_views_are_left_alone() {
        let cfg = config(vec![view("v", "v", "SELECT 1", &["shop.orders"])]);
        assert_eq!(plan(&cfg, &cfg, &[]), (vec![], vec![]));
        // A table step on a relation the view does not read leaves it alone too.
        assert_eq!(
            plan(&cfg, &cfg, &[table_step("shop", "customers")]),
            (vec![], vec![])
        );
    }

    #[test]
    fn changed_body_rebuilds_the_view() {
        let old = config(vec![view("v", "v", "SELECT 1", &[])]);
        let new = config(vec![view("v", "v", "SELECT 2", &[])]);
        assert_eq!(
            plan(&old, &new, &[]),
            (vec!["drop_view:v".into()], vec!["create_view:v".into()])
        );
    }

    #[test]
    fn touched_tables_rebuild_their_readers_and_dependent_views() {
        let cfg = config(vec![
            view("v_base", "base", "SELECT 1", &["shop.orders"]),
            view("v_top", "top", "SELECT 1", &["an.base"]),
            view("v_other", "other", "SELECT 1", &["shop.customers"]),
        ]);
        assert_eq!(
            plan(&cfg, &cfg, &[table_step("SHOP", "Orders")]),
            (
                // Dependents drop before what they read; creation runs the other way.
                vec!["drop_view:top".into(), "drop_view:base".into()],
                vec!["create_view:base".into(), "create_view:top".into()]
            )
        );
    }

    #[test]
    fn views_without_reads_rebuild_on_any_table_step() {
        let cfg = config(vec![view("v", "v", "SELECT 1", &[])]);
        assert_eq!(
            plan(&cfg, &cfg, &[table_step("shop", "anything")]),
            (vec!["drop_view:v".into()], vec!["create_view:v".into()])
        );
    }

    #[test]
    fn plan_wraps_table_steps_with_view_steps() {
        let old = config(vec![view("v", "v", "SELECT 1", &[])]);
        let new = config(vec![view("v", "v", "SELECT 2", &[])]);
        let dialect = crate::db::active_dialect();
        let p = compute_migration_plan(&old, &new, None, None, dialect.as_ref(), &HashMap::new())
            .unwrap();
        assert_eq!(names(&p.steps), vec!["drop_view:v", "create_view:v"]);
        assert_eq!(
            p.steps.iter().map(|s| s.step).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }
}

#[cfg(test)]
mod generated_column_tests {
    use super::*;
    #[cfg(feature = "sqlite")]
    use crate::db::sqlite::SqliteDialect;

    fn col(id: &str, name: &str, ty: &str, generated: Option<&str>) -> ColumnConfig {
        ColumnConfig {
            id: id.into(),
            table_id: "t1".into(),
            name: name.into(),
            type_: ColumnTypeConfig::Simple(ty.into()),
            nullable: true,
            default: None,
            comment: None,
            asset: None,
            extensible: false,
            generated: generated.map(|e| GeneratedColumnConfig {
                expression: e.into(),
                expression_by_dialect: Default::default(),
            }),
        }
    }

    fn config(columns: Vec<ColumnConfig>) -> FullConfig {
        FullConfig {
            schemas: vec![SchemaConfig {
                id: "s1".into(),
                name: "app".into(),
                comment: None,
            }],
            tables: vec![TableConfig {
                id: "t1".into(),
                schema_id: Some("s1".into()),
                name: "lines".into(),
                comment: None,
                primary_key: PrimaryKeyConfig::Single("id".into()),
                unique: vec![],
                check: vec![],
                audit_log: false,
                versioning: None,
                global: false,
            }],
            columns,
            ..Default::default()
        }
    }

    /// Same schema, no tables — so the plan creates the table.
    #[cfg(feature = "sqlite")]
    fn empty() -> FullConfig {
        let mut cfg = config(vec![]);
        cfg.tables.clear();
        cfg
    }

    fn base(total: Option<&str>) -> FullConfig {
        config(vec![
            col("c0", "id", "uuid", None),
            col("c1", "qty", "integer", None),
            col("c2", "price", "integer", None),
            col("c3", "total", "integer", total),
        ])
    }

    fn steps(old: &FullConfig, new: &FullConfig, dialect: &dyn Dialect) -> Vec<MigrationStep> {
        compute_migration_plan(old, new, None, None, dialect, &HashMap::new())
            .unwrap()
            .steps
    }

    #[cfg(feature = "sqlite")]
    fn ddls(steps: &[MigrationStep]) -> Vec<&str> {
        steps.iter().filter_map(|s| s.ddl.as_deref()).collect()
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn create_table_emits_stored_generated_column() {
        let s = steps(&empty(), &base(Some("qty * price")), &SqliteDialect);
        assert!(
            ddls(&s)
                .iter()
                .any(|d| d.contains(r#""total" INTEGER GENERATED ALWAYS AS (qty * price) STORED"#)),
            "{:?}",
            ddls(&s)
        );
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn generated_clause_precedes_not_null() {
        // MySQL only accepts the generated clause directly after the type.
        let mut c = col("c3", "total", "integer", Some("qty * price"));
        c.nullable = false;
        let def = column_def_suffix(&c, &SqliteDialect, false);
        assert!(
            def.ends_with(" GENERATED ALWAYS AS (qty * price) STORED NOT NULL"),
            "{}",
            def
        );
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn add_column_on_sqlite_is_virtual() {
        let mut old = base(None);
        old.columns.pop();
        let s = steps(&old, &base(Some("qty * price")), &SqliteDialect);
        let add = s
            .iter()
            .find(|s| matches!(s.operation, MigrationOperation::AddColumn))
            .and_then(|s| s.ddl.clone())
            .unwrap();
        assert!(
            add.ends_with("GENERATED ALWAYS AS (qty * price) VIRTUAL"),
            "{}",
            add
        );
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn dialect_override_wins() {
        let mut new = base(Some("qty * price"));
        new.columns[3]
            .generated
            .as_mut()
            .unwrap()
            .expression_by_dialect
            .insert("sqlite".into(), "qty * price * 1".into());
        let s = steps(&empty(), &new, &SqliteDialect);
        assert!(ddls(&s)
            .iter()
            .any(|d| d.contains("AS (qty * price * 1) STORED")));
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn unchanged_expression_emits_nothing() {
        let s = steps(
            &base(Some("qty * price")),
            &base(Some("qty * price")),
            &SqliteDialect,
        );
        assert!(s.is_empty());
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_expression_changes_are_manual() {
        for (old, new) in [
            (Some("qty * price"), Some("qty * price * 2")),
            (Some("qty * price"), None),
            (None, Some("qty * price")),
        ] {
            let s = steps(&base(old), &base(new), &SqliteDialect);
            assert_eq!(s.len(), 1, "{:?} → {:?}", old, new);
            assert!(s[0].ddl.is_none());
            assert!(matches!(s[0].risk, MigrationRisk::ManualActionRequired));
        }
    }

    #[test]
    fn generated_step_on_missing_column_is_skipped() {
        let step = MigrationStep {
            step: 0,
            operation: MigrationOperation::SetGeneratedExpression,
            schema: "app".into(),
            table: Some("lines".into()),
            object: "total".into(),
            object_type: "column".into(),
            from_object: None,
            description: String::new(),
            ddl: Some("ALTER TABLE …".into()),
            safety: MigrationSafety::BestEffort,
            risk: MigrationRisk::MayFail,
            risk_detail: None,
        };
        let mut snap = DbSnapshot::default();
        snap.introspected = true;
        snap.add_table("app", "lines");
        assert!(matches!(
            reconcile_step(&step, &snap),
            StepDecision::Skip(_)
        ));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_changes_expression_in_place() {
        use crate::db::postgres::PostgresDialect;
        let s = steps(
            &base(Some("qty * price")),
            &base(Some("qty * price * 2")),
            &PostgresDialect,
        );
        assert!(matches!(
            s[0].operation,
            MigrationOperation::SetGeneratedExpression
        ));
        assert_eq!(
            s[0].ddl.as_deref(),
            Some(
                r#"ALTER TABLE "app"."lines" ALTER COLUMN "total" SET EXPRESSION AS (qty * price * 2)"#
            )
        );
        let s = steps(&base(Some("qty * price")), &base(None), &PostgresDialect);
        assert_eq!(
            s[0].ddl.as_deref(),
            Some(r#"ALTER TABLE "app"."lines" ALTER COLUMN "total" DROP EXPRESSION"#)
        );
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_changes_expression_with_modify_column() {
        use crate::db::mysql::MySqlDialect;
        let s = steps(
            &base(Some("qty * price")),
            &base(Some("qty * price * 2")),
            &MySqlDialect,
        );
        let ddl = s[0].ddl.as_deref().unwrap();
        assert!(ddl.contains(r#"MODIFY COLUMN "total""#), "{}", ddl);
        assert!(
            ddl.ends_with("GENERATED ALWAYS AS (qty * price * 2) STORED"),
            "{}",
            ddl
        );
    }
}

#[cfg(test)]
mod materialized_view_tests {
    use super::*;

    fn mv() -> ViewConfig {
        ViewConfig {
            id: "v_sales".into(),
            schema_id: Some("s1".into()),
            name: "sales".into(),
            comment: None,
            sql: "SELECT {{tenant_id:o}} AS tenant_id, o.region, COUNT(*) AS n FROM shop.orders o \
                  GROUP BY {{ tenant_id : o }}, o.region"
                .into(),
            sql_by_dialect: HashMap::new(),
            columns: vec![
                ViewColumnConfig {
                    name: "region".into(),
                    type_: ColumnTypeConfig::Simple("text".into()),
                    nullable: true,
                    comment: None,
                },
                ViewColumnConfig {
                    name: "n".into(),
                    type_: ColumnTypeConfig::Simple("bigint".into()),
                    nullable: true,
                    comment: None,
                },
            ],
            primary_key: Some(PrimaryKeyConfig::Single("region".into())),
            reads: vec!["shop.orders".into()],
            materialized: true,
            refresh: None,
        }
    }

    fn config(v: ViewConfig) -> FullConfig {
        FullConfig {
            schemas: vec![SchemaConfig {
                id: "s1".into(),
                name: "an".into(),
                comment: None,
            }],
            views: vec![v],
            ..Default::default()
        }
    }

    fn statements(f: impl Fn(&ViewDdl<'_>) -> Vec<(&'static str, String)>) -> Vec<String> {
        let cfg = config(mv());
        let dialect = crate::db::active_dialect();
        let views = ordered_views(&cfg, None, dialect.as_ref()).unwrap();
        f(&views[0]).into_iter().map(|(_, s)| s).collect()
    }

    #[test]
    fn tenant_tokens_expand_per_target() {
        let dialect = crate::db::active_dialect();
        let sql = "SELECT {{tenant_id:o}}, {{ tenant_id }}, {{tenant_key}}x";
        assert_eq!(
            render_tenant_tokens(sql, true, dialect.as_ref()),
            "SELECT o.\"tenant_id\", \"tenant_id\", \"tenant_id\", x"
        );
        let null = dialect.null_text_expr();
        assert_eq!(
            render_tenant_tokens(sql, false, dialect.as_ref()),
            format!("SELECT {null}, {null}, x")
        );
        // Text that is not a token is left alone.
        assert_eq!(
            render_tenant_tokens("SELECT '{{tenant}}'", true, dialect.as_ref()),
            "SELECT '{{tenant}}'"
        );
    }

    #[test]
    fn a_materialized_view_owns_its_backing_relation() {
        let cfg = config(mv());
        let dialect = crate::db::active_dialect();
        assert_eq!(
            own_view_keys(&cfg, None, dialect.as_ref()).unwrap(),
            vec!["an.sales", "an.sales__mv"]
        );
    }

    #[test]
    fn dropping_removes_the_wrapper_before_the_backing_relation() {
        let drops = statements(|v| v.drop_statements(crate::db::active_dialect().as_ref()));
        assert_eq!(drops.len(), 2);
        assert!(drops[0].contains("VIEW") && drops[0].contains("\"an\".\"sales\""));
        assert!(drops[1].contains("\"an\".\"sales__mv\""));
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_backs_a_materialized_view_with_a_table_and_an_unfiltered_wrapper() {
        let creates = statements(|v| v.create_statements(crate::db::active_dialect().as_ref()));
        assert_eq!(
            creates,
            vec![
                format!(
                    "CREATE TABLE IF NOT EXISTS \"an\".\"sales__mv\" AS {}",
                    mv().sql
                ),
                "CREATE VIEW IF NOT EXISTS \"an\".\"sales\" AS SELECT \"region\", \"n\" FROM \"an\".\"sales__mv\""
                    .to_string(),
            ]
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_keys_the_materialized_view_by_tenant_and_filters_the_wrapper() {
        let creates = statements(|v| v.create_statements(crate::db::active_dialect().as_ref()));
        assert_eq!(creates.len(), 3);
        assert!(
            creates[0].starts_with("CREATE MATERIALIZED VIEW IF NOT EXISTS \"an\".\"sales__mv\"")
        );
        assert_eq!(
            creates[1],
            "CREATE UNIQUE INDEX IF NOT EXISTS \"sales__mv_key\" ON \"an\".\"sales__mv\" ({{tenant_key}}\"region\")"
        );
        assert!(creates[2].contains(
            "SELECT \"region\", \"n\" FROM \"an\".\"sales__mv\" WHERE \"tenant_id\" IS NOT DISTINCT FROM NULLIF(current_setting('app.tenant_id', true), '')"
        ));
    }

    #[test]
    fn switching_to_materialized_rebuilds_the_view() {
        let plain = config(ViewConfig {
            materialized: false,
            ..mv()
        });
        let materialized = config(mv());
        let dialect = crate::db::active_dialect();
        let (drops, creates) =
            view_plan_steps(&plain, &materialized, &[], None, dialect.as_ref()).unwrap();
        assert_eq!(drops.len(), 1);
        assert!(creates.len() >= 2);
        assert!(creates
            .iter()
            .all(|s| matches!(s.operation, MigrationOperation::CreateView)));
        assert_eq!(creates[0].object_type, "materialized_view");
        assert_eq!(creates.last().unwrap().object_type, "view");
    }

    #[test]
    fn plans_split_off_trailing_view_creations_and_render_per_target() {
        let dialect = crate::db::active_dialect();
        let old = config(ViewConfig {
            sql: "SELECT 1".into(),
            ..mv()
        });
        let plan = compute_migration_plan(
            &old,
            &config(mv()),
            None,
            None,
            dialect.as_ref(),
            &HashMap::new(),
        )
        .unwrap();
        let (before, creates) = plan.split_view_creates();
        assert!(before
            .steps
            .iter()
            .all(|s| matches!(s.operation, MigrationOperation::DropView)));
        assert!(!creates.steps.is_empty());
        assert_eq!(before.steps.len() + creates.steps.len(), plan.steps.len());

        let rls = plan.for_target(true, dialect.as_ref());
        let ddl = rls
            .steps
            .iter()
            .filter_map(|s| s.ddl.as_deref())
            .collect::<String>();
        assert!(ddl.contains("o.\"tenant_id\"") && !ddl.contains("{{"));
    }
}
