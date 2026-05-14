//! Apply config to the database: DDL for schemas, enums, tables, indexes, and foreign keys.
//! Order follows PostgreSQL dependencies (see docs/postgres-config-schema.md § 3.5).

use crate::config::types::*;
use crate::config::{validate, FullConfig};
use crate::error::AppError;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};

fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

/// Name of the column added to app tables when RLS is enabled. Used by migration and CRUD.
pub const RLS_TENANT_COLUMN: &str = "tenant_id";

/// Apply full config to the database: CREATE SCHEMA, CREATE TYPE, CREATE TABLE, CREATE INDEX, ADD FK.
/// Validates config first. Idempotent for schemas and types (IF NOT EXISTS); tables are CREATE TABLE only (fails if exists).
/// When `schema_override` is `Some(s)`, app tables/indexes/FKs are created in schema `s` instead of config schema names (e.g. for schema-strategy tenants).
/// When `rls_tenant_column` is `Some(col)`, each table gets that column (if missing), RLS enabled, and policies using `current_setting('app.tenant_id', true)`.
pub async fn apply_migrations(
    pool: &PgPool,
    config: &FullConfig,
    schema_override: Option<&str>,
    rls_tenant_column: Option<&str>,
) -> Result<(), AppError> {
    validate(config)?;
    let default_sid = config
        .schemas
        .first()
        .map(|s| s.id.as_str())
        .ok_or_else(|| AppError::Config(crate::error::ConfigError::Validation("at least one schema required".into())))?;

    if let Some(s) = schema_override {
        let name = quote(s);
        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {}", name))
            .execute(pool)
            .await?;
    }

    let schemas_by_id: HashMap<_, _> = config.schemas.iter().map(|s| (s.id.as_str(), s)).collect();
    let tables_by_id: HashMap<_, _> = config.tables.iter().map(|t| (t.id.as_str(), t)).collect();
    let columns_by_table: HashMap<_, Vec<&ColumnConfig>> = config.columns.iter().fold(
        HashMap::new(),
        |mut m, c| {
            m.entry(c.table_id.as_str()).or_default().push(c);
            m
        },
    );

    // When schema_override is set, we only create the override schema; otherwise create config schemas.
    if schema_override.is_none() {
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
        let schema = schemas_by_id
            .get(sid)
            .ok_or_else(|| AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "schema",
                id: sid.to_string(),
            }))?;
        let schema_name = quote(schema_override.unwrap_or(&schema.name));
        let type_name = quote(&e.name);
        let values: Vec<String> = e.values.iter().map(|v| format!("'{}'", v.replace('\'', "''"))).collect();
        let sql = format!(
            "CREATE TYPE {}.{} AS ENUM ({})",
            schema_name,
            type_name,
            values.join(", ")
        );
        let _ = sqlx::query(&sql).execute(pool).await;
    }

    for t in &config.tables {
        let sid = t.schema_id.as_deref().unwrap_or(default_sid);
        let schema = schemas_by_id
            .get(sid)
            .ok_or_else(|| AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "schema",
                id: sid.to_string(),
            }))?;
        let schema_name = quote(schema_override.unwrap_or(&schema.name));
        let table_name = quote(&t.name);
        let full_name = format!("{}.{}", schema_name, table_name);

        let cols = columns_by_table
            .get(t.id.as_str())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let mut col_defs: Vec<String> = Vec::new();
        for c in cols {
            let typ = type_str(&c.type_, &schemas_by_id);
            let mut def = format!("{} {}", quote(&c.name), typ);
            if !c.nullable {
                def.push_str(" NOT NULL");
            }
            if let Some(ref d) = c.default {
                def.push_str(" DEFAULT ");
                match d {
                    ColumnDefaultConfig::Literal(s) => def.push_str(s),
                    ColumnDefaultConfig::Expression { expression } => def.push_str(expression),
                }
            }
            col_defs.push(def);
        }

        let config_col_names: HashSet<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        for (name, def_suffix) in [
            ("created_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
            ("updated_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
            ("archived_at", "TIMESTAMPTZ"),
        ] {
            if !config_col_names.contains(name) {
                col_defs.push(format!("{} {}", quote(name), def_suffix));
            }
        }

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

        if let Some(col) = rls_tenant_column {
            if !config_col_names.contains(col) {
                let q_col = quote(col);
                let add_col = format!("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} TEXT", full_name, q_col);
                sqlx::query(&add_col).execute(pool).await?;
            }
            let enable_rls = format!("ALTER TABLE {} ENABLE ROW LEVEL SECURITY", full_name);
            sqlx::query(&enable_rls).execute(pool).await?;
            let q_col = quote(col);
            let setting = "current_setting('app.tenant_id', true)";
            let cond = format!("{} = {}", q_col, setting);
            let policy_prefix = format!("rls_tenant_{}", t.name);
            let policies: &[(&str, &str, Option<&str>, Option<&str>)] = &[
                ("select", "SELECT", Some(cond.as_str()), None),
                ("insert", "INSERT", None, Some(cond.as_str())),
                ("update", "UPDATE", Some(cond.as_str()), Some(cond.as_str())),
                ("delete", "DELETE", Some(cond.as_str()), None),
            ];
            for (suffix, cmd, using_cond, with_check) in policies.iter() {
                let policy_name = format!("{}_{}", policy_prefix, suffix);
                let drop_sql = format!("DROP POLICY IF EXISTS {} ON {}", quote(&policy_name), full_name);
                let _ = sqlx::query(&drop_sql).execute(pool).await;
                let create_sql = match (using_cond, with_check) {
                    (Some(u), Some(w)) => format!(
                        "CREATE POLICY {} ON {} FOR {} USING ( {} ) WITH CHECK ( {} )",
                        quote(&policy_name), full_name, cmd, u, w
                    ),
                    (Some(u), None) => format!(
                        "CREATE POLICY {} ON {} FOR {} USING ( {} )",
                        quote(&policy_name), full_name, cmd, u
                    ),
                    (None, Some(w)) => format!(
                        "CREATE POLICY {} ON {} FOR {} WITH CHECK ( {} )",
                        quote(&policy_name), full_name, cmd, w
                    ),
                    (None, None) => continue,
                };
                sqlx::query(&create_sql).execute(pool).await?;
            }
        }
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
                IndexColumnEntry::Spec { name, direction, .. } => {
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
        let from_sid = rel.from_schema_id.as_str();
        let to_sid = rel.to_schema_id.as_str();
        let from_schema = schemas_by_id.get(from_sid).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "schema",
                id: from_sid.to_string(),
            })
        })?;
        let from_table = tables_by_id.get(rel.from_table_id.as_str()).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "table",
                id: rel.from_table_id.clone(),
            })
        })?;
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

        let from_schema_name = schema_override.unwrap_or(&from_schema.name);
        let to_schema_name = schema_override.unwrap_or(&to_schema.name);

        let from_col = config
            .columns
            .iter()
            .find(|c| c.id == rel.from_column_id)
            .map(|c| c.name.as_str())
            .ok_or_else(|| AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "column",
                id: rel.from_column_id.clone(),
            }))?;
        let to_col = config
            .columns
            .iter()
            .find(|c| c.id == rel.to_column_id)
            .map(|c| c.name.as_str())
            .ok_or_else(|| AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "column",
                id: rel.to_column_id.clone(),
            }))?;

        let from_full = format!("{}.{}", quote(from_schema_name), quote(&from_table.name));
        let to_full = format!("{}.{}", quote(to_schema_name), quote(&to_table.name));
        let constraint_name = rel
            .name
            .as_deref()
            .unwrap_or(&rel.id);
        let on_update = rel
            .on_update
            .as_deref()
            .unwrap_or("NO ACTION");
        let on_delete = rel
            .on_delete
            .as_deref()
            .unwrap_or("NO ACTION");

        let sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}) ON UPDATE {} ON DELETE {}",
            from_full,
            quote(constraint_name),
            quote(from_col),
            to_full,
            quote(to_col),
            on_update,
            on_delete
        );
        let _ = sqlx::query(&sql).execute(pool).await;
    }

    Ok(())
}

/// Revert migrations for a package: drop tables, enum types, and schema (if not public) in reverse order of apply.
/// Uses the same schema_override as apply_migrations (tables/enums live in that schema).
pub async fn revert_migrations(
    pool: &PgPool,
    config: &FullConfig,
    schema_override: Option<&str>,
) -> Result<(), AppError> {
    let default_sid = config
        .schemas
        .first()
        .map(|s| s.id.as_str())
        .ok_or_else(|| AppError::Config(crate::error::ConfigError::Validation("at least one schema required".into())))?;

    let schemas_by_id: HashMap<_, _> = config.schemas.iter().map(|s| (s.id.as_str(), s)).collect();

    // 1. Drop tables (CASCADE drops FKs and dependent objects)
    for t in &config.tables {
        let sid = t.schema_id.as_deref().unwrap_or(default_sid);
        let schema = schemas_by_id.get(sid).ok_or_else(|| {
            AppError::Config(crate::error::ConfigError::MissingReference {
                kind: "schema",
                id: sid.to_string(),
            })
        })?;
        let schema_name = quote(schema_override.unwrap_or(&schema.name));
        let table_name = quote(&t.name);
        let full_name = format!("{}.{}", schema_name, table_name);
        let drop_sql = format!("DROP TABLE IF EXISTS {} CASCADE", full_name);
        let _ = sqlx::query(&drop_sql).execute(pool).await;
    }

    // 2. Drop enum types
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
        let drop_sql = format!("DROP TYPE IF EXISTS {}.{} CASCADE", schema_name, type_name);
        let _ = sqlx::query(&drop_sql).execute(pool).await;
    }

    // 3. Drop schema only if not public (shared schema)
    if schema_override.is_none() {
        for s in &config.schemas {
            if s.name.eq_ignore_ascii_case("public") {
                continue;
            }
            let schema_name = quote(&s.name);
            let drop_sql = format!("DROP SCHEMA IF EXISTS {} CASCADE", schema_name);
            let _ = sqlx::query(&drop_sql).execute(pool).await;
        }
    }

    Ok(())
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
    BackfillNulls,
    SetNotNull,
    DropNotNull,
    SetDefault,
    DropDefault,
    CreateIndex,
    DropIndex,
    AddForeignKey,
    DropForeignKey,
}

impl std::fmt::Display for MigrationOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = serde_json::to_value(self).ok()
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
    /// "column" | "table" | "index" | "foreign_key" | "enum" | "enum_value" | "schema"
    pub object_type: String,
    pub description: String,
    /// The SQL to execute. None for WarnOnly steps.
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
}

impl MigrationPlan {
    pub fn summary(&self) -> MigrationSummary {
        let (mut safe, mut best_effort, mut warn_only) = (0, 0, 0);
        for s in &self.steps {
            match s.safety {
                MigrationSafety::Safe => safe += 1,
                MigrationSafety::BestEffort => best_effort += 1,
                MigrationSafety::WarnOnly => warn_only += 1,
            }
        }
        MigrationSummary { total: self.steps.len(), safe, best_effort, warn_only }
    }
}

/// Result returned by `execute_migration_plan`.
pub struct MigrationExecutionResult {
    pub applied: usize,
    pub warned: usize,
    pub warnings: Vec<String>,
}

fn default_str(d: &ColumnDefaultConfig) -> String {
    match d {
        ColumnDefaultConfig::Literal(s) => s.clone(),
        ColumnDefaultConfig::Expression { expression } => expression.clone(),
    }
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
) -> Result<MigrationPlan, AppError> {
    validate(new)?;

    let default_old_sid = old.schemas.first().map(|s| s.id.as_str()).unwrap_or("");
    let default_new_sid = new.schemas.first().map(|s| s.id.as_str()).unwrap_or("");

    let old_schemas: HashMap<&str, &SchemaConfig> = old.schemas.iter().map(|s| (s.id.as_str(), s)).collect();
    let new_schemas: HashMap<&str, &SchemaConfig> = new.schemas.iter().map(|s| (s.id.as_str(), s)).collect();
    let old_tables: HashMap<&str, &TableConfig> = old.tables.iter().map(|t| (t.id.as_str(), t)).collect();
    let new_tables: HashMap<&str, &TableConfig> = new.tables.iter().map(|t| (t.id.as_str(), t)).collect();
    let old_columns: HashMap<&str, &ColumnConfig> = old.columns.iter().map(|c| (c.id.as_str(), c)).collect();
    let old_enums: HashMap<&str, &EnumConfig> = old.enums.iter().map(|e| (e.id.as_str(), e)).collect();
    let new_enums: HashMap<&str, &EnumConfig> = new.enums.iter().map(|e| (e.id.as_str(), e)).collect();
    let old_indexes: HashMap<&str, &IndexConfig> = old.indexes.iter().map(|i| (i.id.as_str(), i)).collect();
    let new_indexes: HashMap<&str, &IndexConfig> = new.indexes.iter().map(|i| (i.id.as_str(), i)).collect();
    let old_rels: HashMap<&str, &RelationshipConfig> = old.relationships.iter().map(|r| (r.id.as_str(), r)).collect();
    let new_rels: HashMap<&str, &RelationshipConfig> = new.relationships.iter().map(|r| (r.id.as_str(), r)).collect();

    let empty: HashMap<&str, &SchemaConfig> = HashMap::new();
    let mut steps: Vec<MigrationStep> = Vec::new();

    let schema_name_for = |sid: &str, schemas: &HashMap<&str, &SchemaConfig>| -> String {
        schema_override
            .map(String::from)
            .unwrap_or_else(|| schemas.get(sid).map(|s| s.name.clone()).unwrap_or_else(|| sid.to_string()))
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
            for val in new_enum.values.iter().map(String::as_str).filter(|v| !old_vals.contains(v)) {
                steps.push(MigrationStep {
                    step: 0,
                    operation: MigrationOperation::AddEnumValue,
                    schema: schema.clone(),
                    table: None,
                    object: format!("{}:{}", new_enum.name, val),
                    object_type: "enum_value".into(),
                    description: format!("Add value '{}' to enum \"{}\".\"{}\"", val, schema, new_enum.name),
                    ddl: Some(format!(
                        "ALTER TYPE {}.{} ADD VALUE IF NOT EXISTS '{}'",
                        quote(&schema), quote(&new_enum.name), val.replace('\'', "''")
                    )),
                    safety: MigrationSafety::Safe,
                    risk: MigrationRisk::None,
                    risk_detail: None,
                });
            }
            for val in old_enum.values.iter().map(String::as_str).filter(|v| !new_vals.contains(v)) {
                steps.push(MigrationStep {
                    step: 0,
                    operation: MigrationOperation::RemoveEnumValue,
                    schema: schema.clone(),
                    table: None,
                    object: format!("{}:{}", new_enum.name, val),
                    object_type: "enum_value".into(),
                    description: format!("Enum value '{}' removed from config on \"{}\".\"{}\"", val, schema, new_enum.name),
                    ddl: None,
                    safety: MigrationSafety::WarnOnly,
                    risk: MigrationRisk::ManualActionRequired,
                    risk_detail: Some(format!(
                        "PostgreSQL does not support removing enum values. '{}' was removed from config but NOT from the database type. Recreate the type manually if needed.",
                        val
                    )),
                });
            }
        } else {
            let values: Vec<String> = new_enum.values.iter().map(|v| format!("'{}'", v.replace('\'', "''"))).collect();
            steps.push(MigrationStep {
                step: 0,
                operation: MigrationOperation::CreateEnum,
                schema: schema.clone(),
                table: None,
                object: new_enum.name.clone(),
                object_type: "enum".into(),
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
                description: format!("Enum \"{}\".\"{}\" removed from config", schema, old_enum.name),
                ddl: None,
                safety: MigrationSafety::WarnOnly,
                risk: MigrationRisk::ManualActionRequired,
                risk_detail: Some("Enum type NOT dropped from database (data safety). Run DROP TYPE manually if intended.".into()),
            });
        }
    }

    // ── 3. New and removed tables ────────────────────────────────────────────
    let added_table_ids: HashSet<&str> = new.tables.iter()
        .filter(|t| !old_tables.contains_key(t.id.as_str()))
        .map(|t| t.id.as_str())
        .collect();

    let cols_by_table: HashMap<&str, Vec<&ColumnConfig>> = new.columns.iter()
        .fold(HashMap::new(), |mut m, c| { m.entry(c.table_id.as_str()).or_default().push(c); m });

    for new_table in &new.tables {
        if !added_table_ids.contains(new_table.id.as_str()) {
            continue;
        }
        let sid = new_table.schema_id.as_deref().unwrap_or(default_new_sid);
        let schema = schema_name_for(sid, &new_schemas);
        let full = format!("{}.{}", quote(&schema), quote(&new_table.name));

        let cols = cols_by_table.get(new_table.id.as_str()).map(|v| v.as_slice()).unwrap_or(&[]);
        let mut col_defs: Vec<String> = Vec::new();
        for c in cols {
            let typ = type_str(&c.type_, &empty);
            let mut def = format!("{} {}", quote(&c.name), typ);
            if !c.nullable { def.push_str(" NOT NULL"); }
            if let Some(ref d) = c.default {
                def.push_str(" DEFAULT ");
                match d {
                    ColumnDefaultConfig::Literal(s) => def.push_str(s),
                    ColumnDefaultConfig::Expression { expression } => def.push_str(expression),
                }
            }
            col_defs.push(def);
        }
        let cfg_col_names: HashSet<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        for (name, suf) in [("created_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"), ("updated_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"), ("archived_at", "TIMESTAMPTZ")] {
            if !cfg_col_names.contains(name) { col_defs.push(format!("{} {}", quote(name), suf)); }
        }
        let pk_cols = match &new_table.primary_key {
            PrimaryKeyConfig::Single(s) => vec![quote(s)],
            PrimaryKeyConfig::Composite(v) => v.iter().map(|s| quote(s)).collect(),
        };
        col_defs.push(format!("PRIMARY KEY ({})", pk_cols.join(", ")));
        for u in &new_table.unique { col_defs.push(format!("UNIQUE ({})", u.iter().map(|s| quote(s)).collect::<Vec<_>>().join(", "))); }
        for ch in &new_table.check { col_defs.push(format!("CONSTRAINT {} CHECK ({})", quote(&ch.name), ch.expression)); }

        steps.push(MigrationStep {
            step: 0,
            operation: MigrationOperation::CreateTable,
            schema: schema.clone(),
            table: Some(new_table.name.clone()),
            object: new_table.name.clone(),
            object_type: "table".into(),
            description: format!("Create table \"{}\".\"{}\"", schema, new_table.name),
            ddl: Some(format!("CREATE TABLE IF NOT EXISTS {} (\n  {}\n)", full, col_defs.join(",\n  "))),
            safety: MigrationSafety::Safe,
            risk: MigrationRisk::None,
            risk_detail: None,
        });
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
        if added_table_ids.contains(new_col.table_id.as_str()) { continue; }
        let table = match new_tables.get(new_col.table_id.as_str()) { Some(t) => t, None => continue };
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
                    description: format!("Rename column \"{}\" → \"{}\" on \"{}\".\"{}\"", old_col.name, new_col.name, schema, table.name),
                    ddl: Some(format!("ALTER TABLE {} RENAME COLUMN {} TO {}", full, quote(&old_col.name), quote(&new_col.name))),
                    safety: MigrationSafety::Safe,
                    risk: MigrationRisk::None,
                    risk_detail: None,
                });
            }

            // Type change
            let old_type = type_str(&old_col.type_, &empty);
            let new_type = type_str(&new_col.type_, &empty);
            if old_type.to_uppercase() != new_type.to_uppercase() {
                let col_name = &new_col.name;
                steps.push(MigrationStep {
                    step: 0,
                    operation: MigrationOperation::AlterColumnType,
                    schema: schema.clone(),
                    table: Some(table.name.clone()),
                    object: col_name.clone(),
                    object_type: "column".into(),
                    description: format!("Change type of \"{}\".\"{}\".\"{}\": {} → {}", schema, table.name, col_name, old_type, new_type),
                    ddl: Some(format!("ALTER TABLE {} ALTER COLUMN {} TYPE {} USING {}::{}", full, quote(col_name), new_type, quote(col_name), new_type)),
                    safety: MigrationSafety::BestEffort,
                    risk: MigrationRisk::MayFail,
                    risk_detail: Some(format!("USING {}::{} cast may fail for incompatible values. Provide a custom USING expression if needed.", col_name, new_type)),
                });
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
                    description: format!("Drop NOT NULL on \"{}\".\"{}\".\"{}\": column becomes nullable", schema, table.name, new_col.name),
                    ddl: Some(format!("ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL", full, quote(&new_col.name))),
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
                            description: format!("Set DEFAULT {} on \"{}\".\"{}\".\"{}\": was {}", val, schema, table.name, new_col.name, old_def.as_deref().unwrap_or("none")),
                            ddl: Some(format!("ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}", full, quote(&new_col.name), val)),
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
                            description: format!("Drop DEFAULT on \"{}\".\"{}\".\"{}\": was {}", schema, table.name, new_col.name, old_def.as_deref().unwrap_or("none")),
                            ddl: Some(format!("ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT", full, quote(&new_col.name))),
                            safety: MigrationSafety::Safe,
                            risk: MigrationRisk::None,
                            risk_detail: None,
                        });
                    }
                }
            }
        } else {
            // New column: ADD COLUMN
            let new_type = type_str(&new_col.type_, &empty);
            let mut col_def = format!("{} {}", quote(&new_col.name), new_type);
            if !new_col.nullable { col_def.push_str(" NOT NULL"); }
            if let Some(ref d) = new_col.default {
                col_def.push_str(" DEFAULT ");
                match d {
                    ColumnDefaultConfig::Literal(s) => col_def.push_str(s),
                    ColumnDefaultConfig::Expression { expression } => col_def.push_str(expression),
                }
            }
            steps.push(MigrationStep {
                step: 0,
                operation: MigrationOperation::AddColumn,
                schema: schema.clone(),
                table: Some(table.name.clone()),
                object: new_col.name.clone(),
                object_type: "column".into(),
                description: format!("Add column \"{}\" {} to \"{}\".\"{}\"", new_col.name, new_type, schema, table.name),
                ddl: Some(format!("ALTER TABLE {} ADD COLUMN {}", full, col_def)),
                safety: MigrationSafety::Safe,
                risk: MigrationRisk::None,
                risk_detail: None,
            });
        }
    }

    // Removed columns (warn only)
    for old_col in &old.columns {
        if new.columns.iter().any(|c| c.id == old_col.id) { continue; }
        if !new_tables.contains_key(old_col.table_id.as_str()) { continue; }
        let table_name = old_tables.get(old_col.table_id.as_str()).map(|t| t.name.as_str()).unwrap_or(&old_col.table_id);
        let sid = old_tables.get(old_col.table_id.as_str()).and_then(|t| t.schema_id.as_deref()).unwrap_or(default_old_sid);
        let schema = schema_name_for(sid, &old_schemas);
        steps.push(MigrationStep {
            step: 0,
            operation: MigrationOperation::DropColumn,
            schema: schema.clone(),
            table: Some(table_name.to_string()),
            object: old_col.name.clone(),
            object_type: "column".into(),
            description: format!("Column \"{}\" removed from config on \"{}\".\"{}\"", old_col.name, schema, table_name),
            ddl: None,
            safety: MigrationSafety::WarnOnly,
            risk: MigrationRisk::ManualActionRequired,
            risk_detail: Some("Column NOT dropped from database (data safety). Run ALTER TABLE DROP COLUMN manually if intended.".into()),
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
                table: old_tables.get(old_idx.table_id.as_str()).map(|t| t.name.clone()),
                object: old_idx.name.clone(),
                object_type: "index".into(),
                description: format!("Drop index \"{}\" in schema \"{}\"", old_idx.name, schema),
                ddl: Some(format!("DROP INDEX IF EXISTS {}.{}", quote(&schema), quote(&old_idx.name))),
                safety: MigrationSafety::Safe,
                risk: MigrationRisk::None,
                risk_detail: None,
            });
        }
    }
    for new_idx in &new.indexes {
        if old_indexes.contains_key(new_idx.id.as_str()) || added_table_ids.contains(new_idx.table_id.as_str()) { continue; }
        let sid = new_idx.schema_id.as_deref().unwrap_or(default_new_sid);
        let schema = match new_schemas.get(sid) { Some(s) => schema_override.unwrap_or(&s.name).to_string(), None => continue };
        let table = match new_tables.get(new_idx.table_id.as_str()) { Some(t) => t, None => continue };
        let full_table = format!("{}.{}", quote(&schema), quote(&table.name));
        let mut col_parts: Vec<String> = Vec::new();
        for col in &new_idx.columns {
            match col {
                IndexColumnEntry::Name(n) => col_parts.push(quote(n)),
                IndexColumnEntry::Spec { name, direction, .. } => {
                    let dir = direction.as_deref().map(|d| format!(" {}", d.to_uppercase())).unwrap_or_default();
                    col_parts.push(format!("{}{}", quote(name), dir));
                }
                IndexColumnEntry::Expression { expression } => col_parts.push(expression.clone()),
            }
        }
        let method = new_idx.method.as_deref().unwrap_or("btree");
        let unique_kw = if new_idx.unique { "UNIQUE " } else { "" };
        let include = if new_idx.include.is_empty() { String::new() } else { format!(" INCLUDE ({})", new_idx.include.iter().map(|s| quote(s)).collect::<Vec<_>>().join(", ")) };
        let where_clause = new_idx.where_.as_ref().map(|w| format!(" WHERE {}", w)).unwrap_or_default();
        steps.push(MigrationStep {
            step: 0,
            operation: MigrationOperation::CreateIndex,
            schema: schema.clone(),
            table: Some(table.name.clone()),
            object: new_idx.name.clone(),
            object_type: "index".into(),
            description: format!("Create {}index \"{}\" on \"{}\".\"{}\"", if new_idx.unique { "unique " } else { "" }, new_idx.name, schema, table.name),
            ddl: Some(format!("CREATE {}INDEX IF NOT EXISTS {} ON {} USING {} ({}){}{}", unique_kw, quote(&new_idx.name), full_table, method, col_parts.join(", "), include, where_clause)),
            safety: MigrationSafety::Safe,
            risk: MigrationRisk::None,
            risk_detail: None,
        });
    }

    // ── 6. Foreign keys ──────────────────────────────────────────────────────
    for old_rel in &old.relationships {
        if !new_rels.contains_key(old_rel.id.as_str()) {
            let from_schema = old_schemas.get(old_rel.from_schema_id.as_str()).map(|s| s.name.as_str()).unwrap_or(&old_rel.from_schema_id);
            let from_table = old_tables.get(old_rel.from_table_id.as_str()).map(|t| t.name.as_str()).unwrap_or(&old_rel.from_table_id);
            let constraint = old_rel.name.as_deref().unwrap_or(&old_rel.id);
            let schema_q = quote(schema_override.unwrap_or(from_schema));
            steps.push(MigrationStep {
                step: 0,
                operation: MigrationOperation::DropForeignKey,
                schema: schema_override.unwrap_or(from_schema).to_string(),
                table: Some(from_table.to_string()),
                object: constraint.to_string(),
                object_type: "foreign_key".into(),
                description: format!("Drop FK \"{}\" from \"{}\".\"{}\"", constraint, schema_override.unwrap_or(from_schema), from_table),
                ddl: Some(format!("ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {}", schema_q, quote(from_table), quote(constraint))),
                safety: MigrationSafety::Safe,
                risk: MigrationRisk::None,
                risk_detail: None,
            });
        }
    }
    for new_rel in &new.relationships {
        if old_rels.contains_key(new_rel.id.as_str()) || added_table_ids.contains(new_rel.from_table_id.as_str()) || added_table_ids.contains(new_rel.to_table_id.as_str()) { continue; }
        let from_schema = match new_schemas.get(new_rel.from_schema_id.as_str()) { Some(s) => s, None => continue };
        let from_table = match new_tables.get(new_rel.from_table_id.as_str()) { Some(t) => t, None => continue };
        let to_schema = match new_schemas.get(new_rel.to_schema_id.as_str()) { Some(s) => s, None => continue };
        let to_table = match new_tables.get(new_rel.to_table_id.as_str()) { Some(t) => t, None => continue };
        let from_col = new.columns.iter().find(|c| c.id == new_rel.from_column_id).map(|c| c.name.clone()).unwrap_or_else(|| new_rel.from_column_id.clone());
        let to_col = new.columns.iter().find(|c| c.id == new_rel.to_column_id).map(|c| c.name.clone()).unwrap_or_else(|| new_rel.to_column_id.clone());
        let from_q = format!("{}.{}", quote(schema_override.unwrap_or(&from_schema.name)), quote(&from_table.name));
        let to_q = format!("{}.{}", quote(schema_override.unwrap_or(&to_schema.name)), quote(&to_table.name));
        let constraint = new_rel.name.as_deref().unwrap_or(&new_rel.id);
        let on_update = new_rel.on_update.as_deref().unwrap_or("NO ACTION");
        let on_delete = new_rel.on_delete.as_deref().unwrap_or("NO ACTION");
        steps.push(MigrationStep {
            step: 0,
            operation: MigrationOperation::AddForeignKey,
            schema: schema_override.unwrap_or(&from_schema.name).to_string(),
            table: Some(from_table.name.clone()),
            object: constraint.to_string(),
            object_type: "foreign_key".into(),
            description: format!("Add FK \"{}\" on \"{}\".\"{}\" → \"{}\".\"{}\"", constraint, schema_override.unwrap_or(&from_schema.name), from_table.name, schema_override.unwrap_or(&to_schema.name), to_table.name),
            ddl: Some(format!("ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}) ON UPDATE {} ON DELETE {}", from_q, quote(constraint), quote(&from_col), to_q, quote(&to_col), on_update, on_delete)),
            safety: MigrationSafety::BestEffort,
            risk: MigrationRisk::None,
            risk_detail: Some("PostgreSQL has no ADD CONSTRAINT IF NOT EXISTS; ignored if constraint already exists.".into()),
        });
    }

    // Assign sequential step numbers
    for (i, s) in steps.iter_mut().enumerate() {
        s.step = i + 1;
    }

    Ok(MigrationPlan { steps })
}

// ─── execute_migration_plan ──────────────────────────────────────────────────

/// Execute a pre-computed `MigrationPlan` against the tenant database.
/// Writes per-step audit records to the config (architect) database.
/// Returns counts and any warning messages collected from best-effort failures.
pub async fn execute_migration_plan(
    migration_pool: &PgPool,
    config_pool: &PgPool,
    plan: &MigrationPlan,
    migration_plan_id: &str,
    package_id: &str,
    tenant_id: &str,
    from_version: Option<&str>,
    to_version: &str,
) -> Result<MigrationExecutionResult, AppError> {
    let mut applied = 0usize;
    let mut warned = 0usize;
    let mut warnings: Vec<String> = Vec::new();

    for step in &plan.steps {
        let op = step.operation.to_string();
        let safety_str = format!("{:?}", step.safety);
        let risk_str = format!("{:?}", step.risk);

        match step.safety {
            MigrationSafety::WarnOnly => {
                let msg = step.risk_detail.clone().unwrap_or_else(|| step.description.clone());
                tracing::warn!(step = step.step, %op, "migration plan warning (no DDL)");
                warnings.push(format!("[Step {}] {}", step.step, msg));
                let _ = crate::store::insert_migration_audit(
                    config_pool, migration_plan_id, package_id, tenant_id,
                    from_version, to_version, step.step as i32, &op,
                    &step.schema, step.table.as_deref(), &step.object, &step.object_type,
                    &step.description, step.ddl.as_deref(), &safety_str, &risk_str,
                    "skipped", None,
                ).await;
                warned += 1;
            }
            MigrationSafety::Safe | MigrationSafety::BestEffort => {
                if let Some(ref sql) = step.ddl {
                    tracing::info!(step = step.step, %op, %sql, "executing migration step");
                    match sqlx::query(sql).execute(migration_pool).await {
                        Ok(_) => {
                            let _ = crate::store::insert_migration_audit(
                                config_pool, migration_plan_id, package_id, tenant_id,
                                from_version, to_version, step.step as i32, &op,
                                &step.schema, step.table.as_deref(), &step.object, &step.object_type,
                                &step.description, step.ddl.as_deref(), &safety_str, &risk_str,
                                "applied", None,
                            ).await;
                            applied += 1;
                        }
                        Err(e) => {
                            let err_str = e.to_string();
                            if matches!(step.safety, MigrationSafety::BestEffort) {
                                tracing::warn!(step = step.step, %op, error = %e, "migration step failed (best-effort, continuing)");
                                let msg = format!("[Step {}] {} — Error: {}", step.step, step.description, err_str);
                                warnings.push(msg);
                                let _ = crate::store::insert_migration_audit(
                                    config_pool, migration_plan_id, package_id, tenant_id,
                                    from_version, to_version, step.step as i32, &op,
                                    &step.schema, step.table.as_deref(), &step.object, &step.object_type,
                                    &step.description, step.ddl.as_deref(), &safety_str, &risk_str,
                                    "warned", Some(&err_str),
                                ).await;
                                warned += 1;
                            } else {
                                let _ = crate::store::insert_migration_audit(
                                    config_pool, migration_plan_id, package_id, tenant_id,
                                    from_version, to_version, step.step as i32, &op,
                                    &step.schema, step.table.as_deref(), &step.object, &step.object_type,
                                    &step.description, step.ddl.as_deref(), &safety_str, &risk_str,
                                    "failed", Some(&err_str),
                                ).await;
                                return Err(AppError::Db(e));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(MigrationExecutionResult { applied, warned, warnings })
}

fn type_str(ty: &ColumnTypeConfig, _schemas_by_id: &HashMap<&str, &SchemaConfig>) -> String {
    match ty {
        ColumnTypeConfig::Simple(s) => {
            if s.eq_ignore_ascii_case("asset[]") {
                // Asset array columns store JSONB arrays of relative storage paths.
                "JSONB".to_string()
            } else if s.eq_ignore_ascii_case("asset") {
                // Asset columns are stored as plain text (relative storage path).
                "TEXT".to_string()
            } else {
                s.clone()
            }
        }
        ColumnTypeConfig::Parameterized { name, params } => {
            let p = params
                .as_ref()
                .map(|v| v.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(", "))
                .unwrap_or_default();
            if p.is_empty() {
                name.clone()
            } else {
                format!("{}({})", name, p)
            }
        }
    }
}
