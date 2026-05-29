//! Entity CRUD handlers: create, read, update, delete, list, bulk.
//! Request bodies and query param keys are accepted in camelCase and converted to snake_case for DB; response row keys are converted to camelCase.

use crate::case::{
    hashmap_keys_to_snake_case, to_camel_case, to_snake_case, value_keys_to_camel_case,
};
use crate::config::{
    load_from_pool, resolve, IncludeDirection, PkType, ResolvedEntity, ResolvedModel,
};
use crate::error::{AppError, BulkFieldError};
use crate::events::spawn_events;
use crate::extractors::tenant::TenantId;
use crate::extractors::user::UserId;
use crate::service::{CrudService, RequestValidator, TenantExecutor};
use crate::sql::{parse_rsql, parse_sort, FilterNode, IncludeSelect};
use crate::state::AppState;
use crate::storage::{compress, resolve_prefix, validate_asset_field};
use crate::store::DEFAULT_PACKAGE_ID;
use crate::tenant::TenantStrategy;
use axum::{
    extract::{FromRequest, Path, Query, Request, State},
    Json,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Remove sensitive column keys from a row object. No-op if sensitive_columns is empty.
fn strip_sensitive_columns(row: &mut Value, sensitive_columns: &HashSet<String>) {
    if sensitive_columns.is_empty() {
        return;
    }
    if let Value::Object(map) = row {
        map.retain(|k, _| !sensitive_columns.contains(k));
    }
}

fn parse_id(id_str: &str, pk_type: &PkType) -> Result<Value, AppError> {
    Ok(match pk_type {
        PkType::Uuid => {
            let u = uuid::Uuid::parse_str(id_str)
                .map_err(|_| AppError::BadRequest("invalid uuid".into()))?;
            Value::String(u.to_string())
        }
        PkType::BigInt | PkType::Int => {
            let n: i64 = id_str
                .parse()
                .map_err(|_| AppError::BadRequest("invalid id".into()))?;
            Value::Number(n.into())
        }
        PkType::Text => Value::String(id_str.to_string()),
    })
}

fn body_to_map(value: Value) -> Result<HashMap<String, Value>, AppError> {
    match value {
        Value::Object(m) => Ok(m.into_iter().collect()),
        _ => Err(AppError::BadRequest("body must be a JSON object".into())),
    }
}

/// Convert a vec of (row_index, AppError) from CrudService collecting methods into BulkFieldErrors.
/// Parses PostgreSQL error detail to extract the offending column name.
fn db_errors_to_bulk_field_errors(row_errors: Vec<(usize, AppError)>) -> Vec<BulkFieldError> {
    use crate::error::{db_error_field, db_error_message};
    row_errors
        .into_iter()
        .map(|(index, e)| {
            let raw_field = db_error_field(&e);
            let message = db_error_message(&e, raw_field.as_deref());
            let field = raw_field
                .map(|f| to_camel_case(&f))
                .unwrap_or_else(|| "unknown".to_string());
            BulkFieldError {
                index,
                field,
                message,
            }
        })
        .collect()
}

/// For entities with `parent_ref_column` set, resolves each row's `parent_id` UUID to the
/// natural-key value and injects it as `parent_ref` (snake_case, converted to camelCase later).
/// Rows with no `parent_id` are skipped. One batch SELECT covers all distinct parent UUIDs.
async fn enrich_with_parent_ref<'a>(
    executor: &mut crate::service::TenantExecutor<'a>,
    rows: &mut [Value],
    entity: &ResolvedEntity,
    ref_col: &str,
    schema_override: Option<&str>,
) -> Result<(), AppError> {
    let q = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let schema = schema_override.unwrap_or(&entity.schema_name);
    let table_q = format!("{}.{}", q(schema), q(&entity.table_name));
    let pk = &entity.pk_columns[0];

    // Collect distinct parent UUIDs present in the result set.
    let parent_ids: Vec<uuid::Uuid> = rows
        .iter()
        .filter_map(|row| {
            row.as_object()
                .and_then(|o| o.get("parent_id"))
                .and_then(|v| v.as_str())
                .and_then(|s| uuid::Uuid::parse_str(s).ok())
        })
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    if parent_ids.is_empty() {
        return Ok(());
    }

    // Fetch id → natural_key for all parent rows in one query using portable IN (...).
    let d = executor.dialect;
    let phs: Vec<String> = (1..=parent_ids.len()).map(|i| d.placeholder(i)).collect();
    let select_sql = format!(
        "SELECT {pk_q}, {ref_q} FROM {table} WHERE {pk_q} IN ({placeholders})",
        pk_q = d.quote_ident(pk),
        ref_q = d.quote_ident(ref_col),
        table = table_q,
        placeholders = phs.join(", "),
    );
    let db_rows: Vec<(String, String)> = {
        let mut qry = sqlx::query_as::<_, (String, String)>(&select_sql);
        for id in &parent_ids {
            qry = qry.bind(*id);
        }
        match executor.executor {
            crate::service::TenantExecutorInner::Pool(pool) => qry.fetch_all(pool).await?,
            crate::service::TenantExecutorInner::Conn(ref mut conn) => {
                qry.fetch_all(&mut **conn).await?
            }
        }
    };
    let uuid_to_ref: HashMap<String, String> = db_rows.into_iter().collect();

    // Inject parent_ref into each row.
    for row in rows.iter_mut() {
        let parent_id_str = row
            .as_object()
            .and_then(|o| o.get("parent_id"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        if let Some(pid) = parent_id_str {
            if let Some(ref_val) = uuid_to_ref.get(&pid) {
                if let Some(obj) = row.as_object_mut() {
                    obj.insert("parent_ref".to_string(), Value::String(ref_val.clone()));
                }
            }
        }
    }
    Ok(())
}

/// Strips `parent_ref` from each item (mutating in place) and returns a parallel vec of the
/// extracted natural-key strings. Indices with no `parent_ref` get `None`.
fn extract_parent_refs(items: &mut [HashMap<String, Value>]) -> Vec<Option<String>> {
    items
        .iter_mut()
        .map(|item| {
            item.remove("parent_ref").and_then(|v| match v {
                Value::String(s) => Some(s),
                _ => None,
            })
        })
        .collect()
}

/// After a successful bulk insert, resolve `parentRef` natural-key values to UUIDs, issue
/// UPDATE queries to write `parent_id`, and update `rows` in place.
///
/// Resolution order: same-batch parents first (looked up from `rows`), then pre-existing rows
/// already in the DB (fetched with a single SELECT … WHERE ref_col = ANY($1)).
async fn resolve_and_update_parent_refs<'a>(
    executor: &mut crate::service::TenantExecutor<'a>,
    rows: &mut [Value],
    parent_refs: &[Option<String>],
    entity: &ResolvedEntity,
    ref_col: &str,
    schema_override: Option<&str>,
) -> Result<(), AppError> {
    let q = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let schema = schema_override.unwrap_or(&entity.schema_name);
    let table_q = format!("{}.{}", q(schema), q(&entity.table_name));
    let pk = entity.pk_columns[0].clone();

    // Collect distinct ref values that need resolution.
    let needed: Vec<String> = parent_refs
        .iter()
        .filter_map(|o| o.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    if needed.is_empty() {
        return Ok(());
    }

    // Build ref → uuid from same-batch rows first.
    let mut ref_to_uuid: HashMap<String, String> = HashMap::new();
    for row in rows.iter() {
        if let Some(obj) = row.as_object() {
            let ref_val = obj.get(ref_col).and_then(|v| v.as_str());
            let uuid_val = obj.get(&pk).and_then(|v| v.as_str());
            if let (Some(r), Some(u)) = (ref_val, uuid_val) {
                ref_to_uuid.insert(r.to_string(), u.to_string());
            }
        }
    }

    // Fetch any pre-existing parents not covered by the batch.
    let missing: Vec<&str> = needed
        .iter()
        .filter(|r| !ref_to_uuid.contains_key(*r))
        .map(|r| r.as_str())
        .collect();

    if !missing.is_empty() {
        // Cast pk to text in the SELECT so sqlx can decode it as String regardless of column type.
        let d = executor.dialect;
        let phs: Vec<String> = (1..=missing.len()).map(|i| d.placeholder(i)).collect();
        let select_sql = format!(
            "SELECT {pk_q}, {ref_q} FROM {table} WHERE {ref_q} IN ({placeholders})",
            pk_q = d.quote_ident(&pk),
            ref_q = d.quote_ident(ref_col),
            table = table_q,
            placeholders = phs.join(", "),
        );
        let db_rows: Vec<(String, String)> = {
            let mut qry = sqlx::query_as::<_, (String, String)>(&select_sql);
            for s in missing.iter() {
                qry = qry.bind(s.to_string());
            }
            match executor.executor {
                crate::service::TenantExecutorInner::Pool(pool) => qry.fetch_all(pool).await?,
                crate::service::TenantExecutorInner::Conn(ref mut conn) => {
                    qry.fetch_all(&mut **conn).await?
                }
            }
        };
        for (uuid, ref_val) in db_rows {
            ref_to_uuid.insert(ref_val, uuid);
        }
    }

    // Determine whether parent_id and the PK are uuid columns from the entity's column metadata.
    let pk_is_uuid = entity
        .columns
        .iter()
        .find(|c| c.name == pk)
        .map(|c| {
            c.pg_type.as_deref() == Some("uuid")
                || matches!(c.pk_type, Some(crate::config::PkType::Uuid))
        })
        .unwrap_or(true); // default: treat as uuid (all standard tables use uuid PKs)

    // Issue UPDATE parent_id for each row that had a parentRef, and patch in-memory row.
    let update_sql = format!(
        "UPDATE {table} SET {pid} = $1 WHERE {pk_q} = $2",
        table = table_q,
        pid = q("parent_id"),
        pk_q = q(&pk),
    );
    for (i, opt_ref) in parent_refs.iter().enumerate() {
        let Some(ref_val) = opt_ref else { continue };
        let Some(parent_uuid_str) = ref_to_uuid.get(ref_val) else {
            continue; // unresolvable ref — leave parent_id NULL
        };
        let row_uuid_str = rows[i]
            .as_object()
            .and_then(|o| o.get(&pk))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let Some(row_uuid_str) = row_uuid_str else {
            continue;
        };

        if pk_is_uuid {
            let parent_uuid = uuid::Uuid::parse_str(parent_uuid_str)
                .map_err(|_| AppError::BadRequest(format!("invalid uuid: {}", parent_uuid_str)))?;
            let row_uuid = uuid::Uuid::parse_str(&row_uuid_str)
                .map_err(|_| AppError::BadRequest(format!("invalid uuid: {}", row_uuid_str)))?;
            match executor.executor {
                crate::service::TenantExecutorInner::Pool(pool) => {
                    sqlx::query(&update_sql)
                        .bind(parent_uuid)
                        .bind(row_uuid)
                        .execute(pool)
                        .await?;
                }
                crate::service::TenantExecutorInner::Conn(ref mut conn) => {
                    sqlx::query(&update_sql)
                        .bind(parent_uuid)
                        .bind(row_uuid)
                        .execute(&mut **conn)
                        .await?;
                }
            }
        } else {
            match executor.executor {
                crate::service::TenantExecutorInner::Pool(pool) => {
                    sqlx::query(&update_sql)
                        .bind(parent_uuid_str)
                        .bind(&row_uuid_str)
                        .execute(pool)
                        .await?;
                }
                crate::service::TenantExecutorInner::Conn(ref mut conn) => {
                    sqlx::query(&update_sql)
                        .bind(parent_uuid_str)
                        .bind(&row_uuid_str)
                        .execute(&mut **conn)
                        .await?;
                }
            }
        }

        if let Some(obj) = rows[i].as_object_mut() {
            obj.insert(
                "parent_id".to_string(),
                Value::String(parent_uuid_str.clone()),
            );
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn query_value_for_column(entity: &ResolvedEntity, col: &str, s: &str) -> Value {
    let col_info = entity.columns.iter().find(|c| c.name == col);
    let is_uuid = col_info
        .and_then(|c| c.pk_type.as_ref())
        .map(|t| matches!(t, PkType::Uuid))
        .unwrap_or(false)
        || col_info
            .and_then(|c| c.pg_type.as_deref())
            .map(|t| t.to_lowercase().contains("uuid"))
            .unwrap_or(false);
    let is_int = col_info
        .and_then(|c| c.pk_type.as_ref())
        .map(|t| matches!(t, PkType::BigInt | PkType::Int))
        .unwrap_or(false)
        || col_info
            .and_then(|c| c.pg_type.as_deref())
            .map(|t| {
                let l = t.to_lowercase();
                l.contains("int") || l.contains("serial")
            })
            .unwrap_or(false);
    let is_bool = col_info
        .and_then(|c| c.pg_type.as_deref())
        .map(|t| t.to_lowercase().starts_with("bool"))
        .unwrap_or(false);

    if is_uuid {
        if let Ok(u) = uuid::Uuid::parse_str(s) {
            return Value::String(u.to_string());
        }
    }
    if is_int {
        if let Ok(n) = s.parse::<i64>() {
            return Value::Number(n.into());
        }
    }
    if is_bool {
        if s.eq_ignore_ascii_case("true") {
            return Value::Bool(true);
        }
        if s.eq_ignore_ascii_case("false") {
            return Value::Bool(false);
        }
    }
    Value::String(s.to_string())
}

/// Collected file from a multipart upload field.
struct UploadedFile {
    field_name: String,
    filename: String,
    content_type: String,
    data: Vec<u8>,
}

/// Parse a multipart request into a text body map and a list of file fields.
///
/// Repeated text parts with the same field name are collected into a `Value::Array`
/// so that callers can send multiple existing paths for an `asset[]` column alongside
/// new file uploads under the same field name.
async fn parse_multipart(
    mut multipart: axum::extract::Multipart,
) -> Result<(HashMap<String, Value>, Vec<UploadedFile>), AppError> {
    // Accumulate all text values per field before converting to Value.
    let mut text_fields: HashMap<String, Vec<String>> = HashMap::new();
    let mut files: Vec<UploadedFile> = Vec::new();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?
    {
        let field_name = field.name().unwrap_or("").to_string();
        let filename = field.file_name().map(|s| s.to_string());
        let content_type = field
            .content_type()
            .unwrap_or("application/octet-stream")
            .to_string();
        let data = field
            .bytes()
            .await
            .map_err(|e| AppError::BadRequest(e.to_string()))?
            .to_vec();

        if let Some(fname) = filename {
            files.push(UploadedFile {
                field_name,
                filename: fname,
                content_type,
                data,
            });
        } else {
            let text = String::from_utf8(data).map_err(|e| {
                AppError::BadRequest(format!("field '{}' is not valid UTF-8: {}", field_name, e))
            })?;
            text_fields.entry(field_name).or_default().push(text);
        }
    }

    // Convert: single value → Value::String, multiple values → Value::Array.
    let body = text_fields
        .into_iter()
        .map(|(k, mut vals)| {
            let v = if vals.len() == 1 {
                Value::String(vals.remove(0))
            } else {
                Value::Array(vals.into_iter().map(Value::String).collect())
            };
            (k, v)
        })
        .collect();

    Ok((body, files))
}

/// Return an error if the entity has asset/asset[] columns but no storage provider is configured.
/// Called at the top of every write handler so the error is immediate and descriptive.
fn require_storage_for_assets(state: &AppState, entity: &ResolvedEntity) -> Result<(), AppError> {
    if state.storage.is_none() {
        let asset_cols: Vec<&str> = entity
            .columns
            .iter()
            .filter(|c| c.is_asset)
            .map(|c| c.name.as_str())
            .collect();
        if !asset_cols.is_empty() {
            return Err(AppError::BadRequest(format!(
                "entity '{}' has asset column(s) [{}] but no storage provider is configured. \
                 Set the STORAGE_PROVIDER environment variable (s3 | azure | gcs | rustfs).",
                entity.path_segment,
                asset_cols.join(", ")
            )));
        }
    }
    Ok(())
}

/// Upload all file fields that correspond to asset columns, inserting paths into body.
/// For `asset[]` columns, multiple files with the same field name are collected into a JSON array.
async fn process_asset_uploads(
    state: &AppState,
    entity: &ResolvedEntity,
    tenant_id: &str,
    body: &mut HashMap<String, Value>,
    files: Vec<UploadedFile>,
) -> Result<(), AppError> {
    if files.is_empty() {
        return Ok(());
    }
    let storage = state.storage.as_ref().ok_or_else(|| {
        AppError::BadRequest("storage is not configured (set STORAGE_PROVIDER env var)".into())
    })?;

    // Group files by field name to support asset[] (multiple files per column).
    let mut groups: std::collections::BTreeMap<String, Vec<UploadedFile>> =
        std::collections::BTreeMap::new();
    for file in files {
        groups
            .entry(file.field_name.clone())
            .or_default()
            .push(file);
    }

    for (field_name, group) in groups {
        let col_name = to_snake_case(&field_name);
        let col = entity
            .columns
            .iter()
            .find(|c| c.name == col_name)
            .ok_or_else(|| AppError::BadRequest(format!("unknown field: {}", field_name)))?;

        if !col.is_asset {
            return Err(AppError::BadRequest(format!(
                "field '{}' is not an asset column",
                field_name
            )));
        }

        if col.asset_is_array {
            // Seed with any existing paths sent as text parts (Option B merge semantics):
            // clients re-send paths they want to keep; omitted paths are dropped.
            let mut paths: Vec<Value> = match body.get(&col_name) {
                Some(Value::Array(arr)) => arr.clone(),
                Some(Value::String(s)) if !s.is_empty() => vec![Value::String(s.clone())],
                _ => Vec::new(),
            };
            // Upload new files and append their paths.
            for file in group {
                if let Some(rule) = entity.validation.get(&col_name) {
                    validate_asset_field(
                        &col_name,
                        &file.filename,
                        &file.content_type,
                        file.data.len(),
                        rule,
                    )?;
                }
                let asset_cfg = col.asset_config.as_ref();
                let compression = asset_cfg
                    .and_then(|c| c.compression.as_deref())
                    .unwrap_or("none");
                let data = compress(file.data, compression)?;
                let prefix_template = asset_cfg
                    .and_then(|c| c.prefix.as_deref())
                    .unwrap_or("{entity}/{yyyy}/{mm}/{dd}");
                let prefix = resolve_prefix(prefix_template, tenant_id, &entity.table_name);
                let ext = std::path::Path::new(&file.filename)
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(|e| format!(".{}", e))
                    .unwrap_or_default();
                let object_name = format!("{}/{}{}", prefix, uuid::Uuid::new_v4(), ext);
                storage
                    .upload(&object_name, data, &file.content_type)
                    .await?;
                paths.push(Value::String(object_name));
            }
            body.insert(col_name, Value::Array(paths));
        } else {
            // Single-file asset: take the first (and expected only) file.
            let file = group.into_iter().next().unwrap();
            if let Some(rule) = entity.validation.get(&col_name) {
                validate_asset_field(
                    &col_name,
                    &file.filename,
                    &file.content_type,
                    file.data.len(),
                    rule,
                )?;
            }
            let asset_cfg = col.asset_config.as_ref();
            let compression = asset_cfg
                .and_then(|c| c.compression.as_deref())
                .unwrap_or("none");
            let data = compress(file.data, compression)?;
            let prefix_template = asset_cfg
                .and_then(|c| c.prefix.as_deref())
                .unwrap_or("{entity}/{yyyy}/{mm}/{dd}");
            let prefix = resolve_prefix(prefix_template, tenant_id, &entity.table_name);
            let ext = std::path::Path::new(&file.filename)
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| format!(".{}", e))
                .unwrap_or_default();
            let object_name = format!("{}/{}{}", prefix, uuid::Uuid::new_v4(), ext);
            storage
                .upload(&object_name, data, &file.content_type)
                .await?;
            body.insert(col_name, Value::String(object_name));
        }
    }
    Ok(())
}

/// Upload a single JSON value (Object or Array) to storage and return its path.
async fn upload_json_value(
    state: &AppState,
    entity: &ResolvedEntity,
    tenant_id: &str,
    col_name: &str,
    asset_cfg: Option<&crate::config::AssetColumnConfig>,
    val: &Value,
) -> Result<String, AppError> {
    let storage = state.storage.as_ref().ok_or_else(|| {
        AppError::BadRequest("storage is not configured (set STORAGE_PROVIDER env var)".into())
    })?;

    let data = serde_json::to_vec(val)
        .map_err(|e| AppError::BadRequest(format!("failed to serialize {}: {}", col_name, e)))?;

    // Validate size if rules exist.
    if let Some(rule) = entity.validation.get(col_name) {
        if let Some(max_mb) = rule.max_size_mb {
            let limit = (max_mb * 1024.0 * 1024.0) as usize;
            if data.len() > limit {
                return Err(AppError::Validation(format!(
                    "{}: JSON payload {} bytes exceeds maximum of {:.1} MB",
                    col_name,
                    data.len(),
                    max_mb
                )));
            }
        }
    }

    let compression = asset_cfg
        .and_then(|c| c.compression.as_deref())
        .unwrap_or("none");
    let data = compress(data, compression)?;

    let prefix_template = asset_cfg
        .and_then(|c| c.prefix.as_deref())
        .unwrap_or("{entity}/{yyyy}/{mm}/{dd}");
    let prefix = resolve_prefix(prefix_template, tenant_id, &entity.table_name);
    let object_name = format!("{}/{}.json", prefix, uuid::Uuid::new_v4());

    storage
        .upload(&object_name, data, "application/json")
        .await?;
    Ok(object_name)
}

/// For JSON create/update: detect asset columns whose value needs upload treatment.
///
/// `asset` columns:  Object/Array value → serialize to JSON, upload, replace with path string.
///                   Plain strings pass through unchanged (pre-existing path).
///
/// `asset[]` columns: Value::Array of elements is processed element-by-element:
///                    - String elements pass through unchanged (pre-existing paths).
///                    - Object/Array elements are serialized to JSON, uploaded, replaced with paths.
///                    A top-level Value::Array whose elements are already plain strings is a no-op.
async fn process_json_asset_fields(
    state: &AppState,
    entity: &ResolvedEntity,
    tenant_id: &str,
    body: &mut HashMap<String, Value>,
) -> Result<(), AppError> {
    let asset_cols: Vec<_> = entity.columns.iter().filter(|c| c.is_asset).collect();
    if asset_cols.is_empty() {
        return Ok(());
    }

    // Quick check: any asset column has a value that needs upload.
    let needs_upload = asset_cols.iter().any(|c| {
        match body.get(&c.name) {
            Some(Value::Object(_)) => true,
            Some(Value::Array(arr)) => {
                if c.asset_is_array {
                    // needs upload if any element is not a plain string
                    arr.iter().any(|el| !matches!(el, Value::String(_)))
                } else {
                    // single asset: an Array value means serialize-the-whole-thing
                    true
                }
            }
            _ => false,
        }
    });
    if !needs_upload {
        return Ok(());
    }

    for col in asset_cols {
        let asset_cfg = col.asset_config.as_ref();

        if col.asset_is_array {
            // Expect a JSON array of path strings or JSON objects/arrays to upload.
            let arr = match body.get(&col.name) {
                Some(Value::Array(a)) => a.clone(),
                Some(Value::Null) | None => continue,
                Some(other) => {
                    // Wrap a single value into an array for convenience.
                    vec![other.clone()]
                }
            };

            let mut paths: Vec<Value> = Vec::with_capacity(arr.len());
            for element in arr {
                match element {
                    Value::String(s) => {
                        // Pre-existing path — pass through.
                        paths.push(Value::String(s));
                    }
                    other @ Value::Object(_) | other @ Value::Array(_) => {
                        let path = upload_json_value(
                            state, entity, tenant_id, &col.name, asset_cfg, &other,
                        )
                        .await?;
                        paths.push(Value::String(path));
                    }
                    _ => {
                        // Null, number, bool — pass through as-is.
                        paths.push(element);
                    }
                }
            }
            body.insert(col.name.clone(), Value::Array(paths));
        } else {
            // Single-asset column.
            let val = match body.get(&col.name) {
                Some(v @ Value::Object(_)) | Some(v @ Value::Array(_)) => v.clone(),
                _ => continue,
            };
            let path =
                upload_json_value(state, entity, tenant_id, &col.name, asset_cfg, &val).await?;
            body.insert(col.name.clone(), Value::String(path));
        }
    }
    Ok(())
}

/// Replace asset column values in a row with presigned URLs for the columns listed in `sign_cols`.
/// `sign_cols` is None → sign all asset columns. Some(set) → sign only those columns.
/// For `asset[]` columns the stored value is a JSON array; each path string is presigned individually.
async fn sign_row_assets(
    state: &AppState,
    entity: &ResolvedEntity,
    row: &mut Value,
    sign_cols: &Option<HashSet<String>>,
    expires: u64,
) -> Result<(), AppError> {
    let storage = match &state.storage {
        Some(s) => s,
        None => return Ok(()),
    };
    if let Value::Object(map) = row {
        for col in &entity.columns {
            if !col.is_asset {
                continue;
            }
            let should_sign = sign_cols
                .as_ref()
                .map(|s| s.contains(&col.name))
                .unwrap_or(true);
            if !should_sign {
                continue;
            }
            let camel = crate::case::to_camel_case(&col.name);
            let key = if map.contains_key(&col.name) {
                col.name.as_str()
            } else {
                camel.as_str()
            };

            if col.asset_is_array {
                // asset[] — presign each path string in the array.
                if let Some(Value::Array(arr)) = map.get(key).cloned() {
                    let mut signed: Vec<Value> = Vec::with_capacity(arr.len());
                    for el in arr {
                        if let Value::String(path) = &el {
                            if path.is_empty() {
                                signed.push(el);
                            } else {
                                let result = storage.presign_url(path, expires).await?;
                                signed.push(Value::String(result.url));
                            }
                        } else {
                            signed.push(el);
                        }
                    }
                    map.insert(key.to_string(), Value::Array(signed));
                }
            } else {
                // Single asset — presign the path string.
                if let Some(Value::String(path)) = map.get(key).cloned() {
                    if path.is_empty() {
                        continue;
                    }
                    let result = storage.presign_url(&path, expires).await?;
                    map.insert(key.to_string(), Value::String(result.url));
                }
            }
        }
    }
    Ok(())
}

// ── Asset storage cleanup helpers ────────────────────────────────────────────

/// Extract all non-empty path strings stored in an asset or asset[] column of a DB row.
fn collect_row_asset_paths(row: &Value, col_name: &str) -> Vec<String> {
    match row.get(col_name) {
        Some(Value::String(s)) if !s.is_empty() => vec![s.clone()],
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| {
                if let Value::String(s) = v {
                    if !s.is_empty() {
                        Some(s.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Best-effort delete of asset files that were present in `old_row` but are absent from
/// `new_body` after a PATCH update. Only checks columns that appear in `new_body` (PATCH
/// partial-update semantics: columns not included in the body are not being changed).
/// Errors are logged as warnings — storage failures never abort the database write.
async fn delete_dropped_asset_paths(
    state: &AppState,
    entity: &ResolvedEntity,
    old_row: &Value,
    new_body: &HashMap<String, Value>,
) {
    let storage = match &state.storage {
        Some(s) => s,
        None => return,
    };

    let new_paths_for = |col_name: &str| -> HashSet<String> {
        match new_body.get(col_name) {
            Some(Value::String(s)) if !s.is_empty() => {
                let mut set = HashSet::new();
                set.insert(s.clone());
                set
            }
            Some(Value::Array(arr)) => arr
                .iter()
                .filter_map(|v| {
                    if let Value::String(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
                .collect(),
            _ => HashSet::new(),
        }
    };

    for col in &entity.columns {
        if !col.is_asset {
            continue;
        }
        // Skip columns not being updated in this PATCH (not in body → unchanged).
        if !new_body.contains_key(&col.name) {
            continue;
        }
        let old_paths = collect_row_asset_paths(old_row, &col.name);
        let new_paths = new_paths_for(&col.name);
        for path in old_paths {
            if !new_paths.contains(&path) {
                if let Err(e) = storage.delete(&path).await {
                    tracing::warn!(path = %path, error = %e, "failed to delete dropped asset from storage");
                }
            }
        }
    }
}

/// Best-effort delete of all asset files attached to a row, called when the DB record
/// itself is being deleted. Errors are logged as warnings.
async fn delete_all_asset_paths(state: &AppState, entity: &ResolvedEntity, row: &Value) {
    let storage = match &state.storage {
        Some(s) => s,
        None => return,
    };
    for col in &entity.columns {
        if !col.is_asset {
            continue;
        }
        for path in collect_row_asset_paths(row, &col.name) {
            if let Err(e) = storage.delete(&path).await {
                tracing::warn!(path = %path, error = %e, "failed to delete asset from storage on record delete");
            }
        }
    }
}

/// Resolve include names to (name, spec, related_entity). Call with model read lock held.
fn resolve_includes(
    model: &ResolvedModel,
    entity: &ResolvedEntity,
    include_names: &[String],
) -> Result<Vec<(String, crate::config::IncludeSpec, ResolvedEntity)>, AppError> {
    let mut out = Vec::new();
    for name in include_names {
        let spec = entity
            .includes
            .iter()
            .find(|i| i.name.as_str() == name.as_str())
            .ok_or_else(|| AppError::BadRequest(format!("unknown include: {}", name)))?
            .clone();
        let related = model
            .entity_by_path(&spec.related_path_segment)
            .cloned()
            .ok_or_else(|| {
                AppError::BadRequest(format!(
                    "related entity not found: {}",
                    spec.related_path_segment
                ))
            })?;
        out.push((name.clone(), spec, related));
    }
    Ok(out)
}

/// Resolved tenant context: pool (or pool to acquire from for RLS), schema override, and for RLS the tenant_id to set.
pub enum TenantContext {
    Pool {
        pool: crate::db::pool::Pool,
        schema_override: Option<String>,
        config_pool: crate::db::pool::Pool,
        package_cache_key: String,
    },
    Rls {
        tenant_id: String,
        pool: crate::db::pool::Pool,
        config_pool: crate::db::pool::Pool,
        package_cache_key: String,
    },
}

impl TenantContext {
    pub fn config_pool(&self) -> &crate::db::pool::Pool {
        match self {
            TenantContext::Pool { config_pool, .. } | TenantContext::Rls { config_pool, .. } => {
                config_pool
            }
        }
    }
    /// Pool used for DDL (migrations) and entity data. For schema/rls with database_url this is the tenant DB; otherwise architect DB.
    pub fn migration_pool(&self) -> &crate::db::pool::Pool {
        match self {
            TenantContext::Pool { pool, .. } | TenantContext::Rls { pool, .. } => pool,
        }
    }
    /// When set (schema strategy), create schemas/tables in this schema on the migration pool.
    pub fn schema_override(&self) -> Option<&str> {
        match self {
            TenantContext::Pool {
                schema_override, ..
            } => schema_override.as_deref(),
            TenantContext::Rls { .. } => None,
        }
    }
    pub fn package_cache_key(&self) -> &str {
        match self {
            TenantContext::Pool {
                package_cache_key, ..
            }
            | TenantContext::Rls {
                package_cache_key, ..
            } => package_cache_key,
        }
    }
    /// When RLS strategy: column name to set on INSERT (e.g. "tenant_id"). Used by migrations and CRUD.
    pub fn rls_tenant_column(&self) -> Option<&'static str> {
        match self {
            TenantContext::Rls { .. } => Some(crate::migration::RLS_TENANT_COLUMN),
            TenantContext::Pool { .. } => None,
        }
    }
    /// When RLS strategy: value to set for tenant_id on INSERT (the tenant id from X-Tenant-ID).
    pub fn rls_tenant_id(&self) -> Option<&str> {
        match self {
            TenantContext::Rls { tenant_id, .. } => Some(tenant_id),
            TenantContext::Pool { .. } => None,
        }
    }
}

/// Resolve execution context from tenant id. X-Tenant-ID is required; returns 400 if missing, 404 if tenant unknown.
/// For package_id_opt: when None (default routes), package_cache_key is DEFAULT_PACKAGE_ID.
pub async fn resolve_tenant_context(
    state: &AppState,
    tenant_id_opt: Option<&str>,
    package_id_opt: Option<&str>,
) -> Result<TenantContext, AppError> {
    let tenant_id = tenant_id_opt
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("X-Tenant-ID header is required".into()))?;

    let package_id = package_id_opt.unwrap_or(DEFAULT_PACKAGE_ID);
    let package_cache_key = package_id.to_string();

    let entry = state
        .tenant_registry
        .get(tenant_id)
        .ok_or_else(|| AppError::NotFound(format!("tenant not found: {}", tenant_id)))?;

    // Architect schema and _sys_* config tables exist only in DATABASE_URL (from .env), always in the architect schema. Tenant DBs are used for app data/migrations only.
    let architect_pool = state.pool.clone();

    match &entry.strategy {
        TenantStrategy::Database => {
            let database_url = entry.database_url.as_deref().ok_or_else(|| {
                AppError::BadRequest(format!(
                    "tenant {}: strategy database requires database_url",
                    tenant_id
                ))
            })?;
            let pool = get_or_create_tenant_pool(state, tenant_id, database_url).await?;
            Ok(TenantContext::Pool {
                pool: pool.clone(),
                schema_override: None,
                config_pool: architect_pool,
                package_cache_key: format!("{}:{}", package_id, tenant_id),
            })
        }
        TenantStrategy::Rls => {
            let pool = match entry.database_url.as_deref() {
                Some(url) => get_or_create_tenant_pool(state, tenant_id, url).await?,
                None => architect_pool.clone(),
            };
            Ok(TenantContext::Rls {
                tenant_id: tenant_id.to_string(),
                pool,
                config_pool: architect_pool,
                package_cache_key,
            })
        }
    }
}

/// Get or create a pool for the given tenant_id and database_url. Config lives in architect DB; this pool is for app data when tenant uses a different DB.
pub async fn get_or_create_tenant_pool(
    state: &AppState,
    tenant_id: &str,
    database_url: &str,
) -> Result<crate::db::pool::Pool, AppError> {
    let existing = {
        let guard = state
            .tenant_pools
            .read()
            .map_err(|_| AppError::BadRequest("state lock".into()))?;
        guard.get(tenant_id).cloned()
    };
    if let Some(p) = existing {
        return Ok(p);
    }
    let new_pool = {
        #[cfg(feature = "postgres")]
        {
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(5)
                .connect(database_url)
                .await?
        }
        #[cfg(feature = "mysql")]
        {
            sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(5)
                .connect(database_url)
                .await?
        }
        #[cfg(feature = "sqlite")]
        {
            sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(5)
                .connect(database_url)
                .await?
        }
        #[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
        {
            return Err(AppError::BadRequest(
                "no database dialect feature enabled".into(),
            ));
        }
    };
    {
        let mut guard = state
            .tenant_pools
            .write()
            .map_err(|_| AppError::BadRequest("state lock".into()))?;
        guard
            .entry(tenant_id.to_string())
            .or_insert_with(|| new_pool.clone());
    }
    Ok(new_pool)
}

/// Get resolved model for a package from cache, or load from config_pool and cache it under cache_key.
/// package_id is used for load_from_pool (config table package_id); cache_key is for the in-memory cache (e.g. "pkg" or "pkg:tenant_id").
pub(crate) async fn get_or_load_package_model(
    state: &AppState,
    config_pool: &crate::db::pool::Pool,
    cache_key: &str,
    package_id: &str,
) -> Result<ResolvedModel, AppError> {
    {
        let guard = state
            .package_models
            .read()
            .map_err(|_| AppError::BadRequest("state lock".into()))?;
        if let Some(m) = guard.get(cache_key) {
            return Ok(m.clone());
        }
    }
    let config = load_from_pool(config_pool, package_id)
        .await
        .map_err(AppError::Config)?;
    let model = resolve(&config)
        .map_err(AppError::Config)?
        .with_package_id(package_id);
    state
        .package_models
        .write()
        .map_err(|_| AppError::BadRequest("state lock".into()))?
        .insert(cache_key.to_string(), model.clone());
    Ok(model)
}

/// Post-process rows from single-query list_with_includes: parse JSON include columns if string, strip sensitive and camelCase nested objects.
fn post_process_include_columns(
    rows: &mut [Value],
    resolved_includes: &[(String, crate::config::IncludeSpec, ResolvedEntity)],
) {
    for row in rows.iter_mut() {
        if let Value::Object(map) = row {
            for (name, _spec, related) in resolved_includes {
                let Some(included) = map.get_mut(name) else {
                    continue;
                };
                if let Value::String(s) = included {
                    if let Ok(parsed) = serde_json::from_str::<Value>(s) {
                        *included = parsed;
                    }
                }
                match included {
                    Value::Array(arr) => {
                        for item in arr.iter_mut() {
                            if let Value::Object(_) = item {
                                strip_sensitive_columns(item, &related.sensitive_columns);
                                value_keys_to_camel_case(item);
                            }
                        }
                    }
                    Value::Object(_) => {
                        strip_sensitive_columns(included, &related.sensitive_columns);
                        value_keys_to_camel_case(included);
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Attach related-entity data to rows. Modifies each row in place. resolved_includes from resolve_includes (so lock can be dropped before calling).
async fn attach_includes<'a>(
    executor: &mut TenantExecutor<'a>,
    schema_override: Option<&str>,
    _entity: &ResolvedEntity,
    rows: &mut [Value],
    resolved_includes: &[(String, crate::config::IncludeSpec, ResolvedEntity)],
    dialect: &dyn crate::db::Dialect,
) -> Result<(), AppError> {
    if resolved_includes.is_empty() || rows.is_empty() {
        return Ok(());
    }
    for (name, spec, related) in resolved_includes {
        match &spec.direction {
            IncludeDirection::ToOne => {
                let keys: Vec<Value> = rows
                    .iter()
                    .filter_map(|r| r.get(&spec.our_key_column).cloned())
                    .collect();
                let related_rows = CrudService::fetch_where_column_in(
                    executor,
                    related,
                    &spec.their_key_column,
                    &keys,
                    schema_override,
                    dialect,
                )
                .await?;
                let mut key_to_row: HashMap<String, Value> = HashMap::new();
                for mut r in related_rows {
                    let k = r
                        .get(&spec.their_key_column)
                        .cloned()
                        .map(|v| serde_json::to_string(&v).unwrap_or_default())
                        .unwrap_or_default();
                    key_to_row.entry(k).or_insert_with(|| {
                        strip_sensitive_columns(&mut r, &related.sensitive_columns);
                        value_keys_to_camel_case(&mut r);
                        r
                    });
                }
                for row in rows.iter_mut() {
                    if let Value::Object(ref mut map) = row {
                        let key_val = map
                            .get(&spec.our_key_column)
                            .cloned()
                            .unwrap_or(Value::Null);
                        let key = serde_json::to_string(&key_val).unwrap_or_default();
                        let included = key_to_row.get(&key).cloned().unwrap_or(Value::Null);
                        map.insert(name.clone(), included);
                    }
                }
            }
            IncludeDirection::ToMany => {
                let keys: Vec<Value> = rows
                    .iter()
                    .filter_map(|r| r.get(&spec.our_key_column).cloned())
                    .collect();
                let related_rows = CrudService::fetch_where_column_in(
                    executor,
                    related,
                    &spec.their_key_column,
                    &keys,
                    schema_override,
                    dialect,
                )
                .await?;
                let mut grouped: HashMap<String, Vec<Value>> = HashMap::new();
                for mut r in related_rows {
                    let k = r
                        .get(&spec.their_key_column)
                        .cloned()
                        .map(|v| serde_json::to_string(&v).unwrap_or_default())
                        .unwrap_or_default();
                    strip_sensitive_columns(&mut r, &related.sensitive_columns);
                    value_keys_to_camel_case(&mut r);
                    grouped.entry(k).or_default().push(r);
                }
                for row in rows.iter_mut() {
                    if let Value::Object(ref mut map) = row {
                        let key_val = map
                            .get(&spec.our_key_column)
                            .cloned()
                            .unwrap_or(Value::Null);
                        let key = serde_json::to_string(&key_val).unwrap_or_default();
                        let arr = grouped.get(&key).cloned().unwrap_or_default();
                        map.insert(name.clone(), Value::Array(arr));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Collect unique include-name prefixes from dotted filter fields (e.g. "transport_unit" from "transport_unit.bay==x").
fn collect_dotted_prefixes(filter: Option<&FilterNode>) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(node) = filter {
        collect_dotted_prefixes_rec(node, &mut out);
    }
    out
}

fn collect_dotted_prefixes_rec(node: &FilterNode, out: &mut Vec<String>) {
    match node {
        FilterNode::And(children) | FilterNode::Or(children) => {
            for c in children {
                collect_dotted_prefixes_rec(c, out);
            }
        }
        FilterNode::Leaf { field, .. } => {
            if let Some(dot_pos) = field.find('.') {
                let prefix = field[..dot_pos].to_string();
                if !out.contains(&prefix) {
                    out.push(prefix);
                }
            }
        }
    }
}

pub async fn list(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path(path_segment): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), None).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };

    let entity = state
        .model
        .read()
        .map_err(|_| AppError::BadRequest("state lock".into()))?
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "read") {
        return Err(AppError::BadRequest("read not allowed".into()));
    }
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "get",
    )
    .await?;
    let mut limit: Option<u32> = None;
    let mut offset: Option<u32> = None;
    let mut include_names: Vec<String> = Vec::new();
    let mut filter_str: Option<String> = None;
    let mut sort_str: Option<String> = None;
    let mut sign_param: Option<String> = None;
    let mut sign_expires: u64 = 900;

    for (k, v) in params {
        match k.as_str() {
            "limit" => limit = v.parse().ok(),
            "offset" => offset = v.parse().ok(),
            "include" => {
                include_names = v
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            }
            "q" => filter_str = Some(v),
            "sort" => sort_str = Some(v),
            "sign" => sign_param = Some(v),
            "sign_expires" => sign_expires = v.parse().unwrap_or(900),
            _ => {}
        }
    }

    let filter: Option<FilterNode> = filter_str.as_deref().map(parse_rsql).transpose()?;
    let sort = sort_str.as_deref().map(parse_sort).unwrap_or_default();

    // Resolve which asset columns to sign (None = all, Some(set) = named subset).
    let sign_cols: Option<HashSet<String>> = sign_param.as_deref().and_then(|s| {
        if s == "true" {
            None // sign all asset columns
        } else {
            Some(s.split(',').map(|c| to_snake_case(c.trim())).collect())
        }
    });

    // All includes needed: explicit include= names + any prefixes from dotted filter fields.
    // Resolved once so the model lock is acquired only once.
    let filter_prefix_names = collect_dotted_prefixes(filter.as_ref());
    let all_include_names: Vec<String> = {
        let mut names = include_names.clone();
        for n in &filter_prefix_names {
            if !names.contains(n) {
                names.push(n.clone());
            }
        }
        names
    };
    let resolved_all: Vec<(String, crate::config::IncludeSpec, ResolvedEntity)> =
        if !all_include_names.is_empty() {
            let model = state
                .model
                .read()
                .map_err(|_| AppError::BadRequest("state lock".into()))?;
            resolve_includes(&model, &entity, &all_include_names)?
        } else {
            Vec::new()
        };

    // filter_includes = all resolved (for EXISTS generation on dotted filters)
    let filter_include_selects: Vec<IncludeSelect> = resolved_all
        .iter()
        .map(|(name, spec, related)| IncludeSelect {
            name: name.as_str(),
            direction: spec.direction.clone(),
            related,
            our_key: spec.our_key_column.as_str(),
            their_key: spec.their_key_column.as_str(),
        })
        .collect();

    // data includes = only those explicitly requested via include= param (scalar subqueries)
    let resolved_data: Vec<_> = resolved_all
        .iter()
        .filter(|(name, _, _)| include_names.contains(name))
        .cloned()
        .collect();

    let mut rows = if include_names.is_empty() {
        CrudService::list(
            &mut executor,
            &entity,
            filter.as_ref(),
            &sort,
            limit,
            offset,
            &filter_include_selects,
            schema_override,
            state.dialect.as_ref(),
        )
        .await?
    } else {
        let data_include_selects: Vec<IncludeSelect> = resolved_data
            .iter()
            .map(|(name, spec, related)| IncludeSelect {
                name: name.as_str(),
                direction: spec.direction.clone(),
                related,
                our_key: spec.our_key_column.as_str(),
                their_key: spec.their_key_column.as_str(),
            })
            .collect();
        let mut rows = CrudService::list_with_includes(
            &mut executor,
            &entity,
            filter.as_ref(),
            &sort,
            limit,
            offset,
            data_include_selects.as_slice(),
            &filter_include_selects,
            schema_override,
            state.dialect.as_ref(),
        )
        .await?;
        post_process_include_columns(&mut rows, &resolved_data);
        rows
    };
    if let Some(ref ref_col) = entity.parent_ref_column.clone() {
        enrich_with_parent_ref(&mut executor, &mut rows, &entity, ref_col, schema_override).await?;
    }
    for row in &mut rows {
        strip_sensitive_columns(row, &entity.sensitive_columns);
        value_keys_to_camel_case(row);
    }

    // Presign asset columns when ?sign= is present.
    if sign_param.is_some() {
        for row in &mut rows {
            sign_row_assets(&state, &entity, row, &sign_cols, sign_expires).await?;
        }
    }

    let count = rows.len() as u64;
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessMany {
            data: rows,
            meta: crate::response::MetaCount { count },
        }),
    ))
}

pub async fn create(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path(path_segment): Path<String>,
    request: Request,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let tenant_id_str = tenant_id_opt.as_deref().unwrap_or("").to_string();
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), None).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = state
        .model
        .read()
        .map_err(|_| AppError::BadRequest("state lock".into()))?
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "create") {
        return Err(AppError::BadRequest("create not allowed".into()));
    }
    require_storage_for_assets(&state, &entity)?;
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "post",
    )
    .await?;

    // Dispatch by Content-Type: multipart for file uploads, JSON for everything else.
    let is_multipart = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.contains("multipart/form-data"))
        .unwrap_or(false);

    let mut body;
    if is_multipart {
        let multipart = axum::extract::Multipart::from_request(request, &state)
            .await
            .map_err(|e| AppError::BadRequest(e.to_string()))?;
        let (text_fields, files) = parse_multipart(multipart).await?;
        body = hashmap_keys_to_snake_case(&text_fields);
        process_asset_uploads(&state, &entity, &tenant_id_str, &mut body, files).await?;
    } else {
        let Json(json_body) = Json::<Value>::from_request(request, &state)
            .await
            .map_err(|e| AppError::BadRequest(e.to_string()))?;
        body = hashmap_keys_to_snake_case(&body_to_map(json_body)?);
        process_json_asset_fields(&state, &entity, &tenant_id_str, &mut body).await?;
    }

    RequestValidator::validate(&body, &entity.validation)?;
    let mut row = CrudService::create(
        &mut executor,
        &entity,
        &body,
        schema_override,
        ctx.rls_tenant_id(),
        user_id_opt.as_deref(),
        state.dialect.as_ref(),
    )
    .await?;
    let raw_row = row.clone();
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    if let Some(client) = &state.event_client {
        spawn_events(
            std::sync::Arc::clone(client),
            &entity,
            "create",
            raw_row,
            row.clone(),
            tenant_id_str,
            None,
        );
    }
    Ok((
        axum::http::StatusCode::CREATED,
        Json(crate::response::SuccessOne {
            data: row,
            meta: None,
        }),
    ))
}

pub async fn read(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path((path_segment, id_str)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), None).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = state
        .model
        .read()
        .map_err(|_| AppError::BadRequest("state lock".into()))?
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "read") {
        return Err(AppError::BadRequest("read not allowed".into()));
    }
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "get",
    )
    .await?;
    let id = parse_id(&id_str, &entity.pk_type)?;
    let mut row = CrudService::read(
        &mut executor,
        &entity,
        &id,
        schema_override,
        state.dialect.as_ref(),
    )
    .await?
    .ok_or_else(|| AppError::NotFound(id_str))?;
    let include_names: Vec<String> = params
        .get("include")
        .map(|s| {
            s.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if !include_names.is_empty() {
        let resolved = {
            let model = state
                .model
                .read()
                .map_err(|_| AppError::BadRequest("state lock".into()))?;
            resolve_includes(&model, &entity, &include_names)?
        };
        let mut rows = [row];
        attach_includes(
            &mut executor,
            schema_override,
            &entity,
            &mut rows,
            &resolved,
            state.dialect.as_ref(),
        )
        .await?;
        row = rows[0].clone();
    }
    if let Some(ref ref_col) = entity.parent_ref_column.clone() {
        let mut rows = [row];
        enrich_with_parent_ref(&mut executor, &mut rows, &entity, ref_col, schema_override).await?;
        row = rows.into_iter().next().unwrap();
    }
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);

    // Presign asset columns when ?sign= is present.
    let sign_param = params.get("sign").cloned();
    if sign_param.is_some() {
        let sign_expires: u64 = params
            .get("sign_expires")
            .and_then(|s| s.parse().ok())
            .unwrap_or(900);
        let sign_cols: Option<HashSet<String>> = sign_param.as_deref().and_then(|s| {
            if s == "true" {
                None
            } else {
                Some(s.split(',').map(|c| to_snake_case(c.trim())).collect())
            }
        });
        sign_row_assets(&state, &entity, &mut row, &sign_cols, sign_expires).await?;
    }

    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessOne {
            data: row,
            meta: None,
        }),
    ))
}

pub async fn update(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path((path_segment, id_str)): Path<(String, String)>,
    request: Request,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let tenant_id_str = tenant_id_opt.as_deref().unwrap_or("").to_string();
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), None).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = state
        .model
        .read()
        .map_err(|_| AppError::BadRequest("state lock".into()))?
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "update") {
        return Err(AppError::BadRequest("update not allowed".into()));
    }
    require_storage_for_assets(&state, &entity)?;
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "patch",
    )
    .await?;
    let id = parse_id(&id_str, &entity.pk_type)?;

    let is_multipart = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.contains("multipart/form-data"))
        .unwrap_or(false);

    let mut body;
    if is_multipart {
        let multipart = axum::extract::Multipart::from_request(request, &state)
            .await
            .map_err(|e| AppError::BadRequest(e.to_string()))?;
        let (text_fields, files) = parse_multipart(multipart).await?;
        body = hashmap_keys_to_snake_case(&text_fields);
        process_asset_uploads(&state, &entity, &tenant_id_str, &mut body, files).await?;
    } else {
        let Json(json_body) = Json::<Value>::from_request(request, &state)
            .await
            .map_err(|e| AppError::BadRequest(e.to_string()))?;
        body = hashmap_keys_to_snake_case(&body_to_map(json_body)?);
        process_json_asset_fields(&state, &entity, &tenant_id_str, &mut body).await?;
    }

    RequestValidator::validate_partial(&body, &entity.validation)?;

    // Pre-fetch the current DB row when needed:
    //   • entity has asset columns + storage configured → hard-delete dropped files after update
    //   • event triggers with changed_to conditions → detect genuine field transitions
    let entity_has_assets = entity.columns.iter().any(|c| c.is_asset);
    let needs_pre_read = (entity_has_assets && state.storage.is_some())
        || (state.event_client.is_some()
            && entity.events.iter().any(|e| {
                e.on == "update" && e.condition.as_ref().is_some_and(|c| c.changed_to.is_some())
            }));
    let pre_update_row = if needs_pre_read {
        CrudService::read(
            &mut executor,
            &entity,
            &id,
            schema_override,
            state.dialect.as_ref(),
        )
        .await?
    } else {
        None
    };

    let mut row = CrudService::update(
        &mut executor,
        &entity,
        &id,
        &body,
        schema_override,
        user_id_opt.as_deref(),
        state.dialect.as_ref(),
    )
    .await?
    .ok_or_else(|| AppError::NotFound(id_str))?;

    // Hard-delete any asset files dropped from storage after a successful DB write.
    if let Some(ref old_row) = pre_update_row {
        if entity_has_assets {
            delete_dropped_asset_paths(&state, &entity, old_row, &body).await;
        }
    }

    let raw_row = row.clone();
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    if let Some(client) = &state.event_client {
        spawn_events(
            std::sync::Arc::clone(client),
            &entity,
            "update",
            raw_row,
            row.clone(),
            tenant_id_str,
            pre_update_row,
        );
    }
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessOne {
            data: row,
            meta: None,
        }),
    ))
}

pub async fn delete(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path((path_segment, id_str)): Path<(String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), None).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = state
        .model
        .read()
        .map_err(|_| AppError::BadRequest("state lock".into()))?
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "delete") {
        return Err(AppError::BadRequest("delete not allowed".into()));
    }
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "delete",
    )
    .await?;
    let id = parse_id(&id_str, &entity.pk_type)?;
    // Prefetch the full row before deletion for event triggers and asset storage cleanup.
    let entity_has_assets = entity.columns.iter().any(|c| c.is_asset);
    let pre_delete_row = if (state.event_client.is_some() && !entity.events.is_empty())
        || (entity_has_assets && state.storage.is_some())
    {
        CrudService::read(
            &mut executor,
            &entity,
            &id,
            schema_override,
            state.dialect.as_ref(),
        )
        .await?
    } else {
        None
    };
    CrudService::delete(
        &mut executor,
        &entity,
        &id,
        schema_override,
        user_id_opt.as_deref(),
        state.dialect.as_ref(),
    )
    .await?;

    // Hard-delete all asset files belonging to this record after a successful DB delete.
    if let Some(ref old_row) = pre_delete_row {
        if entity_has_assets {
            delete_all_asset_paths(&state, &entity, old_row).await;
        }
    }

    if let Some(client) = &state.event_client {
        let raw_row = pre_delete_row.unwrap_or_else(|| serde_json::json!({ "id": id_str }));
        let mut api_row = raw_row.clone();
        strip_sensitive_columns(&mut api_row, &entity.sensitive_columns);
        value_keys_to_camel_case(&mut api_row);
        spawn_events(
            std::sync::Arc::clone(client),
            &entity,
            "delete",
            raw_row,
            api_row,
            tenant_id_opt.as_deref().unwrap_or("").to_string(),
            None,
        );
    }
    Ok(axum::http::StatusCode::NO_CONTENT)
}

pub async fn bulk_create(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path(path_segment): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), None).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = state
        .model
        .read()
        .map_err(|_| AppError::BadRequest("state lock".into()))?
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "bulk_create") {
        return Err(AppError::BadRequest("bulk_create not allowed".into()));
    }
    require_storage_for_assets(&state, &entity)?;
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "post",
    )
    .await?;
    let mut items: Vec<HashMap<String, Value>> = match body {
        Value::Array(arr) => {
            let mut out = Vec::new();
            for v in arr {
                out.push(hashmap_keys_to_snake_case(&body_to_map(v)?));
            }
            out
        }
        _ => return Err(AppError::BadRequest("body must be a JSON array".into())),
    };
    // Strip parentRef before validation so it doesn't trigger unknown-field errors.
    let parent_refs = if entity.parent_ref_column.is_some() {
        extract_parent_refs(&mut items)
    } else {
        vec![None; items.len()]
    };
    let mut all_errors: Vec<BulkFieldError> = Vec::new();
    for (index, item) in items.iter().enumerate() {
        for (field, message) in RequestValidator::validate_collecting(item, &entity.validation) {
            all_errors.push(BulkFieldError {
                index,
                field: to_camel_case(&field),
                message,
            });
        }
    }
    if !all_errors.is_empty() {
        return Err(AppError::BulkValidation(all_errors));
    }
    let (mut rows, db_errs) = CrudService::bulk_create_collecting(
        &mut executor,
        &entity,
        &items,
        schema_override,
        ctx.rls_tenant_id(),
        user_id_opt.as_deref(),
        state.dialect.as_ref(),
    )
    .await?;
    if !db_errs.is_empty() {
        return Err(AppError::BulkValidation(db_errors_to_bulk_field_errors(
            db_errs,
        )));
    }
    if let Some(ref ref_col) = entity.parent_ref_column.clone() {
        if parent_refs.iter().any(|r| r.is_some()) {
            resolve_and_update_parent_refs(
                &mut executor,
                &mut rows,
                &parent_refs,
                &entity,
                ref_col,
                schema_override,
            )
            .await?;
        }
    }
    let raw_rows = rows.clone();
    for row in &mut rows {
        strip_sensitive_columns(row, &entity.sensitive_columns);
        value_keys_to_camel_case(row);
    }
    if let Some(client) = &state.event_client {
        let tid = tenant_id_opt.as_deref().unwrap_or("").to_string();
        for (raw_row, api_row) in raw_rows.into_iter().zip(rows.iter().cloned()) {
            spawn_events(
                std::sync::Arc::clone(client),
                &entity,
                "create",
                raw_row,
                api_row,
                tid.clone(),
                None,
            );
        }
    }
    let count = rows.len() as u64;
    Ok((
        axum::http::StatusCode::CREATED,
        Json(crate::response::SuccessMany {
            data: rows,
            meta: crate::response::MetaCount { count },
        }),
    ))
}

pub async fn bulk_update(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path(path_segment): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), None).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = state
        .model
        .read()
        .map_err(|_| AppError::BadRequest("state lock".into()))?
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "bulk_update") {
        return Err(AppError::BadRequest("bulk_update not allowed".into()));
    }
    require_storage_for_assets(&state, &entity)?;
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "patch",
    )
    .await?;
    let items: Vec<HashMap<String, Value>> = match body {
        Value::Array(arr) => {
            let mut out = Vec::new();
            for v in arr {
                out.push(hashmap_keys_to_snake_case(&body_to_map(v)?));
            }
            out
        }
        _ => return Err(AppError::BadRequest("body must be a JSON array".into())),
    };
    let mut all_errors: Vec<BulkFieldError> = Vec::new();
    for (index, item) in items.iter().enumerate() {
        for (field, message) in RequestValidator::validate_collecting(item, &entity.validation) {
            all_errors.push(BulkFieldError {
                index,
                field: to_camel_case(&field),
                message,
            });
        }
    }
    if !all_errors.is_empty() {
        return Err(AppError::BulkValidation(all_errors));
    }
    let (mut rows, db_errs) = CrudService::bulk_update_collecting(
        &mut executor,
        &entity,
        &items,
        schema_override,
        user_id_opt.as_deref(),
        state.dialect.as_ref(),
    )
    .await?;
    if !db_errs.is_empty() {
        return Err(AppError::BulkValidation(db_errors_to_bulk_field_errors(
            db_errs,
        )));
    }
    let raw_rows = rows.clone();
    for row in &mut rows {
        strip_sensitive_columns(row, &entity.sensitive_columns);
        value_keys_to_camel_case(row);
    }
    if let Some(client) = &state.event_client {
        let tid = tenant_id_opt.as_deref().unwrap_or("").to_string();
        for (raw_row, api_row) in raw_rows.into_iter().zip(rows.iter().cloned()) {
            // bulk_update doesn't pre-fetch old rows; changed_to checks post-update value only.
            spawn_events(
                std::sync::Arc::clone(client),
                &entity,
                "update",
                raw_row,
                api_row,
                tid.clone(),
                None,
            );
        }
    }
    let count = rows.len() as u64;
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessMany {
            data: rows,
            meta: crate::response::MetaCount { count },
        }),
    ))
}

// ---- Package-scoped handlers: /api/v1/package/:package_id/:path_segment ----

pub async fn list_package(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path((package_id, path_segment)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), Some(&package_id)).await?;
    let model = get_or_load_package_model(
        &state,
        ctx.config_pool(),
        ctx.package_cache_key(),
        &package_id,
    )
    .await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = model
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "read") {
        return Err(AppError::BadRequest("read not allowed".into()));
    }
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "get",
    )
    .await?;
    let mut limit: Option<u32> = None;
    let mut offset: Option<u32> = None;
    let mut include_names: Vec<String> = Vec::new();
    let mut filter_str: Option<String> = None;
    let mut sort_str: Option<String> = None;
    let mut sign_param: Option<String> = None;
    let mut sign_expires: u64 = 900;
    for (k, v) in params {
        match k.as_str() {
            "limit" => limit = v.parse().ok(),
            "offset" => offset = v.parse().ok(),
            "include" => {
                include_names = v
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            }
            "q" => filter_str = Some(v),
            "sort" => sort_str = Some(v),
            "sign" => sign_param = Some(v),
            "sign_expires" => sign_expires = v.parse().unwrap_or(900),
            _ => {}
        }
    }
    let sign_cols: Option<HashSet<String>> = sign_param.as_deref().and_then(|s| {
        if s == "true" {
            None
        } else {
            Some(s.split(',').map(|c| to_snake_case(c.trim())).collect())
        }
    });
    let filter: Option<FilterNode> = filter_str.as_deref().map(parse_rsql).transpose()?;
    let sort = sort_str.as_deref().map(parse_sort).unwrap_or_default();

    let filter_prefix_names = collect_dotted_prefixes(filter.as_ref());
    let all_include_names: Vec<String> = {
        let mut names = include_names.clone();
        for n in &filter_prefix_names {
            if !names.contains(n) {
                names.push(n.clone());
            }
        }
        names
    };
    let resolved_all: Vec<(String, crate::config::IncludeSpec, ResolvedEntity)> =
        if !all_include_names.is_empty() {
            resolve_includes(&model, &entity, &all_include_names)?
        } else {
            Vec::new()
        };
    let filter_include_selects: Vec<IncludeSelect> = resolved_all
        .iter()
        .map(|(name, spec, related)| IncludeSelect {
            name: name.as_str(),
            direction: spec.direction.clone(),
            related,
            our_key: spec.our_key_column.as_str(),
            their_key: spec.their_key_column.as_str(),
        })
        .collect();
    let resolved_data: Vec<_> = resolved_all
        .iter()
        .filter(|(name, _, _)| include_names.contains(name))
        .cloned()
        .collect();

    let mut rows = if include_names.is_empty() {
        CrudService::list(
            &mut executor,
            &entity,
            filter.as_ref(),
            &sort,
            limit,
            offset,
            &filter_include_selects,
            schema_override,
            state.dialect.as_ref(),
        )
        .await?
    } else {
        let data_include_selects: Vec<IncludeSelect> = resolved_data
            .iter()
            .map(|(name, spec, related)| IncludeSelect {
                name: name.as_str(),
                direction: spec.direction.clone(),
                related,
                our_key: spec.our_key_column.as_str(),
                their_key: spec.their_key_column.as_str(),
            })
            .collect();
        let mut rows = CrudService::list_with_includes(
            &mut executor,
            &entity,
            filter.as_ref(),
            &sort,
            limit,
            offset,
            data_include_selects.as_slice(),
            &filter_include_selects,
            schema_override,
            state.dialect.as_ref(),
        )
        .await?;
        post_process_include_columns(&mut rows, &resolved_data);
        rows
    };
    if let Some(ref ref_col) = entity.parent_ref_column.clone() {
        enrich_with_parent_ref(&mut executor, &mut rows, &entity, ref_col, schema_override).await?;
    }
    for row in &mut rows {
        strip_sensitive_columns(row, &entity.sensitive_columns);
        value_keys_to_camel_case(row);
    }
    if sign_param.is_some() {
        for row in &mut rows {
            sign_row_assets(&state, &entity, row, &sign_cols, sign_expires).await?;
        }
    }
    let count = rows.len() as u64;
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessMany {
            data: rows,
            meta: crate::response::MetaCount { count },
        }),
    ))
}

pub async fn create_package(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path((package_id, path_segment)): Path<(String, String)>,
    request: Request,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let tenant_id_str = tenant_id_opt.as_deref().unwrap_or("").to_string();
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), Some(&package_id)).await?;
    let model = get_or_load_package_model(
        &state,
        ctx.config_pool(),
        ctx.package_cache_key(),
        &package_id,
    )
    .await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = model
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "create") {
        return Err(AppError::BadRequest("create not allowed".into()));
    }
    require_storage_for_assets(&state, &entity)?;
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "post",
    )
    .await?;

    let is_multipart = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.contains("multipart/form-data"))
        .unwrap_or(false);

    let mut body;
    if is_multipart {
        let multipart = axum::extract::Multipart::from_request(request, &state)
            .await
            .map_err(|e| AppError::BadRequest(e.to_string()))?;
        let (text_fields, files) = parse_multipart(multipart).await?;
        body = hashmap_keys_to_snake_case(&text_fields);
        process_asset_uploads(&state, &entity, &tenant_id_str, &mut body, files).await?;
    } else {
        let Json(json_body) = Json::<Value>::from_request(request, &state)
            .await
            .map_err(|e| AppError::BadRequest(e.to_string()))?;
        body = hashmap_keys_to_snake_case(&body_to_map(json_body)?);
        process_json_asset_fields(&state, &entity, &tenant_id_str, &mut body).await?;
    }

    RequestValidator::validate(&body, &entity.validation)?;
    let mut row = CrudService::create(
        &mut executor,
        &entity,
        &body,
        schema_override,
        ctx.rls_tenant_id(),
        user_id_opt.as_deref(),
        state.dialect.as_ref(),
    )
    .await?;
    let raw_row = row.clone();
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    if let Some(client) = &state.event_client {
        spawn_events(
            std::sync::Arc::clone(client),
            &entity,
            "create",
            raw_row,
            row.clone(),
            tenant_id_str,
            None,
        );
    }
    Ok((
        axum::http::StatusCode::CREATED,
        Json(crate::response::SuccessOne {
            data: row,
            meta: None,
        }),
    ))
}

pub async fn read_package(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path((package_id, path_segment, id_str)): Path<(String, String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), Some(&package_id)).await?;
    let model = get_or_load_package_model(
        &state,
        ctx.config_pool(),
        ctx.package_cache_key(),
        &package_id,
    )
    .await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = model
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "read") {
        return Err(AppError::BadRequest("read not allowed".into()));
    }
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "get",
    )
    .await?;
    let id = parse_id(&id_str, &entity.pk_type)?;
    let mut row = CrudService::read(
        &mut executor,
        &entity,
        &id,
        schema_override,
        state.dialect.as_ref(),
    )
    .await?
    .ok_or_else(|| AppError::NotFound(id_str.clone()))?;
    let include_names: Vec<String> = params
        .get("include")
        .map(|s| {
            s.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if !include_names.is_empty() {
        let resolved = resolve_includes(&model, &entity, &include_names)?;
        let mut rows = [row];
        attach_includes(
            &mut executor,
            schema_override,
            &entity,
            &mut rows,
            &resolved,
            state.dialect.as_ref(),
        )
        .await?;
        row = rows[0].clone();
    }
    if let Some(ref ref_col) = entity.parent_ref_column.clone() {
        let mut rows = [row];
        enrich_with_parent_ref(&mut executor, &mut rows, &entity, ref_col, schema_override).await?;
        row = rows.into_iter().next().unwrap();
    }
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);

    let sign_param = params.get("sign").cloned();
    if sign_param.is_some() {
        let sign_expires: u64 = params
            .get("sign_expires")
            .and_then(|s| s.parse().ok())
            .unwrap_or(900);
        let sign_cols: Option<HashSet<String>> = sign_param.as_deref().and_then(|s| {
            if s == "true" {
                None
            } else {
                Some(s.split(',').map(|c| to_snake_case(c.trim())).collect())
            }
        });
        sign_row_assets(&state, &entity, &mut row, &sign_cols, sign_expires).await?;
    }

    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessOne {
            data: row,
            meta: None,
        }),
    ))
}

pub async fn update_package(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path((package_id, path_segment, id_str)): Path<(String, String, String)>,
    request: Request,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let tenant_id_str = tenant_id_opt.as_deref().unwrap_or("").to_string();
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), Some(&package_id)).await?;
    let model = get_or_load_package_model(
        &state,
        ctx.config_pool(),
        ctx.package_cache_key(),
        &package_id,
    )
    .await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = model
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "update") {
        return Err(AppError::BadRequest("update not allowed".into()));
    }
    require_storage_for_assets(&state, &entity)?;
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "patch",
    )
    .await?;
    let id = parse_id(&id_str, &entity.pk_type)?;

    let is_multipart = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.contains("multipart/form-data"))
        .unwrap_or(false);

    let mut body;
    if is_multipart {
        let multipart = axum::extract::Multipart::from_request(request, &state)
            .await
            .map_err(|e| AppError::BadRequest(e.to_string()))?;
        let (text_fields, files) = parse_multipart(multipart).await?;
        body = hashmap_keys_to_snake_case(&text_fields);
        process_asset_uploads(&state, &entity, &tenant_id_str, &mut body, files).await?;
    } else {
        let Json(json_body) = Json::<Value>::from_request(request, &state)
            .await
            .map_err(|e| AppError::BadRequest(e.to_string()))?;
        body = hashmap_keys_to_snake_case(&body_to_map(json_body)?);
        process_json_asset_fields(&state, &entity, &tenant_id_str, &mut body).await?;
    }

    RequestValidator::validate_partial(&body, &entity.validation)?;

    // Pre-read for asset hard-delete on PATCH.
    let entity_has_assets = entity.columns.iter().any(|c| c.is_asset);
    let pre_update_row = if entity_has_assets && state.storage.is_some() {
        CrudService::read(
            &mut executor,
            &entity,
            &id,
            schema_override,
            state.dialect.as_ref(),
        )
        .await?
    } else {
        None
    };

    let mut row = CrudService::update(
        &mut executor,
        &entity,
        &id,
        &body,
        schema_override,
        user_id_opt.as_deref(),
        state.dialect.as_ref(),
    )
    .await?
    .ok_or_else(|| AppError::NotFound(id_str))?;

    // Hard-delete dropped asset files after a successful DB write.
    if let Some(ref old_row) = pre_update_row {
        if entity_has_assets {
            delete_dropped_asset_paths(&state, &entity, old_row, &body).await;
        }
    }

    let raw_row = row.clone();
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    if let Some(client) = &state.event_client {
        spawn_events(
            std::sync::Arc::clone(client),
            &entity,
            "update",
            raw_row,
            row.clone(),
            tenant_id_str,
            None,
        );
    }
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessOne {
            data: row,
            meta: None,
        }),
    ))
}

pub async fn delete_package(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path((package_id, path_segment, id_str)): Path<(String, String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), Some(&package_id)).await?;
    let model = get_or_load_package_model(
        &state,
        ctx.config_pool(),
        ctx.package_cache_key(),
        &package_id,
    )
    .await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = model
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "delete") {
        return Err(AppError::BadRequest("delete not allowed".into()));
    }
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "delete",
    )
    .await?;
    let id = parse_id(&id_str, &entity.pk_type)?;
    // Prefetch the full row before deletion for event triggers and asset storage cleanup.
    let entity_has_assets = entity.columns.iter().any(|c| c.is_asset);
    let pre_delete_row = if (state.event_client.is_some() && !entity.events.is_empty())
        || (entity_has_assets && state.storage.is_some())
    {
        CrudService::read(
            &mut executor,
            &entity,
            &id,
            schema_override,
            state.dialect.as_ref(),
        )
        .await?
    } else {
        None
    };
    CrudService::delete(
        &mut executor,
        &entity,
        &id,
        schema_override,
        user_id_opt.as_deref(),
        state.dialect.as_ref(),
    )
    .await?;

    // Hard-delete all asset files belonging to this record after a successful DB delete.
    if let Some(ref old_row) = pre_delete_row {
        if entity_has_assets {
            delete_all_asset_paths(&state, &entity, old_row).await;
        }
    }

    if let Some(client) = &state.event_client {
        let raw_row = pre_delete_row.unwrap_or_else(|| serde_json::json!({ "id": id_str }));
        let mut api_row = raw_row.clone();
        strip_sensitive_columns(&mut api_row, &entity.sensitive_columns);
        value_keys_to_camel_case(&mut api_row);
        spawn_events(
            std::sync::Arc::clone(client),
            &entity,
            "delete",
            raw_row,
            api_row,
            tenant_id_opt.as_deref().unwrap_or("").to_string(),
            None,
        );
    }
    Ok(axum::http::StatusCode::NO_CONTENT)
}

pub async fn bulk_create_package(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path((package_id, path_segment)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), Some(&package_id)).await?;
    let model = get_or_load_package_model(
        &state,
        ctx.config_pool(),
        ctx.package_cache_key(),
        &package_id,
    )
    .await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = model
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "bulk_create") {
        return Err(AppError::BadRequest("bulk_create not allowed".into()));
    }
    require_storage_for_assets(&state, &entity)?;
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "post",
    )
    .await?;
    let mut items: Vec<HashMap<String, Value>> = match body {
        Value::Array(arr) => {
            let mut out = Vec::new();
            for v in arr {
                out.push(hashmap_keys_to_snake_case(&body_to_map(v)?));
            }
            out
        }
        _ => return Err(AppError::BadRequest("body must be a JSON array".into())),
    };
    let parent_refs = if entity.parent_ref_column.is_some() {
        extract_parent_refs(&mut items)
    } else {
        vec![None; items.len()]
    };
    let mut all_errors: Vec<BulkFieldError> = Vec::new();
    for (index, item) in items.iter().enumerate() {
        for (field, message) in RequestValidator::validate_collecting(item, &entity.validation) {
            all_errors.push(BulkFieldError {
                index,
                field: to_camel_case(&field),
                message,
            });
        }
    }
    if !all_errors.is_empty() {
        return Err(AppError::BulkValidation(all_errors));
    }
    let (mut rows, db_errs) = CrudService::bulk_create_collecting(
        &mut executor,
        &entity,
        &items,
        schema_override,
        ctx.rls_tenant_id(),
        user_id_opt.as_deref(),
        state.dialect.as_ref(),
    )
    .await?;
    if !db_errs.is_empty() {
        return Err(AppError::BulkValidation(db_errors_to_bulk_field_errors(
            db_errs,
        )));
    }
    if let Some(ref ref_col) = entity.parent_ref_column.clone() {
        if parent_refs.iter().any(|r| r.is_some()) {
            resolve_and_update_parent_refs(
                &mut executor,
                &mut rows,
                &parent_refs,
                &entity,
                ref_col,
                schema_override,
            )
            .await?;
        }
    }
    let raw_rows = rows.clone();
    for row in &mut rows {
        strip_sensitive_columns(row, &entity.sensitive_columns);
        value_keys_to_camel_case(row);
    }
    let tid = tenant_id_opt.as_deref().unwrap_or("").to_string();
    if let Some(client) = &state.event_client {
        for (raw_row, api_row) in raw_rows.into_iter().zip(rows.iter().cloned()) {
            spawn_events(
                std::sync::Arc::clone(client),
                &entity,
                "create",
                raw_row,
                api_row,
                tid.clone(),
                None,
            );
        }
    }
    let count = rows.len() as u64;
    Ok((
        axum::http::StatusCode::CREATED,
        Json(crate::response::SuccessMany {
            data: rows,
            meta: crate::response::MetaCount { count },
        }),
    ))
}

pub async fn bulk_update_package(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path((package_id, path_segment)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), Some(&package_id)).await?;
    let model = get_or_load_package_model(
        &state,
        ctx.config_pool(),
        ctx.package_cache_key(),
        &package_id,
    )
    .await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = model
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "bulk_update") {
        return Err(AppError::BadRequest("bulk_update not allowed".into()));
    }
    require_storage_for_assets(&state, &entity)?;
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "patch",
    )
    .await?;
    let items: Vec<HashMap<String, Value>> = match body {
        Value::Array(arr) => {
            let mut out = Vec::new();
            for v in arr {
                out.push(hashmap_keys_to_snake_case(&body_to_map(v)?));
            }
            out
        }
        _ => return Err(AppError::BadRequest("body must be a JSON array".into())),
    };
    let mut all_errors: Vec<BulkFieldError> = Vec::new();
    for (index, item) in items.iter().enumerate() {
        for (field, message) in RequestValidator::validate_collecting(item, &entity.validation) {
            all_errors.push(BulkFieldError {
                index,
                field: to_camel_case(&field),
                message,
            });
        }
    }
    if !all_errors.is_empty() {
        return Err(AppError::BulkValidation(all_errors));
    }
    let (raw_rows, db_errs) = CrudService::bulk_update_collecting(
        &mut executor,
        &entity,
        &items,
        schema_override,
        user_id_opt.as_deref(),
        state.dialect.as_ref(),
    )
    .await?;
    if !db_errs.is_empty() {
        return Err(AppError::BulkValidation(db_errors_to_bulk_field_errors(
            db_errs,
        )));
    }
    let mut rows = raw_rows.clone();
    for row in &mut rows {
        strip_sensitive_columns(row, &entity.sensitive_columns);
        value_keys_to_camel_case(row);
    }
    let tid = tenant_id_opt.as_deref().unwrap_or("").to_string();
    if let Some(client) = &state.event_client {
        for (raw_row, api_row) in raw_rows.into_iter().zip(rows.iter().cloned()) {
            spawn_events(
                std::sync::Arc::clone(client),
                &entity,
                "update",
                raw_row,
                api_row,
                tid.clone(),
                None,
            );
        }
    }
    let count = rows.len() as u64;
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessMany {
            data: rows,
            meta: crate::response::MetaCount { count },
        }),
    ))
}

/// Archive a single entity by id (default model).
/// POST /api/v1/:path_segment/:id/archive
/// Stamps archive_field with NOW(); returns 404 if not found or already archived.
/// Authrs action: archive{PascalCaseName} (dedicated permission, not reusing patch/delete).
pub async fn archive(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path((path_segment, id_str)): Path<(String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let tenant_id_str = tenant_id_opt.as_deref().unwrap_or("").to_string();
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), None).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = state
        .model
        .read()
        .map_err(|_| AppError::BadRequest("state lock".into()))?
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "archive") {
        return Err(AppError::BadRequest("archive not allowed".into()));
    }
    let archive_field = entity.archive_field.as_deref().ok_or_else(|| {
        AppError::BadRequest("archive_field is not configured for this entity".into())
    })?;
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "archive",
    )
    .await?;
    let id = parse_id(&id_str, &entity.pk_type)?;
    let mut row = CrudService::archive(
        &mut executor,
        &entity,
        archive_field,
        &id,
        schema_override,
        state.dialect.as_ref(),
    )
    .await?
    .ok_or_else(|| AppError::NotFound(format!("{} not found or already archived", id_str)))?;
    let raw_row = row.clone();
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    if let Some(client) = &state.event_client {
        spawn_events(
            std::sync::Arc::clone(client),
            &entity,
            "archive",
            raw_row,
            row.clone(),
            tenant_id_str,
            None,
        );
    }
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessOne {
            data: row,
            meta: None,
        }),
    ))
}

/// Unarchive a single entity by id (default model).
/// POST /api/v1/:path_segment/:id/unarchive
/// Clears archive_field (sets to NULL); returns 404 if not found or not currently archived.
/// Authrs action: unarchive{PascalCaseName} (dedicated permission, separate from archive).
pub async fn unarchive(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path((path_segment, id_str)): Path<(String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let tenant_id_str = tenant_id_opt.as_deref().unwrap_or("").to_string();
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), None).await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = state
        .model
        .read()
        .map_err(|_| AppError::BadRequest("state lock".into()))?
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "unarchive") {
        return Err(AppError::BadRequest("unarchive not allowed".into()));
    }
    let archive_field = entity.archive_field.as_deref().ok_or_else(|| {
        AppError::BadRequest("archive_field is not configured for this entity".into())
    })?;
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "unarchive",
    )
    .await?;
    let id = parse_id(&id_str, &entity.pk_type)?;
    let mut row = CrudService::unarchive(
        &mut executor,
        &entity,
        archive_field,
        &id,
        schema_override,
        state.dialect.as_ref(),
    )
    .await?
    .ok_or_else(|| AppError::NotFound(format!("{} not found or not currently archived", id_str)))?;
    let raw_row = row.clone();
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    if let Some(client) = &state.event_client {
        spawn_events(
            std::sync::Arc::clone(client),
            &entity,
            "unarchive",
            raw_row,
            row.clone(),
            tenant_id_str,
            None,
        );
    }
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessOne {
            data: row,
            meta: None,
        }),
    ))
}

/// Unarchive a single entity by id (package-scoped model).
/// POST /api/v1/package/:package_id/:path_segment/:id/unarchive
pub async fn unarchive_package(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path((package_id, path_segment, id_str)): Path<(String, String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let tenant_id_str = tenant_id_opt.as_deref().unwrap_or("").to_string();
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), Some(&package_id)).await?;
    let model = get_or_load_package_model(
        &state,
        ctx.config_pool(),
        ctx.package_cache_key(),
        &package_id,
    )
    .await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = model
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "unarchive") {
        return Err(AppError::BadRequest("unarchive not allowed".into()));
    }
    let archive_field = entity.archive_field.as_deref().ok_or_else(|| {
        AppError::BadRequest("archive_field is not configured for this entity".into())
    })?;
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "unarchive",
    )
    .await?;
    let id = parse_id(&id_str, &entity.pk_type)?;
    let mut row = CrudService::unarchive(
        &mut executor,
        &entity,
        archive_field,
        &id,
        schema_override,
        state.dialect.as_ref(),
    )
    .await?
    .ok_or_else(|| AppError::NotFound(format!("{} not found or not currently archived", id_str)))?;
    let raw_row = row.clone();
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    if let Some(client) = &state.event_client {
        spawn_events(
            std::sync::Arc::clone(client),
            &entity,
            "unarchive",
            raw_row,
            row.clone(),
            tenant_id_str,
            None,
        );
    }
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessOne {
            data: row,
            meta: None,
        }),
    ))
}

/// Archive a single entity by id (package-scoped model).
/// POST /api/v1/package/:package_id/:path_segment/:id/archive
pub async fn archive_package(
    State(state): State<AppState>,
    TenantId(tenant_id_opt): TenantId,
    UserId(user_id_opt): UserId,
    Path((package_id, path_segment, id_str)): Path<(String, String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let tenant_id_str = tenant_id_opt.as_deref().unwrap_or("").to_string();
    let ctx = resolve_tenant_context(&state, tenant_id_opt.as_deref(), Some(&package_id)).await?;
    let model = get_or_load_package_model(
        &state,
        ctx.config_pool(),
        ctx.package_cache_key(),
        &package_id,
    )
    .await?;
    #[allow(unused_assignments)] // set in Rls branch; Pool branch does not use it
    let mut rls_conn: Option<crate::db::pool::DbConnection> = None;
    let (mut executor, schema_override) = match &ctx {
        TenantContext::Pool {
            pool,
            schema_override,
            ..
        } => (
            TenantExecutor::pool(pool, state.dialect.as_ref()),
            schema_override.as_deref(),
        ),
        TenantContext::Rls {
            tenant_id, pool, ..
        } => {
            let mut conn = pool.acquire().await?;
            if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
                sqlx::query(&set_sql).execute(&mut *conn).await?;
            }
            rls_conn = Some(conn);
            (
                TenantExecutor::conn(&mut *rls_conn.as_mut().unwrap(), state.dialect.as_ref()),
                None,
            )
        }
    };
    let entity = model
        .entity_by_path(&path_segment)
        .cloned()
        .ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "archive") {
        return Err(AppError::BadRequest("archive not allowed".into()));
    }
    let archive_field = entity.archive_field.as_deref().ok_or_else(|| {
        AppError::BadRequest("archive_field is not configured for this entity".into())
    })?;
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        tenant_id_opt.as_deref(),
        user_id_opt.as_deref(),
        &entity,
        "archive",
    )
    .await?;
    let id = parse_id(&id_str, &entity.pk_type)?;
    let mut row = CrudService::archive(
        &mut executor,
        &entity,
        archive_field,
        &id,
        schema_override,
        state.dialect.as_ref(),
    )
    .await?
    .ok_or_else(|| AppError::NotFound(format!("{} not found or already archived", id_str)))?;
    let raw_row = row.clone();
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    if let Some(client) = &state.event_client {
        spawn_events(
            std::sync::Arc::clone(client),
            &entity,
            "archive",
            raw_row,
            row.clone(),
            tenant_id_str,
            None,
        );
    }
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessOne {
            data: row,
            meta: None,
        }),
    ))
}
