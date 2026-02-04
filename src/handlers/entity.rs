//! Entity CRUD handlers: create, read, update, delete, list, bulk.
//! Request bodies and query param keys are accepted in camelCase and converted to snake_case for DB; response row keys are converted to camelCase.

use crate::case::{hashmap_keys_to_snake_case, to_snake_case, value_keys_to_camel_case};
use crate::config::{IncludeDirection, PkType, ResolvedModel, ResolvedEntity};
use crate::error::AppError;
use crate::service::{CrudService, RequestValidator};
use crate::state::AppState;
use axum::{
    extract::{Path, Query, State},
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
            let u = uuid::Uuid::parse_str(id_str).map_err(|_| AppError::BadRequest("invalid uuid".into()))?;
            Value::String(u.to_string())
        }
        PkType::BigInt | PkType::Int => {
            let n: i64 = id_str.parse().map_err(|_| AppError::BadRequest("invalid id".into()))?;
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
            .ok_or_else(|| AppError::BadRequest(format!("related entity not found: {}", spec.related_path_segment)))?;
        out.push((name.clone(), spec, related));
    }
    Ok(out)
}

/// Attach related-entity data to rows. Modifies each row in place. resolved_includes from resolve_includes (so lock can be dropped before calling).
async fn attach_includes(
    pool: &sqlx::PgPool,
    _entity: &ResolvedEntity,
    rows: &mut [Value],
    resolved_includes: &[(String, crate::config::IncludeSpec, ResolvedEntity)],
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
                    pool,
                    related,
                    &spec.their_key_column,
                    &keys,
                )
                .await?;
                let mut key_to_row: HashMap<String, Value> = HashMap::new();
                for mut r in related_rows {
                    let k = r
                        .get(&spec.their_key_column)
                        .cloned()
                        .map(|v| serde_json::to_string(&v).unwrap_or_default())
                        .unwrap_or_default();
                    if !key_to_row.contains_key(&k) {
                        strip_sensitive_columns(&mut r, &related.sensitive_columns);
                        value_keys_to_camel_case(&mut r);
                        key_to_row.insert(k, r);
                    }
                }
                for row in rows.iter_mut() {
                    if let Value::Object(ref mut map) = row {
                        let key_val = map.get(&spec.our_key_column).cloned().unwrap_or(Value::Null);
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
                    pool,
                    related,
                    &spec.their_key_column,
                    &keys,
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
                        let key_val = map.get(&spec.our_key_column).cloned().unwrap_or(Value::Null);
                        let key = serde_json::to_string(&key_val).unwrap_or_default();
                        let arr = grouped
                            .get(&key)
                            .cloned()
                            .unwrap_or_default();
                        map.insert(name.clone(), Value::Array(arr));
                    }
                }
            }
        }
    }
    Ok(())
}

pub async fn list(
    State(state): State<AppState>,
    Path(path_segment): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let entity = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "read") {
        return Err(AppError::BadRequest("read not allowed".into()));
    }
    let column_names: std::collections::HashSet<&str> = entity.columns.iter().map(|c| c.name.as_str()).collect();

    let mut limit: Option<u32> = None;
    let mut offset: Option<u32> = None;
    let mut include_names: Vec<String> = Vec::new();
    let mut filters: Vec<(String, Value)> = Vec::new();

    for (k, v) in params {
        match k.as_str() {
            "limit" => {
                limit = v.parse().ok();
            }
            "offset" => {
                offset = v.parse().ok();
            }
            "include" => {
                include_names = v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
            }
            _ => {
                let col_key = to_snake_case(&k);
                if column_names.contains(col_key.as_str()) {
                    let val = query_value_for_column(&entity, &col_key, &v);
                    filters.push((col_key, val));
                }
            }
        }
    }

    let mut rows = CrudService::list(&state.pool, &entity, &filters, limit, offset).await?;
    let resolved = {
        let model = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?;
        resolve_includes(&model, &entity, &include_names)?
    };
    attach_includes(&state.pool, &entity, &mut rows, &resolved).await?;
    for row in &mut rows {
        strip_sensitive_columns(row, &entity.sensitive_columns);
        value_keys_to_camel_case(row);
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
    Path(path_segment): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let entity = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "create") {
        return Err(AppError::BadRequest("create not allowed".into()));
    }
    let body = body_to_map(body)?;
    let body = hashmap_keys_to_snake_case(&body);
    RequestValidator::validate(&body, &entity.validation)?;
    let mut row = CrudService::create(&state.pool, &entity, &body).await?;
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    Ok((axum::http::StatusCode::CREATED, Json(crate::response::SuccessOne { data: row, meta: None })))
}

pub async fn read(
    State(state): State<AppState>,
    Path((path_segment, id_str)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let entity = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "read") {
        return Err(AppError::BadRequest("read not allowed".into()));
    }
    let id = parse_id(&id_str, &entity.pk_type)?;
    let mut row = CrudService::read(&state.pool, &entity, &id).await?
        .ok_or_else(|| AppError::NotFound(id_str))?;
    let include_names: Vec<String> = params
        .get("include")
        .map(|s| s.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
        .unwrap_or_default();
    if !include_names.is_empty() {
        let resolved = {
            let model = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?;
            resolve_includes(&model, &entity, &include_names)?
        };
        let mut rows = [row];
        attach_includes(&state.pool, &entity, &mut rows, &resolved).await?;
        row = rows[0].clone();
    }
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    Ok((axum::http::StatusCode::OK, Json(crate::response::SuccessOne { data: row, meta: None })))
}

pub async fn update(
    State(state): State<AppState>,
    Path((path_segment, id_str)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let entity = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "update") {
        return Err(AppError::BadRequest("update not allowed".into()));
    }
    let id = parse_id(&id_str, &entity.pk_type)?;
    let body = body_to_map(body)?;
    let body = hashmap_keys_to_snake_case(&body);
    RequestValidator::validate(&body, &entity.validation)?;
    let mut row = CrudService::update(&state.pool, &entity, &id, &body).await?
        .ok_or_else(|| AppError::NotFound(id_str))?;
    strip_sensitive_columns(&mut row, &entity.sensitive_columns);
    value_keys_to_camel_case(&mut row);
    Ok((axum::http::StatusCode::OK, Json(crate::response::SuccessOne { data: row, meta: None })))
}

pub async fn delete(
    State(state): State<AppState>,
    Path((path_segment, id_str)): Path<(String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let entity = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "delete") {
        return Err(AppError::BadRequest("delete not allowed".into()));
    }
    let id = parse_id(&id_str, &entity.pk_type)?;
    CrudService::delete(&state.pool, &entity, &id).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

pub async fn bulk_create(
    State(state): State<AppState>,
    Path(path_segment): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let entity = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "bulk_create") {
        return Err(AppError::BadRequest("bulk_create not allowed".into()));
    }
    let items: Vec<HashMap<String, Value>> = match body {
        Value::Array(arr) => {
            let mut out = Vec::new();
            for v in arr {
                let map = body_to_map(v)?;
                out.push(hashmap_keys_to_snake_case(&map));
            }
            out
        }
        _ => return Err(AppError::BadRequest("body must be a JSON array".into())),
    };
    for item in &items {
        RequestValidator::validate(item, &entity.validation)?;
    }
    let mut rows = CrudService::bulk_create(&state.pool, &entity, &items).await?;
    for row in &mut rows {
        strip_sensitive_columns(row, &entity.sensitive_columns);
        value_keys_to_camel_case(row);
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
    Path(path_segment): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let entity = state.model.read().map_err(|_| AppError::BadRequest("state lock".into()))?.entity_by_path(&path_segment).cloned().ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "bulk_update") {
        return Err(AppError::BadRequest("bulk_update not allowed".into()));
    }
    let items: Vec<HashMap<String, Value>> = match body {
        Value::Array(arr) => {
            let mut out = Vec::new();
            for v in arr {
                let map = body_to_map(v)?;
                out.push(hashmap_keys_to_snake_case(&map));
            }
            out
        }
        _ => return Err(AppError::BadRequest("body must be a JSON array".into())),
    };
    for item in &items {
        RequestValidator::validate(item, &entity.validation)?;
    }
    let mut rows = CrudService::bulk_update(&state.pool, &entity, &items).await?;
    for row in &mut rows {
        strip_sensitive_columns(row, &entity.sensitive_columns);
        value_keys_to_camel_case(row);
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
