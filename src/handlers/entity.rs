//! Entity CRUD handlers: create, read, update, delete, list, bulk.

use crate::config::{PkType, ResolvedEntity};
use crate::error::AppError;
use crate::service::{CrudService, RequestValidator};
use crate::state::AppState;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde_json::Value;
use std::collections::HashMap;

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

pub async fn list(
    State(state): State<AppState>,
    Path(path_segment): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let entity = state.model.entity_by_path(&path_segment).ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "read") {
        return Err(AppError::BadRequest("read not allowed".into()));
    }
    let column_names: std::collections::HashSet<&str> = entity.columns.iter().map(|c| c.name.as_str()).collect();

    let mut limit: Option<u32> = None;
    let mut offset: Option<u32> = None;
    let mut filters: Vec<(String, Value)> = Vec::new();

    for (k, v) in params {
        match k.as_str() {
            "limit" => {
                limit = v.parse().ok();
            }
            "offset" => {
                offset = v.parse().ok();
            }
            _ => {
                if column_names.contains(k.as_str()) {
                    let val = query_value_for_column(entity, &k, &v);
                    filters.push((k, val));
                }
            }
        }
    }

    let rows = CrudService::list(&state.pool, entity, &filters, limit, offset).await?;
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
    let entity = state.model.entity_by_path(&path_segment).ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "create") {
        return Err(AppError::BadRequest("create not allowed".into()));
    }
    let body = body_to_map(body)?;
    RequestValidator::validate(&body, &entity.validation)?;
    let row = CrudService::create(&state.pool, entity, &body).await?;
    Ok((axum::http::StatusCode::CREATED, Json(crate::response::SuccessOne { data: row, meta: None })))
}

pub async fn read(
    State(state): State<AppState>,
    Path((path_segment, id_str)): Path<(String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let entity = state.model.entity_by_path(&path_segment).ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "read") {
        return Err(AppError::BadRequest("read not allowed".into()));
    }
    let id = parse_id(&id_str, &entity.pk_type)?;
    let row = CrudService::read(&state.pool, entity, &id).await?
        .ok_or_else(|| AppError::NotFound(id_str))?;
    Ok((axum::http::StatusCode::OK, Json(crate::response::SuccessOne { data: row, meta: None })))
}

pub async fn update(
    State(state): State<AppState>,
    Path((path_segment, id_str)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let entity = state.model.entity_by_path(&path_segment).ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "update") {
        return Err(AppError::BadRequest("update not allowed".into()));
    }
    let id = parse_id(&id_str, &entity.pk_type)?;
    let body = body_to_map(body)?;
    RequestValidator::validate(&body, &entity.validation)?;
    let row = CrudService::update(&state.pool, entity, &id, &body).await?
        .ok_or_else(|| AppError::NotFound(id_str))?;
    Ok((axum::http::StatusCode::OK, Json(crate::response::SuccessOne { data: row, meta: None })))
}

pub async fn delete(
    State(state): State<AppState>,
    Path((path_segment, id_str)): Path<(String, String)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let entity = state.model.entity_by_path(&path_segment).ok_or_else(|| AppError::NotFound(path_segment))?;
    if !entity.operations.iter().any(|o| o == "delete") {
        return Err(AppError::BadRequest("delete not allowed".into()));
    }
    let id = parse_id(&id_str, &entity.pk_type)?;
    CrudService::delete(&state.pool, entity, &id).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

pub async fn bulk_create(
    State(state): State<AppState>,
    Path(path_segment): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let entity = state.model.entity_by_path(&path_segment).ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "bulk_create") {
        return Err(AppError::BadRequest("bulk_create not allowed".into()));
    }
    let items: Vec<HashMap<String, Value>> = match body {
        Value::Array(arr) => {
            let mut out = Vec::new();
            for v in arr {
                out.push(body_to_map(v)?);
            }
            out
        }
        _ => return Err(AppError::BadRequest("body must be a JSON array".into())),
    };
    for item in &items {
        RequestValidator::validate(item, &entity.validation)?;
    }
    let rows = CrudService::bulk_create(&state.pool, entity, &items).await?;
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
    let entity = state.model.entity_by_path(&path_segment).ok_or_else(|| AppError::NotFound(path_segment.clone()))?;
    if !entity.operations.iter().any(|o| o == "bulk_update") {
        return Err(AppError::BadRequest("bulk_update not allowed".into()));
    }
    let items: Vec<HashMap<String, Value>> = match body {
        Value::Array(arr) => {
            let mut out = Vec::new();
            for v in arr {
                out.push(body_to_map(v)?);
            }
            out
        }
        _ => return Err(AppError::BadRequest("body must be a JSON array".into())),
    };
    for item in &items {
        RequestValidator::validate(item, &entity.validation)?;
    }
    let rows = CrudService::bulk_update(&state.pool, entity, &items).await?;
    let count = rows.len() as u64;
    Ok((
        axum::http::StatusCode::OK,
        Json(crate::response::SuccessMany {
            data: rows,
            meta: crate::response::MetaCount { count },
        }),
    ))
}
