//! Typed errors and HTTP mapping.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use thiserror::Error;

#[derive(Serialize, Debug, Clone)]
pub struct BulkFieldError {
    pub index: usize,
    pub field: String,
    pub message: String,
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("missing reference: {kind} id '{id}'")]
    MissingReference { kind: &'static str, id: String },
    #[error("invalid primary key: table {table_id} column {column}")]
    InvalidPrimaryKey { table_id: String, column: String },
    #[error("duplicate path segment: {0}")]
    DuplicatePathSegment(String),
    #[error("config load: {0}")]
    Load(String),
    #[error("validation: {0}")]
    Validation(String),
}

#[derive(Error, Debug)]
pub enum AppError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("validation: {0}")]
    Validation(String),
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("storage: {0}")]
    Storage(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("bulk validation failed")]
    BulkValidation(Vec<BulkFieldError>),
}

#[derive(Serialize)]
pub struct ErrorBody {
    pub error: ErrorDetail,
}

#[derive(Serialize)]
pub struct ErrorDetail {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        if let AppError::BulkValidation(ref errors) = self {
            let affected: std::collections::HashSet<usize> = errors.iter().map(|e| e.index).collect();
            let body = ErrorBody {
                error: ErrorDetail {
                    code: "bulk_validation_error".to_string(),
                    message: format!("Validation failed for {} item(s)", affected.len()),
                    details: Some(serde_json::to_value(errors).unwrap_or(serde_json::Value::Null)),
                },
            };
            return (StatusCode::UNPROCESSABLE_ENTITY, Json(body)).into_response();
        }
        let (status, code) = match &self {
            AppError::Config(_) => (StatusCode::INTERNAL_SERVER_ERROR, "config_error"),
            AppError::NotFound(_) => (StatusCode::NOT_FOUND, "not_found"),
            AppError::Validation(_) => (StatusCode::UNPROCESSABLE_ENTITY, "validation_error"),
            AppError::Db(e) => {
                if let sqlx::Error::RowNotFound = e {
                    (StatusCode::NOT_FOUND, "not_found")
                } else {
                    (StatusCode::INTERNAL_SERVER_ERROR, "database_error")
                }
            }
            AppError::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
            AppError::BadRequest(_) => (StatusCode::BAD_REQUEST, "bad_request"),
            AppError::Storage(_) => (StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
            AppError::Unauthorized(_) => (StatusCode::UNAUTHORIZED, "unauthorized"),
            AppError::BulkValidation(_) => unreachable!(),
        };
        let body = ErrorBody {
            error: ErrorDetail {
                code: code.to_string(),
                message: self.to_string(),
                details: None,
            },
        };
        (status, Json(body)).into_response()
    }
}
