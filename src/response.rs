//! Standard response envelope helpers.

use axum::{http::StatusCode, Json};
use serde::Serialize;

#[derive(Serialize)]
pub struct SuccessOne<T> {
    pub data: T,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<serde_json::Value>,
}

#[derive(Serialize)]
pub struct SuccessMany<T> {
    pub data: Vec<T>,
    pub meta: MetaCount,
}

#[derive(Serialize)]
pub struct MetaCount {
    pub count: u64,
}

pub fn success_one<T: Serialize>(data: T) -> (StatusCode, Json<SuccessOne<T>>) {
    (
        StatusCode::CREATED,
        Json(SuccessOne {
            data,
            meta: None,
        }),
    )
}

pub fn success_one_ok<T: Serialize>(data: T) -> (StatusCode, Json<SuccessOne<T>>) {
    (
        StatusCode::OK,
        Json(SuccessOne {
            data,
            meta: None,
        }),
    )
}

pub fn success_many<T: Serialize>(data: Vec<T>) -> (StatusCode, Json<SuccessMany<T>>) {
    let count = data.len() as u64;
    (
        StatusCode::OK,
        Json(SuccessMany {
            data,
            meta: MetaCount { count },
        }),
    )
}

pub fn success_many_created<T: Serialize>(data: Vec<T>) -> (StatusCode, Json<SuccessMany<T>>) {
    let count = data.len() as u64;
    (
        StatusCode::CREATED,
        Json(SuccessMany {
            data,
            meta: MetaCount { count },
        }),
    )
}

pub fn error_body(code: &str, message: String, details: Option<serde_json::Value>) -> serde_json::Value {
    serde_json::json!({
        "error": {
            "code": code,
            "message": message,
            "details": details
        }
    })
}
