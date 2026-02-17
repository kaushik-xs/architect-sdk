//! Request validation from config rules.

use crate::config::ValidationRule;
use crate::error::AppError;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;

pub struct RequestValidator;

impl RequestValidator {
    /// Validate body against per-column rules. All required fields must be present.
    pub fn validate(
        body: &HashMap<String, Value>,
        rules: &HashMap<String, ValidationRule>,
    ) -> Result<(), AppError> {
        for (col, rule) in rules {
            let val = body.get(col);
            if rule.required == Some(true) && (val.is_none() || val == Some(&Value::Null)) {
                return Err(AppError::Validation(format!("{} is required", col)));
            }
            if let Some(v) = val {
                validate_field(col, v, rule)?;
            }
        }
        Ok(())
    }

    /// Validate only the fields present in body (for PATCH). Required is not enforced for missing fields.
    pub fn validate_partial(
        body: &HashMap<String, Value>,
        rules: &HashMap<String, ValidationRule>,
    ) -> Result<(), AppError> {
        for (col, v) in body {
            if let Some(rule) = rules.get(col) {
                validate_field(col, v, rule)?;
            }
        }
        Ok(())
    }
}

fn validate_field(col: &str, v: &Value, rule: &ValidationRule) -> Result<(), AppError> {
    if v.is_null() {
        return Ok(());
    }
    if let Some(format) = &rule.format {
        validate_format(col, v, format)?;
    }
    if let Some(max) = rule.max_length {
        if let Some(s) = v.as_str() {
            if s.len() > max as usize {
                return Err(AppError::Validation(format!(
                    "{} must be at most {} characters",
                    col, max
                )));
            }
        }
    }
    if let Some(min) = rule.min_length {
        if let Some(s) = v.as_str() {
            if s.len() < min as usize {
                return Err(AppError::Validation(format!(
                    "{} must be at least {} characters",
                    col, min
                )));
            }
        }
    }
    if let Some(ref pattern) = rule.pattern {
        let re = Regex::new(pattern).map_err(|_| AppError::Validation(format!("invalid pattern for {}", col)))?;
        if let Some(s) = v.as_str() {
            if !re.is_match(s) {
                return Err(AppError::Validation(format!("{} does not match required pattern", col)));
            }
        }
    }
    if let Some(ref allowed) = rule.allowed {
        let mut ok = false;
        for a in allowed {
            if value_eq(v, a) {
                ok = true;
                break;
            }
        }
        if !ok {
            return Err(AppError::Validation(format!(
                "{} must be one of: {:?}",
                col,
                allowed.iter().take(5).collect::<Vec<_>>()
            )));
        }
    }
    if let Some(min) = rule.minimum {
        if let Some(n) = v.as_f64() {
            if n < min {
                return Err(AppError::Validation(format!("{} must be at least {}", col, min)));
            }
        }
    }
    if let Some(max) = rule.maximum {
        if let Some(n) = v.as_f64() {
            if n > max {
                return Err(AppError::Validation(format!("{} must be at most {}", col, max)));
            }
        }
    }
    Ok(())
}

fn value_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::String(s), Value::String(t)) => s == t,
        (Value::Number(n), Value::Number(m)) => n.as_f64() == m.as_f64(),
        _ => a == b,
    }
}

fn validate_format(col: &str, v: &Value, format: &str) -> Result<(), AppError> {
    match format.to_lowercase().as_str() {
        "email" => {
            if let Some(s) = v.as_str() {
                if !s.contains('@') || s.len() < 3 {
                    return Err(AppError::Validation(format!("{} must be a valid email", col)));
                }
            }
        }
        "uuid" => {
            if let Some(s) = v.as_str() {
                if uuid::Uuid::parse_str(s).is_err() {
                    return Err(AppError::Validation(format!("{} must be a valid UUID", col)));
                }
            }
        }
        _ => {}
    }
    Ok(())
}
