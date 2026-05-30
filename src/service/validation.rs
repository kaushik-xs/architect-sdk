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

    /// Like `validate` but collects all errors instead of stopping at the first.
    /// Returns a vec of (field, message) pairs.
    pub fn validate_collecting(
        body: &HashMap<String, Value>,
        rules: &HashMap<String, ValidationRule>,
    ) -> Vec<(String, String)> {
        let mut errors = Vec::new();
        for (col, rule) in rules {
            let val = body.get(col);
            if rule.required == Some(true) && (val.is_none() || val == Some(&Value::Null)) {
                errors.push((col.clone(), format!("{} is required", col)));
                continue;
            }
            if let Some(v) = val {
                if let Err(AppError::Validation(msg)) = validate_field(col, v, rule) {
                    errors.push((col.clone(), msg));
                }
            }
        }
        errors
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
        let re = Regex::new(pattern)
            .map_err(|_| AppError::Validation(format!("invalid pattern for {}", col)))?;
        if let Some(s) = v.as_str() {
            if !re.is_match(s) {
                return Err(AppError::Validation(format!(
                    "{} does not match required pattern",
                    col
                )));
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
                return Err(AppError::Validation(format!(
                    "{} must be at least {}",
                    col, min
                )));
            }
        }
    }
    if let Some(max) = rule.maximum {
        if let Some(n) = v.as_f64() {
            if n > max {
                return Err(AppError::Validation(format!(
                    "{} must be at most {}",
                    col, max
                )));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ValidationRule;
    use serde_json::json;

    fn rule(f: impl FnOnce(&mut ValidationRule)) -> ValidationRule {
        let mut r = ValidationRule::default();
        f(&mut r);
        r
    }

    fn body(pairs: &[(&str, serde_json::Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn rules_map(pairs: &[(&str, ValidationRule)]) -> HashMap<String, ValidationRule> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    // --- required ---

    #[test]
    fn required_field_present_passes() {
        let rules = rules_map(&[("name", rule(|r| r.required = Some(true)))]);
        let b = body(&[("name", json!("Alice"))]);
        assert!(RequestValidator::validate(&b, &rules).is_ok());
    }

    #[test]
    fn required_field_missing_fails() {
        let rules = rules_map(&[("name", rule(|r| r.required = Some(true)))]);
        let b = body(&[]);
        assert!(RequestValidator::validate(&b, &rules).is_err());
    }

    #[test]
    fn required_field_null_fails() {
        let rules = rules_map(&[("name", rule(|r| r.required = Some(true)))]);
        let b = body(&[("name", json!(null))]);
        assert!(RequestValidator::validate(&b, &rules).is_err());
    }

    #[test]
    fn optional_field_absent_passes() {
        let rules = rules_map(&[("bio", rule(|_| {}))]);
        let b = body(&[]);
        assert!(RequestValidator::validate(&b, &rules).is_ok());
    }

    // --- partial validation ---

    #[test]
    fn partial_skips_missing_required() {
        let rules = rules_map(&[("name", rule(|r| r.required = Some(true)))]);
        let b = body(&[]); // name absent — OK for PATCH
        assert!(RequestValidator::validate_partial(&b, &rules).is_ok());
    }

    #[test]
    fn partial_validates_present_field() {
        let rules = rules_map(&[("email", rule(|r| r.format = Some("email".into())))]);
        let b = body(&[("email", json!("not-an-email"))]);
        assert!(RequestValidator::validate_partial(&b, &rules).is_err());
    }

    // --- format: email ---

    #[test]
    fn email_valid() {
        let rules = rules_map(&[("email", rule(|r| r.format = Some("email".into())))]);
        let b = body(&[("email", json!("user@example.com"))]);
        assert!(RequestValidator::validate(&b, &rules).is_ok());
    }

    #[test]
    fn email_invalid_no_at() {
        let rules = rules_map(&[("email", rule(|r| r.format = Some("email".into())))]);
        let b = body(&[("email", json!("notanemail"))]);
        assert!(RequestValidator::validate(&b, &rules).is_err());
    }

    // --- format: uuid ---

    #[test]
    fn uuid_valid() {
        let rules = rules_map(&[("id", rule(|r| r.format = Some("uuid".into())))]);
        let b = body(&[("id", json!("550e8400-e29b-41d4-a716-446655440000"))]);
        assert!(RequestValidator::validate(&b, &rules).is_ok());
    }

    #[test]
    fn uuid_invalid() {
        let rules = rules_map(&[("id", rule(|r| r.format = Some("uuid".into())))]);
        let b = body(&[("id", json!("not-a-uuid"))]);
        assert!(RequestValidator::validate(&b, &rules).is_err());
    }

    // --- max_length / min_length ---

    #[test]
    fn max_length_pass() {
        let rules = rules_map(&[("bio", rule(|r| r.max_length = Some(10)))]);
        let b = body(&[("bio", json!("hello"))]);
        assert!(RequestValidator::validate(&b, &rules).is_ok());
    }

    #[test]
    fn max_length_fail() {
        let rules = rules_map(&[("bio", rule(|r| r.max_length = Some(3)))]);
        let b = body(&[("bio", json!("toolong"))]);
        assert!(RequestValidator::validate(&b, &rules).is_err());
    }

    #[test]
    fn min_length_pass() {
        let rules = rules_map(&[("code", rule(|r| r.min_length = Some(3)))]);
        let b = body(&[("code", json!("abc"))]);
        assert!(RequestValidator::validate(&b, &rules).is_ok());
    }

    #[test]
    fn min_length_fail() {
        let rules = rules_map(&[("code", rule(|r| r.min_length = Some(5)))]);
        let b = body(&[("code", json!("hi"))]);
        assert!(RequestValidator::validate(&b, &rules).is_err());
    }

    // --- pattern ---

    #[test]
    fn pattern_match_passes() {
        let rules = rules_map(&[("zip", rule(|r| r.pattern = Some(r"^\d{5}$".into())))]);
        let b = body(&[("zip", json!("12345"))]);
        assert!(RequestValidator::validate(&b, &rules).is_ok());
    }

    #[test]
    fn pattern_no_match_fails() {
        let rules = rules_map(&[("zip", rule(|r| r.pattern = Some(r"^\d{5}$".into())))]);
        let b = body(&[("zip", json!("abc"))]);
        assert!(RequestValidator::validate(&b, &rules).is_err());
    }

    // --- allowed ---

    #[test]
    fn allowed_values_pass() {
        let rules = rules_map(&[(
            "status",
            rule(|r| r.allowed = Some(vec![json!("active"), json!("inactive")])),
        )]);
        let b = body(&[("status", json!("active"))]);
        assert!(RequestValidator::validate(&b, &rules).is_ok());
    }

    #[test]
    fn allowed_values_fail() {
        let rules = rules_map(&[(
            "status",
            rule(|r| r.allowed = Some(vec![json!("active"), json!("inactive")])),
        )]);
        let b = body(&[("status", json!("pending"))]);
        assert!(RequestValidator::validate(&b, &rules).is_err());
    }

    // --- minimum / maximum ---

    #[test]
    fn minimum_passes() {
        let rules = rules_map(&[("age", rule(|r| r.minimum = Some(0.0)))]);
        let b = body(&[("age", json!(5))]);
        assert!(RequestValidator::validate(&b, &rules).is_ok());
    }

    #[test]
    fn minimum_fails() {
        let rules = rules_map(&[("age", rule(|r| r.minimum = Some(18.0)))]);
        let b = body(&[("age", json!(10))]);
        assert!(RequestValidator::validate(&b, &rules).is_err());
    }

    #[test]
    fn maximum_passes() {
        let rules = rules_map(&[("score", rule(|r| r.maximum = Some(100.0)))]);
        let b = body(&[("score", json!(99))]);
        assert!(RequestValidator::validate(&b, &rules).is_ok());
    }

    #[test]
    fn maximum_fails() {
        let rules = rules_map(&[("score", rule(|r| r.maximum = Some(100.0)))]);
        let b = body(&[("score", json!(101))]);
        assert!(RequestValidator::validate(&b, &rules).is_err());
    }

    // --- null value skips field-level checks ---

    #[test]
    fn null_value_skips_format_check() {
        let rules = rules_map(&[("email", rule(|r| r.format = Some("email".into())))]);
        let b = body(&[("email", json!(null))]);
        // null is not required, and null skips format validation
        assert!(RequestValidator::validate(&b, &rules).is_ok());
    }

    // --- validate_collecting ---

    #[test]
    fn collecting_returns_all_errors() {
        let rules = rules_map(&[
            ("name", rule(|r| r.required = Some(true))),
            ("email", rule(|r| r.required = Some(true))),
        ]);
        let b = body(&[]);
        let errors = RequestValidator::validate_collecting(&b, &rules);
        assert_eq!(errors.len(), 2);
    }

    #[test]
    fn collecting_returns_empty_on_success() {
        let rules = rules_map(&[("name", rule(|r| r.required = Some(true)))]);
        let b = body(&[("name", json!("Alice"))]);
        let errors = RequestValidator::validate_collecting(&b, &rules);
        assert!(errors.is_empty());
    }
}

fn validate_format(col: &str, v: &Value, format: &str) -> Result<(), AppError> {
    match format.to_lowercase().as_str() {
        "email" => {
            if let Some(s) = v.as_str() {
                if !s.contains('@') || s.len() < 3 {
                    return Err(AppError::Validation(format!(
                        "{} must be a valid email",
                        col
                    )));
                }
            }
        }
        "uuid" => {
            if let Some(s) = v.as_str() {
                if uuid::Uuid::parse_str(s).is_err() {
                    return Err(AppError::Validation(format!(
                        "{} must be a valid UUID",
                        col
                    )));
                }
            }
        }
        _ => {}
    }
    Ok(())
}
