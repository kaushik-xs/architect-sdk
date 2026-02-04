//! Case conversion for API: request keys camelCase -> snake_case (for DB), response keys snake_case -> camelCase (for client).

use serde_json::{Map, Value};
use std::collections::HashMap;

/// Convert a single identifier from snake_case to camelCase.
/// e.g. "user_id" -> "userId", "created_at" -> "createdAt"
pub fn to_camel_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut capitalize_next = false;
    for c in s.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            out.extend(c.to_uppercase());
            capitalize_next = false;
        } else {
            out.push(c);
        }
    }
    out
}

/// Convert a single identifier from camelCase to snake_case.
/// e.g. "userId" -> "user_id", "createdAt" -> "created_at"
pub fn to_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// Convert all keys of a JSON object from snake_case to camelCase (in place).
/// Used for API responses so the client receives camelCase keys.
pub fn object_keys_to_camel_case(obj: &mut Map<String, Value>) {
    let keys: Vec<String> = obj.keys().cloned().collect();
    for k in keys {
        let camel = to_camel_case(&k);
        if camel != k {
            if let Some(v) = obj.remove(&k) {
                obj.insert(camel, v);
            }
        }
    }
}

/// Convert all keys of a JSON object from camelCase to snake_case (in place).
/// Used for request bodies and query params so we use snake_case for DB column names.
pub fn object_keys_to_snake_case(obj: &mut Map<String, Value>) {
    let keys: Vec<String> = obj.keys().cloned().collect();
    for k in keys {
        let snake = to_snake_case(&k);
        if snake != k {
            if let Some(v) = obj.remove(&k) {
                obj.insert(snake, v);
            }
        }
    }
}

/// Apply camelCase conversion to a Value. If it's an object, converts its keys; otherwise no-op.
pub fn value_keys_to_camel_case(value: &mut Value) {
    if let Value::Object(ref mut map) = value {
        object_keys_to_camel_case(map);
    }
}

/// Recursively apply camelCase to all object keys in a Value (objects and arrays of objects).
pub fn value_keys_to_camel_case_recursive(value: &mut Value) {
    match value {
        Value::Object(map) => {
            object_keys_to_camel_case(map);
            for (_, v) in map.iter_mut() {
                value_keys_to_camel_case_recursive(v);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                value_keys_to_camel_case_recursive(v);
            }
        }
        _ => {}
    }
}

/// Apply snake_case conversion to a Value. If it's an object, converts its keys; otherwise no-op.
pub fn value_keys_to_snake_case(value: &mut Value) {
    if let Value::Object(ref mut map) = value {
        object_keys_to_snake_case(map);
    }
}

/// Convert a HashMap's keys from camelCase to snake_case. Returns a new map.
pub fn hashmap_keys_to_snake_case(map: &HashMap<String, Value>) -> HashMap<String, Value> {
    map.iter()
        .map(|(k, v)| (to_snake_case(k), v.clone()))
        .collect()
}
