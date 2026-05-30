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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- to_camel_case ---

    #[test]
    fn camel_single_underscore() {
        assert_eq!(to_camel_case("user_id"), "userId");
    }

    #[test]
    fn camel_multiple_underscores() {
        assert_eq!(to_camel_case("created_at_time"), "createdAtTime");
    }

    #[test]
    fn camel_no_underscore() {
        assert_eq!(to_camel_case("name"), "name");
    }

    #[test]
    fn camel_leading_underscore() {
        assert_eq!(to_camel_case("_private"), "Private");
    }

    #[test]
    fn camel_empty() {
        assert_eq!(to_camel_case(""), "");
    }

    #[test]
    fn camel_trailing_underscore() {
        // trailing underscore sets capitalize_next=true but no char follows — no crash
        assert_eq!(to_camel_case("foo_"), "foo");
    }

    // --- to_snake_case ---

    #[test]
    fn snake_simple() {
        assert_eq!(to_snake_case("userId"), "user_id");
    }

    #[test]
    fn snake_multiple_caps() {
        assert_eq!(to_snake_case("createdAt"), "created_at");
    }

    #[test]
    fn snake_already_snake() {
        assert_eq!(to_snake_case("user_id"), "user_id");
    }

    #[test]
    fn snake_leading_cap() {
        // Leading uppercase: no underscore prepended at index 0
        assert_eq!(to_snake_case("UserName"), "user_name");
    }

    #[test]
    fn snake_empty() {
        assert_eq!(to_snake_case(""), "");
    }

    // --- round-trip ---

    #[test]
    fn round_trip_snake_to_camel_to_snake() {
        let original = "order_line_item";
        assert_eq!(to_snake_case(&to_camel_case(original)), original);
    }

    // --- object_keys_to_camel_case ---

    #[test]
    fn object_keys_camel_converts_keys() {
        let mut map = json!({"user_id": 1, "created_at": "2024"})
            .as_object()
            .unwrap()
            .clone();
        object_keys_to_camel_case(&mut map);
        assert!(map.contains_key("userId"));
        assert!(map.contains_key("createdAt"));
        assert!(!map.contains_key("user_id"));
    }

    #[test]
    fn object_keys_snake_converts_keys() {
        let mut map = json!({"userId": 1, "createdAt": "2024"})
            .as_object()
            .unwrap()
            .clone();
        object_keys_to_snake_case(&mut map);
        assert!(map.contains_key("user_id"));
        assert!(map.contains_key("created_at"));
        assert!(!map.contains_key("userId"));
    }

    // --- value_keys_to_camel_case_recursive ---

    #[test]
    fn recursive_camel_nested_object() {
        let mut v = json!({
            "user_id": 1,
            "address": { "zip_code": "12345" }
        });
        value_keys_to_camel_case_recursive(&mut v);
        assert!(v.get("userId").is_some());
        assert!(v.get("address").unwrap().get("zipCode").is_some());
    }

    #[test]
    fn recursive_camel_array_of_objects() {
        let mut v = json!([
            { "first_name": "Alice" },
            { "first_name": "Bob" }
        ]);
        value_keys_to_camel_case_recursive(&mut v);
        let arr = v.as_array().unwrap();
        assert!(arr[0].get("firstName").is_some());
        assert!(arr[1].get("firstName").is_some());
    }

    #[test]
    fn recursive_camel_scalar_is_noop() {
        let mut v = json!(42);
        value_keys_to_camel_case_recursive(&mut v);
        assert_eq!(v, json!(42));
    }

    // --- hashmap_keys_to_snake_case ---

    #[test]
    fn hashmap_snake_keys() {
        let map: HashMap<String, Value> = [
            ("firstName".to_string(), json!("Alice")),
            ("lastName".to_string(), json!("Smith")),
        ]
        .into_iter()
        .collect();
        let result = hashmap_keys_to_snake_case(&map);
        assert!(result.contains_key("first_name"));
        assert!(result.contains_key("last_name"));
        assert_eq!(result["first_name"], json!("Alice"));
    }
}
