//! Package dependencies declared in `manifest.json`.
//!
//! `dependencies` takes two forms:
//! - `["ecommerce", "crm"]`: the packages must be installed, any version.
//! - `{"ecommerce": ">=1.2.0, <2.0.0", "crm": "*"}`: each value is a semver requirement
//!   (Cargo syntax, comparators separated by commas) the installed version must satisfy.
//!
//! The requirement is checked when the package is installed, and again whenever a package it
//! depends on is upgraded, so a domain package cannot move outside a range its dependents declare.

use crate::error::AppError;
use semver::{Version, VersionReq};
use serde_json::Value;

/// One declared dependency: the package id and, in the object form, its version requirement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Dependency {
    pub package_id: String,
    pub requirement: Option<VersionReq>,
}

/// Parse `manifest.dependencies`. A missing field means no dependencies.
pub(crate) fn manifest_dependencies(manifest: &Value) -> Result<Vec<Dependency>, AppError> {
    match manifest.get("dependencies") {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(ids)) => ids
            .iter()
            .map(|v| {
                v.as_str()
                    .map(|id| Dependency {
                        package_id: id.to_string(),
                        requirement: None,
                    })
                    .ok_or_else(|| {
                        AppError::BadRequest("manifest dependencies must be package id strings".into())
                    })
            })
            .collect(),
        Some(Value::Object(map)) => map
            .iter()
            .map(|(id, req)| {
                let req = req.as_str().ok_or_else(|| {
                    AppError::BadRequest(format!(
                        "manifest dependency '{}' must map to a version requirement string",
                        id
                    ))
                })?;
                let requirement = VersionReq::parse(req).map_err(|e| {
                    AppError::BadRequest(format!(
                        "manifest dependency '{}': invalid version requirement '{}': {} \
                         (use semver syntax such as \">=1.2.0, <2.0.0\")",
                        id, req, e
                    ))
                })?;
                Ok(Dependency {
                    package_id: id.clone(),
                    requirement: Some(requirement),
                })
            })
            .collect(),
        Some(_) => Err(AppError::BadRequest(
            "manifest dependencies must be an array of package ids or an object of id → version requirement"
                .into(),
        )),
    }
}

/// Parse a package version, accepting a missing minor or patch (`1` → `1.0.0`, `1.2` → `1.2.0`).
pub(crate) fn parse_version(v: &str) -> Option<Version> {
    let v = v.trim().trim_start_matches('v');
    Version::parse(v).ok().or_else(|| {
        let parts = v.split('.').count();
        if v.contains(['-', '+']) || parts >= 3 {
            return None;
        }
        Version::parse(&format!("{}{}", v, ".0".repeat(3 - parts))).ok()
    })
}

/// Whether `version` satisfies `dep`. No requirement always does; an unparsable version never
/// satisfies a requirement.
pub(crate) fn satisfies(dep: &Dependency, version: Option<&str>) -> bool {
    match &dep.requirement {
        None => true,
        Some(req) => version
            .and_then(parse_version)
            .is_some_and(|v| req.matches(&v)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dep(id: &str, req: Option<&str>) -> Dependency {
        Dependency {
            package_id: id.into(),
            requirement: req.map(|r| VersionReq::parse(r).unwrap()),
        }
    }

    #[test]
    fn array_form_has_no_requirements() {
        let m = json!({ "dependencies": ["ecommerce", "crm"] });
        assert_eq!(
            manifest_dependencies(&m).unwrap(),
            vec![dep("ecommerce", None), dep("crm", None)]
        );
    }

    #[test]
    fn object_form_parses_requirements() {
        let m = json!({ "dependencies": { "ecommerce": ">=1.2.0, <2.0.0" } });
        assert_eq!(
            manifest_dependencies(&m).unwrap(),
            vec![dep("ecommerce", Some(">=1.2.0, <2.0.0"))]
        );
    }

    #[test]
    fn missing_dependencies_is_empty() {
        assert!(manifest_dependencies(&json!({})).unwrap().is_empty());
    }

    #[test]
    fn invalid_forms_are_rejected() {
        assert!(manifest_dependencies(&json!({ "dependencies": "ecommerce" })).is_err());
        assert!(manifest_dependencies(&json!({ "dependencies": [1] })).is_err());
        assert!(manifest_dependencies(&json!({ "dependencies": { "x": 1 } })).is_err());
        assert!(manifest_dependencies(&json!({ "dependencies": { "x": "not a range" } })).is_err());
    }

    #[test]
    fn short_versions_are_padded() {
        assert_eq!(parse_version("1"), Some(Version::new(1, 0, 0)));
        assert_eq!(parse_version("1.2"), Some(Version::new(1, 2, 0)));
        assert_eq!(parse_version("v1.2.3"), Some(Version::new(1, 2, 3)));
        assert_eq!(parse_version("banana"), None);
    }

    #[test]
    fn requirement_checks() {
        let d = dep("ecommerce", Some(">=1.2.0, <2.0.0"));
        assert!(satisfies(&d, Some("1.4.0")));
        assert!(satisfies(&d, Some("1.2")));
        assert!(!satisfies(&d, Some("2.0.0")));
        assert!(!satisfies(&d, Some("1.1.9")));
        assert!(!satisfies(&d, None));
        assert!(satisfies(&dep("crm", None), None));
    }
}
