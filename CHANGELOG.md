# Changelog

All notable changes to `foundry-rs` will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-05-29

### Added
- Configuration-driven REST API generation from JSON schemas
- PostgreSQL CRUD with parameterized queries via SQLx
- Multi-tenancy: per-tenant Database strategy and Row-Level Security (RLS) strategy
- Package system: install/uninstall domain packages as ZIP archives
- Request validation: required, format, length, pattern, allowed values, numeric range
- Automatic camelCase ↔ snake_case conversion between API and DB
- Sensitive column stripping from all responses
- Related entity includes via scalar subqueries (no N+1)
- Bulk create and bulk delete operations
- KV store API (multi-tenant key-value namespace)
- OpenAPI 3.0 spec generation from config
- Optional cloud storage backends: AWS S3, Azure Blob, Google Cloud Storage
- Async event publishing to decision-hub after CRUD operations
