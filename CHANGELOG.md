# Changelog

All notable changes to `foundry-rs` will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Event includes on every write lifecycle**: a trigger's `include` list (related entities expanded into the decision-hub payload's `context.entity`) is now honoured by `archive`, `unarchive`, `create_graph` (parent *and* each child row), `bulk_create`, and `bulk_update`, on both the unprefixed and `/api/v1/package/:package_id/...` routes. Previously only single-row `create`/`update` expanded them; every other lifecycle silently published the flat row. `delete` still publishes the flat row by design — the row is gone by publish time.
  - Bulk paths resolve the include set once per batch and re-point it per row (`EventIncludeCtx::with_pk_value`) rather than re-walking the model for every row.

- **Extensible fields**: per-tenant custom fields on JSON/JSONB columns flagged `"extensible": true`.
  - Field definitions live in a per-tenant **registry** (KV store, reserved namespace `__extensible_fields__`, keyed by `path_segment`), not in the schema.
  - Registry keys become first-class **RSQL filterable/sortable** fields via the `<column>.<key>` dotted syntax (e.g. `q=attributes.warrantyMonths=ge=12`, `sort=-attributes.voltage`), with dialect-aware typed JSON extraction (Postgres `->>` + `::cast`; MySQL/SQLite `->>'$.key'` + `CAST`).
  - Write-time validation (create/update/bulk) against the registry: unknown keys, type/bounds/length/pattern, and required-on-create → `422`.
  - **Admin API** (authrs-gated, requires `X-Tenant-ID`):
    - `GET`/`PUT`/`DELETE` `/api/v1/:entity/extensible-fields` — manage the registry.
    - `GET`/`POST` `/api/v1/:entity/extensible-fields/indexes` — review / apply suggested `CREATE INDEX` DDL for queryable fields (RLS tenants get partial indexes scoped by tenant).
    - All admin routes also have a **package-scoped** form under `/api/v1/package/:package_id/:entity/extensible-fields[/indexes]` (resolves the entity from that package's model).
  - New authrs action verbs: `getExtensibleFields<Table>`, `putExtensibleFields<Table>`, `deleteExtensibleFields<Table>`.
  - Read-through registry cache on `AppState` (TTL-bounded, evicted on write).
  - Multiple extensible columns ("bags") per entity supported; disambiguated by the column prefix.

### Changed
- **Breaking (struct):** `AppState` gained a public `extensible_cache` field. Construct it with `extensible_cache: Default::default()`.

### Fixed
- **Event includes were silently dropped on package-scoped routes.** `build_event_include_ctx` resolved include names against `state.model` (only ever the `_default` package) even when the entity came from a package model, so every configured `include` on `/api/v1/package/:package_id/...` degraded to the flat row with no log line. The caller's model is now passed in, and an unresolvable include logs a warning instead of failing silently.
- Case-insensitive RSQL operators (`=ilike=`/`=contains=`/`=starts=`/`=ends=`) now work on MySQL and SQLite (previously hardcoded `ILIKE`, Postgres-only) via a new `Dialect::case_insensitive_like`.

## [0.1.2] - 2026-05-29

### Fixed
- Resolve all clippy warnings (`redundant_field_names`, `collapsible_match`, `map_entry`, `too_many_arguments`, `type_complexity`, `useless_conversion`, `explicit_auto_deref`, `cloned_ref_to_slice_refs`)

## [0.1.1] - 2026-05-29

### Changed
- Apply `cargo fmt` across all source files

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
