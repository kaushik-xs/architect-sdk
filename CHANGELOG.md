# Changelog

All notable changes to `foundry-rs` will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Reports (read-only reporting queries)**: a new config kind `reports` that exposes trusted, parameterized SQL as a read-only API — including cross-package queries (RLS tenants share one DB).
  - Report definitions live in `_sys_reports`, loadable either via a package (`reports.json` / `reports/*.json` in the ZIP) **or** standalone via the config API. Reports carry **no DDL** — registering/updating/removing one is a pure metadata write + `reload_model()`.
  - Definition = `{ id, name, description?, schemas[], sql, params[], validate_on_register? }`. SQL uses **named params** (`:from`, `:to`) translated to positional `$N` at resolve time (repeated names dedupe; `::` casts and string literals are preserved). Each param reuses the entity `ValidationRule` engine (`required`/`allowed`/`minimum`/`pattern`/`format`/…) plus `default` and an optional `db_type` cast injected onto its placeholder.
  - **Data plane** (any valid tenant, authorized per-run via authrs — resource `service:{svc}/package:{pkg}/report:{id}`, action `run`):
    - `GET /api/v1/reports` — list report metadata (never the SQL).
    - `GET /api/v1/reports/:report_id` — one report's metadata/param schema.
    - `POST /api/v1/reports/:report_id/run` — execute; body `{ "params": { ... } }`. Params are validated (422 on failure), bound positionally, and results are returned in the standard envelope with `meta.count` / `meta.report` / `meta.truncated`, keys converted snake→camel.
  - **Config plane** (Platform Admin tenant only — the SQL is executed verbatim):
    - `GET`/`POST /api/v1/config/reports` — read raw definitions / replace the whole standalone (`_default`) set.
    - `PUT`/`DELETE /api/v1/config/reports/:report_id` — upsert / remove a single report by id (read-merge-write; the rest of the set is preserved).
  - **Sandboxed execution**: every run (and EXPLAIN-on-register) opens a transaction that is `SET TRANSACTION READ ONLY` (blocks writes and writable CTEs) + `SET LOCAL statement_timeout` + an SDK-enforced outer `LIMIT` row cap; RLS tenants also get `SET LOCAL app.tenant_id` so policies enforce isolation regardless of the SQL; an optional dedicated read-only role is applied via `SET LOCAL ROLE`. New `Dialect` methods `set_read_only_sql` / `set_statement_timeout_sql` / `set_role_sql` (Postgres implemented; default `None` elsewhere).
  - `validate_on_register` (default true) EXPLAIN-validates the SQL against the live schema at registration so missing tables/columns fail fast; set false for reports referencing packages installed later.
  - **Optional result cache** (off by default; enable with `ARCHITECT_REPORT_CACHE`): a run's rows are cached and re-served until they expire. Cached envelopes are stored in the existing `_sys_kv_data` under the reserved namespace `__report_cache__` (no new table; tenant- and package-scoped, so a package uninstall drops its caches too). The **cache key is auto-constructed** at run time as a bounded FNV-1a hash of `{package_id, effective_tenant, report_id, sql, sorted-params}` — different params/tenants/SQL never collide, and a changed definition invalidates automatically. **TTL** is a field inside the stored JSON envelope (`expires_at`); per-report override via `cache_ttl_secs` (0 = never cache this report), else `ARCHITECT_REPORT_CACHE_TTL_SECS` (default 300s). Responses carry `meta.cached` (true on a hit). Cached rows are verified against the exact report/tenant/params on read, so a hash collision is a harmless miss, never a wrong hit.
  - New env vars: `ARCHITECT_REPORT_TIMEOUT_MS` (default 30000), `ARCHITECT_REPORT_MAX_ROWS` (default 10000), `ARCHITECT_REPORT_ROLE` (optional), `ARCHITECT_REPORT_CACHE` (default off), `ARCHITECT_REPORT_CACHE_TTL_SECS` (default 300).
  - **Caveat**: raw report SQL bypasses `sensitive_columns` stripping — rely on a read-only role that lacks `SELECT` on sensitive columns. Cross-package joins only work for RLS-strategy tenants (Database-strategy tenants can only reference packages in their own DB).

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
