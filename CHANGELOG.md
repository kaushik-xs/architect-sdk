# Changelog

All notable changes to `foundry-rs` will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Generated (computed) columns**: `ColumnConfig.generated = { expression, expression_by_dialect? }` creates a `GENERATED ALWAYS AS (…) STORED` column (`VIRTUAL` when added to an existing SQLite table).
  - Read-only over the API: never written by INSERT/UPDATE (client values are ignored), omitted from OpenAPI/MCP request schemas, still returned and filterable on reads. `ColumnInfo` has a new `generated` flag.
  - Validation rejects a generated column with a `default`, in the primary key, or flagged as asset/extensible.
  - Upgrades: new migration operations `set_generated_expression` (Postgres 17+ `SET EXPRESSION`, MySQL `MODIFY COLUMN`) and `drop_generated_expression` (Postgres 13+ `DROP EXPRESSION`, MySQL `MODIFY COLUMN`). Regular → generated, and any change on SQLite, is a warn-only manual step.
  - New `Dialect` methods `generated_column_clause`, `alter_generated_expression_sql`, `drop_generated_expression_sql`.
- **Views**: a new package config kind `views` (`views.json`, stored in `_sys_views`) for read-only SQL views, e.g. analytics joins across packages and schemas in the same database.
  - Definition = `{ id, schema_id?, name, sql, sql_by_dialect?, columns[], primary_key?, reads[], materialized?, comment? }`. `sql_by_dialect` overrides `sql` per dialect (`postgres` / `mysql` / `sqlite`).
  - Install checks each view's SQL against the database and fails if its output columns differ from `columns`. Views are created after tables and FKs, in `reads` order, in every target database (central RLS DB and each Database-strategy tenant DB).
  - Postgres creates views `WITH (security_invoker = true)` so base-table RLS policies apply to the caller; **views require PostgreSQL 15+**. MySQL uses `SQL SECURITY INVOKER`; SQLite emulates replace with drop + create.
  - `api_entities.entity_id` may name a view. View entities allow only `list` and `read` (with filters, sorting, pagination and `sensitive_columns`) and need a `primary_key`.
  - Upgrades drop affected views (changed, new, reading a touched table, or reading a rebuilt view) before table steps and recreate them afterwards (`drop_view` / `create_view` steps). Uninstall drops the package's views first.
  - New `Dialect` methods `create_view_sql`, `create_view_replaces`, `drop_view_sql`.
- **Materialized views**: `"materialized": true` on a view stores its result until refreshed.
  - The SQL returns the row's tenant with the new tenant token `{{tenant_id:<alias>}} AS tenant_id`, which expands to `<alias>."tenant_id"` in shared (RLS) databases and to a typed `NULL` in per-tenant databases (`migration::render_tenant_tokens`). Plans are rendered per target (`MigrationPlan::for_target`); RLS targets reconcile the tenant column before view creation (`MigrationPlan::split_view_creates`).
  - Stored in `<name>__mv` (Postgres: native materialized view plus a unique index on the tenant column and primary key for `REFRESH … CONCURRENTLY`; MySQL/SQLite: a table) and read through a plain wrapper view `<name>` that returns the declared columns filtered to the session tenant. The filter fails closed: a shared-database session without `app.tenant_id` sees no rows.
  - New routes: `POST /api/v1/views/:view_id/refresh`, `GET /api/v1/views/:view_id` and their `/api/v1/package/:package_id/...` forms (authrs resource `view:{id}`, actions `refresh` / `get`). The last refresh per database is kept in `_sys_kv_data` under the reserved namespace `__view_refresh__`.
  - New `Dialect` methods `create_materialized_view_sql`, `materialized_view_key_sql`, `drop_materialized_view_sql`, `refresh_materialized_view_sql`, `tenant_session_filter_sql`, `null_text_expr`. `ResolvedModel` has a new `views` field.
- **Scheduled refresh of materialized views**: a view's `refresh: { "max_age_secs": N }` (materialized only, N ≥ 60) sets how fresh it must stay. `POST /api/v1/views/refresh-stale` (Platform Admin tenant only) refreshes every view past its `max_age_secs`, in every installed package and every database (`ddl_targets`), and is meant to be called by one scheduler job such as a workflow-engine cron schedule. Attempts are recorded before they run; a failed or in-progress attempt is not retried for 60 seconds. Response: `data.refreshed[]`, `data.failed[]` (each `{packageId, viewId, target, …}`), `meta.{refreshed, failed, skipped}`. `ResolvedView` has a new `max_age_secs` field.
- **Version ranges in package dependencies**: `manifest.dependencies` may be an object mapping package id → semver requirement (`{"shop": ">=1.0.0, <2.0.0"}`) as well as the existing list of ids. Install rejects a missing or out-of-range dependency, and installing or upgrading a package to a version a dependent's range excludes is rejected with `409` (install, migration preview and apply).
- **Upgrade guard for dependent views**: an upgrade that retypes or drops a column read by a view outside the package is rejected with `409` naming the view and column, checked on every target database before any DDL runs. Previously Postgres refused the `ALTER`, the step was recorded as a warning, and the stored config moved ahead of the database.
- **Uninstall guard for dependent views**: uninstalling a package is rejected with `409` when views outside its own schemas read its tables, directly or through other views (Postgres and MySQL 8.0.13+). Previously `DROP TABLE … CASCADE` dropped such views silently on Postgres. New `Dialect::dependent_views_sql`.

### Changed
- `revert_migrations` logs failed `DROP` statements instead of discarding them, and skips `DROP TYPE` / `DROP SCHEMA` on dialects without named enum types or schemas.
- `FullConfig` has a new `views` field; code that builds `FullConfig` with every field listed must add `views` (or use `..Default::default()`).
- `ColumnTypeConfig` now derives `PartialEq` and `Eq`.
- `ResolvedModel` has a new `views` field; code that builds `ResolvedModel` literally must add it.
- `ColumnConfig` and `ColumnInfo` each have a new `generated` field; struct literals must add it.
- **Queries (saved read-only queries)**: a new config kind `queries` that exposes trusted, parameterized SQL as a read-only API — including cross-package queries (RLS tenants share one DB).
  - Query definitions live in `_sys_queries`, loadable either via a package (`queries.json` / `queries/*.json` in the ZIP) **or** standalone via the config API. Queries carry **no DDL** — registering/updating/removing one is a pure metadata write + `reload_model()`.
  - Definition = `{ id, name, description?, schemas[], sql, params[], validate_on_register? }`. SQL uses **named params** (`:from`, `:to`) translated to positional `$N` at resolve time (repeated names dedupe; `::` casts and string literals are preserved). Each param reuses the entity `ValidationRule` engine (`required`/`allowed`/`minimum`/`pattern`/`format`/…) plus `default` and an optional `db_type` cast injected onto its placeholder.
  - **Data plane** (any valid tenant, authorized per-run via authrs — resource `service:{svc}/package:{pkg}/query:{id}`, action `run`):
    - `GET /api/v1/queries` — list query metadata (never the SQL).
    - `GET /api/v1/queries/:query_id` — one query's metadata/param schema.
    - `POST /api/v1/queries/:query_id/run` — execute; body `{ "params": { ... } }`. Params are validated (422 on failure), bound positionally, and results are returned in the standard envelope with `meta.count` / `meta.query` / `meta.truncated`, keys converted snake→camel.
  - **Config plane** (Platform Admin tenant only — the SQL is executed verbatim):
    - `GET`/`POST /api/v1/config/queries` — read raw definitions / replace the whole standalone (`_default`) set.
    - `PUT`/`DELETE /api/v1/config/queries/:query_id` — upsert / remove a single query by id (read-merge-write; the rest of the set is preserved).
  - **Sandboxed execution**: every run (and EXPLAIN-on-register) opens a transaction that is `SET TRANSACTION READ ONLY` (blocks writes and writable CTEs) + `SET LOCAL statement_timeout` + an SDK-enforced outer `LIMIT` row cap; RLS tenants also get `SET LOCAL app.tenant_id` so policies enforce isolation regardless of the SQL; an optional dedicated read-only role is applied via `SET LOCAL ROLE`. New `Dialect` methods `set_read_only_sql` / `set_statement_timeout_sql` / `set_role_sql` (Postgres implemented; default `None` elsewhere).
  - `validate_on_register` (default true) EXPLAIN-validates the SQL against the live schema at registration so missing tables/columns fail fast; set false for queries referencing packages installed later.
  - **Optional result cache** (off by default; enable with `ARCHITECT_QUERY_CACHE`): a run's rows are cached and re-served until they expire. Cached envelopes are stored in the existing `_sys_kv_data` under the reserved namespace `__query_cache__` (no new table; tenant- and package-scoped, so a package uninstall drops its caches too). The **cache key is auto-constructed** at run time as a bounded FNV-1a hash of `{package_id, effective_tenant, query_id, sql, sorted-params}` — different params/tenants/SQL never collide, and a changed definition invalidates automatically. **TTL** is a field inside the stored JSON envelope (`expires_at`); per-query override via `cache_ttl_secs` (0 = never cache this query), else `ARCHITECT_QUERY_CACHE_TTL_SECS` (default 300s). Responses carry `meta.cached` (true on a hit). Cached rows are verified against the exact query/tenant/params on read, so a hash collision is a harmless miss, never a wrong hit.
  - New env vars: `ARCHITECT_QUERY_TIMEOUT_MS` (default 30000), `ARCHITECT_QUERY_MAX_ROWS` (default 10000), `ARCHITECT_QUERY_ROLE` (optional), `ARCHITECT_QUERY_CACHE` (default off), `ARCHITECT_QUERY_CACHE_TTL_SECS` (default 300).
  - **Renamed from `reports`** (hard rename, no aliases): config kind/file `reports` → `queries`, routes `/reports` → `/queries`, types `ReportConfig`/`ReportParam`/`ResolvedReport` → `QueryConfig`/`QueryParam`/`ResolvedQuery`, `report_routes` → `query_routes`, authrs resource `report:{id}` → `query:{id}`, env `ARCHITECT_REPORT_*` → `ARCHITECT_QUERY_*`, run response `meta.report` → `meta.query`. An existing `_sys_reports` table (and its `_history`) is renamed to `_sys_queries` in place by `ensure_sys_tables`, so stored definitions carry over.
  - **Caveat**: raw query SQL bypasses `sensitive_columns` stripping — rely on a read-only role that lacks `SELECT` on sensitive columns. Cross-package joins only work for RLS-strategy tenants (Database-strategy tenants can only reference packages in their own DB).

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
