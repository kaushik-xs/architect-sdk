# Architect SDK

Configuration-driven REST backend library for Rust. Define your entire data model — schemas, tables, columns, indexes, relationships, API endpoints — in JSON. No entity-specific business logic required.

Supports **PostgreSQL**, **MySQL**, and **SQLite** via compile-time dialect selection.

## Features

- **Config-driven schema**: All DB structure defined in JSON; no hardcoded migrations
- **Auto CRUD API**: List, read, create, update, delete, bulk ops — all generated from config
- **Multi-database**: PostgreSQL, MySQL 8+, SQLite 3.35+ — one dialect per binary, zero overhead
- **Canonical type system**: Standard type names in package configs (`uuid`, `json`, `timestamp`, …) mapped to each database's native types automatically
- **Multi-tenancy**: Database-per-tenant or Row-Level Security (RLS, Postgres only), configured per tenant
- **Package system**: Deploy config as versioned ZIP packages with install/uninstall/upgrade
- **Migration planning**: Diff-based upgrades with preview before execution
- **Asset storage**: File uploads with S3, Azure, GCS, or local filesystem backends
- **Extensible fields**: Per-tenant custom fields on JSON/JSONB columns, filterable/sortable via RSQL — no schema change per tenant
- **Request validation**: Per-column rules (required, format, length, pattern, allowed, min/max)
- **Audit logging**: Optional per-table audit trail with row snapshots and change deltas
- **Event publishing**: Optional async event publishing to Decision Hub after CRUD ops
- **Authorization**: Optional permission checks via Authrs integration
- **OpenAPI spec**: Dynamically generated from config at `GET /spec`
- **Safe SQL**: All identifiers from validated config; values always use parameterized placeholders

---

## Quick Start

Add the SDK to your `Cargo.toml`:

```toml
[dependencies]
# PostgreSQL (default)
foundry-rs = "0.2"

# MySQL
foundry-rs = { version = "0.2", default-features = false, features = ["mysql"] }

# SQLite
foundry-rs = { version = "0.2", default-features = false, features = ["sqlite"] }

# With cloud storage
foundry-rs = { version = "0.2", features = ["storage-s3"] }
```

The crate is published as [`foundry-rs`](https://crates.io/crates/foundry-rs) on crates.io. The Rust library name is `architect_sdk` — import it as `use architect_sdk::...`.

Run the bundled example server:

```bash
cp .env.example .env
# Optional: set PACKAGE_PATH=sample to auto-load config from sample/
cargo run --example server
# Starts on http://0.0.0.0:3000
```

---

## Database Dialect Selection

Exactly **one** dialect feature must be enabled per binary. The active dialect is determined at compile time — there is no runtime overhead.

| Database | Feature flag | Notes |
|---|---|---|
| PostgreSQL 12+ | `postgres` *(default)* | Full support including RLS, JSONB, native UUID, named enums |
| MySQL 8.0+ | `mysql` | UUID stored as CHAR(36), JSON (no JSONB), no RLS |
| SQLite 3.35+ | `sqlite` | UUID/JSON/timestamps stored as TEXT, no RLS |

```toml
# Explicit Postgres (same as default)
foundry-rs = { version = "0.2", features = ["postgres"] }

# Switch to MySQL — disable default first
foundry-rs = { version = "0.2", default-features = false, features = ["mysql"] }
```

Enabling more than one dialect feature at a time is a **compile error** (caught by `build.rs`).

### Type mapping

Package configs use canonical type names. The SDK maps them to each database's native type at DDL-generation time — no package changes are needed when switching databases.

| Canonical type | PostgreSQL | MySQL | SQLite |
|---|---|---|---|
| `uuid` | `UUID` | `CHAR(36)` | `TEXT` |
| `json` / `jsonb` | `JSONB` | `JSON` | `TEXT` |
| `timestamp` | `TIMESTAMPTZ` | `DATETIME(6)` | `TEXT` (ISO-8601) |
| `boolean` | `BOOLEAN` | `TINYINT(1)` | `INTEGER` (0/1) |
| `bytes` | `BYTEA` | `BLOB` | `BLOB` |
| `serial` | `SERIAL` | `INT AUTO_INCREMENT` | `INTEGER` |
| `bigserial` | `BIGSERIAL` | `BIGINT AUTO_INCREMENT` | `INTEGER` |
| `array(T)` | `T[]` | `JSON` *(degraded)* | `TEXT` *(degraded)* |

Degraded types emit a `tracing::warn` at startup so operators know what feature was traded.

---

## Testing

Run the unit test suite with:

```bash
cargo test
```

No database or network connection required — all tests are in-process.

### Coverage

Measured with [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) (LLVM instrumentation), across **135 tests** (115 unit + 20 SQLite integration):

```
TOTAL   lines: 30.69%   functions: 33.33%   regions: 31.12%
```

Coverage nearly doubled after adding SQLite integration tests (was 8.15% lines / 14.02% functions). The remaining uncovered code is Axum HTTP handlers, OpenAPI generation, package ZIP processing, event publishing, and Authrs — all of which require a full HTTP stack or external services and are exercised through end-to-end testing.

### Unit tests (no DB required)

| File | Lines | Functions | Regions | Tests |
|---|---|---|---|---|
| `src/case.rs` | **93.41%** | **92.59%** | **94.90%** | 18 |
| `src/service/validation.rs` | **95.71%** | **98.41%** | **96.28%** | 24 |
| `src/config/validator.rs` | **91.52%** | **100.00%** | **89.04%** | 9 |
| `src/sql/rsql.rs` | **85.49%** | **96.97%** | **87.28%** | 14 |

### SQLite integration tests (in-memory DB, no Postgres needed)

These tests run the full CRUD stack — migrations, SQL builder, `CrudService`, config resolution — against an in-memory SQLite database. They cover code paths that are unreachable from unit tests but do **not** cover Postgres-specific features (JSONB, RLS, named enum types, `INTERVAL` arithmetic).

| File | Lines | Functions | Regions |
|---|---|---|---|
| `src/service/crud.rs` | **22.93%** | **33.33%** | **18.66%** |
| `src/sql/builder.rs` | **57.36%** | **55.00%** | **57.20%** |
| `src/store.rs` | **24.34%** | **14.29%** | **28.48%** |
| `src/migration.rs` | **38.52%** | **43.20%** | **35.94%** |
| `src/config/loader.rs` | **39.00%** | **46.67%** | **45.89%** |
| `src/db/sqlite.rs` | **38.13%** | **50.00%** | **39.41%** |

To regenerate coverage numbers and update this file automatically (requires `llvm` via Homebrew):

```bash
cargo install cargo-llvm-cov   # one-time
brew install llvm              # one-time
./scripts/update_coverage.sh
```

Pass `--dry-run` to print what would change without writing.

#### `src/case.rs` — case conversion (18 tests)

- `to_camel_case`: single underscore, multiple underscores, no underscore, leading/trailing underscore, empty string
- `to_snake_case`: basic, multiple capitals, already snake, leading capital, empty string
- Round-trip: `snake → camel → snake` produces the original
- `object_keys_to_camel_case` / `object_keys_to_snake_case`: converts keys, leaves values unchanged
- `value_keys_to_camel_case_recursive`: nested objects, arrays of objects, scalar no-op
- `hashmap_keys_to_snake_case`: key conversion, value preservation

#### `src/service/validation.rs` — request validation (24 tests)

- **Required**: present → pass; absent → fail; explicit `null` → fail
- **Optional**: absent field → pass (no rule triggered)
- **Partial (PATCH)**: missing required field → pass; present invalid field → fail
- **format `email`**: valid address passes; missing `@` fails
- **format `uuid`**: valid RFC 4122 UUID passes; arbitrary string fails
- **`max_length`** / **`min_length`**: boundary values, over/under
- **`pattern`**: regex match passes; no match fails
- **`allowed`**: value in list passes; value outside list fails
- **`minimum`** / **`maximum`**: at boundary passes; beyond boundary fails
- **Null passthrough**: `null` value skips all field-level checks (format, length, etc.)
- **`validate_collecting`**: collects all errors; returns empty vec on success

#### `src/config/validator.rs` — config referential integrity (9 tests)

- Valid minimal config passes without error
- Empty `schemas` list → `ConfigError::Validation`
- `api_entity.entity_id` pointing to nonexistent table → `ConfigError::MissingReference`
- Two `api_entities` sharing the same `path_segment` → `ConfigError::DuplicatePathSegment`
- Column with `table_id` pointing to nonexistent table → `ConfigError::MissingReference`
- Table `primary_key` naming a column not present in `columns` → `ConfigError::InvalidPrimaryKey`
- Table `schema_id` pointing to nonexistent schema → `ConfigError::MissingReference`
- `default_schema_id`: returns first schema's id; errors on empty config

#### `tests/sqlite_integration.rs` — SQLite integration (18 tests)

Uses `sqlite::memory:` — no external process needed.

- **Migration**: `apply_migrations` creates app tables; `ensure_sys_tables` creates all `_sys_*` tables; both are idempotent
- **CRUD (integer PK)**: create → read back, list returns all rows, update changes field, delete removes row, read nonexistent returns `None`, list with limit+offset returns correct pages
- **CRUD (text PK)**: two users created and listed; update nonexistent returns `None`
- **Sensitive columns**: `sensitive_columns` set is populated correctly on the resolved entity
- **Config resolution**: `entity_by_path` map built; auto-appended audit timestamp columns; sensitive columns list populated
- **Validation config**: validation rules (`required`, `max_length`) are wired onto the resolved entity

---

## Usage

### 1. Minimal Server

The full startup sequence: create the DB if missing, ensure system tables exist, load config, resolve the model, build the router.

```rust
use architect_sdk::{
    db::active_dialect,
    ensure_database_exists, ensure_sys_tables,
    load_from_pool, load_registry_from_pool, resolve,
    common_routes_with_ready, config_routes, entity_routes,
    AppState, DEFAULT_PACKAGE_ID,
    events::DecisionHubClient,
    authrs::AuthrsClient,
};
use std::{collections::HashMap, sync::{Arc, RwLock}};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("architect_sdk=info")),
        )
        .init();

    let database_url = std::env::var("DATABASE_URL")?;

    // Create the database if it doesn't exist yet
    ensure_database_exists(&database_url).await?;

    // Build the pool for the compiled-in dialect
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    // Resolve the active dialect (postgres / mysql / sqlite — set by feature flag)
    let dialect = active_dialect();

    // Create architect schema + all _sys_* tables
    ensure_sys_tables(&pool, dialect.as_ref()).await?;

    // Load tenant registry from _sys_tenants
    let tenant_registry = load_registry_from_pool(&pool).await?;

    // Load config from _sys_* tables and compile into an in-memory model
    let config = load_from_pool(&pool, DEFAULT_PACKAGE_ID).await?;
    let model = resolve(&config)?.with_package_id(DEFAULT_PACKAGE_ID);

    let mut package_models = HashMap::new();
    package_models.insert(DEFAULT_PACKAGE_ID.to_string(), model.clone());

    // Initialise optional integrations from env vars
    let storage = architect_sdk::init_storage_provider().await;
    let event_client = DecisionHubClient::from_env();   // reads DECISION_HUB_URL
    let authrs_client = AuthrsClient::from_env();       // reads AUTHRS_URL + SERVICE_NAME

    let state = AppState {
        pool: pool.clone(),
        model: Arc::new(RwLock::new(model)),
        package_models: Arc::new(RwLock::new(package_models)),
        tenant_pools: Arc::new(RwLock::new(HashMap::new())),
        tenant_registry: Arc::new(tenant_registry),
        storage,
        event_client,
        authrs_client,
        dialect,            // <-- new field
    };

    let app = axum::Router::new()
        .merge(common_routes_with_ready(state.clone()))  // /health, /ready, /version, /spec
        .nest("/api/v1", config_routes(state.clone()))   // /api/v1/config/*
        .nest("/api/v1", entity_routes(state));          // /api/v1/:entity/*

    axum::serve(TcpListener::bind("0.0.0.0:3000").await?, app).await?;
    Ok(())
}
```

### 2. Loading Config from a Package Directory

Instead of loading from the database, you can load config from a local directory. Set `PACKAGE_PATH=sample` in your `.env`, or call the loader directly:

```rust
use architect_sdk::{apply_migrations, resolve, db::active_dialect};

// Apply DDL (CREATE SCHEMA / TABLE / INDEX / FK) — idempotent for schemas and enums
let dialect = active_dialect();
apply_migrations(&pool, &config, None, None, dialect.as_ref()).await?;

let model = resolve(&config)?.with_package_id("my-package");
```

For a directory-based package (the same format used by `PACKAGE_PATH`), see [`examples/server.rs`](examples/server.rs).

### 3. Multi-Tenancy

Tenants are stored in `_sys_tenants`. Register tenants by inserting rows directly:

```sql
-- Database-per-tenant (works on all dialects)
INSERT INTO architect._sys_tenants (id, strategy, database_url)
VALUES ('acme', 'database', 'postgres://localhost/acme_db');

-- RLS tenant (Postgres only — shared DB, row-level isolation)
INSERT INTO architect._sys_tenants (id, strategy, database_url)
VALUES ('beta', 'rls', NULL);
```

All entity, config, and KV routes require the `X-Tenant-ID` header:

```http
GET /api/v1/users HTTP/1.1
X-Tenant-ID: acme
```

> **Note:** The RLS strategy uses `CREATE POLICY` and `SET LOCAL app.tenant_id` — Postgres only. MySQL and SQLite tenants must use the Database strategy (separate database per tenant).

For **RLS**, apply migrations with the `rls_tenant_column` parameter:

```rust
apply_migrations(&pool, &config, None, Some("tenant_id"), dialect.as_ref()).await?;
```

After registering new Database-strategy tenants post-install, call the bootstrap endpoint:

```http
POST /api/v1/config/package/my-package/bootstrap
X-Tenant-ID: acme
```

### 4. Packages

A **package** is a versioned ZIP containing `manifest.json` + config JSONs for one domain.

**Install:**
```http
POST /api/v1/config/package
X-Tenant-ID: acme
Content-Type: multipart/form-data

file=@my-package.zip
```

**Preview an upgrade** (diff old config against new ZIP before touching any DB):
```http
POST /api/v1/config/package/migration/preview
X-Tenant-ID: acme
Content-Type: multipart/form-data

file=@my-package-v2.zip
```

Response includes a `migration_id` and ordered DDL steps annotated with `safety` and `risk`:

```json
{
  "migration_id": "abc123",
  "steps": [
    {
      "operation": "AddColumn",
      "sql": "ALTER TABLE public.orders ADD COLUMN notes TEXT",
      "safety": "Safe",
      "risk": "None"
    }
  ]
}
```

**Apply** after reviewing:
```http
POST /api/v1/config/package/migration/apply/abc123
X-Tenant-ID: acme
```

**Uninstall:**
```http
DELETE /api/v1/config/package/my-package
X-Tenant-ID: acme
```

Programmatic usage:

```rust
use architect_sdk::{compute_migration_plan, execute_migration_plan, db::active_dialect};

let dialect = active_dialect();
let plan = compute_migration_plan(&old_config, &new_config, dialect.as_ref())?;
// Inspect plan.steps — check safety/risk before proceeding
let summary = execute_migration_plan(&pool, &plan, "migration-run-id", dialect.as_ref()).await?;
println!("applied: {}, warned: {}", summary.applied, summary.warned);
```

### 5. Asset Uploads

Columns of type `asset` or `asset[]` accept file uploads via `multipart/form-data`. Enable the relevant storage backend in `Cargo.toml` and set env vars:

```toml
foundry-rs = { version = "0.2", features = ["storage-s3"] }
```

```env
STORAGE_PROVIDER=s3
STORAGE_BUCKET=my-bucket
AWS_REGION=us-east-1
```

Upload a file alongside JSON fields:

```http
POST /api/v1/products
X-Tenant-ID: acme
Content-Type: multipart/form-data

name=Widget
price=9.99
image=@widget.jpg
```

The SDK uploads the file to the configured backend and stores the object key in the `image` column. To generate a signed download URL:

```http
GET /api/v1/assets/sign?key=products/2024/06/01/abc.jpg
X-Tenant-ID: acme
```

### 6. Validation

Validation rules are declared per column in config and enforced automatically:

```json
{
  "id": "c_email",
  "table_id": "t_users",
  "name": "email",
  "type": "text",
  "nullable": false,
  "validation": {
    "required": true,
    "format": "email",
    "max_length": 254
  }
}
```

On `POST` (full validation) all `required` fields must be present. On `PATCH` (partial) only provided fields are validated. Failures return HTTP `422`:

```json
{
  "error": {
    "code": "validation_error",
    "message": "Validation failed",
    "details": [
      { "field": "email", "message": "must be a valid email address" }
    ]
  }
}
```

### 7. Audit Logging

Set `audit_log: true` on any table. The SDK creates a companion `{table}_audit` table and records every INSERT, UPDATE, and DELETE automatically.

```json
{ "id": "t1", "schema_id": "s1", "name": "orders", "audit_log": true }
```

Each audit row contains:
- `audit_id` — UUID primary key
- `audit_action` — `create`, `update`, or `delete`
- `audit_at` — timestamp of the change
- `audit_by` — value of `X-User-ID` header
- `changed_fields` — JSON delta (only columns that changed)
- Full nullable copy of every source column for point-in-time snapshots

### 8. Related Entity Includes

Define a relationship in config, then use `?include=` to embed related rows (single query, no N+1):

```http
GET /api/v1/users?include=orders
X-Tenant-ID: acme
```

```json
{
  "data": [
    {
      "id": "u1",
      "name": "Alice",
      "orders": [{ "id": "o1", "total": "49.99" }]
    }
  ],
  "meta": { "count": 1, "offset": 0, "limit": 10 }
}
```

Include multiple relationships: `?include=orders,payments`.

### 9. Event Publishing (Decision Hub)

Set `DECISION_HUB_URL`. The SDK publishes async events after every create, update, delete, and archive — fire-and-forget, never blocks the API caller.

### 10. Authorization (Authrs)

Set `AUTHRS_URL` and `SERVICE_NAME`. The SDK checks permissions before every entity operation using the `X-User-ID` header. Unauthorized requests receive `401`.

### 11. Extensible Fields (per-tenant custom fields)

Let each tenant add their own queryable fields to an entity without changing the schema. Flag a JSON/JSONB column as extensible:

```json
{ "id": "col_products_attributes", "table_id": "tbl_products",
  "name": "attributes", "type": "jsonb", "nullable": false,
  "default": { "expression": "'{}'::jsonb" }, "extensible": true }
```

Each tenant then declares the field definitions for that column via the **registry** admin API:

```http
PUT /api/v1/products/extensible-fields      (X-Tenant-ID: acme)
{
  "attributes": [
    { "key": "warrantyMonths", "type": "int",  "filterable": true, "sortable": true, "min": 0 },
    { "key": "energyRating",   "type": "text", "maxLength": 4 }
  ]
}
```

Now those keys are first-class in writes and queries for that tenant:

```http
# Stored & validated on create/update (unknown keys / bad types → 422)
POST /api/v1/products   { "name": "Drill", "customFields": { "warrantyMonths": 24 } }

# Filter & sort via the <column>.<key> RSQL syntax
GET  /api/v1/products?q=attributes.warrantyMonths=ge=12&sort=-attributes.warrantyMonths
```

Notes:
- **Storage convention**: registry keys are stored verbatim — use **camelCase** so they round-trip to clients unchanged.
- **Multiple bags**: an entity may flag several JSON columns extensible; the RSQL prefix (`attributes.` vs `specs.`) disambiguates them.
- **Indexing** (important at scale): filters/sorts on extensible fields are sequential scans until indexed. Review the suggested DDL with `GET /api/v1/:entity/extensible-fields/indexes` and apply it (deliberately, on large tables) with `POST .../indexes`. RLS tenants get partial indexes scoped by `tenant_id`.
- **Authorization**: the admin/index routes use dedicated verbs — `getExtensibleFields<Table>`, `putExtensibleFields<Table>`, `deleteExtensibleFields<Table>` — so managing definitions is a separate grant from row CRUD.
- **Scale note**: for very large RLS tables, partition the shared table by `tenant_id` (list/hash) so tenant-scoped queries prune to one partition; this pairs with the per-tenant partial indexes above. (Per-tenant *Database*-strategy deployments are already sharded by database.)

---

## Environment Variables

| Variable | Purpose | Default |
|---|---|---|
| `DATABASE_URL` | Connection string for the central architect DB | required |
| `ARCHITECT_SCHEMA` | Schema for `_sys_*` tables | `architect` |
| `PACKAGE_PATH` | Load config from this directory instead of DB | — |
| `RUST_LOG` | Log level filter (e.g. `architect_sdk=debug`) | — |
| `STORAGE_PROVIDER` | Storage backend: `s3`, `azure`, `gcs`, `rustfs` | — |
| `STORAGE_BUCKET` | Bucket/container name (S3, GCS) | — |
| `STORAGE_ENDPOINT` | Filesystem path prefix (RustFS) | — |
| `AWS_REGION` | AWS region (S3) | — |
| `AZURE_STORAGE_ACCOUNT` | Azure storage account name | — |
| `AZURE_STORAGE_CONTAINER` | Azure container name | — |
| `AZURE_STORAGE_ACCESS_KEY` | Azure storage key | — |
| `GCS_SERVICE_ACCOUNT_JSON` | GCS service account JSON path | — |
| `DECISION_HUB_URL` | Event publishing endpoint; events disabled if unset | — |
| `DECISION_HUB_TIMEOUT_SECS` | Event publish timeout | `5` |
| `AUTHRS_URL` | Permission check endpoint; auth disabled if unset | — |
| `SERVICE_NAME` | Service identifier for Authrs resources | — |

---

## API Reference

All data routes require `X-Tenant-ID` header. When Authrs is enabled, `X-User-ID` is also required.

### Common Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check |
| `GET` | `/ready` | Readiness probe (checks DB connectivity) |
| `GET` | `/version` | Package name and version |
| `GET` | `/info` | Alias for `/version` |
| `GET` | `/spec` | OpenAPI 3.0 specification |

### Package Management

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/config/packages` | List installed packages |
| `GET` | `/api/v1/config/packages/:package_id` | Get package details |
| `POST` | `/api/v1/config/package` | Install package (multipart ZIP) |
| `DELETE` | `/api/v1/config/package/:package_id` | Uninstall package |
| `POST` | `/api/v1/config/package/migration/preview` | Preview migration diff |
| `POST` | `/api/v1/config/package/migration/apply/:migration_id` | Apply migration plan |
| `POST` | `/api/v1/config/package/:package_id/bootstrap` | Bootstrap tenant DB (Database strategy) |

### Config Ingestion

| Path | Kind |
|---|---|
| `/api/v1/config/schemas` | Schema definitions |
| `/api/v1/config/enums` | Enum types (Postgres: `CREATE TYPE AS ENUM`; MySQL/SQLite: CHECK constraints) |
| `/api/v1/config/tables` | Table definitions |
| `/api/v1/config/columns` | Column definitions |
| `/api/v1/config/indexes` | Index definitions |
| `/api/v1/config/relationships` | Foreign key relationships |
| `/api/v1/config/api_entities` | API entity mappings |
| `/api/v1/config/kv_stores` | KV namespace definitions |

### Entity CRUD

Replace `:entity` with the entity's `path_segment` from config (e.g. `users`).

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/:entity` | List with filtering, sorting, pagination |
| `POST` | `/api/v1/:entity` | Create (JSON or multipart for assets) |
| `GET` | `/api/v1/:entity/:id` | Read single record |
| `PATCH` | `/api/v1/:entity/:id` | Partial update |
| `DELETE` | `/api/v1/:entity/:id` | Hard delete |
| `POST` | `/api/v1/:entity/:id/archive` | Soft delete (sets `archived_at`) |
| `POST` | `/api/v1/:entity/:id/unarchive` | Restore soft delete |
| `POST` | `/api/v1/:entity/bulk` | Bulk create |
| `PATCH` | `/api/v1/:entity/bulk` | Bulk update |

**Package-scoped routes** follow the same pattern under `/api/v1/package/:package_id/:entity`.

#### Extensible-Field Admin (requires `X-Tenant-ID`)

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/:entity/extensible-fields` | Current registry document for the tenant |
| `PUT` | `/api/v1/:entity/extensible-fields` | Replace the registry (validated) |
| `DELETE` | `/api/v1/:entity/extensible-fields` | Clear the registry |
| `GET` | `/api/v1/:entity/extensible-fields/indexes` | Suggested `CREATE INDEX` statements |
| `POST` | `/api/v1/:entity/extensible-fields/indexes` | Apply the suggested indexes |

Each route also has a **package-scoped** form under `/api/v1/package/:package_id/:entity/extensible-fields[/indexes]`, which resolves the entity from that package's model (keying the registry by the correct package). Use it when the same `path_segment` exists in more than one installed package.

#### List Query Parameters

| Param | Description | Example |
|---|---|---|
| `q` | RSQL/FIQL filter expression | `status==active;createdAt=gt=2024-01-01` |
| `sort` | Comma-separated columns; `+` asc, `-` desc | `+created_at,-status` |
| `limit` | Page size (default 10) | `50` |
| `offset` | Skip N records (default 0) | `100` |
| `include` | Comma-separated related entity path segments | `orders,payments` |

Both `q` and `sort` also accept **extensible-field** keys via the `<column>.<key>` syntax (e.g. `q=attributes.warrantyMonths=ge=12`, `sort=-attributes.warrantyMonths`) when the column is declared `extensible` and the key is in the tenant's registry. See [Extensible Fields](#11-extensible-fields-per-tenant-custom-fields).

#### Response Envelope

```json
// List
{ "data": [...], "meta": { "count": 100, "offset": 0, "limit": 10 } }

// Single
{ "data": { ... } }

// Error
{ "error": { "code": "...", "message": "...", "details": null } }

// Bulk with partial failure (207)
{ "data": [...], "error": { "code": "...", "message": "...", "details": [...] } }
```

HTTP status codes: `200`, `201`, `207`, `401`, `404`, `409`, `422`, `500`.

### Key-Value Store

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/package/:package_id/kv/:namespace` | List all keys |
| `GET` | `/api/v1/package/:package_id/kv/:namespace/:key` | Get value |
| `PUT` | `/api/v1/package/:package_id/kv/:namespace/:key` | Set value (upsert) |
| `DELETE` | `/api/v1/package/:package_id/kv/:namespace/:key` | Delete entry |

### Assets

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/assets/sign` | Get signed download URL |

---

## Configuration Reference

### Canonical Column Types

Use these names in `"type"` fields. The SDK maps them to the correct DDL type for the active database.

| Canonical | Aliases accepted | Description |
|---|---|---|
| `text` | `TEXT` | Unbounded unicode text |
| `varchar` | `VARCHAR(n)`, `character varying` | Variable-length text with optional cap |
| `char` | `CHAR(n)`, `character` | Fixed-length text |
| `int` | `INTEGER`, `INT`, `serial` | 32-bit integer (serial = auto-increment) |
| `bigint` | `BIGINT`, `bigserial` | 64-bit integer |
| `smallint` | `SMALLINT` | 16-bit integer |
| `float` | `DOUBLE PRECISION`, `float8` | 64-bit float |
| `real` | `REAL`, `float4` | 32-bit float |
| `decimal` | `NUMERIC(p,s)`, `DECIMAL` | Fixed-precision decimal |
| `boolean` | `BOOLEAN`, `bool` | True/false |
| `uuid` | `UUID` | 128-bit UUID |
| `json` | `JSON`, `jsonb`, `JSONB` | JSON document (stored as JSONB on Postgres) |
| `timestamp` | `TIMESTAMPTZ`, `timestamp with time zone` | Timestamp with timezone |
| `timestamp_ntz` | `TIMESTAMP WITHOUT TIME ZONE` | Timestamp without timezone |
| `date` | `DATE` | Calendar date |
| `time` | `TIME` | Time of day |
| `timetz` | `TIME WITH TIME ZONE` | Time with timezone |
| `bytes` | `BYTEA`, `bytea` | Binary data |
| `asset` | — | SDK pseudo-type: stores a file path string |
| `asset[]` | — | SDK pseudo-type: stores a JSON array of file paths |

Existing packages using raw SQL type names (e.g. `"TIMESTAMPTZ"`, `"INT"`) continue to work unchanged — they pass through as `Custom` types and are rendered verbatim in DDL.

### Schema

```json
{ "id": "s1", "name": "public", "comment": "optional" }
```

### Enum

```json
{ "id": "e1", "schema_id": "s1", "name": "order_status", "values": ["pending", "shipped", "delivered"] }
```

On Postgres: `CREATE TYPE public.order_status AS ENUM (...)`.  
On MySQL/SQLite: rendered as a `CHECK (col IN (...))` constraint on the column.

### Table

```json
{
  "id": "t1", "schema_id": "s1", "name": "orders",
  "primary_key": "id",
  "unique": [["email"]],
  "check": [],
  "audit_log": true
}
```

Every table automatically gets: `created_at`, `updated_at`, `archived_at`, `created_by`, `updated_by` — each typed to the dialect's timestamp/text equivalent.

### Column

```json
{
  "id": "c1", "table_id": "t1", "name": "email",
  "type": "text",
  "nullable": false,
  "validation": {
    "required": true,
    "format": "email",
    "max_length": 254
  }
}
```

**Validation rules:** `required`, `min_length`, `max_length`, `pattern` (regex), `allowed` (enum list), `minimum`, `maximum`, `format` (`email` | `uuid`).

**`extensible`** *(boolean, JSON/JSONB columns only)*: marks the column as a per-tenant custom-fields bag. Its keys are defined per tenant via the registry admin API and become RSQL filterable/sortable. Ignored (with a warning) on non-JSON columns. See [Extensible Fields](#11-extensible-fields-per-tenant-custom-fields).

### API Entity

```json
{
  "entity_id": "t1",
  "path_segment": "orders",
  "operations": ["list", "read", "create", "update", "delete"],
  "sensitive_columns": ["password_hash"],
  "includes": ["items"]
}
```

### Relationship

```json
{
  "id": "r1",
  "from_schema_id": "s1", "from_table_id": "t1", "from_column_id": "c_user_id",
  "to_schema_id": "s1",   "to_table_id":   "t2", "to_column_id":   "c_id",
  "name": "user",
  "on_delete": "CASCADE"
}
```

### Package Manifest

```json
{
  "id": "my-package",
  "name": "My Package",
  "version": "1.0.0",
  "schema": "my_schema"
}
```

---

## Multi-Tenancy

Tenants are registered in `_sys_tenants`. Two isolation strategies:

**Database** — each tenant has its own database. DDL is broadcast to all tenant DBs on package install/uninstall. Works on all dialects.

**RLS** — tenants share a database. The SDK sets the tenant identifier in the session before each query and PostgreSQL RLS policies enforce row-level isolation. **Postgres only.** All tables get a `tenant_id` column automatically.

All data routes require `X-Tenant-ID: <tenant_id>` header.

---

## Packages

A package is a ZIP containing `manifest.json` plus config JSON files:

```
my-package.zip
├── manifest.json
├── enums.json
├── tables.json
├── columns.json
├── indexes.json
├── relationships.json
└── api_entities.json
```

**Install:** `POST /api/v1/config/package` (multipart)  
**Upgrade:** `POST /api/v1/config/package/migration/preview` → review → `POST /api/v1/config/package/migration/apply/:id`  
**Uninstall:** `DELETE /api/v1/config/package/:package_id`

Migration steps carry `safety` (`Safe` | `BestEffort` | `WarnOnly`) and `risk` (`None` | `MayFail` | `ExistingNullsMustBeAbsent` | `DataWillBeModified` | `ManualActionRequired`) metadata so you know exactly what will happen before applying.

---

## Asset Storage

Columns of type `asset` or `asset[]` enable file uploads via multipart requests. Configure the backend via env:

| Provider | `STORAGE_PROVIDER` value |
|---|---|
| Local filesystem | `rustfs` |
| AWS S3 | `s3` |
| Azure Blob Storage | `azure` |
| Google Cloud Storage | `gcs` |

Enable S3, Azure, or GCS support via Cargo features: `storage-s3`, `storage-azure`, `storage-gcs`, or `storage-all`.

---

## Optional Integrations

### Decision Hub (Event Publishing)

Set `DECISION_HUB_URL` to enable async event publishing after CRUD operations. Events are fire-and-forget and do not block API responses.

### Authrs (Authorization)

Set `AUTHRS_URL` and `SERVICE_NAME` to enable per-request permission checks. The SDK calls Authrs before each entity operation; requests without the required permission receive `401 Unauthorized`.

---

## System Tables

All stored in the `architect` schema (configurable via `ARCHITECT_SCHEMA`):

| Table | Contents |
|---|---|
| `_sys_packages` | Installed package manifests |
| `_sys_schemas` | Schema definitions |
| `_sys_enums` | Enum type definitions |
| `_sys_tables` | Table definitions |
| `_sys_columns` | Column definitions and validation rules |
| `_sys_indexes` | Index definitions |
| `_sys_relationships` | FK relationship definitions |
| `_sys_api_entities` | API endpoint definitions |
| `_sys_kv_stores` | KV namespace definitions |
| `_sys_tenants` | Tenant registry (strategy, database_url) |
| `_sys_kv_data` | KV store data |

---

## Adding a New Dialect

1. Add a Cargo feature (`mysql` / `sqlite` pattern applies).
2. Create `src/db/your_dialect.rs` implementing the `Dialect` trait (~20 methods).
3. Gate it with `#[cfg(feature = "your_dialect")]` in `src/db/mod.rs`.
4. Add it to `active_dialect()` in `src/db/mod.rs`.
5. Add `your_dialect = ["sqlx/your_dialect"]` to `Cargo.toml`.

The `Dialect` trait covers: DDL types, identifier quoting, parameter placeholders, type casting, `NOW()`, UUID defaults, `RETURNING`, upsert conflict, JSON aggregation for includes, system-table DDL helpers, and multi-tenancy session setup.
