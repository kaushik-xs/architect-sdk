# Architect SDK

Configuration-driven REST backend library for Rust with PostgreSQL. Define your entire data model — schemas, tables, columns, indexes, relationships, API endpoints — in JSON. No entity-specific business logic required.

## Features

- **Config-driven schema**: All DB structure defined in JSON; no hardcoded migrations
- **Auto CRUD API**: List, read, create, update, delete, bulk ops — all generated from config
- **Multi-tenancy**: Database-per-tenant or Row-Level Security (RLS), configured per tenant
- **Package system**: Deploy config as versioned ZIP packages with install/uninstall/upgrade
- **Migration planning**: Diff-based upgrades with preview before execution
- **Asset storage**: File uploads with S3, Azure, GCS, or local filesystem backends
- **Request validation**: Per-column rules (required, format, length, pattern, allowed, min/max)
- **Audit logging**: Optional per-table audit trail with row snapshots and change deltas
- **Event publishing**: Optional async event publishing to Decision Hub after CRUD ops
- **Authorization**: Optional permission checks via Authrs integration
- **OpenAPI spec**: Dynamically generated from config at `GET /spec`
- **Safe SQL**: All identifiers from validated config; values always use `$N` placeholders

---

## Quick Start

Add the SDK to your `Cargo.toml`:

```toml
[dependencies]
foundry-rs = "0.1"

# With cloud storage support
foundry-rs = { version = "0.1", features = ["storage-s3"] }
# Other storage features: storage-azure, storage-gcs, storage-all
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

## Usage

### 1. Minimal Server

The full startup sequence: create the DB if missing, ensure system tables exist, load config, resolve the model, build the router.

```rust
use architect_sdk::{
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

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    // Create architect schema + all _sys_* tables
    ensure_sys_tables(&pool).await?;

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

Instead of loading from the database, you can load config from a local directory containing a `manifest.json` and config JSON files. This is useful during development or for seeding a fresh server.

Set `PACKAGE_PATH=sample` in your `.env` (or call the loader directly):

```rust
use architect_sdk::{apply_migrations, resolve, FullConfig, config::{SchemaConfig, TableConfig, ColumnConfig, ApiEntityConfig}};

// Minimal in-code config (no JSON files needed)
let config = FullConfig {
    schemas: serde_json::from_str(r#"[{"id":"s1","name":"public"}]"#)?,
    tables:  serde_json::from_str(r#"[{"id":"t1","schema_id":"s1","name":"users"}]"#)?,
    columns: serde_json::from_str(r#"[
        {"id":"c1","table_id":"t1","name":"name","type":{"Simple":"TEXT"},"nullable":false}
    ]"#)?,
    api_entities: serde_json::from_str(r#"[
        {"id":"ae1","table_id":"t1","path_segment":"users","operations":["list","read","create","update","delete"]}
    ]"#)?,
    enums: vec![],
    indexes: vec![],
    relationships: vec![],
    kv_stores: vec![],
};

// Apply DDL (CREATE SCHEMA / TABLE / INDEX / FK) — idempotent for schemas and enums
apply_migrations(&pool, &config, None, None).await?;

let model = resolve(&config)?.with_package_id("my-package");
```

For a directory-based package (the same format used by `PACKAGE_PATH`), see [`examples/server.rs`](examples/server.rs) — it reads `manifest.json` plus per-kind JSON files (`tables.json`, `columns.json`, etc.) and builds a `FullConfig` from them.

### 3. Multi-Tenancy

Tenants are stored in `_sys_tenants`. Register tenants by inserting rows directly (or via your own admin flow):

```sql
-- Database-per-tenant
INSERT INTO architect._sys_tenants (id, strategy, database_url)
VALUES ('acme', 'database', 'postgres://localhost/acme_db');

-- RLS tenant (shared DB, row-level isolation)
INSERT INTO architect._sys_tenants (id, strategy, database_url)
VALUES ('beta', 'rls', NULL);
```

All entity, config, and KV routes require the `X-Tenant-ID` header:

```http
GET /api/v1/users HTTP/1.1
X-Tenant-ID: acme
```

The SDK resolves the tenant from the registry, connects to the right database (Database strategy) or sets `app.tenant_id` in the session (RLS strategy), then executes the query. Tenant pools are created lazily on first use.

For **RLS**, apply migrations with the `rls_tenant_column` parameter so the SDK adds the `tenant_id` column and RLS policies automatically:

```rust
apply_migrations(&pool, &config, None, Some("tenant_id")).await?;
```

After registering new Database-strategy tenants post-install, call the bootstrap endpoint to create their schema:

```http
POST /api/v1/config/package/my-package/bootstrap
X-Tenant-ID: acme
```

### 4. Packages

A **package** is a versioned ZIP containing `manifest.json` + config JSONs for one domain. This is the recommended way to deploy config changes to a running server.

**Install:**
```http
POST /api/v1/config/package
X-Tenant-ID: acme
Content-Type: multipart/form-data

file=@my-package.zip
```

The SDK extracts the ZIP, validates all config, writes to `_sys_*` tables, runs DDL against every tenant database, and reloads the in-memory model — all atomically in one request.

**Preview an upgrade** (diff old config against new ZIP before touching any DB):
```http
POST /api/v1/config/package/migration/preview
X-Tenant-ID: acme
Content-Type: multipart/form-data

file=@my-package-v2.zip
```

Response includes a `migration_id` and an ordered list of DDL steps, each annotated with `safety` and `risk`:

```json
{
  "migration_id": "abc123",
  "steps": [
    {
      "operation": "AddColumn",
      "sql": "ALTER TABLE public.orders ADD COLUMN notes TEXT",
      "safety": "Safe",
      "risk": "None"
    },
    {
      "operation": "SetNotNull",
      "sql": "ALTER TABLE public.orders ALTER COLUMN status SET NOT NULL",
      "safety": "BestEffort",
      "risk": "ExistingNullsMustBeAbsent"
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

You can also drive migrations programmatically:

```rust
use architect_sdk::{compute_migration_plan, execute_migration_plan};

let plan = compute_migration_plan(&old_config, &new_config)?;
// Inspect plan.steps — check safety/risk before proceeding
let summary = execute_migration_plan(&pool, &plan, "migration-run-id").await?;
println!("applied: {}, warned: {}", summary.applied, summary.warned);
```

### 5. Asset Uploads

Columns of type `asset` or `asset[]` accept file uploads via `multipart/form-data`. Enable the relevant storage backend in `Cargo.toml` and set env vars:

```toml
# Cargo.toml
foundry-rs = { version = "0.1", features = ["storage-s3"] }
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

Validation rules are declared per column in config and are enforced automatically — no handler code required.

```json
{
  "id": "c_email",
  "table_id": "t_users",
  "name": "email",
  "type": { "Simple": "TEXT" },
  "nullable": false,
  "validation": {
    "required": true,
    "format": "email",
    "max_length": 254
  }
}
```

On a `POST` (full validation), all `required` fields must be present. On a `PATCH` (partial validation), only the fields included in the request body are validated. Failures return HTTP `422`:

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

Set `audit_log: true` on any table config. The SDK creates a companion `{table}_audit` table and a trigger that records every INSERT, UPDATE, and DELETE automatically — no application code needed.

```json
{ "id": "t1", "schema_id": "s1", "name": "orders", "audit_log": true }
```

Each audit row contains:
- `audit_id` — UUID primary key
- `audit_action` — `INSERT`, `UPDATE`, or `DELETE`
- `audit_at` — timestamp of the change
- `audit_by` — value of `X-User-ID` header at request time
- `changed_fields` — JSONB delta (only the columns that changed)
- Full nullable copy of every row column for point-in-time snapshots

### 8. Related Entity Includes

Define a relationship in config, then use `?include=` on list or read requests to embed related rows in a single query (no N+1):

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
      "orders": [
        { "id": "o1", "total": "49.99" }
      ]
    }
  ],
  "meta": { "count": 1, "offset": 0, "limit": 10 }
}
```

Include multiple relationships by comma-separating path segments: `?include=orders,payments`.

### 9. Event Publishing (Decision Hub)

Set `DECISION_HUB_URL` in the environment. The SDK automatically publishes async events after every create, update, delete, and archive operation — no code changes required.

```env
DECISION_HUB_URL=http://decision-hub:8080
DECISION_HUB_TIMEOUT_SECS=5
```

Events are fire-and-forget: failures are logged but never surface to the API caller.

### 10. Authorization (Authrs)

Set `AUTHRS_URL` and `SERVICE_NAME`. The SDK checks permissions before every entity operation using the `X-User-ID` header.

```env
AUTHRS_URL=http://authrs:8080
SERVICE_NAME=my-service
```

Every request to `/api/v1/:entity` must include:

```http
X-Tenant-ID: acme
X-User-ID: user-abc
```

The SDK checks `service:my-service/package:default/table:users` with action `getUsers` (for `GET`) before executing the query. A missing or unauthorized user receives `401 Unauthorized`.

---

## Environment Variables

| Variable | Purpose | Default |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection string for the central architect DB | required |
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

Each config kind has a `POST` (create/replace) and `GET` (retrieve) endpoint:

| Path | Kind |
|---|---|
| `/api/v1/config/schemas` | Schema definitions |
| `/api/v1/config/enums` | PostgreSQL ENUM types |
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

#### List Query Parameters

| Param | Description | Example |
|---|---|---|
| `filter` | RSQL/FIQL filter expression | `status==active;created_at>2024-01-01` |
| `sort` | Comma-separated columns; `+` asc, `-` desc | `+created_at,-status` |
| `limit` | Page size (default 10) | `50` |
| `offset` | Skip N records (default 0) | `100` |
| `include` | Comma-separated related entity path segments | `orders,payments` |

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

### Schema

```json
{ "id": "s1", "name": "public", "comment": "optional" }
```

### Enum

```json
{ "id": "e1", "schema_id": "s1", "name": "order_status", "values": ["pending", "shipped", "delivered"] }
```

### Table

```json
{
  "id": "t1", "schema_id": "s1", "name": "orders",
  "primary_key": "single",
  "unique": [["email"]],
  "check": [],
  "audit_log": true
}
```

Setting `audit_log: true` creates a companion `orders_audit` table recording every INSERT, UPDATE, and DELETE with a full row snapshot and `changed_fields` JSONB delta.

Every table automatically gets: `id` (UUID PK), `created_at`, `updated_at`, `archived_at`, `created_by`, `updated_by`.

### Column

```json
{
  "id": "c1", "table_id": "t1", "name": "email",
  "type": { "Simple": "TEXT" },
  "nullable": false,
  "validation": {
    "required": true,
    "format": "email",
    "max_length": 254
  }
}
```

**Supported types:** `TEXT`, `VARCHAR(n)`, `INTEGER`, `BIGINT`, `SMALLINT`, `BOOLEAN`, `NUMERIC(p,s)`, `DECIMAL`, `REAL`, `DOUBLE PRECISION`, `UUID`, `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `JSON`, `JSONB`, `BYTEA`, custom enums, `asset`, `asset[]`.

**Validation rules:** `required`, `min_length`, `max_length`, `pattern` (regex), `allowed` (enum list), `minimum`, `maximum`, `format` (`email` | `uuid`).

### API Entity

```json
{
  "id": "ae1", "table_id": "t1",
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
  "from_table_id": "t1", "from_column_id": "c_user_id",
  "to_table_id": "t2", "to_column_id": "c_id",
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

**Database** — each tenant has its own PostgreSQL database. DDL is broadcast to all tenant DBs on package install/uninstall.

**RLS** — tenants share a database. The SDK sets `app.tenant_id` via `SET LOCAL` before each query. PostgreSQL RLS policies enforce isolation. All tables get a `tenant_id` column automatically.

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

Migration steps carry safety (`Safe` | `BestEffort` | `WarnOnly`) and risk (`None` | `MayFail` | `ExistingNullsMustBeAbsent` | `DataWillBeModified` | `ManualActionRequired`) metadata so you know exactly what will happen before applying.

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

## Build & Test

```bash
cargo build
cargo test
cargo fmt --all
cargo clippy --all -- -D warnings
```

CI (GitHub Actions) runs on push/PR to `main`: fmt check, build, test, clippy.

---

## Sample Packages

| Directory | Description |
|---|---|
| `sample/` | Minimal: 2 tables (users, orders), FK, email unique constraint |
| `sample_ecommerce/` | Full e-commerce: 12 tables, 4 enums, 18 relationships, multi-tenant ready |

---

## Private GitHub and CI

For a private repo, authenticate Cargo:

**Local (HTTPS):**
```bash
git config --global url."https://<PAT>@github.com/".insteadOf "https://github.com/"
```

**CI (GitHub Actions):**
```yaml
- run: |
    git config --global url."https://x-access-token:${{ secrets.GITHUB_TOKEN }}@github.com/".insteadOf "https://github.com/"
```

---

## License

MIT
