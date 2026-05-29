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
# From a local path
[dependencies]
architect-sdk = { path = "/path/to/architect-sdk" }

# From Git
[dependencies]
architect-sdk = { git = "https://github.com/kaushik-xs/architect-sdk" }
```

Build and serve:

```rust
use architect_sdk::{
    resolve, AppState, ensure_sys_tables, load_registry_from_pool,
    common_routes_with_ready, config_routes, entity_routes,
};
use std::sync::Arc;

let pool = sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
ensure_sys_tables(&pool).await?;
let tenant_registry = load_registry_from_pool(&pool).await?;

let config = architect_sdk::load_from_pool(&pool, None).await?;
let model = resolve(&config)?;

let state = AppState {
    pool: pool.clone(),
    model: Arc::new(model),
    tenant_registry,
    ..Default::default()
};

let app = axum::Router::new()
    .merge(common_routes_with_ready(state.clone()))
    .nest("/api/v1", config_routes(state.clone()))
    .nest("/api/v1", entity_routes(state));
```

Run the bundled example server:

```bash
cp .env.example .env
# Optional: set PACKAGE_PATH=sample to auto-load config from sample/
cargo run --example server
# Starts on http://0.0.0.0:3000
```

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
