# Architect SDK

Configuration-driven REST backend library for Rust with PostgreSQL. Entities, fields, and operations are driven by JSON configuration; no hardcoded entity-specific business logic.

## Features

- **Reusable library**: Consume as a dependency (e.g. `architect-sdk` in `Cargo.toml`) in your own binary.
- **Config from external source**: Supply config in-memory, from files, or load from the database (`_sys_*` tables).
- **Entity CRUD API**: Per-entity routes for Create, Read, Update, Delete, Bulk Create, Bulk Update (from config).
- **Config ingestion API**: POST/GET endpoints to feed and read DB configuration (schemas, tables, columns, enums, indexes, relationships, api_entities); stored in `_sys_*` tables.
- **Common endpoints**: Health (`/health`), readiness (`/ready`), version (`/version`, `/info`).
- **Safe SQL**: Parameterized queries only; identifiers from validated config.
- **Standard envelope**: `{ "data": ..., "meta": {} }` and `{ "error": { "code", "message", "details" } }`.

## Config schema

- Reuses the [Postgres config schema](docs/postgres-config-schema.md) (schemas, enums, tables, columns, indexes, relationships).
- The **manifest** (e.g. [sample/manifest.json](sample/manifest.json)) must include **schema** (the PostgreSQL schema name, e.g. `"public"`). All configs (enums, tables, indexes, relationships) use this schema; no separate schemas table or `schema_id` in each config.
- Add an **api_entities** config: `entity_id`, `path_segment`, `operations`, `sensitive_columns` (column names never exposed in responses), `validation` (per-column rules). See [sample/api_entities.json](sample/api_entities.json).

## Quick start

1. Add to `Cargo.toml`:

   ```toml
   [dependencies]
   architect-sdk = { path = "path/to/architect-sdk" }
   ```

2. Load config (from files or `load_from_pool`), resolve, build router:

   ```rust
   use architect_sdk::{resolve, AppState, ensure_sys_tables, common_routes_with_ready, config_routes, entity_routes};
   use std::sync::Arc;

   let pool = sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
   ensure_sys_tables(&pool).await?;

   let config = my_load_config(); // or load_from_pool(&pool).await?
   let model = resolve(&config)?;
   let state = AppState { pool: pool.clone(), model: Arc::new(model) };

   let app = axum::Router::new()
       .merge(common_routes_with_ready(state.clone()))
       .nest("/api/v1", config_routes(state.clone()))
       .nest("/api/v1", entity_routes(state));
   ```

3. Run the example server. The server loads `.env` at startup. Optional env:
   - **`ARCHITECT_SCHEMA`**: PostgreSQL schema for _sys_* config tables (default `architect`). Must be a valid identifier.
   - **`PLUGIN_PATH`**: If set, config is loaded from that plugin directory (must contain `manifest.json` + config JSONs, e.g. `sample`) and migrations are applied. If not set, only _sys_* tables are ensured and config is loaded from the DB (empty until you POST config via `/api/v1/config/...` or install a plugin via `POST /api/v1/config/plugin`).

   ```bash
   cp .env.example .env
   # Optionally set PLUGIN_PATH=sample to load from sample/ (manifest.json + config JSONs)
   cargo run --example server
   ```

## API overview

- **Common**: `GET /health`, `GET /ready`, `GET /version`, `GET /info`
- **Config**: `POST /api/v1/config/plugin` (multipart zip: manifest.json + config JSONs), then `POST`/`GET` per kind: `schemas`, `enums`, `tables`, `columns`, `indexes`, `relationships`, `api_entities`
- **Entities**: For each entity (e.g. `users`): `GET /api/v1/users` (list all, optional filters: `?col=value`, `?limit=100`, `?offset=0`, `?include=orders` to embed related entities as exploded JSON), `POST /api/v1/users`, `GET /api/v1/users/:id` (optional `?include=orders`), `PATCH /api/v1/users/:id`, `DELETE /api/v1/users/:id`, `POST /api/v1/users/bulk`, `PATCH /api/v1/users/bulk`. **Case**: Request bodies and query param keys accept **camelCase** (e.g. `userId`, `createdAt`) and are converted to snake_case for the DB; response row keys are returned in **camelCase**. **Includes**: Use `?include=path_segment1,path_segment2` on list and read; allowed values are the related entities’ path segments defined by relationships (e.g. `orders` for a to-many from users).

## License

See repository license.
