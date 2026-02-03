# Architect SDK

Configuration-driven REST backend library for Rust with PostgreSQL. Entities, fields, and operations are driven by JSON configuration; no hardcoded entity-specific business logic.

## Features

- **Reusable library**: Consume as a dependency (e.g. `architect-sdk` in `Cargo.toml`) in your own binary.
- **Config from external source**: Supply config in-memory, from files, or load from the database (`_private_*` tables).
- **Entity CRUD API**: Per-entity routes for Create, Read, Update, Delete, Bulk Create, Bulk Update (from config).
- **Config ingestion API**: POST/GET endpoints to feed and read DB configuration (schemas, tables, columns, enums, indexes, relationships, api_entities); stored in `_private_*` tables.
- **Common endpoints**: Health (`/health`), readiness (`/ready`), version (`/version`, `/info`).
- **Safe SQL**: Parameterized queries only; identifiers from validated config.
- **Standard envelope**: `{ "data": ..., "meta": {} }` and `{ "error": { "code", "message", "details" } }`.

## Config schema

- Reuses the [Postgres config schema](docs/postgres-config-schema.md) (schemas, enums, tables, columns, indexes, relationships).
- Add an **api_entities** config: `entity_id`, `path_segment`, `operations`, `validation` (per-column rules). See [sample/api_entities.json](sample/api_entities.json).

## Quick start

1. Add to `Cargo.toml`:

   ```toml
   [dependencies]
   architect-sdk = { path = "path/to/architect-sdk" }
   ```

2. Load config (from files or `load_from_pool`), resolve, build router:

   ```rust
   use architect_sdk::{resolve, AppState, ensure_private_tables, common_routes_with_ready, config_routes, entity_routes};
   use std::sync::Arc;

   let pool = sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
   ensure_private_tables(&pool).await?;

   let config = my_load_config(); // or load_from_pool(&pool).await?
   let model = resolve(&config)?;
   let state = AppState { pool: pool.clone(), model: Arc::new(model) };

   let app = axum::Router::new()
       .merge(common_routes_with_ready(state.clone()))
       .nest("/api/v1", config_routes(state.clone()))
       .nest("/api/v1", entity_routes(state));
   ```

3. Run the example server (loads from `sample/` by default, or set `CONFIG_PATH` and `DATABASE_URL`):

   ```bash
   cargo run --example server
   ```

## API overview

- **Common**: `GET /health`, `GET /ready`, `GET /version`, `GET /info`
- **Config**: `POST /api/v1/config/schemas`, `GET /api/v1/config/schemas`, and same for `enums`, `tables`, `columns`, `indexes`, `relationships`, `api_entities`
- **Entities**: For each entity (e.g. `users`): `POST /api/v1/users`, `GET /api/v1/users/:id`, `PATCH /api/v1/users/:id`, `DELETE /api/v1/users/:id`, `POST /api/v1/users/bulk`, `PATCH /api/v1/users/bulk`

## License

See repository license.
