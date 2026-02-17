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

1. Add the SDK to your project’s `Cargo.toml` (private package — use **path** or **git** only):

   **From a local path (same machine or monorepo):**
   ```toml
   [dependencies]
   architect-sdk = { path = "/path/to/architect-sdk" }
   ```

   **From this Git repo (e.g. another repository or CI):**
   ```toml
   [dependencies]
   architect-sdk = { git = "https://github.com/kaushik-xs/architect-sdk" }
   ```
   Optional: use `branch = "multi-tenant"`, `rev = "abc1234"`, or `tag = "v0.1.0"` to pin a ref.

   **Example consumer:** The `example_consumer` crate depends on the SDK from this GitHub repo (see [Private GitHub and CI](#private-github-and-ci)). Run from repo root: `cargo run -p example-consumer`, or `cd example_consumer && cargo run`. For local development against uncommitted SDK changes, switch to `architect-sdk = { path = ".." }` in `example_consumer/Cargo.toml`.

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
   - **`PACKAGE_PATH`**: If set, config is loaded from that package directory (must contain `manifest.json` + config JSONs, e.g. `sample`) and migrations are applied. If not set, only _sys_* tables are ensured and config is loaded from the DB (empty until you POST config via `/api/v1/config/...` or install a package via `POST /api/v1/config/package`).

   ```bash
   cp .env.example .env
   # Optionally set PACKAGE_PATH=sample to load from sample/ (manifest.json + config JSONs)
   cargo run --example server
   ```

## Private GitHub and CI

- **Using this repo as a private dependency:** In another project’s `Cargo.toml`, add `architect-sdk = { git = "https://github.com/kaushik-xs/architect-sdk" }`. For a private repo you must authenticate:
  - **Local:** Use SSH (`git = "ssh://git@github.com/kaushik-xs/architect-sdk"`) with SSH keys, or HTTPS with a [personal access token (PAT)](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens) (e.g. `git config --global url."https://<PAT>@github.com/".insteadOf "https://github.com/"`).
  - **CI (e.g. GitHub Actions):** Configure Git to use the job token so Cargo can fetch the private dependency:
    ```yaml
    - run: |
        git config --global url."https://x-access-token:${{ secrets.GITHUB_TOKEN }}@github.com/".insteadOf "https://github.com/"
    ```
    For cross-repo access use a PAT stored as a repository secret and substitute it for `GITHUB_TOKEN`.

- **Build pipeline:** The repo includes a GitHub Actions workflow in [`.github/workflows/ci.yml`](.github/workflows/ci.yml) that runs on push/PR to `main`, `master`, and `multi-tenant`. It checks formatting, builds the workspace (including `example-consumer` which pulls the SDK via git), runs tests, and runs Clippy. The workflow uses `GITHUB_TOKEN` so the private git dependency can be fetched during the build.

## API overview

- **Common**: `GET /health`, `GET /ready`, `GET /version`, `GET /info`
- **Config**: `POST /api/v1/config/package` (multipart zip: manifest.json + config JSONs), then `POST`/`GET` per kind: `schemas`, `enums`, `tables`, `columns`, `indexes`, `relationships`, `api_entities`
- **Entities**: For each entity (e.g. `users`): `GET /api/v1/users` (list all, optional filters: `?col=value`, `?limit=100`, `?offset=0`, `?include=orders` to embed related entities as exploded JSON), `POST /api/v1/users`, `GET /api/v1/users/:id` (optional `?include=orders`), `PATCH /api/v1/users/:id`, `DELETE /api/v1/users/:id`, `POST /api/v1/users/bulk`, `PATCH /api/v1/users/bulk`. **Case**: Request bodies and query param keys accept **camelCase** (e.g. `userId`, `createdAt`) and are converted to snake_case for the DB; response row keys are returned in **camelCase**. **Includes**: Use `?include=path_segment1,path_segment2` on list and read; allowed values are the related entities’ path segments defined by relationships (e.g. `orders` for a to-many from users). **Multiple packages**: When two packages both define an entity with the same path (e.g. `users`), use package-scoped routes so each package’s data is separate: `GET /api/v1/package/:package_id/users`, `POST /api/v1/package/:package_id/users`, `GET /api/v1/package/:package_id/users/:id`, etc. The default (unprefixed) routes use the default/active model; `package_id` is the package manifest `id` (e.g. `sample`, `sample_ecommerce`).

## License

See repository license.
