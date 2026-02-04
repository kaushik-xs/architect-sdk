# Postman collection

Import **Architect-SDK-API.postman_collection.json** into Postman (File → Import → select the file).

## Collection variables

- **base_url**: `http://localhost:3000` — change if your server runs elsewhere.
- **user_id**: Set after creating a user (e.g. from the Create User response) so Read/Update/Delete User and Create Order can use it.
- **order_id**: Set after creating an order so Read/Update/Delete Order can use it.

## System columns

Every table gets three columns added by default (no need to put them in config):

- **created_at** (timestamptz, NOT NULL, default NOW())
- **updated_at** (timestamptz, NOT NULL, default NOW(); set to NOW() on every update)
- **archived_at** (timestamptz, nullable; use for soft-delete)

Responses include these fields. You do not need to send them on create; you can optionally set **archived_at** when archiving a row.

## Folders

- **Common**: Health, Ready, Version, Info (no `/api/v1` prefix).
- **Config**: Install Package (POST `/api/v1/config/package`, multipart zip with manifest.json + config JSONs), then GET/POST for schemas, enums, tables, columns, indexes, relationships, api_entities (under `/api/v1/config/...`). The sample config omits created_at/updated_at/archived_at; they are added automatically.
- **Users**: List (with optional filters and limit/offset), Create, Read, Update, Delete, Bulk Create, Bulk Update (under `/api/v1/users`).
- **Orders**: Same operations for orders (under `/api/v1/orders`). List supports filters (e.g. `?status=pending&user_id=...`).

Start the server with `cargo run --example server` before running requests.
