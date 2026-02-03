# Postman collection

Import **Architect-SDK-API.postman_collection.json** into Postman (File → Import → select the file).

## Collection variables

- **base_url**: `http://localhost:3000` — change if your server runs elsewhere.
- **user_id**: Set after creating a user (e.g. from the Create User response) so Read/Update/Delete User and Create Order can use it.
- **order_id**: Set after creating an order so Read/Update/Delete Order can use it.

## Folders

- **Common**: Health, Ready, Version, Info (no `/api/v1` prefix).
- **Config**: GET/POST for schemas, enums, tables, columns, indexes, relationships, api_entities (under `/api/v1/config/...`).
- **Users**: Create, Read, Update, Delete, Bulk Create, Bulk Update (under `/api/v1/users`).
- **Orders**: Same operations for orders (under `/api/v1/orders`).

Start the server with `cargo run --example server` before running requests.
