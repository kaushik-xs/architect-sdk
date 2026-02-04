# PostgreSQL config schema: structure and usage

This document describes the JSON/config format used to define and manage a PostgreSQL database declaratively. Configs are split by entity type (enums, tables, columns, indexes, relationships) and can be stored in separate database tables or separate JSON files.

---

## 1. How the structure is organized

### Schema from manifest (no separate schema table required)

The **schema name** (PostgreSQL namespace, e.g. `public` or `sample`) is defined in the **package manifest**, not in a separate schemas config. The manifest must include a `schema` field (string). That schema is used for all configs (enums, tables, indexes, relationships); you do **not** need to point to the schema in each config. When loading from a package directory or installing a package zip, the runtime builds a single schema (id `"default"`, name = `manifest.schema`) and injects it so that enums, tables, and indexes can omit `schema_id`. Relationships require `from_schema_id` and `to_schema_id` (the package installer may inject these when uploading a zip). A separate `schemas.json` or schemas table is not required for package-based config.

### Separate configs per entity

The database definition is **not** a single nested JSON document. Instead, there are config types, each with its own structure and storage:

| Config         | Purpose                      | References others by                    |
| --------------- | ---------------------------- | --------------------------------------- |
| `schemas`       | (Optional) PostgreSQL schema; when using manifest, one schema is derived from `manifest.schema` | — |
| `enums`         | Custom enum types            | `schema_id` (optional if manifest defines schema) |
| `tables`        | Table definitions            | `schema_id` (optional if manifest defines schema) |
| `columns`       | Column definitions           | `table_id`                              |
| `indexes`       | Index definitions            | `schema_id` (optional), `table_id`      |
| `relationships` | Foreign key definitions     | `from_schema_id`, `to_schema_id`, `from_*` / `to_*` ids |

Each config is an **array of records**. One record = one enum, one table, one column, one index, or one foreign key.

### Storage: table vs file

- **Database tables**: You can store each config type in its own table (e.g. `_sys_tables`). Each row is one record; complex fields can be JSON/JSONB columns.
- **JSON files (package)**: Use one JSON file per config type inside a package directory or zip. The directory must contain `manifest.json` (with `schema`). Example layout: `manifest.json`, `enums.json`, `tables.json`, `columns.json`, `indexes.json`, `relationships.json`, `api_entities.json`. No `schemas.json` is required.

The record shape is the same in both cases; only the storage medium changes.

### Identifiers and references

- **Stable ids**: Every record has an `id` (string). Use stable, unique ids (e.g. `sch_public`, `tbl_users`, `col_users_id`) so that other configs can reference them. References stay valid even if names change.
- **References**: Configs reference each other via these ids (e.g. `schema_id`, `table_id`, `from_column_id`, `to_column_id`). No nesting: columns do not live inside table objects; they live in `columns` and point to `tables` with `table_id`.
- **Natural keys (optional)**: You can also support name-based resolution (e.g. `schema_name`, `table_name`) for readability or tooling; the spec is defined in terms of ids, and names can be resolved from the corresponding configs.

---

## 2. Reference: config types and fields

### 2.1 Schemas

When using a **manifest**, you do **not** need a separate schemas config. The manifest’s `schema` field (e.g. `"public"` or `"sample"`) is the PostgreSQL schema name; the runtime creates a single schema (id `"default"`, name = manifest’s `schema`) and uses it for all configs. When loading from the database (no package path), the `schemas` config can still be used to define one or more schemas.

| Field     | Type   | Description                |
| --------- | ------ | -------------------------- |
| `id`      | string | Unique id                  |
| `name`    | string | Schema name (e.g. `public`) |
| `comment` | string | Optional COMMENT           |

**Allowed values**

| Key       | Allowed / format                    | Required |
| --------- | ----------------------------------- | -------- |
| `id`      | Non-empty string; unique across all schema records | Yes |
| `name`    | Valid PostgreSQL identifier (e.g. `public`, `audit`) | Yes |
| `comment` | Any string; omit or `null` if none  | No  |

No references to other configs.

---

### 2.2 Enums

Defines a custom enum type in a schema.

| Field       | Type    | Description                      |
| ----------- | ------- | -------------------------------- |
| `id`        | string  | Unique id                        |
| `schema_id` | string? | FK to `schemas.id`; **optional** when manifest defines schema (default used) |
| `name`      | string  | Enum type name                   |
| `values`    | array   | Ordered list of labels (strings) |
| `comment`   | string  | Optional COMMENT                 |

**Allowed values**

| Key         | Allowed / format                                      | Required |
| ----------- | ------------------------------------------------------ | -------- |
| `id`        | Non-empty string; unique across all enum records       | Yes      |
| `schema_id` | If present, must equal `id` of a record in `schemas`; if omitted, manifest schema (id `"default"`) is used | No (when using manifest) |
| `name`      | Valid PostgreSQL identifier for the type name          | Yes      |
| `values`    | Array of non-empty strings; order is significant       | Yes      |
| `comment`   | Any string; omit or `null` if none                     | No       |

In DDL, created as `CREATE TYPE schema_name.type_name AS ENUM (...)`.

---

### 2.3 Tables

Defines a table in a schema. Column definitions live in the `columns` config; table-level constraints (primary key, unique, check) are on the table record.

| Field         | Type            | Description                                                                 |
| ------------- | --------------- | --------------------------------------------------------------------------- |
| `id`          | string          | Unique id                                                                  |
| `schema_id`   | string?         | FK to `schemas.id`; **optional** when manifest defines schema (default used) |
| `name`        | string          | Table name                                                                 |
| `comment`     | string          | Optional COMMENT                                                            |
| `primary_key` | string or array | Column name(s) for PRIMARY KEY (single or composite)                      |
| `unique`      | array           | Optional: list of UNIQUE constraints, each an array of column names       |
| `check`       | array           | Optional: `[{ "name": "...", "expression": "..." }]`                      |

**Allowed values**

| Key           | Allowed / format                                                    | Required |
| ------------- | ------------------------------------------------------------------- | -------- |
| `id`          | Non-empty string; unique across all table records                   | Yes      |
| `schema_id`   | If present, must equal `id` of a record in `schemas`; if omitted, manifest schema is used | No (when using manifest) |
| `name`        | Valid PostgreSQL identifier for the table name                      | Yes      |
| `comment`     | Any string; omit or `null` if none                                  | No       |
| `primary_key` | String (single column name) or array of strings (composite); each must match a column `name` in `columns` for this table | Yes |
| `unique`      | Array of arrays; each inner array is a list of column names for one UNIQUE constraint; use `[]` if none | No (default `[]`) |
| `check`       | Array of objects: `{ "name": string, "expression": string }`; `name` is constraint name, `expression` is SQL boolean expression; use `[]` if none | No (default `[]`) |

**Check constraint examples**

Each element of `check` is an object with `name` (constraint name) and `expression` (SQL boolean expression). PostgreSQL will reject rows that do not satisfy the expression.

| Example use              | `name`               | `expression`                          |
| ------------------------- | -------------------- | -------------------------------------- |
| Non-negative numeric      | `positive_price`     | `price >= 0`                            |
| Non-negative or null     | `non_negative_qty`   | `quantity IS NULL OR quantity >= 0`    |
| Percentage in range      | `valid_discount`     | `discount_pct >= 0 AND discount_pct <= 100` |
| Start before end (dates)  | `valid_date_range`   | `start_at < end_at`                     |
| Length / format          | `email_not_empty`    | `char_length(trim(email)) > 0`         |
| Enum-like bounds         | `valid_status`       | `status IN ('draft', 'published', 'archived')` |
| Composite                | `sensible_dates`     | `created_at <= updated_at AND updated_at <= COALESCE(closed_at, 'infinity'::timestamptz)` |

Example table record with `check`:

```json
{
  "id": "tbl_products",
  "schema_id": "sch_public",
  "name": "products",
  "comment": "Product catalog",
  "primary_key": "id",
  "unique": [["sku"]],
  "check": [
    { "name": "positive_price", "expression": "price >= 0" },
    { "name": "valid_discount", "expression": "discount_pct >= 0 AND discount_pct <= 100" }
  ]
}
```

---

### 2.4 Columns

Defines a column on a table. Foreign key semantics are in the `relationships` config, not on the column.

| Field       | Type             | Description                                                                 |
| ----------- | ---------------- | --------------------------------------------------------------------------- |
| `id`        | string           | Unique id                                                                   |
| `table_id`  | string           | FK to `tables.id`                                                           |
| `name`      | string           | Column name                                                                 |
| `type`      | string or object | Built-in type or schema-qualified enum; object form for parameterized types |
| `nullable`  | boolean          | Default true; false = NOT NULL                                              |
| `default`   | string or object | Literal or expression object                                                |
| `generated` | object           | Optional: generated column                                                  |
| `comment`   | string           | Optional COMMENT                                                            |

**Allowed values**

| Key         | Allowed / format                                                                 | Required |
| ----------- | -------------------------------------------------------------------------------- | -------- |
| `id`        | Non-empty string; unique across all column records                              | Yes      |
| `table_id`  | Must equal `id` of a record in `tables`                                          | Yes      |
| `name`      | Valid PostgreSQL identifier for the column name                                  | Yes      |
| `type`      | **String**: built-in type name (see below) or `"schema_name.enum_name"`. **Object**: `{ "name": string, "params"?: number[] }` for parameterized types (e.g. `{ "name": "varchar", "params": [255] }`). | Yes |
| `nullable`  | `true` or `false`; default `true`                                                | No       |
| `default`   | String (literal SQL, e.g. `"'pending'"`) or `{ "expression": string }` (e.g. `{ "expression": "NOW()" }`) or `null`/omit | No |
| `generated` | Omit or `null`, or `{ "expression": string, "stored": boolean }`; `stored` defaults to `true` for generated columns | No |
| `comment`   | Any string; omit or `null` if none                                              | No       |

**Column `type` — built-in type names (string):**  
`smallint`, `integer`, `int`, `bigint`, `serial`, `bigserial`, `real`, `double precision`, `numeric`, `decimal`, `money`, `boolean`, `char`, `varchar`, `text`, `uuid`, `date`, `time`, `timestamp`, `timestamptz`, `interval`, `json`, `jsonb`, `xml`, `bytea`, `inet`, `cidr`. Parameterized forms as string: e.g. `varchar(255)`, `numeric(10,2)`, `char(1)`.

---

### 2.5 Indexes

Defines an index on a table. References both schema and table.

| Field       | Type    | Description                                                |
| ----------- | ------- | ---------------------------------------------------------- |
| `id`        | string  | Unique id                                                  |
| `schema_id` | string? | FK to `schemas.id`; **optional** when manifest defines schema |
| `table_id`  | string  | FK to `tables.id`                                          |
| `name`      | string  | Index name (unique per schema)                             |
| `method`    | string  | Index method (see allowed values)                         |
| `unique`    | boolean | UNIQUE index                                               |
| `columns`   | array   | Column(s) or expression(s)—see below                       |
| `include`   | array   | INCLUDE column names (PostgreSQL 11+)                     |
| `where`     | string  | Partial index predicate (SQL expression)                   |
| `comment`   | string  | Optional COMMENT                                           |

**Allowed values**

| Key         | Allowed / format                                                                 | Required |
| ----------- | -------------------------------------------------------------------------------- | -------- |
| `id`        | Non-empty string; unique across all index records                               | Yes      |
| `schema_id` | If present, must equal `id` of a record in `schemas`; if omitted, manifest schema is used | No (when using manifest) |
| `table_id`  | Must equal `id` of a record in `tables`                                          | Yes      |
| `name`      | Valid PostgreSQL identifier; unique within the schema                            | Yes      |
| `method`    | One of: `btree`, `hash`, `gin`, `gist`, `brin`, `spgist`; default `btree`        | No       |
| `unique`    | `true` or `false`; default `false`                                               | No       |
| `columns`   | Array of column entries (see below); at least one required                        | Yes      |
| `include`   | Array of column name strings (INCLUDE columns); use `[]` if none                 | No       |
| `where`     | String: SQL boolean expression for partial index; `null` or omit for full index    | No       |
| `comment`   | Any string; omit or `null` if none                                               | No       |

**Index `columns` entries (each element of `columns` array):**

| Form        | Allowed values / format                                                                 |
| ----------- | --------------------------------------------------------------------------------------- |
| Short       | String: column name (e.g. `"user_id"`).                                                 |
| Column spec | Object: `{ "name": string, "direction"?: "asc" \| "desc", "nulls"?: "first" \| "last" }`. `direction` and `nulls` apply to btree. |
| Expression  | Object: `{ "expression": string }` where string is a SQL expression (e.g. `LOWER(email)`). |

**Index `columns` examples:**

- Simple column: `"column_name"` or `{ "name": "column_name", "direction": "asc", "nulls": "last" }`.
- Expression: `{ "expression": "LOWER(email)" }` or `{ "expression": "(payload->>'type')" }`.

---

### 2.6 Relationships

Defines a foreign key: one column references another table’s column. Stored separately so relationships can be managed and queried without editing column records.

| Field            | Type   | Description                                    |
| ---------------- | ------ | ---------------------------------------------- |
| `id`             | string | Unique id                                      |
| `from_schema_id` | string | FK to `schemas.id` (source table’s schema)     |
| `from_table_id`  | string | FK to `tables.id` (source table)               |
| `from_column_id` | string | FK to `columns.id` (source column)             |
| `to_schema_id`   | string | FK to `schemas.id` (referenced table’s schema)  |
| `to_table_id`    | string | FK to `tables.id` (referenced table)           |
| `to_column_id`   | string | FK to `columns.id` (referenced column)         |
| `on_update`      | string | Referential action (see allowed values)       |
| `on_delete`      | string | Referential action (see allowed values)       |
| `name`           | string | Optional constraint name                        |

**Allowed values**

| Key              | Allowed / format                                                                 | Required |
| ---------------- | -------------------------------------------------------------------------------- | -------- |
| `id`             | Non-empty string; unique across all relationship records                         | Yes      |
| `from_schema_id` | Must equal `id` of a record in `schemas`                                        | Yes      |
| `from_table_id`  | Must equal `id` of a record in `tables`                                          | Yes      |
| `from_column_id` | Must equal `id` of a record in `columns` (and that column’s `table_id` must equal `from_table_id`) | Yes |
| `to_schema_id`   | Must equal `id` of a record in `schemas`                                         | Yes      |
| `to_table_id`    | Must equal `id` of a record in `tables`                                          | Yes      |
| `to_column_id`   | Must equal `id` of a record in `columns` (and that column’s `table_id` must equal `to_table_id`)   | Yes |
| `on_update`      | One of: `NO ACTION`, `RESTRICT`, `CASCADE`, `SET NULL`, `SET DEFAULT`            | No (default typically `NO ACTION`) |
| `on_delete`      | One of: `NO ACTION`, `RESTRICT`, `CASCADE`, `SET NULL`, `SET DEFAULT`           | No (default typically `NO ACTION`) |
| `name`           | Valid PostgreSQL identifier for the constraint name; omit or `null` if none      | No       |

---

## 3. How things work

### 3.1 Load order and dependency graph

Configs depend on each other as follows:

1. **schemas** — no dependencies (when using manifest, one schema is derived from `manifest.schema`).
2. **enums**, **tables** — depend on `schemas` (via optional `schema_id`; default from manifest).
3. **columns** — depend on `tables` (via `table_id`).
4. **indexes** — depend on `schemas` and `tables` (optional `schema_id`, `table_id`).
5. **relationships** — depend on `schemas`, `tables`, and `columns` (required `from_schema_id`, `to_schema_id`, and from/to table/column ids).

Recommended load order when reading from files or DB:

1. Load `schemas`.
2. Load `enums` and `tables` (both need schemas).
3. Load `columns` (needs tables).
4. Load `indexes` and `relationships` (need tables and columns).

This order ensures that when you resolve references (e.g. look up a table by `table_id`), the target records are already in memory.

### 3.2 Resolving references

- **By id**: For each record that has a `*_id` field, look up the referenced record in the corresponding config (e.g. `schema_id` → find in `schemas` by `id`; if `schema_id` is omitted, use the default schema from the manifest). Use a map from id to record for O(1) lookup.
- **By name (optional)**: If you support natural keys, you can resolve e.g. `(schema_name, table_name)` to a table by scanning the `tables` array and matching names; for production, build indexes or maps keyed by (schema_name, table_name) after loading.

All references (schema_id when present, table_id, from_*_id, to_*_id) must point to existing ids in the corresponding config; otherwise the dataset is invalid.

### 3.3 Validation (referential integrity)

Before generating DDL or using the config as a single logical model, validate:

- Every `schema_id` in enums, tables, and indexes when present must exist in `schemas.id` (when omitted, default from manifest is used). Every `from_schema_id` and `to_schema_id` in relationships must exist in `schemas.id`.
- Every `table_id` in `columns` and `indexes` exists in `tables.id`.
- Every `from_column_id` and `to_column_id` in `relationships` exists in `columns.id`.
- Every column name used in a table’s `primary_key` or `unique` exists among that table’s columns (in `columns` filtered by `table_id`).
- Index `columns` refer to column names (or expressions) that make sense for the table (optional strict check).

If any of these fail, report errors and do not generate DDL.

### 3.4 Assembly into a single logical model

You can assemble the six configs into one in-memory model (e.g. a nested structure per schema: schema → enums, tables → columns; and a flat list of indexes and relationships). Steps:

1. Index all configs by `id` (and optionally by (schema_name, table_name) etc.).
2. For each schema, attach its enums and tables (filter by `schema_id`).
3. For each table, attach its columns (filter `columns` by `table_id`).
4. Attach indexes and relationships to the right schema/table by resolving `schema_id` and `table_id`.

This assembled model can then be used for DDL generation, diffing, or export.

### 3.5 DDL generation order

To create the actual PostgreSQL database from the configs, emit DDL in an order that respects PostgreSQL’s dependencies:

1. **CREATE SCHEMA** for each schema (and COMMENT if present).
2. **CREATE TYPE ... AS ENUM** for each enum (schema-qualified).
3. **CREATE TABLE** for each table: add all columns (with types, NULL/NOT NULL, DEFAULT, GENERATED), then add PRIMARY KEY, UNIQUE, and CHECK from the table record. Do not add foreign keys yet.
4. **CREATE INDEX** for each index (optionally CREATE UNIQUE INDEX). Support method (btree, hash, gin, gist, brin, spgist), partial index (`WHERE`), and INCLUDE.
5. **ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY** for each relationship, with ON UPDATE and ON DELETE as specified.

If you create FKs before indexes or tables exist, or create tables that reference enums before those enums exist, PostgreSQL will error. The order above avoids that.

### 3.6 Caveats

- **Enum order**: Enum values are ordered; adding or reordering values in an existing enum in PostgreSQL requires migration (e.g. new type and migration of data). The config format describes the desired state; migration logic is out of scope here.
- **Column type strings**: Types like `varchar(255)` or `numeric(10,2)` can be stored as strings; the DDL generator must parse or pass them through correctly. Schema-qualified enums (e.g. `public.order_status`) must be resolved to the same schema in which the table is created.
- **Table constraints**: UNIQUE and CHECK are defined on the table record; alternatively they can be moved to a separate `table_constraints` config (one row per constraint) if you want to manage them independently.

---

## 4. Examples

### 4.1 Sample config location

A minimal but complete example lives under **`sample/`** in this repo:

- `sample/manifest.json` — includes `schema` (e.g. `"sample"`), the single PostgreSQL schema name used by all configs; no separate `schemas.json` is required.
- `sample/enums.json`
- `sample/tables.json`
- `sample/columns.json`
- `sample/indexes.json`
- `sample/relationships.json`
- `sample/api_entities.json`

The manifest’s `schema` defines the namespace; enums, tables, indexes, and relationships do not need to set `schema_id` (they use the default). The sample defines one enum (`order_status`), two tables (`users`, `orders`), columns for both, two indexes, and one foreign key from `orders.user_id` to `users.id`. All ids are consistent across files so you can trace every reference.

### 4.2 One full chain (schema → table → columns → relationship → index)

1. **Schema** from `manifest.json`: `"schema": "sample"` (runtime creates schema id `"default"` with this name).
2. **Table** `tbl_orders` in `tables.json`: no `schema_id` needed (default schema used), name `orders`, primary_key `id`.
3. **Columns** in `columns.json`: e.g. `col_orders_id` (table_id `tbl_orders`, name `id`, type `bigserial`), `col_orders_user_id` (table_id `tbl_orders`, name `user_id`, type `uuid`).
4. **Relationship** `rel_orders_user` in `relationships.json`: from `tbl_orders.col_orders_user_id` to `tbl_users.col_users_id`, ON DELETE CASCADE.
5. **Index** `idx_orders_user_created` in `indexes.json`: table_id `tbl_orders`, columns `user_id` asc, `created_at` desc.

Together, these configs describe a table `public.orders` with a FK to `public.users` and an index on (user_id, created_at). Loading and resolving by id, then generating DDL in the order in section 3.5, produces the corresponding PostgreSQL objects.

---

## 5. Summary

| Concern       | Config          | Key references                          |
| ------------- | --------------- | --------------------------------------- |
| Schema        | `manifest.schema` or `schemas` | — (manifest defines schema name; no separate schema table required) |
| Enums         | `enums`         | `schema_id` (optional when using manifest) |
| Tables        | `tables`        | `schema_id` (optional when using manifest) |
| Columns       | `columns`       | `table_id`                              |
| Indexes       | `indexes`       | `schema_id` (optional), `table_id`      |
| Relationships | `relationships` | `from_schema_id`, `to_schema_id` (required), `from_*` / `to_*` ids |

Configs are **independent** and **stored separately** (tables or files). The **manifest** defines the schema name for all configs when using package-based config; no `schemas.json` or explicit `schema_id` in each config is required. Use **stable ids** and **reference by id**; validate referential integrity; load in dependency order; generate DDL in the order schemas → enums → tables → indexes → FKs. The sample in `sample/` and this document are the reference for the format and how it works.

---

## 6. What is not covered

This config format defines **schemas** and **schema contents** (tables, columns, enums, indexes, relationships, and table-level constraints). The following are **out of scope** and are not represented in the configs:

| Area | What is not covered |
| ---- | -------------------- |
| **Database-level** | The PostgreSQL database itself (the object created with `CREATE DATABASE`). There is no config for database name, encoding, locale, connection limit, or other database-level settings. The format assumes a database already exists and configures objects inside it. |
| **Views** | Views and materialized views. |
| **Functions and procedures** | Stored functions, procedures, and their signatures. |
| **Triggers** | Triggers on tables. |
| **Row-level security (RLS)** | RLS policies and the ENABLE ROW LEVEL SECURITY flag on tables. |
| **Permissions** | Grants and revokes (e.g. `GRANT SELECT ON schema/table TO role`). |
| **Ownership** | Table/schema ownership (`ALTER ... OWNER TO`). |
| **Extensions** | PostgreSQL extensions (e.g. `CREATE EXTENSION uuid-ossp`). |
| **Sequences (standalone)** | Standalone sequences; only identity/serial columns are covered via column types. |
| **Composite types** | User-defined composite types (aside from enums). |
| **Partitioning** | Table partitioning (PARTITION BY RANGE/LIST/HASH). |

If you need to manage any of these, they would require additional config types and storage (e.g. a `views` config, a `functions` config, or a `databases` config for the top-level database).
