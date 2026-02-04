# Sample E‑commerce Package

A more complex sample package for the Architect SDK: multi-tenant e‑commerce with **12 tables**, **4 enums**, **18 relationships**, and **18 indexes**.

## Schema: `ecommerce`

### Tables

| Table | Description |
|-------|-------------|
| `organizations` | Tenants (multi-tenant root) |
| `users` | User accounts per organization |
| `customers` | Customer records per organization |
| `addresses` | Shipping/billing addresses for customers |
| `warehouses` | Warehouse/stock locations per organization |
| `product_categories` | Hierarchical categories (self-reference via `parent_id`) |
| `products` | Product catalog per organization |
| `product_category_mappings` | Many-to-many product ↔ category |
| `orders` | Customer orders (customer, shipping/billing address) |
| `order_items` | Order line items (order, product, quantity, prices) |
| `payments` | Payments against orders |
| `inventory` | Stock per product and warehouse (unique per product/warehouse) |

### Enums

- **order_status**: draft, pending, confirmed, processing, shipped, delivered, cancelled, refunded  
- **payment_status**: pending, authorized, captured, failed, refunded, cancelled  
- **address_kind**: shipping, billing, both  
- **product_status**: draft, active, archived, out_of_stock  

### Relationships (high level)

- **Organization-scoped**: users, customers, warehouses, product_categories, products, orders → `organization_id` → organizations  
- **Customer**: addresses → customer_id → customers  
- **Category tree**: product_categories.parent_id → product_categories (self)  
- **Product–category**: product_category_mappings → products, product_categories  
- **Orders**: orders → customers, addresses (shipping, billing); order_items → orders, products; payments → orders  
- **Inventory**: inventory → products, warehouses  

### Usage

Use this folder as the package directory (or zip it) when installing the package. The SDK will create the `ecommerce` schema and all objects from `manifest.json`, `enums.json`, `tables.json`, `columns.json`, `indexes.json`, `relationships.json`, and `api_entities.json`.
