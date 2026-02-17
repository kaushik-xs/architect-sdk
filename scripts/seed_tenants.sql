-- Seed default tenants for database and rls strategies.
-- Run after ensure_sys_tables (e.g. after starting the example server once).
-- For default-mode-1 (database), replace :database_url with your tenant DB URL (e.g. postgres://localhost/architect_tenant_default_mode_1).
-- The example server calls seed_default_tenants() which creates the DB and inserts these; this file is for reference or manual runs.

-- Schema is from env ARCHITECT_SCHEMA (default: architect).
INSERT INTO architect._sys_tenants (id, strategy, database_url, updated_at, comment)
VALUES
  (
    'default-mode-1',
    'database',
    'postgres://localhost/architect_tenant_default_mode_1',  -- ensure this DB exists (e.g. create manually or use seed_default_tenants)
    NOW(),
    'Tenant with own database'
  ),
  (
    'default-mode-3',
    'rls',
    'postgres://localhost/temp_2',  -- use temp_2 DB for rls tenant
    NOW(),
    'Tenant with RLS in shared DB'
  )
ON CONFLICT (id) DO NOTHING;
