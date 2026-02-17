-- Seed example KV store data for sample and sample_ecommerce packages (per tenant).
-- Run after ensure_sys_tables and after installing the packages (so kv_stores config exists).
-- Schema is from env ARCHITECT_SCHEMA (default: architect). If your schema differs, replace "architect" below.
-- Replace 'default-mode-3' with your tenant id (must exist in _sys_tenants). Repeat the INSERT block for other tenants if needed.
-- The value column is JSONB; use ::jsonb so literals are stored as JSON.

-- Sample package: user_prefs and app_settings (tenant default-mode-3)
INSERT INTO architect._sys_kv_data (tenant_id, package_id, namespace, key, value, updated_at)
VALUES
  ('default-mode-3', 'sample', 'user_prefs', 'theme', '"dark"'::jsonb, NOW()),
  ('default-mode-3', 'sample', 'user_prefs', 'locale', '"en-US"'::jsonb, NOW()),
  ('default-mode-3', 'sample', 'app_settings', 'maintenance_mode', 'false'::jsonb, NOW()),
  ('default-mode-3', 'sample', 'app_settings', 'max_upload_mb', '10'::jsonb, NOW())
ON CONFLICT (tenant_id, package_id, namespace, key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();

-- Sample E-commerce package: user_prefs and feature_flags (tenant default-mode-3)
INSERT INTO architect._sys_kv_data (tenant_id, package_id, namespace, key, value, updated_at)
VALUES
  ('default-mode-3', 'sample_ecommerce', 'user_prefs', 'theme', '"light"'::jsonb, NOW()),
  ('default-mode-3', 'sample_ecommerce', 'user_prefs', 'currency', '"USD"'::jsonb, NOW()),
  ('default-mode-3', 'sample_ecommerce', 'feature_flags', 'new_checkout', 'true'::jsonb, NOW()),
  ('default-mode-3', 'sample_ecommerce', 'feature_flags', 'guest_checkout', 'true'::jsonb, NOW()),
  ('default-mode-3', 'sample_ecommerce', 'feature_flags', 'beta_catalog', 'false'::jsonb, NOW())
ON CONFLICT (tenant_id, package_id, namespace, key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();
