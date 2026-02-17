-- Seed example KV store data for sample and sample_ecommerce packages.
-- Run after ensure_sys_tables and after installing the packages (so kv_stores config exists).
-- Schema is from env ARCHITECT_SCHEMA (default: architect). If your schema differs, replace "architect" below.
-- For database-strategy tenants, _sys_kv_data lives in the tenant DB; run equivalent INSERTs there if needed.

-- Sample package: user_prefs and app_settings
INSERT INTO architect._sys_kv_data (package_id, namespace, key, value, updated_at)
VALUES
  ('sample', 'user_prefs', 'theme', '"dark"', NOW()),
  ('sample', 'user_prefs', 'locale', '"en-US"', NOW()),
  ('sample', 'app_settings', 'maintenance_mode', 'false', NOW()),
  ('sample', 'app_settings', 'max_upload_mb', '10', NOW())
ON CONFLICT (package_id, namespace, key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();

-- Sample E-commerce package: user_prefs and feature_flags
INSERT INTO architect._sys_kv_data (package_id, namespace, key, value, updated_at)
VALUES
  ('sample_ecommerce', 'user_prefs', 'theme', '"light"', NOW()),
  ('sample_ecommerce', 'user_prefs', 'currency', '"USD"', NOW()),
  ('sample_ecommerce', 'feature_flags', 'new_checkout', 'true', NOW()),
  ('sample_ecommerce', 'feature_flags', 'guest_checkout', 'true', NOW()),
  ('sample_ecommerce', 'feature_flags', 'beta_catalog', 'false', NOW())
ON CONFLICT (package_id, namespace, key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();
