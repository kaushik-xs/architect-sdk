//! Storage provider abstraction: upload, presign, delete.
//!
//! Enable backends via Cargo features:
//!   - `storage-s3`   — AWS S3 and S3-compatible endpoints (RustFS, MinIO)
//!   - `storage-azure` — Azure Blob Storage
//!   - `storage-gcs`  — Google Cloud Storage
//!   - `storage-all`  — all three
//!
//! Set `STORAGE_PROVIDER` env var to `s3`, `rustfs`, `azure`, or `gcs` at runtime.

use crate::error::AppError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;


#[cfg(feature = "storage-azure")]
pub mod azure;
#[cfg(feature = "storage-gcs")]
pub mod gcs;

// ── Public result types ───────────────────────────────────────────────────────

pub struct PresignResult {
    pub url: String,
    pub expires_at: DateTime<Utc>,
    pub expires_in: u64,
}

// ── Trait ─────────────────────────────────────────────────────────────────────

#[async_trait]
pub trait StorageProvider: Send + Sync {
    /// Upload `data` to `path` in the configured bucket. Returns the stored path.
    async fn upload(&self, path: &str, data: Vec<u8>, content_type: &str) -> Result<(), AppError>;
    /// Generate a presigned GET URL for `path` valid for `expires_secs` seconds.
    async fn presign_url(&self, path: &str, expires_secs: u64) -> Result<PresignResult, AppError>;
    /// Delete the object at `path`.
    async fn delete(&self, path: &str) -> Result<(), AppError>;
}

// ── S3 / RustFS provider ──────────────────────────────────────────────────────

#[cfg(feature = "storage-s3")]
pub struct S3Provider {
    client: aws_sdk_s3::Client,
    bucket: String,
}

#[cfg(feature = "storage-s3")]
#[allow(clippy::wildcard_imports)]
use std::time::Duration;

#[cfg(feature = "storage-s3")]
impl S3Provider {
    /// Construct from environment variables.
    /// Required: STORAGE_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.
    /// Optional: STORAGE_ENDPOINT (RustFS / custom), AWS_REGION (default us-east-1).
    pub async fn from_env() -> Option<Self> {
        let bucket = std::env::var("STORAGE_BUCKET").ok()?;
        let endpoint = std::env::var("STORAGE_ENDPOINT").ok();
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());

        let aws_cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(region))
            .load()
            .await;

        let mut builder = aws_sdk_s3::config::Builder::from(&aws_cfg);
        if let Some(ep) = endpoint {
            // Force path-style for S3-compatible endpoints (RustFS, MinIO, etc.)
            builder = builder.endpoint_url(ep).force_path_style(true);
        }
        let client = aws_sdk_s3::Client::from_conf(builder.build());
        Some(S3Provider { client, bucket })
    }
}

#[cfg(feature = "storage-s3")]
#[async_trait]
impl StorageProvider for S3Provider {
    async fn upload(&self, path: &str, data: Vec<u8>, content_type: &str) -> Result<(), AppError> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(path)
            .body(aws_sdk_s3::primitives::ByteStream::from(data))
            .content_type(content_type)
            .send()
            .await
            .map_err(|e| AppError::Storage(e.to_string()))?;
        Ok(())
    }

    async fn presign_url(&self, path: &str, expires_secs: u64) -> Result<PresignResult, AppError> {
        let cfg = aws_sdk_s3::presigning::PresigningConfig::expires_in(
            Duration::from_secs(expires_secs),
        )
        .map_err(|e| AppError::Storage(e.to_string()))?;

        let presigned = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(path)
            .presigned(cfg)
            .await
            .map_err(|e| AppError::Storage(e.to_string()))?;

        Ok(PresignResult {
            url: presigned.uri().to_string(),
            expires_at: Utc::now() + chrono::Duration::seconds(expires_secs as i64),
            expires_in: expires_secs,
        })
    }

    async fn delete(&self, path: &str) -> Result<(), AppError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await
            .map_err(|e| AppError::Storage(e.to_string()))?;
        Ok(())
    }
}

// ── Initialisation ────────────────────────────────────────────────────────────

/// Build a storage provider from env vars. Returns None when STORAGE_PROVIDER is not set.
///
/// Supported values for `STORAGE_PROVIDER`:
///   - `s3` / `rustfs` — AWS S3 or S3-compatible (requires feature `storage-s3`)
///   - `azure`         — Azure Blob Storage      (requires feature `storage-azure`)
///   - `gcs`           — Google Cloud Storage    (requires feature `storage-gcs`)
pub async fn init_storage_provider() -> Option<Arc<dyn StorageProvider>> {
    let provider_type = std::env::var("STORAGE_PROVIDER").ok()?.to_lowercase();
    match provider_type.as_str() {
        #[cfg(feature = "storage-s3")]
        "s3" | "rustfs" => {
            let p = S3Provider::from_env().await?;
            Some(Arc::new(p) as Arc<dyn StorageProvider>)
        }
        #[cfg(feature = "storage-azure")]
        "azure" => {
            let p = azure::AzureProvider::from_env()?;
            Some(Arc::new(p) as Arc<dyn StorageProvider>)
        }
        #[cfg(feature = "storage-gcs")]
        "gcs" => {
            let p = gcs::GcsProvider::from_env().await?;
            Some(Arc::new(p) as Arc<dyn StorageProvider>)
        }
        _ => {
            tracing::warn!(provider = %provider_type, "unknown STORAGE_PROVIDER or feature not enabled; storage disabled");
            None
        }
    }
}

// ── Prefix resolution ─────────────────────────────────────────────────────────

/// Resolve a prefix template at upload time.
/// Supported tokens: {yyyy}, {mm}, {dd}, {hh}, {tenant_id}, {entity}.
pub fn resolve_prefix(template: &str, tenant_id: &str, entity: &str) -> String {
    let now = Utc::now();
    template
        .replace("{yyyy}", &now.format("%Y").to_string())
        .replace("{mm}", &now.format("%m").to_string())
        .replace("{dd}", &now.format("%d").to_string())
        .replace("{hh}", &now.format("%H").to_string())
        .replace("{tenant_id}", tenant_id)
        .replace("{entity}", entity)
}

// ── Compression ───────────────────────────────────────────────────────────────

/// Apply byte-level compression before upload.
/// Supported: "gzip", "zstd", "none" (or any unrecognised value → pass-through).
pub fn compress(data: Vec<u8>, compression: &str) -> Result<Vec<u8>, AppError> {
    match compression.to_lowercase().as_str() {
        "gzip" => {
            use flate2::write::GzEncoder;
            use flate2::Compression;
            use std::io::Write;
            let mut enc = GzEncoder::new(Vec::new(), Compression::default());
            enc.write_all(&data)
                .map_err(|e| AppError::Storage(format!("gzip write: {}", e)))?;
            enc.finish()
                .map_err(|e| AppError::Storage(format!("gzip finish: {}", e)))
        }
        "zstd" => zstd::bulk::compress(&data, 0)
            .map_err(|e| AppError::Storage(format!("zstd compress: {}", e))),
        _ => Ok(data),
    }
}

// ── Asset validation ──────────────────────────────────────────────────────────

/// Validate an uploaded file against the asset validation rules configured in api_entities.
pub fn validate_asset_field(
    col: &str,
    filename: &str,
    content_type: &str,
    size_bytes: usize,
    rule: &crate::config::ValidationRule,
) -> Result<(), AppError> {
    if let Some(ref allowed) = rule.allowed_mime_types {
        let ct = content_type.split(';').next().unwrap_or(content_type).trim();
        if !allowed.iter().any(|m| m.eq_ignore_ascii_case(ct)) {
            return Err(AppError::Validation(format!(
                "{}: mime type '{}' is not allowed; accepted: {}",
                col,
                ct,
                allowed.join(", ")
            )));
        }
    }
    if let Some(ref allowed) = rule.allowed_extensions {
        let ext = std::path::Path::new(filename)
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| format!(".{}", e.to_lowercase()))
            .unwrap_or_default();
        if !allowed.iter().any(|a| a.eq_ignore_ascii_case(&ext)) {
            return Err(AppError::Validation(format!(
                "{}: extension '{}' is not allowed; accepted: {}",
                col,
                ext,
                allowed.join(", ")
            )));
        }
    }
    if let Some(max_mb) = rule.max_size_mb {
        let limit = (max_mb * 1024.0 * 1024.0) as usize;
        if size_bytes > limit {
            return Err(AppError::Validation(format!(
                "{}: file size {} bytes exceeds maximum of {:.1} MB",
                col, size_bytes, max_mb
            )));
        }
    }
    if let Some(min_kb) = rule.min_size_kb {
        let floor = (min_kb * 1024.0) as usize;
        if size_bytes < floor {
            return Err(AppError::Validation(format!(
                "{}: file size {} bytes is below minimum of {:.1} KB",
                col, size_bytes, min_kb
            )));
        }
    }
    if let Some(max_len) = rule.max_filename_length {
        if filename.len() > max_len as usize {
            return Err(AppError::Validation(format!(
                "{}: filename length {} exceeds maximum of {}",
                col,
                filename.len(),
                max_len
            )));
        }
    }
    Ok(())
}
