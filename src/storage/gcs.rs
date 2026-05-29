//! Google Cloud Storage provider.
//!
//! Auth modes (tried in order):
//!   1. `GCS_SERVICE_ACCOUNT_JSON` — inline service account JSON string
//!   2. `GOOGLE_APPLICATION_CREDENTIALS` — path to service account key file (standard ADC)
//!   3. Application Default Credentials — works automatically on GCP (Cloud Run, GKE, Compute Engine)
//!
//! Presigned URL signing:
//!   Modes 1 & 2 set `default_sign_by = PrivateKey(...)` from the key file (local RSA signing).
//!   Mode 3 sets `default_sign_by = SignBytes` which calls the IAM `signBlob` API (one extra HTTP call).

use crate::error::AppError;
use async_trait::async_trait;
use chrono::{Duration, Utc};
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::delete::DeleteObjectRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use google_cloud_storage::sign::{SignedURLMethod, SignedURLOptions};

use super::PresignResult;
use crate::storage::StorageProvider;

// ── Provider ──────────────────────────────────────────────────────────────────

pub struct GcsProvider {
    client: Client,
    bucket: String,
}

impl GcsProvider {
    /// Build from environment variables. Returns `None` if required vars are absent.
    pub async fn from_env() -> Option<Self> {
        let bucket = std::env::var("GCS_BUCKET").ok()?;

        let config = if let Ok(json) = std::env::var("GCS_SERVICE_ACCOUNT_JSON") {
            // Explicit inline JSON key
            let creds = google_cloud_auth::credentials::CredentialsFile::new_from_str(&json)
                .await
                .map_err(|e| tracing::error!("GCS_SERVICE_ACCOUNT_JSON parse error: {}", e))
                .ok()?;
            ClientConfig::default()
                .with_credentials(creds)
                .await
                .map_err(|e| tracing::error!("GCS client config error: {}", e))
                .ok()?
        } else {
            // ADC: picks up GOOGLE_APPLICATION_CREDENTIALS file or metadata server automatically
            ClientConfig::default()
                .with_auth()
                .await
                .map_err(|e| tracing::error!("GCS auth error: {}", e))
                .ok()?
        };

        Some(GcsProvider {
            client: Client::new(config),
            bucket,
        })
    }
}

#[async_trait]
impl StorageProvider for GcsProvider {
    async fn upload(&self, path: &str, data: Vec<u8>, content_type: &str) -> Result<(), AppError> {
        let mut media = Media::new(path.to_string());
        media.content_type = std::borrow::Cow::Owned(content_type.to_string());

        let upload_type = UploadType::Simple(media);
        self.client
            .upload_object(
                &UploadObjectRequest {
                    bucket: self.bucket.clone(),
                    ..Default::default()
                },
                data,
                &upload_type,
            )
            .await
            .map_err(|e| AppError::Storage(format!("GCS upload error: {}", e)))?;
        Ok(())
    }

    async fn presign_url(&self, path: &str, expires_secs: u64) -> Result<PresignResult, AppError> {
        let expires_at = Utc::now() + Duration::seconds(expires_secs as i64);

        let opts = SignedURLOptions {
            method: SignedURLMethod::GET,
            expires: std::time::Duration::from_secs(expires_secs),
            ..Default::default()
        };

        // google_access_id and sign_by are both None → uses defaults set by ClientConfig
        // (PrivateKey when a service account key is loaded; SignBytes on GCP infrastructure)
        let url = self
            .client
            .signed_url(&self.bucket, path, None, None, opts)
            .await
            .map_err(|e| AppError::Storage(format!("GCS signed URL error: {}", e)))?;

        Ok(PresignResult {
            url,
            expires_at,
            expires_in: expires_secs,
        })
    }

    async fn delete(&self, path: &str) -> Result<(), AppError> {
        self.client
            .delete_object(&DeleteObjectRequest {
                bucket: self.bucket.clone(),
                object: path.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|e| AppError::Storage(format!("GCS delete error: {}", e)))?;
        Ok(())
    }
}
