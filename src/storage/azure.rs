//! Azure Blob Storage provider.
//!
//! Auth modes (tried in order):
//!   1. `AZURE_STORAGE_CONNECTION_STRING` — full connection string (SharedKey)
//!   2. `AZURE_STORAGE_ACCOUNT` + `AZURE_STORAGE_ACCESS_KEY` — SharedKey
//!   3. `AZURE_STORAGE_ACCOUNT` alone — Managed Identity / DefaultAzureCredential
//!      (User Delegation SAS used for presign; no account key required)
//!
//! Local dev: set `AZURE_STORAGE_CONNECTION_STRING=UseDevelopmentStorage=true` for Azurite.

use crate::error::AppError;
use async_trait::async_trait;
use azure_storage::shared_access_signature::service_sas::BlobSasPermissions;
use azure_storage::ConnectionString;
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::BlobServiceClient;
use chrono::{Duration, Utc};
use std::sync::Arc;
use time::OffsetDateTime;

use super::PresignResult;
use crate::storage::StorageProvider;

// ── Provider ──────────────────────────────────────────────────────────────────

pub struct AzureProvider {
    service_client: Arc<BlobServiceClient>,
    container: String,
    /// True when credentials are token-based (Managed Identity) → use User Delegation SAS.
    /// False when credentials are SharedKey → use Service SAS.
    use_user_delegation: bool,
}

impl AzureProvider {
    /// Build from environment variables. Returns `None` if required vars are absent.
    pub fn from_env() -> Option<Self> {
        let container = std::env::var("AZURE_STORAGE_CONTAINER").ok()?;

        // Mode 1: full connection string
        if let Ok(conn_str) = std::env::var("AZURE_STORAGE_CONNECTION_STRING") {
            let parsed = ConnectionString::new(&conn_str)
                .map_err(|e| tracing::error!("Azure connection string parse error: {}", e))
                .ok()?;
            let creds = parsed
                .storage_credentials()
                .map_err(|e| tracing::error!("Azure credentials error: {}", e))
                .ok()?;
            let account = parsed.account_name.unwrap_or_default();
            let client = Arc::new(BlobServiceClient::new(account, creds));
            return Some(AzureProvider {
                service_client: client,
                container,
                use_user_delegation: false,
            });
        }

        let account = std::env::var("AZURE_STORAGE_ACCOUNT").ok()?;

        // Mode 2: account + key (SharedKey)
        if let Ok(key) = std::env::var("AZURE_STORAGE_ACCESS_KEY") {
            let creds = StorageCredentials::access_key(&account, key);
            let client = Arc::new(BlobServiceClient::new(&account, creds));
            return Some(AzureProvider {
                service_client: client,
                container,
                use_user_delegation: false,
            });
        }

        // Mode 3: Managed Identity / DefaultAzureCredential
        let token_cred = azure_identity::create_credential()
            .map_err(|e| tracing::error!("Azure identity error: {}", e))
            .ok()?;
        let creds = StorageCredentials::token_credential(token_cred);
        let client = Arc::new(BlobServiceClient::new(&account, creds));
        Some(AzureProvider {
            service_client: client,
            container,
            use_user_delegation: true,
        })
    }
}

#[async_trait]
impl StorageProvider for AzureProvider {
    async fn upload(&self, path: &str, data: Vec<u8>, content_type: &str) -> Result<(), AppError> {
        // Convert &str to String so BlobContentType (Cow<'static, str>) can own the value
        let ct = content_type.to_string();
        self.service_client
            .container_client(&self.container)
            .blob_client(path)
            .put_block_blob(data)
            .content_type(ct)
            .await
            .map_err(|e| AppError::Storage(format!("Azure upload error: {}", e)))?;
        Ok(())
    }

    async fn presign_url(&self, path: &str, expires_secs: u64) -> Result<PresignResult, AppError> {
        let expires_at = Utc::now() + Duration::seconds(expires_secs as i64);
        let expiry = OffsetDateTime::now_utc() + time::Duration::seconds(expires_secs as i64);

        let permissions = BlobSasPermissions {
            read: true,
            ..Default::default()
        };

        let blob_client = self
            .service_client
            .container_client(&self.container)
            .blob_client(path);

        let url = if self.use_user_delegation {
            let start = OffsetDateTime::now_utc();
            let key_response = self
                .service_client
                .get_user_deligation_key(start, expiry)
                .await
                .map_err(|e| AppError::Storage(format!("Azure delegation key error: {}", e)))?;

            let sas = blob_client
                .user_delegation_shared_access_signature(
                    permissions,
                    &key_response.user_deligation_key,
                )
                .await
                .map_err(|e| {
                    AppError::Storage(format!("Azure User Delegation SAS error: {}", e))
                })?;

            blob_client
                .generate_signed_blob_url(&sas)
                .map_err(|e| AppError::Storage(format!("Azure URL generation error: {}", e)))?
                .to_string()
        } else {
            let sas = blob_client
                .shared_access_signature(permissions, expiry)
                .await
                .map_err(|e| AppError::Storage(format!("Azure SAS error: {}", e)))?;

            blob_client
                .generate_signed_blob_url(&sas)
                .map_err(|e| AppError::Storage(format!("Azure URL generation error: {}", e)))?
                .to_string()
        };

        Ok(PresignResult {
            url,
            expires_at,
            expires_in: expires_secs,
        })
    }

    async fn delete(&self, path: &str) -> Result<(), AppError> {
        self.service_client
            .container_client(&self.container)
            .blob_client(path)
            .delete()
            .await
            .map_err(|e| AppError::Storage(format!("Azure delete error: {}", e)))?;
        Ok(())
    }
}
