//! Data store provider abstraction for object storage backends
//!
//! This module provides an abstraction for different storage backends (local filesystem, S3, etc.)
//! that parallels the MetadataProvider pattern. It handles object store registration and path resolution.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use url::Url;

use crate::Result;

/// Trait for providing data store access to DuckLake tables
///
/// Implementations handle registration of object stores (S3, local filesystem, etc.)
/// with DataFusion's runtime environment and path resolution.
pub trait DataStoreProvider: Send + Sync + Debug {
    /// Ensure the appropriate object store is registered for the given path
    ///
    /// This method should:
    /// 1. Detect the storage scheme from the path (file://, s3://, etc.)
    /// 2. Create and configure the appropriate ObjectStore implementation
    /// 3. Register it with the RuntimeEnv
    /// 4. Return the ObjectStoreUrl to use for DataFusion operations
    ///
    /// # Arguments
    /// * `path` - The data path (could be from data_path, schema path, table path, etc.)
    /// * `runtime_env` - DataFusion's runtime environment to register the store with
    fn ensure_object_store(
        &self,
        path: &str,
        runtime_env: &Arc<RuntimeEnv>,
    ) -> Result<ObjectStoreUrl>;

    /// Extract the file path component from a full path for object store operations
    ///
    /// Converts a full path to the format expected by the object store.
    /// For example:
    /// - `s3://bucket/path/file.parquet` -> `path/file.parquet`
    /// - `/absolute/path/file.parquet` -> `absolute/path/file.parquet` (strip leading /)
    ///
    /// # Arguments
    /// * `full_path` - The complete path to the file
    /// * `scheme` - The storage scheme ("file", "s3", etc.)
    fn extract_file_path(&self, full_path: &str, scheme: &str) -> Result<String>;
}

/// Auto-detecting data store provider that supports local filesystem and S3
///
/// This provider automatically detects the storage backend from path schemes and uses
/// environment variables for configuration (AWS credentials, region, etc.).
#[derive(Debug, Clone)]
pub struct AutoDetectProvider;

impl AutoDetectProvider {
    /// Create a new auto-detecting provider
    pub fn new() -> Self {
        Self
    }
}

impl Default for AutoDetectProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl DataStoreProvider for AutoDetectProvider {
    fn ensure_object_store(
        &self,
        path: &str,
        runtime_env: &Arc<RuntimeEnv>,
    ) -> Result<ObjectStoreUrl> {
        if path.starts_with("s3://") {
            // S3 object store
            let url = Url::parse(path).map_err(|e| {
                crate::DuckLakeError::Internal(format!("Failed to parse S3 URL '{}': {}", path, e))
            })?;

            let bucket = url.host_str().ok_or_else(|| {
                crate::DuckLakeError::Internal(format!("S3 URL missing bucket: {}", path))
            })?;

            // Build S3 client from environment
            // This automatically uses:
            // - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
            // - AWS_REGION or AWS_DEFAULT_REGION
            // - IAM role credentials if on EC2/ECS
            // - AWS credential file (~/.aws/credentials)
            let s3_store = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(|e| {
                    crate::DuckLakeError::Internal(format!(
                        "Failed to build S3 client for bucket '{}': {}",
                        bucket, e
                    ))
                })?;

            let object_store: Arc<dyn ObjectStore> = Arc::new(s3_store);

            // Register with clean s3://bucket/ URL
            let register_url = Url::parse(&format!("s3://{}/", bucket)).map_err(|e| {
                crate::DuckLakeError::Internal(format!(
                    "Failed to parse S3 registration URL: {}",
                    e
                ))
            })?;
            runtime_env.register_object_store(&register_url, object_store);

            // Create ObjectStoreUrl
            ObjectStoreUrl::parse(format!("s3://{}/", bucket)).map_err(|e| {
                crate::DuckLakeError::Internal(format!("Failed to parse ObjectStoreUrl: {}", e))
            })
        } else if path.starts_with("file://") || path.starts_with('/') || !path.contains("://") {
            // Local filesystem
            let url = Url::parse("file:///").map_err(|e| {
                crate::DuckLakeError::Internal(format!("Failed to parse file URL: {}", e))
            })?;

            let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
            runtime_env.register_object_store(&url, object_store);

            ObjectStoreUrl::parse("file:///").map_err(|e| {
                crate::DuckLakeError::Internal(format!("Failed to parse ObjectStoreUrl: {}", e))
            })
        } else {
            Err(crate::DuckLakeError::Unsupported(format!(
                "Unsupported storage scheme in path: {}. Supported: file://, s3://",
                path
            )))
        }
    }

    fn extract_file_path(&self, full_path: &str, scheme: &str) -> Result<String> {
        match scheme {
            "file" => {
                // For local files, keep the absolute path as-is
                // Strip file:// prefix if present, otherwise return as-is
                if full_path.starts_with("file://") {
                    Ok(full_path.strip_prefix("file://").unwrap().to_string())
                } else {
                    Ok(full_path.to_string())
                }
            }
            "s3" => {
                // For S3, parse URL and extract path component
                let url = Url::parse(full_path).map_err(|e| {
                    crate::DuckLakeError::Internal(format!(
                        "Failed to parse S3 path '{}': {}",
                        full_path, e
                    ))
                })?;

                // Get path without leading '/'
                let path = url.path().strip_prefix('/').unwrap_or(url.path());
                Ok(path.to_string())
            }
            _ => Err(crate::DuckLakeError::Unsupported(format!(
                "Unsupported scheme: {}",
                scheme
            ))),
        }
    }
}

/// Local filesystem data store provider
///
/// This provider only supports local file paths and will error on other schemes.
#[derive(Debug, Clone)]
pub struct LocalFileSystemProvider;

impl LocalFileSystemProvider {
    /// Create a new local filesystem provider
    pub fn new() -> Self {
        Self
    }
}

impl Default for LocalFileSystemProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl DataStoreProvider for LocalFileSystemProvider {
    fn ensure_object_store(
        &self,
        path: &str,
        runtime_env: &Arc<RuntimeEnv>,
    ) -> Result<ObjectStoreUrl> {
        if !path.starts_with("file://") && !path.starts_with('/') && path.contains("://") {
            return Err(crate::DuckLakeError::InvalidConfig(format!(
                "LocalFileSystemProvider only supports local paths, got: {}",
                path
            )));
        }

        let url = Url::parse("file:///").map_err(|e| {
            crate::DuckLakeError::Internal(format!("Failed to parse file URL: {}", e))
        })?;

        let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        runtime_env.register_object_store(&url, object_store);

        ObjectStoreUrl::parse("file:///").map_err(|e| {
            crate::DuckLakeError::Internal(format!("Failed to parse ObjectStoreUrl: {}", e))
        })
    }

    fn extract_file_path(&self, full_path: &str, _scheme: &str) -> Result<String> {
        // For local files, keep the absolute path as-is
        // Strip file:// prefix if present, otherwise return as-is
        if full_path.starts_with("file://") {
            Ok(full_path.strip_prefix("file://").unwrap().to_string())
        } else {
            Ok(full_path.to_string())
        }
    }
}

/// S3 data store provider with explicit configuration
///
/// This provider supports S3 paths and allows explicit configuration of credentials,
/// region, and endpoint. Falls back to environment variables if not explicitly set.
#[derive(Debug, Clone)]
pub struct S3Provider {
    /// AWS region (e.g., "us-east-1")
    region: Option<String>,
    /// Custom S3 endpoint (for S3-compatible services)
    endpoint: Option<String>,
    /// AWS access key ID
    access_key_id: Option<String>,
    /// AWS secret access key
    secret_access_key: Option<String>,
    /// Allow HTTP (non-TLS) connections (for local MinIO, etc.)
    allow_http: bool,
}

impl S3Provider {
    /// Create a new S3 provider that uses environment variables
    pub fn new() -> Self {
        Self {
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            allow_http: false,
        }
    }

    /// Set the AWS region
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set a custom S3 endpoint (for S3-compatible services like MinIO)
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Set AWS credentials explicitly
    pub fn with_credentials(
        mut self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        self.access_key_id = Some(access_key_id.into());
        self.secret_access_key = Some(secret_access_key.into());
        self
    }

    /// Allow HTTP (non-TLS) connections
    ///
    /// This is useful for local development with MinIO or other S3-compatible
    /// services running without TLS.
    pub fn with_allow_http(mut self, allow_http: bool) -> Self {
        self.allow_http = allow_http;
        self
    }
}

impl Default for S3Provider {
    fn default() -> Self {
        Self::new()
    }
}

impl DataStoreProvider for S3Provider {
    fn ensure_object_store(
        &self,
        path: &str,
        runtime_env: &Arc<RuntimeEnv>,
    ) -> Result<ObjectStoreUrl> {
        if !path.starts_with("s3://") {
            return Err(crate::DuckLakeError::InvalidConfig(format!(
                "S3Provider only supports s3:// paths, got: {}",
                path
            )));
        }

        let url = Url::parse(path).map_err(|e| {
            crate::DuckLakeError::Internal(format!("Failed to parse S3 URL '{}': {}", path, e))
        })?;

        let bucket = url.host_str().ok_or_else(|| {
            crate::DuckLakeError::Internal(format!("S3 URL missing bucket: {}", path))
        })?;

        // Build S3 client with explicit config or from environment
        // If we have explicit credentials, start with a fresh builder
        // Otherwise, use from_env() to pick up AWS credentials from environment
        let has_explicit_creds = self.access_key_id.is_some() && self.secret_access_key.is_some();
        let mut builder = if has_explicit_creds {
            println!("DEBUG: Using explicit credentials (not from environment)");
            AmazonS3Builder::new().with_bucket_name(bucket)
        } else {
            println!("DEBUG: Using credentials from environment");
            AmazonS3Builder::from_env().with_bucket_name(bucket)
        };

        if let Some(ref region) = self.region {
            println!("DEBUG: Setting S3 region: {}", region);
            builder = builder.with_region(region);
        }

        if let Some(ref endpoint) = self.endpoint {
            println!("DEBUG: Setting S3 endpoint: {}", endpoint);
            builder = builder.with_endpoint(endpoint);
        }

        if let (Some(key_id), Some(secret)) =
            (&self.access_key_id, &self.secret_access_key)
        {
            println!("DEBUG: Setting explicit credentials (access_key_id: {}...)", &key_id[..key_id.len().min(4)]);
            builder = builder.with_access_key_id(key_id).with_secret_access_key(secret);
        }

        if self.allow_http {
            println!("DEBUG: Allowing HTTP connections");
            builder = builder.with_allow_http(true);
        }

        // For MinIO and other S3-compatible services, use path-style requests
        // instead of virtual-hosted-style
        if self.endpoint.is_some() {
            println!("DEBUG: Using path-style requests (for MinIO/S3-compatible)");
            builder = builder.with_virtual_hosted_style_request(false);
        }

        println!("DEBUG: Building S3 client for bucket: {}", bucket);
        let s3_store = builder.build().map_err(|e| {
            crate::DuckLakeError::Internal(format!(
                "Failed to build S3 client for bucket '{}': {}",
                bucket, e
            ))
        })?;
        println!("DEBUG: S3 client built successfully");

        let object_store: Arc<dyn ObjectStore> = Arc::new(s3_store);

        // Register with clean s3://bucket/ URL
        let register_url = Url::parse(&format!("s3://{}/", bucket)).map_err(|e| {
            crate::DuckLakeError::Internal(format!("Failed to parse S3 registration URL: {}", e))
        })?;
        println!("DEBUG: Registering object store with URL: {}", register_url);
        runtime_env.register_object_store(&register_url, object_store.clone());

        // Test if we can access a file through the registered store
        println!("DEBUG: Testing if object store is accessible after registration");

        // Create ObjectStoreUrl
        let object_store_url = ObjectStoreUrl::parse(format!("s3://{}/", bucket)).map_err(|e| {
            crate::DuckLakeError::Internal(format!("Failed to parse ObjectStoreUrl: {}", e))
        })?;
        println!("DEBUG: Created ObjectStoreUrl: {:?}", object_store_url);

        Ok(object_store_url)
    }

    fn extract_file_path(&self, full_path: &str, _scheme: &str) -> Result<String> {
        // For S3, parse URL and extract path component
        let url = Url::parse(full_path).map_err(|e| {
            crate::DuckLakeError::Internal(format!("Failed to parse S3 path '{}': {}", full_path, e))
        })?;

        // Get path without leading '/'
        let path = url.path().strip_prefix('/').unwrap_or(url.path());
        Ok(path.to_string())
    }
}