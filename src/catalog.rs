//! DuckLake catalog provider implementation

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::metadata_provider::{MetadataProvider, SchemaMetadata};
use crate::schema::DuckLakeSchema;
use crate::{DuckLakeError, Result};
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::object_store::ObjectStoreUrl;

/// DuckLake catalog provider
///
/// Connects to a DuckLake catalog database and provides access to schemas and tables.
#[derive(Debug)]
pub struct DuckLakeCatalog {
    /// Metadata provider for querying catalog
    provider: Arc<dyn MetadataProvider>,
    /// Latest snapshot ID
    snapshot_id: i64,
    #[allow(dead_code)]
    /// Base data path for resolving relative file paths
    /// example: s3://ducklake-data/prefix/
    base_data_path: String,
    /// Base key path for data. this is used to compute the key for the table files
    /// example: prefix/
    base_key_path: String,
    /// the parsed ObjectStoreUrl of base_data_path
    base_data_url: Arc<ObjectStoreUrl>,
    /// Cached schema metadata (schema_name -> SchemaMetadata)
    schemas: HashMap<String, SchemaMetadata>,
}

impl DuckLakeCatalog {
    /// Create a new DuckLake catalog with a metadata provider
    pub fn new(provider: impl MetadataProvider + 'static) -> Result<Self> {
        let provider = Arc::new(provider) as Arc<dyn MetadataProvider>;
        let snapshot_id = provider.get_current_snapshot()?;
        let base_data_path = provider.get_data_path()?;
        let (base_data_url, base_key_path) =
            DuckLakeCatalog::parse_object_store_url(&base_data_path)?;

        // List and cache schemas
        let schema_list = provider.list_schemas()?;
        println!("schemas: {:?}", &schema_list);
        let schemas = schema_list
            .into_iter()
            .map(|meta| (meta.schema_name.clone(), meta))
            .collect();

        Ok(Self {
            provider,
            snapshot_id,
            base_data_path,
            base_key_path,
            base_data_url: Arc::new(base_data_url),
            schemas,
        })
    }

    /// Get the latest snapshot ID
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Resolve data paths to ObjectStoreUrl and Data key prefix
    fn parse_object_store_url(data_path: &str) -> Result<(ObjectStoreUrl, String)> {
        // Determine scheme and extract object store URL
        let result = if data_path.starts_with("s3://") {
            // Extract bucket from s3://bucket/path
            let url = url::Url::parse(data_path).map_err(|e| {
                DuckLakeError::InvalidConfig(format!(
                    "Failed to parse S3 URL '{}': {}",
                    data_path, e
                ))
            })?;

            let bucket = url.host_str().ok_or_else(|| {
                DuckLakeError::InvalidConfig(format!("S3 URL missing bucket: {}", data_path))
            })?;

            let object_store_url =
                ObjectStoreUrl::parse(format!("s3://{}/", bucket)).map_err(|e| {
                    DuckLakeError::InvalidConfig(format!("Failed to create ObjectStoreUrl: {}", e))
                });

            (object_store_url?, url.path().to_owned())
        // todo: add support for filepath
        } else {
            return Err(DuckLakeError::InvalidConfig(format!(
                "Unsupported storage scheme in path: {}",
                data_path
            )));
        };

        Ok(result)
    }
}

impl CatalogProvider for DuckLakeCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).map(|meta| {
            // Resolve schema path hierarchically
            let schema_path = if meta.path_is_relative {
                // Schema path is relative to global data_path
                format!("{}{}", self.base_key_path, meta.path)
            } else {
                // Schema path is absolute
                meta.path.clone()
            };

            Arc::new(DuckLakeSchema::new(
                meta.schema_id,
                meta.schema_name.clone(),
                Arc::clone(&self.provider),
                self.snapshot_id,
                self.base_data_url.clone(),
                schema_path,
            )) as Arc<dyn SchemaProvider>
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_object_store_url() {
        let (url, path) = DuckLakeCatalog::parse_object_store_url("s3://bucket/prefix").unwrap();
        assert_eq!("/prefix", path);
        assert_eq!(ObjectStoreUrl::parse("s3://bucket/").unwrap(), url);
    }
}
