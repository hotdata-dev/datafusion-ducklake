//! DuckLake catalog provider implementation

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::metadata_provider::{MetadataProvider, SchemaMetadata};
use crate::path_resolver::parse_object_store_url;
use crate::schema::DuckLakeSchema;
use crate::Result;
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
    /// Object store URL for resolving file paths (e.g., s3://bucket/ or file:///)
    object_store_url: Arc<ObjectStoreUrl>,
    /// Catalog base path component for resolving relative schema paths (e.g., /prefix/)
    catalog_path: String,
    /// Cached schema metadata (schema_name -> SchemaMetadata)
    schemas: HashMap<String, SchemaMetadata>,
}

impl DuckLakeCatalog {
    /// Create a new DuckLake catalog with a metadata provider
    pub fn new(provider: impl MetadataProvider + 'static) -> Result<Self> {
        let provider = Arc::new(provider) as Arc<dyn MetadataProvider>;
        let snapshot_id = provider.get_current_snapshot()?;
        let data_path = provider.get_data_path()?;
        let (object_store_url, catalog_path) = parse_object_store_url(&data_path)?;

        // List and cache schemas
        let schema_list = provider.list_schemas()?;
        tracing::debug!(schemas = ?schema_list, "loaded schemas from catalog");
        let schemas = schema_list
            .into_iter()
            .map(|meta| (meta.schema_name.clone(), meta))
            .collect();

        Ok(Self {
            provider,
            snapshot_id,
            object_store_url: Arc::new(object_store_url),
            catalog_path,
            schemas,
        })
    }

    /// Get the latest snapshot ID
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
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
                // Schema path is relative to catalog path
                format!("{}{}", self.catalog_path, meta.path)
            } else {
                // Schema path is absolute
                meta.path.clone()
            };

            Arc::new(DuckLakeSchema::new(
                meta.schema_id,
                meta.schema_name.clone(),
                Arc::clone(&self.provider),
                self.snapshot_id,
                self.object_store_url.clone(),
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
        let (url, path) = parse_object_store_url("s3://bucket/prefix").unwrap();
        assert_eq!("/prefix", path);
        assert_eq!(ObjectStoreUrl::parse("s3://bucket/").unwrap(), url);
    }
}
