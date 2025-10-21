//! DuckLake catalog provider implementation

use std::any::Any;
use std::sync::Arc;

use crate::metadata_provider::MetadataProvider;
use crate::path_resolver::parse_object_store_url;
use crate::schema::DuckLakeSchema;
use crate::Result;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::object_store::ObjectStoreUrl;

/// DuckLake catalog provider
///
/// Connects to a DuckLake catalog database and provides access to schemas and tables.
/// Uses dynamic metadata lookup - schemas are queried on-demand from the catalog database.
#[derive(Debug)]
pub struct DuckLakeCatalog {
    /// Metadata provider for querying catalog
    provider: Arc<dyn MetadataProvider>,
    /// Object store URL for resolving file paths (e.g., s3://bucket/ or file:///)
    object_store_url: Arc<ObjectStoreUrl>,
    /// Catalog base path component for resolving relative schema paths (e.g., /prefix/)
    catalog_path: String,
}

impl DuckLakeCatalog {
    /// Create a new DuckLake catalog with a metadata provider
    pub fn new(provider: impl MetadataProvider + 'static) -> Result<Self> {
        let provider = Arc::new(provider) as Arc<dyn MetadataProvider>;
        let data_path = provider.get_data_path()?;
        let (object_store_url, catalog_path) = parse_object_store_url(&data_path)?;

        Ok(Self {
            provider,
            object_store_url: Arc::new(object_store_url),
            catalog_path,
        })
    }
}

impl CatalogProvider for DuckLakeCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        // Query database on every call
        self.provider
            .list_schemas()
            .unwrap_or_default()
            .iter()
            .map(|s| s.schema_name.clone())
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // Query database on every call
        match self.provider.get_schema_by_name(name) {
            Ok(Some(meta)) => {
                // Resolve schema path hierarchically
                let schema_path = if meta.path_is_relative {
                    // Schema path is relative to catalog path
                    format!("{}{}", self.catalog_path, meta.path)
                } else {
                    // Schema path is absolute
                    meta.path.clone()
                };

                Some(Arc::new(DuckLakeSchema::new(
                    meta.schema_id,
                    meta.schema_name.clone(),
                    Arc::clone(&self.provider),
                    self.object_store_url.clone(),
                    schema_path,
                )) as Arc<dyn SchemaProvider>)
            }
            _ => None,
        }
    }
}
