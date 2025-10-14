//! DuckLake catalog provider implementation

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};

use crate::data_store_provider::{AutoDetectProvider, DataStoreProvider};
use crate::metadata_provider::{MetadataProvider, SchemaMetadata};
use crate::schema::DuckLakeSchema;
use crate::Result;

/// DuckLake catalog provider
///
/// Connects to a DuckLake catalog database and provides access to schemas and tables.
#[derive(Debug)]
pub struct DuckLakeCatalog {
    /// Metadata provider for querying catalog
    provider: Arc<dyn MetadataProvider>,
    /// Data store provider for object storage access
    data_store_provider: Arc<dyn DataStoreProvider>,
    /// Latest snapshot ID
    snapshot_id: i64,
    /// Base data path for resolving relative file paths
    data_path: String,
    /// Cached schema metadata (schema_name -> SchemaMetadata)
    schemas: HashMap<String, SchemaMetadata>,
}

impl DuckLakeCatalog {
    /// Create a new DuckLake catalog with a metadata provider and data store provider
    pub fn new_with_store(
        provider: impl MetadataProvider + 'static,
        data_store_provider: impl DataStoreProvider + 'static,
    ) -> Result<Self> {
        // Wrap providers in Arc for sharing
        let provider = Arc::new(provider) as Arc<dyn MetadataProvider>;
        let data_store_provider = Arc::new(data_store_provider) as Arc<dyn DataStoreProvider>;

        // Get current snapshot
        let snapshot_id = provider.get_current_snapshot()?;

        // Get data path for file resolution
        let data_path = provider.get_data_path()?;

        // List and cache schemas
        let schema_list = provider.list_schemas()?;
        println!("schemas: {:?}", &schema_list);
        let schemas = schema_list
            .into_iter()
            .map(|meta| (meta.schema_name.clone(), meta))
            .collect();


        Ok(Self {
            provider,
            data_store_provider,
            snapshot_id,
            data_path,
            schemas,
        })
    }

    /// Create a new DuckLake catalog with auto-detecting data store provider
    ///
    /// This is a convenience method that uses `AutoDetectProvider` for storage.
    /// It will automatically detect local filesystem or S3 based on path schemes.
    pub fn new(provider: impl MetadataProvider + 'static) -> Result<Self> {
        Self::new_with_store(provider, AutoDetectProvider::new())
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
                // Schema path is relative to global data_path
                format!("{}{}", self.data_path, meta.path)
            } else {
                // Schema path is absolute
                meta.path.clone()
            };

            Arc::new(DuckLakeSchema::new(
                meta.schema_id,
                meta.schema_name.clone(),
                Arc::clone(&self.provider),
                Arc::clone(&self.data_store_provider),
                self.snapshot_id,
                schema_path,
            )) as Arc<dyn SchemaProvider>
        })
    }
}