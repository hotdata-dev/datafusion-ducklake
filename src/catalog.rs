//! DuckLake catalog provider implementation

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};

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
    /// Latest snapshot ID
    snapshot_id: i64,
    /// Cached schema metadata (schema_name -> SchemaMetadata)
    schemas: HashMap<String, SchemaMetadata>,
}

impl DuckLakeCatalog {
    /// Create a new DuckLake catalog
    pub fn new(provider: impl MetadataProvider + 'static) -> Result<Self> {
        // Wrap provider in Arc for sharing
        let provider = Arc::new(provider) as Arc<dyn MetadataProvider>;

        // Get current snapshot
        let snapshot_id = provider.get_current_snapshot()?;

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
            Arc::new(DuckLakeSchema::new(
                meta.schema_id,
                meta.schema_name.clone(),
                Arc::clone(&self.provider),
                self.snapshot_id,
            )) as Arc<dyn SchemaProvider>
        })
    }
}