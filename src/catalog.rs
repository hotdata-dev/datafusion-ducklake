//! DuckLake catalog provider implementation

use std::any::Any;
use std::sync::Arc;

use crate::Result;
use crate::metadata_provider::MetadataProvider;
use crate::path_resolver::parse_object_store_url;
use crate::schema::DuckLakeSchema;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::object_store::ObjectStoreUrl;

/// DuckLake catalog provider
///
/// Connects to a DuckLake catalog database and provides access to schemas and tables.
/// Uses dynamic metadata lookup - schemas are queried on-demand from the catalog database.
/// Bound to a specific snapshot ID for query consistency.
#[derive(Debug)]
pub struct DuckLakeCatalog {
    /// Metadata provider for querying catalog
    provider: Arc<dyn MetadataProvider>,
    /// Snapshot ID this catalog is bound to (for query consistency)
    snapshot_id: i64,
    /// Object store URL for resolving file paths (e.g., s3://bucket/ or file:///)
    object_store_url: Arc<ObjectStoreUrl>,
    /// Catalog base path component for resolving relative schema paths (e.g., /prefix/)
    catalog_path: String,
}

impl DuckLakeCatalog {
    /// Create a new DuckLake catalog with a metadata provider
    ///
    /// Gets the current snapshot ID at creation time and binds the catalog to it.
    /// For backward compatibility. For explicit snapshot control, use `with_snapshot()`.
    pub fn new(provider: impl MetadataProvider + 'static) -> Result<Self> {
        let provider = Arc::new(provider) as Arc<dyn MetadataProvider>;
        let snapshot_id = provider.get_current_snapshot()?;
        let data_path = provider.get_data_path()?;
        let (object_store_url, catalog_path) = parse_object_store_url(&data_path)?;

        Ok(Self {
            provider,
            snapshot_id,
            object_store_url: Arc::new(object_store_url),
            catalog_path,
        })
    }

    /// Create a catalog bound to a specific snapshot ID
    ///
    /// All schemas and tables returned will use this snapshot, guaranteeing
    /// query consistency even if multiple catalog/schema/table lookups occur
    /// during query planning.
    
    pub fn with_snapshot(provider: Arc<dyn MetadataProvider>, snapshot_id: i64) -> Result<Self> {
        let data_path = provider.get_data_path()?;
        let (object_store_url, catalog_path) = parse_object_store_url(&data_path)?;

        Ok(Self {
            provider,
            snapshot_id,
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
        // Use the catalog's pinned snapshot_id
        self.provider
            .list_schemas(self.snapshot_id)
            .unwrap_or_default()
            .into_iter()
            .map(|s| s.schema_name)
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // Use the catalog's pinned snapshot_id
        match self.provider.get_schema_by_name(name, self.snapshot_id) {
            Ok(Some(meta)) => {
                // Resolve schema path hierarchically
                let schema_path = if meta.path_is_relative {
                    // Schema path is relative to catalog path
                    format!("{}{}", self.catalog_path, meta.path)
                } else {
                    // Schema path is absolute
                    meta.path
                };

                // Pass the pinned snapshot_id to schema
                Some(Arc::new(DuckLakeSchema::new(
                    meta.schema_id,
                    meta.schema_name,
                    Arc::clone(&self.provider),
                    self.snapshot_id, // Propagate pinned snapshot_id
                    self.object_store_url.clone(),
                    schema_path,
                )) as Arc<dyn SchemaProvider>)
            },
            _ => None,
        }
    }
}
