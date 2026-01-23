//! DuckLake catalog provider implementation

use std::any::Any;
use std::sync::Arc;

use crate::Result;
use crate::information_schema::InformationSchemaProvider;
use crate::metadata_provider::MetadataProvider;
use crate::path_resolver::{parse_object_store_url, resolve_path};
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

    /// Get the metadata provider for this catalog
    ///
    /// This is useful when you need to register table functions separately.
    pub fn provider(&self) -> Arc<dyn MetadataProvider> {
        self.provider.clone()
    }
}

impl CatalogProvider for DuckLakeCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        // Start with information_schema
        let mut names = vec!["information_schema".to_string()];

        // Add data schemas from catalog using the pinned snapshot_id
        let data_schemas = self
            .provider
            .list_schemas(self.snapshot_id)
            .inspect_err(|e| {
                tracing::error!(
                    error = %e,
                    snapshot_id = %self.snapshot_id,
                    "Failed to list schemas from catalog"
                )
            })
            .unwrap_or_default()
            .into_iter()
            .map(|s| s.schema_name);

        names.extend(data_schemas);

        // Ensure deterministic order and no duplicates
        names.sort();
        names.dedup();

        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // Handle information_schema specially
        if name == "information_schema" {
            return Some(Arc::new(InformationSchemaProvider::new(Arc::clone(
                &self.provider,
            ))));
        }

        // Query database with the pinned snapshot_id for data schemas
        match self.provider.get_schema_by_name(name, self.snapshot_id) {
            Ok(Some(meta)) => {
                // Resolve schema path hierarchically using path_resolver utility
                let schema_path =
                    resolve_path(&self.catalog_path, &meta.path, meta.path_is_relative);

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
