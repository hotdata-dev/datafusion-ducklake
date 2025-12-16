//! DuckLake catalog provider implementation

use std::any::Any;
use std::sync::Arc;

use crate::Result;
use crate::information_schema::InformationSchemaProvider;
use crate::metadata_provider::MetadataProvider;
use crate::path_resolver::parse_object_store_url;
use crate::schema::DuckLakeSchema;
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

    /// Get the metadata provider for this catalog
    ///
    /// This is useful when you need to register table functions separately.
    pub fn provider(&self) -> Arc<dyn MetadataProvider> {
        self.provider.clone()
    }

    fn get_current_snapshot_id(&self) -> Result<i64> {
        self.provider
            .get_current_snapshot()
            .inspect_err(|e| tracing::error!(error = %e, "Failed to get current snapshot"))
    }
}

impl CatalogProvider for DuckLakeCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let snapshot_id = match self.get_current_snapshot_id() {
            Ok(id) => id,
            Err(_) => return vec!["information_schema".to_string()],
        };

        // Start with information_schema
        let mut names = vec!["information_schema".to_string()];

        // Add data schemas from catalog
        let data_schemas = self
            .provider
            .list_schemas(snapshot_id)
            .inspect_err(|e| {
                tracing::error!(
                    error = %e,
                    snapshot_id = %snapshot_id,
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

        let snapshot_id = match self.get_current_snapshot_id() {
            Ok(id) => id,
            Err(_) => return None,
        };

        // Query database with snapshot_id for data schemas
        match self.provider.get_schema_by_name(name, snapshot_id) {
            Ok(Some(meta)) => {
                // Resolve schema path hierarchically
                let schema_path = if meta.path_is_relative {
                    // Schema path is relative to catalog path
                    format!("{}{}", self.catalog_path, meta.path)
                } else {
                    // Schema path is absolute
                    meta.path
                };

                // Pass snapshot_id to schema
                Some(Arc::new(DuckLakeSchema::new(
                    meta.schema_id,
                    meta.schema_name,
                    Arc::clone(&self.provider),
                    snapshot_id, // Propagate snapshot_id
                    self.object_store_url.clone(),
                    schema_path,
                )) as Arc<dyn SchemaProvider>)
            },
            _ => None,
        }
    }
}
