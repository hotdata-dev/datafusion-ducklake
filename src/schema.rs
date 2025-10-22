//! DuckLake schema provider implementation

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::Result as DataFusionResult;

use crate::metadata_provider::MetadataProvider;
use crate::path_resolver::resolve_path;
use crate::table::DuckLakeTable;

/// DuckLake schema provider
///
/// Represents a schema within a DuckLake catalog and provides access to tables.
/// Uses dynamic metadata lookup - tables are queried on-demand from the catalog database.
/// Caches snapshot_id received from catalog.schema() call for query consistency.
#[derive(Debug)]
pub struct DuckLakeSchema {
    schema_id: i64,
    #[allow(dead_code)]
    schema_name: String,
    /// Object store URL for resolving file paths (e.g., s3://bucket/ or file:///)
    object_store_url: Arc<ObjectStoreUrl>,
    provider: Arc<dyn MetadataProvider>,
    /// Cached snapshot_id from catalog.schema() call
    snapshot_id: i64,
    /// Schema path for resolving relative table paths
    schema_path: String,
}

impl DuckLakeSchema {
    /// Create a new DuckLake schema
    pub fn new(
        schema_id: i64,
        schema_name: impl Into<String>,
        provider: Arc<dyn MetadataProvider>,
        snapshot_id: i64, // Received from catalog
        object_store_url: Arc<ObjectStoreUrl>,
        schema_path: String,
    ) -> Self {
        Self {
            schema_id,
            schema_name: schema_name.into(),
            provider,
            snapshot_id,
            object_store_url,
            schema_path,
        }
    }
}

#[async_trait]
impl SchemaProvider for DuckLakeSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // Use cached snapshot_id
        self.provider
            .list_tables(self.schema_id, self.snapshot_id)
            .unwrap_or_default()
            .into_iter()
            .map(|t| t.table_name)
            .collect()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        // Use cached snapshot_id
        match self
            .provider
            .get_table_by_name(self.schema_id, name, self.snapshot_id)
        {
            Ok(Some(meta)) => {
                // Resolve table path hierarchically using path_resolver utility
                let table_path = resolve_path(&self.schema_path, &meta.path, meta.path_is_relative);

                // Pass snapshot_id to table
                let table = DuckLakeTable::new(
                    meta.table_id,
                    meta.table_name,
                    self.provider.clone(),
                    self.snapshot_id, // Propagate snapshot_id
                    self.object_store_url.clone(),
                    table_path,
                )
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                Ok(Some(Arc::new(table) as Arc<dyn TableProvider>))
            },
            Ok(None) => Ok(None),
            Err(e) => Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        // Use cached snapshot_id
        self.provider
            .table_exists(self.schema_id, name, self.snapshot_id)
            .unwrap_or(false)
    }
}
