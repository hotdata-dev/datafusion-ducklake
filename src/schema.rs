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
#[derive(Debug)]
pub struct DuckLakeSchema {
    schema_id: i64,
    #[allow(dead_code)]
    schema_name: String,
    /// Object store URL for resolving file paths (e.g., s3://bucket/ or file:///)
    object_store_url: Arc<ObjectStoreUrl>,
    provider: Arc<dyn MetadataProvider>,
    /// Schema path for resolving relative table paths
    schema_path: String,
}

impl DuckLakeSchema {
    /// Create a new DuckLake schema
    pub fn new(
        schema_id: i64,
        schema_name: impl Into<String>,
        provider: Arc<dyn MetadataProvider>,
        object_store_url: Arc<ObjectStoreUrl>,
        schema_path: String,
    ) -> Self {
        Self {
            schema_id,
            schema_name: schema_name.into(),
            provider,
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
        // Query database on every call
        self.provider
            .list_tables(self.schema_id)
            .unwrap_or_default()
            .iter()
            .map(|t| t.table_name.clone())
            .collect()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        // Query database on every call
        match self.provider.get_table_by_name(self.schema_id, name) {
            Ok(Some(meta)) => {
                // Resolve table path hierarchically using path_resolver utility
                let table_path = resolve_path(&self.schema_path, &meta.path, meta.path_is_relative);

                let table = DuckLakeTable::new(
                    meta.table_id,
                    meta.table_name.clone(),
                    Arc::clone(&self.provider),
                    self.object_store_url.clone(),
                    table_path,
                )
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                Ok(Some(Arc::new(table) as Arc<dyn TableProvider>))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        // Query database on every call
        self.provider
            .table_exists(self.schema_id, name)
            .unwrap_or(false)
    }
}
