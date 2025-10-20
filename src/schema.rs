//! DuckLake schema provider implementation

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::Result as DataFusionResult;

use crate::metadata_provider::{MetadataProvider, TableMetadata};
use crate::path_resolver::resolve_path;
use crate::table::DuckLakeTable;

/// DuckLake schema provider
///
/// Represents a schema within a DuckLake catalog and provides access to tables.
#[derive(Debug)]
pub struct DuckLakeSchema {
    #[allow(dead_code)]
    schema_id: i64,
    #[allow(dead_code)]
    schema_name: String,
    /// Object store URL for resolving file paths (e.g., s3://bucket/ or file:///)
    object_store_url: Arc<ObjectStoreUrl>,
    provider: Arc<dyn MetadataProvider>,
    snapshot_id: i64,
    /// Schema path for resolving relative table paths
    schema_path: String,
    /// Cached table metadata (table_name -> TableMetadata)
    tables: HashMap<String, TableMetadata>,
}

impl DuckLakeSchema {
    /// Create a new DuckLake schema
    pub fn new(
        schema_id: i64,
        schema_name: impl Into<String>,
        provider: Arc<dyn MetadataProvider>,
        snapshot_id: i64,
        object_store_url: Arc<ObjectStoreUrl>,
        schema_path: String,
    ) -> Self {
        // Query and cache tables for this schema
        let tables = match provider.list_tables(schema_id) {
            Ok(tables) => tables
                .into_iter()
                .map(|meta| (meta.table_name.clone(), meta))
                .collect(),
            Err(e) => {
                tracing::error!(
                    schema_id = schema_id,
                    error = %e,
                    "failed to list tables for schema"
                );
                HashMap::new()
            }
        };

        Self {
            schema_id,
            schema_name: schema_name.into(),
            provider,
            snapshot_id,
            object_store_url,
            schema_path,
            tables,
        }
    }
}

#[async_trait]
impl SchemaProvider for DuckLakeSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        match self.tables.get(name) {
            Some(meta) => {
                // Resolve table path hierarchically using path_resolver utility
                let table_path = resolve_path(&self.schema_path, &meta.path, meta.path_is_relative);

                let table = DuckLakeTable::new(
                    meta.table_id,
                    meta.table_name.clone(),
                    Arc::clone(&self.provider),
                    self.snapshot_id,
                    self.object_store_url.clone(),
                    table_path,
                )
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                Ok(Some(Arc::new(table) as Arc<dyn TableProvider>))
            }
            None => Ok(None),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
