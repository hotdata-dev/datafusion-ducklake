//! DuckLake schema provider implementation

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::error::Result as DataFusionResult;

use crate::metadata_provider::{MetadataProvider, TableMetadata};
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
    provider: Arc<dyn MetadataProvider>,
    snapshot_id: i64,
    /// Base data path for resolving relative file paths
    data_path: String,
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
        data_path: String,
    ) -> Self {
        // Query and cache tables for this schema
        let tables = provider
            .list_tables(schema_id)
            .unwrap_or_default()
            .into_iter()
            .map(|meta| (meta.table_name.clone(), meta))
            .collect();

        Self {
            schema_id,
            schema_name: schema_name.into(),
            provider,
            snapshot_id,
            data_path,
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
                // Resolve table path hierarchically
                let table_path = if meta.path_is_relative {
                    // Table path is relative to schema path
                    format!("{}{}", self.data_path, meta.path)
                } else {
                    // Table path is absolute
                    meta.path.clone()
                };

                let table = DuckLakeTable::new(
                    meta.table_id,
                    meta.table_name.clone(),
                    Arc::clone(&self.provider),
                    self.snapshot_id,
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