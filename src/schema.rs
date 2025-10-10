//! DuckLake schema provider implementation

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::error::Result as DataFusionResult;

/// DuckLake schema provider
///
/// Represents a schema within a DuckLake catalog and provides access to tables.
#[derive(Debug)]
pub struct DuckLakeSchema {
    schema_id: i64,
    schema_name: String,
    catalog_path: String,
    snapshot_id: i64,
}

impl DuckLakeSchema {
    /// Create a new DuckLake schema
    pub fn new(
        schema_id: i64,
        schema_name: impl Into<String>,
        catalog_path: impl Into<String>,
        snapshot_id: i64,
    ) -> Self {
        Self {
            schema_id,
            schema_name: schema_name.into(),
            catalog_path: catalog_path.into(),
            snapshot_id,
        }
    }
}

#[async_trait]
impl SchemaProvider for DuckLakeSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // TODO: Query ducklake_table to get table names for this schema
        vec![]
    }

    async fn table(&self, _name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        // TODO: Query ducklake_table to get table_id for this name
        // TODO: Create and return DuckLakeTable
        Ok(None)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names().contains(&name.to_string())
    }
}