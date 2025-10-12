//! DuckLake table provider implementation

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::Result;

/// DuckLake table provider
///
/// Represents a table within a DuckLake schema and provides access to data via Parquet files.
#[derive(Debug)]
pub struct DuckLakeTable {
    table_id: i64,
    table_name: String,
    catalog_path: String,
    snapshot_id: i64,
    schema: SchemaRef,
    data_files: Vec<String>,
}

impl DuckLakeTable {
    /// Create a new DuckLake table
    pub fn new(
        table_id: i64,
        table_name: impl Into<String>,
        catalog_path: impl Into<String>,
        snapshot_id: i64,
    ) -> Result<Self> {
        // TODO: Query ducklake_column to get column definitions
        // TODO: Build Arrow schema from column definitions
        // TODO: Query ducklake_data_file to get Parquet file paths

        Ok(Self {
            table_id,
            table_name: table_name.into(),
            catalog_path: catalog_path.into(),
            snapshot_id,
            schema: Arc::new(arrow::datatypes::Schema::empty()), // TODO: Build actual schema
            data_files: vec![], // TODO: Get actual data files
        })
    }
}

#[async_trait]
impl TableProvider for DuckLakeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // TODO: Create ParquetExec plan for data_files
        // TODO: Apply projection, filters, and limit

        Err(datafusion::error::DataFusionError::NotImplemented(
            "DuckLakeTable::scan not yet implemented".to_string(),
        ))
    }
}