//! DuckLake table provider implementation

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::metadata_provider::MetadataProvider;
use crate::types::build_arrow_schema;
use crate::Result;

/// DuckLake table provider
///
/// Represents a table within a DuckLake schema and provides access to data via Parquet files.
#[derive(Debug)]
pub struct DuckLakeTable {
    table_id: i64,
    table_name: String,
    #[allow(dead_code)]
    provider: Arc<dyn MetadataProvider>,
    snapshot_id: i64,
    schema: SchemaRef,
    data_files: Vec<String>,
}

impl DuckLakeTable {
    /// Create a new DuckLake table
    pub fn new(
        table_id: i64,
        table_name: impl Into<String>,
        provider: Arc<dyn MetadataProvider>,
        snapshot_id: i64,
    ) -> Result<Self> {
        // Get table structure (columns)
        let columns = provider.get_table_structure(table_id)?;

        // Build Arrow schema from column definitions
        let schema = Arc::new(build_arrow_schema(&columns)?);

        // Get data files
        let table_files = provider.get_table_files_for_select(table_id)?;
        let data_files = table_files
            .into_iter()
            .map(|tf| tf.file.path)
            .collect();

        Ok(Self {
            table_id,
            table_name: table_name.into(),
            provider,
            snapshot_id,
            schema,
            data_files,
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