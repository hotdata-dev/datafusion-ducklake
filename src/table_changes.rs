//! Table changes (CDC) functionality for DuckLake
//!
//! This module provides the `ducklake_table_changes()` table function that returns
//! actual row data from Parquet files with additional CDC metadata columns.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;

use crate::metadata_provider::MetadataProvider;
use crate::path_resolver::resolve_path;

#[derive(Debug)]
pub struct TableChangesTable {
    provider: Arc<dyn MetadataProvider>,
    table_id: i64,
    start_snapshot: i64,
    end_snapshot: i64,
    /// Object store URL for resolving file paths
    object_store_url: Arc<ObjectStoreUrl>,
    /// Table path for resolving relative file paths
    table_path: String,
    /// Original table schema (without CDC columns)
    table_schema: SchemaRef,
    /// Combined schema: table columns + snapshot_id + change_type
    output_schema: SchemaRef,
}

impl TableChangesTable {
    pub fn new(
        provider: Arc<dyn MetadataProvider>,
        table_id: i64,
        start_snapshot: i64,
        end_snapshot: i64,
        object_store_url: Arc<ObjectStoreUrl>,
        table_path: String,
        table_schema: SchemaRef,
    ) -> Self {
        // Build output schema: table columns + CDC metadata columns
        let mut fields: Vec<Field> = table_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new("snapshot_id", DataType::Int64, false));
        fields.push(Field::new("change_type", DataType::Utf8, false));
        let output_schema = Arc::new(Schema::new(fields));

        Self {
            provider,
            table_id,
            start_snapshot,
            end_snapshot,
            object_store_url,
            table_path,
            table_schema,
            output_schema,
        }
    }

    /// Append snapshot_id and change_type columns to a record batch
    fn append_cdc_columns(
        &self,
        batch: RecordBatch,
        snapshot_id: i64,
        change_type: &str,
    ) -> DataFusionResult<RecordBatch> {
        let num_rows = batch.num_rows();

        // Create CDC column arrays
        let snapshot_ids: ArrayRef = Arc::new(Int64Array::from(vec![snapshot_id; num_rows]));
        let change_types: ArrayRef = Arc::new(StringArray::from(vec![change_type; num_rows]));

        // Combine original columns with CDC columns
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(snapshot_ids);
        columns.push(change_types);

        RecordBatch::try_new(self.output_schema.clone(), columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }
}

#[async_trait]
impl TableProvider for TableChangesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Get data files added between snapshots (INSERT changes)
        let data_files = self
            .provider
            .get_data_files_added_between_snapshots(
                self.table_id,
                self.start_snapshot,
                self.end_snapshot,
            )
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // For now, we only handle INSERTs (Phase 2 INSERT-only)
        // DELETE changes will be added in a future phase

        // Collect all batches from all files
        let mut all_batches: Vec<RecordBatch> = Vec::new();

        for data_file in &data_files {
            // Resolve file path
            let resolved_path = resolve_path(
                &self.table_path,
                &data_file.path,
                data_file.path_is_relative,
            );

            // Create PartitionedFile with footer size hint if available
            let mut pf = PartitionedFile::new(&resolved_path, data_file.file_size_bytes as u64);
            if let Some(footer_size) = data_file.footer_size {
                pf = pf.with_metadata_size_hint(footer_size as usize);
            }

            // Create file scan config
            let file_scan_config = FileScanConfigBuilder::new(
                self.object_store_url.as_ref().clone(),
                self.table_schema.clone(),
                Arc::new(ParquetSource::default()),
            )
            .with_file_group(FileGroup::new(vec![pf]))
            .build();

            // Create Parquet execution plan
            let format = ParquetFormat::new();
            let exec = format.create_physical_plan(state, file_scan_config).await?;

            // Execute and collect batches
            let task_ctx = state.task_ctx();
            let stream = exec.execute(0, task_ctx)?;

            let batches: Vec<RecordBatch> = stream
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<DataFusionResult<Vec<_>>>()?;

            // Append CDC columns to each batch
            for batch in batches {
                if batch.num_rows() > 0 {
                    let batch_with_cdc =
                        self.append_cdc_columns(batch, data_file.begin_snapshot, "insert")?;
                    all_batches.push(batch_with_cdc);
                }
            }
        }

        // Create MemTable from collected batches and delegate to its scan
        let mem_table = MemTable::try_new(self.output_schema.clone(), vec![all_batches])?;
        mem_table.scan(state, projection, _filters, _limit).await
    }
}
