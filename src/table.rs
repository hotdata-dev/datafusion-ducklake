//! DuckLake table provider implementation

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use crate::Result;
use crate::delete_filter::DeleteFilterExec;
use crate::metadata_provider::{DuckLakeFileData, MetadataProvider};
use crate::types::build_arrow_schema;
use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;

/// DuckLake table provider
///
/// Represents a table within a DuckLake schema and provides access to data via Parquet files.
#[derive(Debug)]
pub struct DuckLakeTable {
    #[allow(dead_code)]
    table_id: i64,
    #[allow(dead_code)]
    table_name: String,
    #[allow(dead_code)]
    provider: Arc<dyn MetadataProvider>,
    #[allow(dead_code)]
    snapshot_id: i64,
    /// the base path to the data, e.g. s3://ducklake-data
    base_data_url: Arc<ObjectStoreUrl>,
    /// data path for this table, used to resolve relative file paths on-the-fly
    data_path: String,
    schema: SchemaRef,
    /// Table files with paths as stored in metadata (resolved on-the-fly when needed)
    table_files: Vec<crate::metadata_provider::DuckLakeTableFile>,
}

impl DuckLakeTable {
    /// Create a new DuckLake table
    pub fn new(
        table_id: i64,
        table_name: impl Into<String>,
        provider: Arc<dyn MetadataProvider>,
        snapshot_id: i64,
        base_data_url: Arc<ObjectStoreUrl>,
        data_path: String,
    ) -> Result<Self> {
        // Get table structure (columns)
        let columns = provider.get_table_structure(table_id)?;

        // Build Arrow schema from column definitions
        let schema = Arc::new(build_arrow_schema(&columns)?);

        // Get data files - keep paths as-is from database
        let table_files = provider.get_table_files_for_select(table_id)?;

        Ok(Self {
            table_id,
            table_name: table_name.into(),
            provider,
            snapshot_id,
            base_data_url,
            data_path,
            schema,
            table_files,
        })
    }

    /// Resolve a file path (data or delete file) to its absolute path
    fn resolve_file_path(&self, file: &DuckLakeFileData) -> String {
        if file.path_is_relative {
            // Ensure data_path ends with separator before concatenating
            if self.data_path.ends_with('/') || self.data_path.ends_with('\\') {
                format!("{}{}", self.data_path, file.path)
            } else {
                format!("{}/{}", self.data_path, file.path)
            }
        } else {
            file.path.clone()
        }
    }

    /// Read a delete file and extract all deleted row positions
    ///
    /// The delete file is already associated with a specific data file via metadata.
    /// We only need to extract the "pos" column - the "file_path" column is
    /// metadata/documentation only (for Iceberg compatibility).
    async fn read_delete_file_positions(
        &self,
        state: &dyn Session,
        delete_file: &DuckLakeFileData,
    ) -> DataFusionResult<HashSet<i64>> {
        // Expected schema for delete files
        let delete_schema = Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ]));

        // Resolve the delete file path
        let resolved_delete_path = self.resolve_file_path(delete_file);

        // Create file scan config for the delete file
        let file_scan_config = FileScanConfigBuilder::new(
            self.base_data_url.as_ref().clone(),
            delete_schema,
            Arc::new(ParquetSource::default()),
        )
        .with_file_group(FileGroup::new(vec![PartitionedFile::new(
            &resolved_delete_path,
            delete_file.file_size_bytes as u64,
        )]))
        .build();

        // Create Parquet execution plan
        let format = ParquetFormat::new();
        let exec = format.create_physical_plan(state, file_scan_config).await?;

        // Execute and collect all batches
        let task_ctx = state.task_ctx();
        let stream = exec.execute(0, task_ctx)?;

        let batches: Vec<RecordBatch> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<DataFusionResult<Vec<_>>>()?;

        // Extract all positions from all batches
        let mut positions = HashSet::new();
        for batch in batches {
            extract_deleted_positions_from_batch(&batch, &mut positions)?;
        }

        Ok(positions)
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Mark all filters as Inexact because we apply delete filters after the scan.
        // DataFusion will reapply these filters after DeleteFilterExec to ensure
        // correctness, but Parquet can still use them for:
        // - Row group pruning via statistics
        // - Page-level filtering with late materialization
        // - Bloom filter lookups (if available)
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        // Filters are received here for informational purposes. DataFusion's optimizer
        // automatically pushes them down to the Parquet scanner for row group pruning and
        // page-level filtering since we declared support via supports_filters_pushdown().
        // We mark them as Inexact, so DataFusion will reapply them after our scan.
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let format = ParquetFormat::new();

        // Create separate execution plans for each file
        let mut execs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

        for table_file in &self.table_files {
            // Resolve the data file path for scanning
            let resolved_path = self.resolve_file_path(&table_file.file);

            let mut builder = FileScanConfigBuilder::new(
                self.base_data_url.as_ref().clone(),
                self.schema.clone(),
                Arc::new(ParquetSource::default()),
            )
            .with_limit(limit)
            .with_file_group(FileGroup::new(vec![PartitionedFile::new(
                &resolved_path,
                table_file.file.file_size_bytes as u64,
            )]));

            // Apply projection if provided
            if let Some(proj) = projection {
                builder = builder.with_projection(Some(proj.clone()));
            }

            let file_scan_config = builder.build();

            let parquet_exec = format.create_physical_plan(state, file_scan_config).await?;

            // Wrap with delete filter if this file has a delete file
            // The metadata already tells us which delete file goes with this data file
            let exec = if let Some(ref delete_file) = table_file.delete_file {
                let deleted_positions = self.read_delete_file_positions(state, delete_file).await?;

                if !deleted_positions.is_empty() {
                    Arc::new(DeleteFilterExec::new(
                        parquet_exec,
                        table_file.file.path.clone(),
                        deleted_positions,
                    )) as Arc<dyn ExecutionPlan>
                } else {
                    parquet_exec
                }
            } else {
                parquet_exec
            };

            execs.push(exec);
        }

        // If we have multiple files, we need to union them
        if execs.is_empty() {
            Err(DataFusionError::Internal("No data files found".into()))
        } else if execs.len() == 1 {
            Ok(execs.into_iter().next().unwrap())
        } else {
            // Use UnionExec to combine multiple file scans
            use datafusion::physical_plan::union::UnionExec;
            Ok(Arc::new(UnionExec::new(execs)))
        }
    }
}

/// Extract deleted row positions from a delete file RecordBatch
///
/// Delete files have schema: (file_path: VARCHAR, pos: INT64)
/// We only extract the "pos" column - the "file_path" column is metadata/documentation
/// only (for Iceberg compatibility). The metadata catalog already tells us which delete
/// file is associated with which data file.
fn extract_deleted_positions_from_batch(
    batch: &RecordBatch,
    positions: &mut HashSet<i64>,
) -> DataFusionResult<()> {
    // Get the pos column (index 1)
    let pos_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("pos column not found or wrong type".into()))?;

    // Extract all non-null positions
    for i in 0..batch.num_rows() {
        if !pos_array.is_null(i) {
            positions.insert(pos_array.value(i));
        }
    }

    Ok(())
}
