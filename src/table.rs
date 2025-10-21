//! DuckLake table provider implementation

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use crate::Result;
use crate::delete_filter::DeleteFilterExec;
use crate::metadata_provider::{DuckLakeFileData, MetadataProvider};
use crate::path_resolver::resolve_path;
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

// Delete file schema constants (public for testing)
pub const DELETE_FILE_PATH_COL: &str = "file_path";
pub const DELETE_POS_COL: &str = "pos";

/// Returns the expected schema for DuckLake delete files
///
/// Delete files have a standard schema: (file_path: VARCHAR, pos: INT64)
/// The file_path column is metadata/documentation only (for Iceberg compatibility).
/// The pos column contains the row positions to delete.
pub fn delete_file_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(DELETE_FILE_PATH_COL, DataType::Utf8, false),
        Field::new(DELETE_POS_COL, DataType::Int64, false),
    ]))
}

/// DuckLake table provider
///
/// Represents a table within a DuckLake schema and provides access to data via Parquet files.
/// Caches snapshot_id and uses it to load all metadata atomically.
#[derive(Debug)]
pub struct DuckLakeTable {
    #[allow(dead_code)]
    table_id: i64,
    #[allow(dead_code)]
    table_name: String,
    #[allow(dead_code)]
    provider: Arc<dyn MetadataProvider>,
    /// Object store URL for resolving file paths (e.g., s3://bucket/ or file:///)
    object_store_url: Arc<ObjectStoreUrl>,
    /// Table path for resolving relative file paths
    table_path: String,
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
        snapshot_id: i64,  // Received from schema
        object_store_url: Arc<ObjectStoreUrl>,
        table_path: String,
    ) -> Result<Self> {
        // Load ALL metadata with this snapshot_id
        let columns = provider.get_table_structure(table_id)?;
        let schema = Arc::new(build_arrow_schema(&columns)?);
        let table_files = provider.get_table_files_for_select(table_id, snapshot_id)?;

        Ok(Self {
            table_id,
            table_name: table_name.into(),
            provider,
            object_store_url,
            table_path,
            schema,
            table_files,
        })
    }

    /// Resolve a file path (data or delete file) to its absolute path
    fn resolve_file_path(&self, file: &DuckLakeFileData) -> String {
        resolve_path(&self.table_path, &file.path, file.path_is_relative)
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
        // Get the standard delete file schema
        let delete_schema = delete_file_schema();

        // Resolve the delete file path
        let resolved_delete_path = self.resolve_file_path(delete_file);

        // Create PartitionedFile with footer size hint if available
        let mut pf = PartitionedFile::new(&resolved_delete_path, delete_file.file_size_bytes as u64);
        if let Some(footer_size) = delete_file.footer_size {
            pf = pf.with_metadata_size_hint(footer_size as usize);
        }

        // Create file scan config for the delete file
        let file_scan_config = FileScanConfigBuilder::new(
            self.object_store_url.as_ref().clone(),
            delete_schema,
            Arc::new(ParquetSource::default()),
        )
        .with_file_group(FileGroup::new(vec![pf]))
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

    /// Build a single execution plan for all files without delete files
    ///
    /// Groups multiple files into a single efficient execution plan since they don't
    /// need delete filtering.
    async fn build_exec_for_files_without_deletes(
        &self,
        state: &dyn Session,
        files: &[&crate::metadata_provider::DuckLakeTableFile],
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let partitioned_files: Vec<PartitionedFile> = files
            .iter()
            .map(|table_file| {
                let resolved_path = self.resolve_file_path(&table_file.file);
                let mut pf = PartitionedFile::new(&resolved_path, table_file.file.file_size_bytes as u64);

                // Apply footer size hint if available from DuckLake metadata
                // This reduces I/O from 2 reads to 1 read per file (especially beneficial for S3/MinIO)
                if let Some(footer_size) = table_file.file.footer_size {
                    pf = pf.with_metadata_size_hint(footer_size as usize);
                }

                pf
            })
            .collect();

        let mut builder = FileScanConfigBuilder::new(
            self.object_store_url.as_ref().clone(),
            self.schema.clone(),
            Arc::new(ParquetSource::default()),
        )
        .with_limit(limit)
        .with_file_group(FileGroup::new(partitioned_files));

        // Apply projection if provided
        if let Some(proj) = projection {
            builder = builder.with_projection(Some(proj.clone()));
        }

        let file_scan_config = builder.build();
        let format = ParquetFormat::new();
        format.create_physical_plan(state, file_scan_config).await
    }

    /// Build an execution plan for a single file with delete filtering
    ///
    /// Creates a Parquet scan wrapped with a delete filter to exclude deleted rows.
    async fn build_exec_for_file_with_deletes(
        &self,
        state: &dyn Session,
        table_file: &crate::metadata_provider::DuckLakeTableFile,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Resolve the data file path for scanning
        let resolved_path = self.resolve_file_path(&table_file.file);

        // Create PartitionedFile with footer size hint if available
        let mut pf = PartitionedFile::new(&resolved_path, table_file.file.file_size_bytes as u64);
        if let Some(footer_size) = table_file.file.footer_size {
            pf = pf.with_metadata_size_hint(footer_size as usize);
        }

        let mut builder = FileScanConfigBuilder::new(
            self.object_store_url.as_ref().clone(),
            self.schema.clone(),
            Arc::new(ParquetSource::default()),
        )
        .with_limit(limit)
        .with_file_group(FileGroup::new(vec![pf]));

        // Apply projection if provided
        if let Some(proj) = projection {
            builder = builder.with_projection(Some(proj.clone()));
        }

        let file_scan_config = builder.build();
        let format = ParquetFormat::new();
        let parquet_exec = format.create_physical_plan(state, file_scan_config).await?;

        // Wrap with delete filter - we know there's a delete file since we partitioned
        // The metadata already tells us which delete file goes with this data file
        if let Some(ref delete_file) = table_file.delete_file {
            let deleted_positions = self.read_delete_file_positions(state, delete_file).await?;

            if !deleted_positions.is_empty() {
                Ok(Arc::new(DeleteFilterExec::new(
                    parquet_exec,
                    table_file.file.path.clone(),
                    deleted_positions,
                )) as Arc<dyn ExecutionPlan>)
            } else {
                Ok(parquet_exec)
            }
        } else {
            Ok(parquet_exec)
        }
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
        // Separate files into two groups: with deletes and without deletes
        // This allows us to create a single efficient exec for files without deletes
        let (files_with_deletes, files_without_deletes): (Vec<_>, Vec<_>) = self
            .table_files
            .iter()
            .partition(|tf| tf.delete_file.is_some());

        let mut execs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

        // Create single exec for all files without deletes (more efficient)
        if !files_without_deletes.is_empty() {
            let exec = self.build_exec_for_files_without_deletes(
                state,
                &files_without_deletes,
                projection,
                limit,
            ).await?;
            execs.push(exec);
        }

        // Only create separate execs for files with deletes
        for table_file in files_with_deletes {
            let exec = self.build_exec_for_file_with_deletes(
                state,
                table_file,
                projection,
                limit,
            ).await?;
            execs.push(exec);
        }

        // Combine execution plans
        combine_execution_plans(execs)
    }
}

/// Combines multiple execution plans into a single plan
///
/// Returns an error if no plans are provided, a single plan if only one exists,
/// or a UnionExec if multiple plans need to be combined.
fn combine_execution_plans(
    execs: Vec<Arc<dyn ExecutionPlan>>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
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
    // Get the pos column index by name (not magic number)
    let schema = batch.schema();
    let pos_idx = schema.index_of(DELETE_POS_COL)?;

    // Get the pos column
    let pos_array = batch
        .column(pos_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal(format!("{} column not found or wrong type", DELETE_POS_COL)))?;

    // Extract all non-null positions
    for i in 0..batch.num_rows() {
        if !pos_array.is_null(i) {
            positions.insert(pos_array.value(i));
        }
    }

    Ok(())
}
