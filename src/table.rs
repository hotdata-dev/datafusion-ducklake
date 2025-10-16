//! DuckLake table provider implementation

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::Result;
use crate::delete_filter::DeleteFilterExec;
use crate::metadata_provider::{DuckLakeFileData, MetadataProvider};
use crate::types::build_arrow_schema;
use arrow::array::{Array, Int64Array, StringArray};
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
use datafusion::logical_expr::{Expr, TableType};
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
    /// relative data path from catalog/schema to this table for resolving relative file paths
    #[allow(dead_code)]
    data_path: String,
    schema: SchemaRef,
    /// Table files with resolved absolute paths and metadata
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

        // Get data files and resolve paths
        let table_files = provider.get_table_files_for_select(table_id)?;

        // Ensure data_path ends with a separator
        let data_path_normalized = if data_path.ends_with('/') || data_path.ends_with('\\') {
            data_path.clone()
        } else {
            format!("{}/", data_path)
        };

        let table_files: Vec<_> = table_files
            .into_iter()
            .map(|mut tf| {
                // Resolve data file relative paths to absolute paths
                if tf.file.path_is_relative {
                    // Join data_path with relative path
                    tf.file.path = format!("{}{}", data_path_normalized, tf.file.path);
                    tf.file.path_is_relative = false;
                }

                // Resolve delete file relative paths to absolute paths
                if let Some(ref mut delete_file) = tf.delete_file {
                    if delete_file.path_is_relative {
                        delete_file.path = format!("{}{}", data_path_normalized, delete_file.path);
                        delete_file.path_is_relative = false;
                    }
                }

                tf
            })
            .collect();

        println!("data files: {:?}", table_files.iter().map(|tf| &tf.file.path).collect::<Vec<_>>());

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

    /// Read all delete files and build a map of deleted row positions
    ///
    /// Returns: HashMap<data_file_path, HashSet<deleted_positions>>
    async fn read_delete_files(
        &self,
        state: &dyn Session,
    ) -> DataFusionResult<HashMap<String, HashSet<i64>>> {
        let mut deleted_positions: HashMap<String, HashSet<i64>> = HashMap::new();

        for table_file in &self.table_files {
            if let Some(ref delete_file) = table_file.delete_file {
                // Read the delete file Parquet
                let batches = self.read_delete_file_parquet(state, delete_file).await?;

                // Extract positions and merge into global map
                for batch in batches {
                    let positions = extract_deleted_positions(&batch)?;

                    for (file_path, pos_set) in positions {
                        deleted_positions
                            .entry(file_path)
                            .or_insert_with(HashSet::new)
                            .extend(pos_set);
                    }
                }
            }
        }

        Ok(deleted_positions)
    }

    /// Read a single delete file Parquet and return RecordBatches
    async fn read_delete_file_parquet(
        &self,
        state: &dyn Session,
        delete_file: &DuckLakeFileData,
    ) -> DataFusionResult<Vec<RecordBatch>> {
        // Expected schema for delete files
        let delete_schema = Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ]));

        // Create file scan config for the delete file
        let file_scan_config = FileScanConfigBuilder::new(
            self.base_data_url.as_ref().clone(),
            delete_schema,
            Arc::new(ParquetSource::default()),
        )
        .with_file_group(FileGroup::new(vec![PartitionedFile::new(
            &delete_file.path,
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

        Ok(batches)
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let format = ParquetFormat::new();

        // Read all delete files and build position map
        let deleted_positions = self.read_delete_files(state).await?;

        // Create separate execution plans for each file to track file paths accurately
        let mut execs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

        for table_file in &self.table_files {
            let mut builder = FileScanConfigBuilder::new(
                self.base_data_url.as_ref().clone(),
                self.schema.clone(),
                Arc::new(ParquetSource::default()),
            )
            .with_limit(limit)
            .with_file_group(FileGroup::new(vec![PartitionedFile::new(
                &table_file.file.path,
                table_file.file.file_size_bytes as u64,
            )]));

            // Apply projection if provided
            if let Some(proj) = projection {
                builder = builder.with_projection(Some(proj.clone()));
            }

            let file_scan_config = builder.build();

            let parquet_exec = format.create_physical_plan(state, file_scan_config).await?;

            // Wrap with delete filter if this file has deletes
            let exec = if let Some(file_deletes) = deleted_positions.get(&table_file.file.path) {
                if !file_deletes.is_empty() {
                    Arc::new(DeleteFilterExec::new(
                        parquet_exec,
                        table_file.file.path.clone(),
                        file_deletes.clone(),
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
            return Err(DataFusionError::Internal("No data files found".into()));
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
/// Returns: HashMap<file_path, HashSet<positions>>
fn extract_deleted_positions(
    batch: &RecordBatch,
) -> DataFusionResult<HashMap<String, HashSet<i64>>> {
    let mut deleted_positions: HashMap<String, HashSet<i64>> = HashMap::new();

    // Get columns
    let file_paths = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("file_path column not found or wrong type".into())
        })?;

    let positions = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("pos column not found or wrong type".into()))?;

    // Build HashMap of file_path -> Set<position>
    for i in 0..batch.num_rows() {
        if file_paths.is_null(i) || positions.is_null(i) {
            continue; // Skip null entries
        }

        let file_path = file_paths.value(i).to_string();

        // Normalize the file path to absolute path if possible
        // This ensures it matches the resolved paths in table_files
        let normalized_path = std::path::PathBuf::from(&file_path)
            .canonicalize()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or(file_path);

        let pos = positions.value(i);

        deleted_positions
            .entry(normalized_path)
            .or_insert_with(HashSet::new)
            .insert(pos);
    }

    Ok(deleted_positions)
}
