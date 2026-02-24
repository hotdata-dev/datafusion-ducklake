//! DuckLake table provider implementation

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::Result;
use crate::column_rename::ColumnRenameExec;
use crate::delete_filter::DeleteFilterExec;
use crate::metadata_provider::{
    DuckLakeFileData, DuckLakeTableColumn, DuckLakeTableFile, MetadataProvider,
};
use crate::path_resolver::resolve_path;
use crate::types::{
    build_arrow_schema, build_read_schema_with_field_id_mapping, extract_parquet_field_ids,
};

#[cfg(feature = "write")]
use crate::insert_exec::DuckLakeInsertExec;
#[cfg(feature = "write")]
use crate::metadata_writer::{MetadataWriter, WriteMode};

#[cfg(feature = "encryption")]
use crate::encryption::EncryptionFactoryBuilder;
use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::object_store::ObjectStoreUrl;
#[cfg(feature = "write")]
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use tokio::sync::OnceCell;

#[cfg(feature = "encryption")]
use datafusion::execution::parquet_encryption::EncryptionFactory;

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

/// Cached schema mapping for renamed columns
type SchemaMappingCache = (SchemaRef, HashMap<String, String>);

/// DuckLake table provider
///
/// Represents a table within a DuckLake schema and provides access to data via Parquet files.
/// Caches snapshot_id and uses it to load all metadata atomically.
pub struct DuckLakeTable {
    #[allow(dead_code)]
    table_id: i64,
    table_name: String,
    #[allow(dead_code)]
    provider: Arc<dyn MetadataProvider>,
    /// Object store URL for resolving file paths (e.g., s3://bucket/ or file:///)
    object_store_url: Arc<ObjectStoreUrl>,
    /// Table path for resolving relative file paths
    table_path: String,
    /// Current schema with potentially renamed column names
    schema: SchemaRef,
    /// Column metadata from DuckLake (needed for field_id mapping)
    columns: Vec<DuckLakeTableColumn>,
    /// Table files with paths as stored in metadata (resolved on-the-fly when needed)
    table_files: Vec<DuckLakeTableFile>,
    /// Cached schema mapping (read_schema, name_mapping) - computed once on first scan
    schema_mapping_cache: OnceCell<SchemaMappingCache>,
    /// Encryption factory for decrypting encrypted Parquet files (when encryption feature is enabled)
    #[cfg(feature = "encryption")]
    encryption_factory: Option<Arc<dyn EncryptionFactory>>,
    /// Schema name (needed for write operations)
    #[cfg(feature = "write")]
    schema_name: Option<String>,
    /// Metadata writer for write operations (when write feature is enabled)
    #[cfg(feature = "write")]
    writer: Option<Arc<dyn MetadataWriter>>,
}

impl std::fmt::Debug for DuckLakeTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckLakeTable")
            .field("table_id", &self.table_id)
            .field("table_name", &self.table_name)
            .field("table_path", &self.table_path)
            .field("schema", &self.schema)
            .field("columns", &self.columns)
            .field("table_files", &self.table_files)
            .finish_non_exhaustive()
    }
}

impl DuckLakeTable {
    /// Create a new DuckLake table
    pub fn new(
        table_id: i64,
        table_name: impl Into<String>,
        provider: Arc<dyn MetadataProvider>,
        snapshot_id: i64, // Received from schema
        object_store_url: Arc<ObjectStoreUrl>,
        table_path: String,
    ) -> Result<Self> {
        // Load ALL metadata with this snapshot_id
        let columns = provider.get_table_structure(table_id)?;
        let schema = Arc::new(build_arrow_schema(&columns)?);
        let table_files = provider.get_table_files_for_select(table_id, snapshot_id)?;

        // Build encryption factory from file encryption keys (when encryption feature is enabled)
        #[cfg(feature = "encryption")]
        let encryption_factory = {
            let mut builder = EncryptionFactoryBuilder::new();
            for table_file in &table_files {
                // Resolve the file path for the mapping
                let resolved_path = resolve_path(
                    &table_path,
                    &table_file.file.path,
                    table_file.file.path_is_relative,
                );
                builder.add_file(&resolved_path, table_file.file.encryption_key.as_deref());

                // Also add delete file encryption key if present
                if let Some(ref delete_file) = table_file.delete_file {
                    let resolved_delete_path =
                        resolve_path(&table_path, &delete_file.path, delete_file.path_is_relative);
                    builder.add_file(&resolved_delete_path, delete_file.encryption_key.as_deref());
                }
            }
            let factory = builder.build();
            if factory.has_encrypted_files() {
                Some(Arc::new(factory) as Arc<dyn EncryptionFactory>)
            } else {
                None
            }
        };

        Ok(Self {
            table_id,
            table_name: table_name.into(),
            provider,
            object_store_url,
            table_path,
            schema,
            columns,
            table_files,
            #[cfg(feature = "encryption")]
            encryption_factory,
            schema_mapping_cache: OnceCell::new(),
            #[cfg(feature = "write")]
            schema_name: None,
            #[cfg(feature = "write")]
            writer: None,
        })
    }

    /// Resolve a file path (data or delete file) to its absolute path
    fn resolve_file_path(&self, file: &DuckLakeFileData) -> String {
        resolve_path(&self.table_path, &file.path, file.path_is_relative)
    }

    /// Create a ParquetSource with encryption support if enabled and needed
    fn create_parquet_source(&self) -> ParquetSource {
        #[cfg(feature = "encryption")]
        if let Some(ref factory) = self.encryption_factory {
            return ParquetSource::default().with_encryption_factory(Arc::clone(factory));
        }
        ParquetSource::default()
    }

    /// Get the cached schema mapping, computing it once from the first file if needed.
    /// All files in a DuckLake table have the same schema structure, so we only need to check one.
    async fn get_schema_mapping(
        &self,
        state: &dyn Session,
    ) -> DataFusionResult<&SchemaMappingCache> {
        self.schema_mapping_cache
            .get_or_try_init(|| async {
                // If no files, use current schema with no rename mapping
                let Some(first_file) = self.table_files.first() else {
                    return Ok((self.schema.clone(), HashMap::new()));
                };

                let resolved_path = self.resolve_file_path(&first_file.file);
                let object_store = state
                    .runtime_env()
                    .object_store(self.object_store_url.as_ref())?;
                let object_path = ObjectPath::from(resolved_path.as_str());

                let reader = ParquetObjectReader::new(object_store, object_path);

                // Build the ParquetRecordBatchStreamBuilder with decryption if needed
                #[cfg(feature = "encryption")]
                let builder = {
                    use parquet::arrow::arrow_reader::ArrowReaderOptions;

                    // Check if file has encryption key
                    let options = if let Some(ref key) = first_file.file.encryption_key {
                        if !key.is_empty() {
                            let key_bytes =
                                crate::encryption::DuckLakeEncryptionFactory::decode_key(key)?;
                            let decryption_props =
                                parquet::encryption::decrypt::FileDecryptionProperties::builder(
                                    key_bytes,
                                )
                                .build()
                                .map_err(|e| {
                                    DataFusionError::Execution(format!(
                                        "Failed to create decryption properties: {}",
                                        e
                                    ))
                                })?;
                            ArrowReaderOptions::new()
                                .with_file_decryption_properties(decryption_props)
                        } else {
                            ArrowReaderOptions::new()
                        }
                    } else {
                        ArrowReaderOptions::new()
                    };

                    ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?
                };

                #[cfg(not(feature = "encryption"))]
                let builder = ParquetRecordBatchStreamBuilder::new(reader)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let field_id_map = extract_parquet_field_ids(builder.metadata());

                // No field_ids means external file - use current schema directly
                if field_id_map.is_empty() {
                    return Ok((self.schema.clone(), HashMap::new()));
                }

                let (read_schema, name_mapping) =
                    build_read_schema_with_field_id_mapping(&self.columns, &field_id_map)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                Ok((Arc::new(read_schema), name_mapping))
            })
            .await
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
        let mut pf =
            PartitionedFile::new(&resolved_delete_path, delete_file.file_size_bytes as u64);
        if let Some(footer_size) = delete_file.footer_size {
            pf = pf.with_metadata_size_hint(footer_size as usize);
        }

        // Create file scan config for the delete file
        let file_scan_config = FileScanConfigBuilder::new(
            self.object_store_url.as_ref().clone(),
            delete_schema,
            Arc::new(self.create_parquet_source()),
        )
        .with_file_group(FileGroup::new(vec![pf]))
        .build();

        // Use DataSourceExec directly to preserve our ParquetSource with encryption factory
        let exec = DataSourceExec::from_data_source(file_scan_config);

        // Execute and collect all batches
        let task_ctx = state.task_ctx();
        let stream = exec.execute(0, task_ctx)?;

        let batches: Vec<RecordBatch> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<DataFusionResult<Vec<_>>>()
            .map_err(|e| {
                if is_object_store_not_found(&e) {
                    DataFusionError::Execution(format!(
                        "Delete file '{}' referenced in catalog metadata was not found.                          This may indicate catalog corruption or that the file was                          deleted outside of DuckLake.",
                        resolved_delete_path
                    ))
                } else {
                    e
                }
            })?;

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
        files: &[&DuckLakeTableFile],
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let (read_schema, name_mapping) = self.get_schema_mapping(state).await?;

        let partitioned_files: Vec<PartitionedFile> = files
            .iter()
            .map(|table_file| {
                let resolved_path = self.resolve_file_path(&table_file.file);
                let mut pf =
                    PartitionedFile::new(&resolved_path, table_file.file.file_size_bytes as u64);

                // Apply footer size hint if available from DuckLake metadata
                // This reduces I/O from 2 reads to 1 read per file (especially beneficial for S3/MinIO)
                if let Some(footer_size) = table_file.file.footer_size {
                    pf = pf.with_metadata_size_hint(footer_size as usize);
                }

                pf
            })
            .collect();

        // Use read_schema (with original Parquet names) for reading
        let mut builder = FileScanConfigBuilder::new(
            self.object_store_url.as_ref().clone(),
            read_schema.clone(),
            Arc::new(self.create_parquet_source()),
        )
        .with_limit(limit)
        .with_file_group(FileGroup::new(partitioned_files));

        // Apply projection if provided
        if let Some(proj) = projection {
            builder = builder.with_projection_indices(Some(proj.clone()));
        }

        let file_scan_config = builder.build();
        // Use DataSourceExec directly to preserve our ParquetSource with encryption factory
        let parquet_exec: Arc<dyn ExecutionPlan> =
            DataSourceExec::from_data_source(file_scan_config);

        // Wrap with ColumnRenameExec if column names differ
        if !name_mapping.is_empty() {
            let output_schema = match projection {
                Some(indices) => Arc::new(self.schema.project(indices)?),
                None => self.schema.clone(),
            };
            Ok(Arc::new(ColumnRenameExec::new(
                parquet_exec,
                output_schema,
                name_mapping.clone(),
            )))
        } else {
            Ok(parquet_exec)
        }
    }

    /// Configure this table for write operations.
    ///
    /// This method enables write support by attaching a metadata writer and data path.
    /// Once configured, the table can handle INSERT INTO operations.
    ///
    /// # Arguments
    /// * `schema_name` - Name of the schema this table belongs to
    /// * `writer` - Metadata writer for catalog operations
    #[cfg(feature = "write")]
    pub fn with_writer(mut self, schema_name: String, writer: Arc<dyn MetadataWriter>) -> Self {
        self.schema_name = Some(schema_name);
        self.writer = Some(writer);
        self
    }

    /// Build an execution plan for a single file with delete filtering
    ///
    /// Creates a Parquet scan wrapped with a delete filter to exclude deleted rows.
    async fn build_exec_for_file_with_deletes(
        &self,
        state: &dyn Session,
        table_file: &DuckLakeTableFile,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let (read_schema, name_mapping) = self.get_schema_mapping(state).await?;

        // Resolve the data file path for scanning
        let resolved_path = self.resolve_file_path(&table_file.file);

        // Create PartitionedFile with footer size hint if available
        let mut pf = PartitionedFile::new(&resolved_path, table_file.file.file_size_bytes as u64);
        if let Some(footer_size) = table_file.file.footer_size {
            pf = pf.with_metadata_size_hint(footer_size as usize);
        }

        // Use read_schema (with original Parquet names) for reading
        let mut builder = FileScanConfigBuilder::new(
            self.object_store_url.as_ref().clone(),
            read_schema.clone(),
            Arc::new(self.create_parquet_source()),
        )
        .with_limit(limit)
        .with_file_group(FileGroup::new(vec![pf]));

        // Apply projection if provided
        if let Some(proj) = projection {
            builder = builder.with_projection_indices(Some(proj.clone()));
        }

        let file_scan_config = builder.build();
        // Use DataSourceExec directly to preserve our ParquetSource with encryption factory
        let parquet_exec: Arc<dyn ExecutionPlan> =
            DataSourceExec::from_data_source(file_scan_config);

        // Wrap with delete filter - we know there's a delete file since we partitioned
        // The metadata already tells us which delete file goes with this data file
        let exec_after_delete: Arc<dyn ExecutionPlan> =
            if let Some(ref delete_file) = table_file.delete_file {
                let deleted_positions = self.read_delete_file_positions(state, delete_file).await?;

                if !deleted_positions.is_empty() {
                    Arc::new(DeleteFilterExec::new(
                        parquet_exec,
                        table_file.file.path.clone(),
                        Arc::new(deleted_positions),
                    ))
                } else {
                    parquet_exec
                }
            } else {
                parquet_exec
            };

        // Wrap with ColumnRenameExec if column names differ
        if !name_mapping.is_empty() {
            let output_schema = match projection {
                Some(indices) => Arc::new(self.schema.project(indices)?),
                None => self.schema.clone(),
            };
            Ok(Arc::new(ColumnRenameExec::new(
                exec_after_delete,
                output_schema,
                name_mapping.clone(),
            )))
        } else {
            Ok(exec_after_delete)
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
            let exec = self
                .build_exec_for_files_without_deletes(
                    state,
                    &files_without_deletes,
                    projection,
                    limit,
                )
                .await?;
            execs.push(exec);
        }

        // Only create separate execs for files with deletes
        for table_file in files_with_deletes {
            let exec = self
                .build_exec_for_file_with_deletes(state, table_file, projection, limit)
                .await?;
            execs.push(exec);
        }

        // Handle empty tables (no data files)
        if execs.is_empty() {
            use datafusion::physical_plan::empty::EmptyExec;
            let projected_schema = match projection {
                Some(indices) => Arc::new(self.schema.project(indices)?),
                None => self.schema.clone(),
            };
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        // Combine execution plans
        combine_execution_plans(execs)
    }

    #[cfg(feature = "write")]
    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let writer = self.writer.as_ref().ok_or_else(|| {
            DataFusionError::Plan(
                "Table is read-only. Use DuckLakeCatalog::with_writer() to enable writes."
                    .to_string(),
            )
        })?;

        let schema_name = self.schema_name.as_ref().ok_or_else(|| {
            DataFusionError::Internal("Schema name not set for writable table".to_string())
        })?;

        let write_mode = match insert_op {
            InsertOp::Append => WriteMode::Append,
            InsertOp::Overwrite | InsertOp::Replace => WriteMode::Replace,
        };

        Ok(Arc::new(DuckLakeInsertExec::new(
            input,
            Arc::clone(writer),
            schema_name.clone(),
            self.table_name.clone(),
            self.schema(),
            write_mode,
            self.object_store_url.clone(),
        )))
    }
}

/// Combines multiple execution plans into a single plan
fn combine_execution_plans(
    execs: Vec<Arc<dyn ExecutionPlan>>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    if execs.len() == 1 {
        Ok(execs.into_iter().next().unwrap())
    } else {
        use datafusion::physical_plan::union::UnionExec;
        UnionExec::try_new(execs)
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
        .ok_or_else(|| {
            DataFusionError::Internal(format!("{} column not found or wrong type", DELETE_POS_COL))
        })?;

    // Extract all non-null positions
    for i in 0..batch.num_rows() {
        if !pos_array.is_null(i) {
            positions.insert(pos_array.value(i));
        }
    }

    Ok(())
}

/// Check if a DataFusion error is caused by an object store NotFound error.
fn is_object_store_not_found(err: &DataFusionError) -> bool {
    if let DataFusionError::ObjectStore(os_err) = err {
        return matches!(os_err.as_ref(), object_store::Error::NotFound { .. });
    }
    let mut source = std::error::Error::source(err);
    while let Some(e) = source {
        if let Some(os_err) = e.downcast_ref::<object_store::Error>() {
            return matches!(os_err, object_store::Error::NotFound { .. });
        }
        source = e.source();
    }
    false
}
