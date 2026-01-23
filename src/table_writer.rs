//! High-level table writer for DuckLake catalogs.
//!
//! This module provides `DuckLakeTableWriter`, which orchestrates the complete write process:
//! creating snapshots, managing schemas and tables, writing Parquet files with field_id metadata,
//! and registering files in the catalog.
//!
//! ## Streaming API
//!
//! For large tables, use the streaming API to avoid loading all data into memory:
//!
//! ```ignore
//! let session = table_writer.begin_write("main", "users", &schema, true)?;
//! session.write_batch(&batch1)?;
//! session.write_batch(&batch2)?;
//! let result = session.finish()?;
//! ```
//!
//! ## Custom File Paths
//!
//! For integration with external storage managers (e.g., S3), use `begin_write_to_path`:
//!
//! ```ignore
//! let session = table_writer.begin_write_to_path(
//!     "main", "users", &schema, local_path, "data.parquet", true
//! )?;
//! ```

use std::collections::HashMap;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::Result;
use crate::metadata_writer::{ColumnDef, DataFileInfo, MetadataWriter, WriteResult};
use crate::types::arrow_to_ducklake_type;

/// High-level writer for DuckLake tables.
///
/// Orchestrates the write process:
/// 1. Creates a new snapshot
/// 2. Gets or creates schema and table
/// 3. Sets column definitions (with assigned column_ids)
/// 4. Ends existing files (for replace semantics)
/// 5. Writes Parquet files with field_id metadata embedded
/// 6. Registers new data files in the catalog
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use datafusion_ducklake::{SqliteMetadataWriter, DuckLakeTableWriter};
///
/// let writer = SqliteMetadataWriter::new_with_init("sqlite:///catalog.db?mode=rwc").await?;
/// writer.set_data_path("/data")?;
///
/// let table_writer = DuckLakeTableWriter::new(Arc::new(writer))?;
/// let result = table_writer.write_table("main", "users", &[batch])?;
/// ```
#[derive(Debug)]
pub struct DuckLakeTableWriter {
    metadata: Arc<dyn MetadataWriter>,
    data_path: PathBuf,
}

impl DuckLakeTableWriter {
    /// Create a new table writer with the given metadata writer.
    ///
    /// The data path is retrieved from the catalog metadata.
    pub fn new(metadata: Arc<dyn MetadataWriter>) -> Result<Self> {
        let data_path_str = metadata.get_data_path()?;
        let data_path = PathBuf::from(data_path_str);

        Ok(Self {
            metadata,
            data_path,
        })
    }

    /// Begin a streaming write session to a table.
    ///
    /// This creates the snapshot, schema, table, and columns in the catalog,
    /// but defers file registration until `finish()` is called on the session.
    ///
    /// Use this for large tables to avoid loading all data into memory.
    ///
    /// # Arguments
    /// * `schema_name` - Target schema name (created if not exists)
    /// * `table_name` - Target table name (created if not exists)
    /// * `arrow_schema` - Arrow schema for the data
    /// * `replace` - If true, ends existing files (replace semantics); if false, appends
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut session = table_writer.begin_write("main", "users", &schema, true)?;
    /// session.write_batch(&batch1)?;
    /// session.write_batch(&batch2)?;
    /// let result = session.finish()?;
    /// ```
    pub fn begin_write(
        &self,
        schema_name: &str,
        table_name: &str,
        arrow_schema: &Schema,
        replace: bool,
    ) -> Result<TableWriteSession> {
        // Generate file path in standard location
        let table_path = self.data_path.join(schema_name).join(table_name);
        let file_name = format!("{}.parquet", Uuid::new_v4());

        self.begin_write_internal(
            schema_name,
            table_name,
            arrow_schema,
            table_path,
            file_name.clone(),
            file_name, // catalog_path is just the filename (relative)
            true,      // path_is_relative
            replace,
        )
    }

    /// Begin a streaming write session with a custom file path.
    ///
    /// Use this when integrating with external storage managers (e.g., S3) that
    /// control where files are written. The file is registered in the catalog
    /// with an absolute path.
    ///
    /// # Arguments
    /// * `schema_name` - Target schema name (created if not exists)
    /// * `table_name` - Target table name (created if not exists)
    /// * `arrow_schema` - Arrow schema for the data
    /// * `file_dir` - Directory where the Parquet file will be written
    /// * `file_name` - Name of the Parquet file
    /// * `replace` - If true, ends existing files (replace semantics); if false, appends
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Integration with external storage manager
    /// let handle = storage.prepare_cache_write(conn_id, "schema", "table");
    /// let mut session = table_writer.begin_write_to_path(
    ///     "main", "users", &schema, handle.local_path, "data.parquet", true
    /// )?;
    /// session.write_batch(&batch)?;
    /// let result = session.finish()?;
    /// storage.finalize_cache_write(&handle).await?; // Upload to S3 if needed
    /// ```
    pub fn begin_write_to_path(
        &self,
        schema_name: &str,
        table_name: &str,
        arrow_schema: &Schema,
        file_dir: PathBuf,
        file_name: String,
        replace: bool,
    ) -> Result<TableWriteSession> {
        // For custom paths, register with absolute path
        let file_path = file_dir.join(&file_name);
        let catalog_path = file_path.to_string_lossy().to_string();

        self.begin_write_internal(
            schema_name,
            table_name,
            arrow_schema,
            file_dir,
            file_name,
            catalog_path, // absolute path
            false,        // path_is_relative = false
            replace,
        )
    }

    /// Internal method that handles the common write session setup.
    #[allow(clippy::too_many_arguments)]
    fn begin_write_internal(
        &self,
        schema_name: &str,
        table_name: &str,
        arrow_schema: &Schema,
        file_dir: PathBuf,
        file_name: String,
        catalog_path: String,
        path_is_relative: bool,
        replace: bool,
    ) -> Result<TableWriteSession> {
        // 1. Create snapshot
        let snapshot_id = self.metadata.create_snapshot()?;

        // 2. Get or create schema
        let (schema_id, _schema_created) =
            self.metadata
                .get_or_create_schema(schema_name, None, snapshot_id)?;

        // 3. Get or create table
        let (table_id, _table_created) =
            self.metadata
                .get_or_create_table(schema_id, table_name, None, snapshot_id)?;

        // 4. Set columns (derive from Arrow schema)
        let columns = arrow_schema_to_column_defs(arrow_schema)?;
        let column_ids = self.metadata.set_columns(table_id, &columns)?;

        // 5. For replace semantics, end existing files
        if replace {
            self.metadata.end_table_files(table_id, snapshot_id)?;
        }

        // 6. Build schema with field_ids for Parquet
        let schema_with_ids = Arc::new(build_schema_with_field_ids(arrow_schema, &column_ids));

        // 7. Create directory and file
        std::fs::create_dir_all(&file_dir)?;
        let file_path = file_dir.join(&file_name);

        // 8. Create Parquet writer
        let props = WriterProperties::builder()
            .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
            .build();

        let file = File::create(&file_path)?;
        let writer = ArrowWriter::try_new(file, schema_with_ids.clone(), Some(props))?;

        Ok(TableWriteSession {
            metadata: Arc::clone(&self.metadata),
            snapshot_id,
            schema_id,
            table_id,
            column_ids,
            schema_with_ids,
            writer: Some(writer),
            file_path,
            catalog_path,
            path_is_relative,
            row_count: 0,
        })
    }

    /// Write batches to a table, replacing any existing data.
    ///
    /// This creates a new snapshot, ends all existing files for the table,
    /// writes the new data, and registers the new files.
    ///
    /// For large tables, prefer `begin_write()` for streaming writes.
    ///
    /// # Arguments
    /// * `schema_name` - Target schema name (created if not exists)
    /// * `table_name` - Target table name (created if not exists)
    /// * `batches` - Record batches to write
    ///
    /// # Returns
    /// `WriteResult` with snapshot_id, table_id, and statistics
    pub fn write_table(
        &self,
        schema_name: &str,
        table_name: &str,
        batches: &[RecordBatch],
    ) -> Result<WriteResult> {
        if batches.is_empty() {
            return Err(crate::error::DuckLakeError::InvalidConfig(
                "No batches to write".to_string(),
            ));
        }

        let arrow_schema = batches[0].schema();
        let mut session = self.begin_write(schema_name, table_name, &arrow_schema, true)?;

        for batch in batches {
            session.write_batch(batch)?;
        }

        session.finish()
    }

    /// Write batches to a table, appending to existing data.
    ///
    /// This creates a new snapshot and registers new files without ending existing files.
    ///
    /// For large tables, prefer `begin_write()` for streaming writes.
    ///
    /// # Arguments
    /// * `schema_name` - Target schema name (created if not exists)
    /// * `table_name` - Target table name (created if not exists)
    /// * `batches` - Record batches to write
    ///
    /// # Returns
    /// `WriteResult` with snapshot_id, table_id, and statistics
    pub fn append_table(
        &self,
        schema_name: &str,
        table_name: &str,
        batches: &[RecordBatch],
    ) -> Result<WriteResult> {
        if batches.is_empty() {
            return Err(crate::error::DuckLakeError::InvalidConfig(
                "No batches to write".to_string(),
            ));
        }

        let arrow_schema = batches[0].schema();
        let mut session = self.begin_write(schema_name, table_name, &arrow_schema, false)?;

        for batch in batches {
            session.write_batch(batch)?;
        }

        session.finish()
    }
}

/// A streaming write session for writing batches incrementally to a DuckLake table.
///
/// Created by [`DuckLakeTableWriter::begin_write()`] or [`DuckLakeTableWriter::begin_write_to_path()`].
///
/// The session holds the Parquet writer and catalog metadata. Batches are written
/// incrementally via `write_batch()`, and the file is registered in the catalog
/// when `finish()` is called.
///
/// # Lifecycle
///
/// ```text
/// begin_write() → write_batch()* → finish()
/// ```
///
/// # Error Handling and Cleanup
///
/// - **Dropped without `finish()`**: The Parquet file may be partially written but is
///   NOT registered in the catalog. The orphaned file remains on disk and should be
///   cleaned up by the caller or a separate garbage collection process.
///
/// - **Error during `write_batch()`**: The session remains valid and you can continue
///   writing. The Parquet file may contain partial data.
///
/// - **Error during `finish()`**: The Parquet file is closed but may not be registered
///   in the catalog. The file remains on disk.
///
/// For robust error handling in production, consider:
/// 1. Wrapping writes in a try block
/// 2. Implementing cleanup logic for orphaned files
/// 3. Using the file path from `file_path()` to clean up on error
///
/// # Atomicity
///
/// The catalog registration in `finish()` is the commit point. Until `finish()`
/// completes successfully, the data is not visible to readers. However, the
/// snapshot, schema, table, and columns are created during `begin_write()`,
/// so a failed write may leave empty catalog entries.
#[derive(Debug)]
pub struct TableWriteSession {
    metadata: Arc<dyn MetadataWriter>,
    snapshot_id: i64,
    schema_id: i64,
    table_id: i64,
    #[allow(dead_code)]
    column_ids: Vec<i64>,
    schema_with_ids: SchemaRef,
    writer: Option<ArrowWriter<File>>,
    file_path: PathBuf,
    /// Path to register in catalog (may be relative filename or absolute path)
    catalog_path: String,
    /// Whether the catalog_path is relative to table path
    path_is_relative: bool,
    row_count: i64,
}

impl TableWriteSession {
    /// Write a batch to the Parquet file.
    ///
    /// The batch schema must match the schema provided when creating the session.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            crate::error::DuckLakeError::Internal("Writer already closed".to_string())
        })?;

        // Remap batch to use schema with field_ids
        let batch_with_ids =
            RecordBatch::try_new(self.schema_with_ids.clone(), batch.columns().to_vec())?;

        writer.write(&batch_with_ids)?;
        self.row_count += batch.num_rows() as i64;

        Ok(())
    }

    /// Returns the number of rows written so far.
    pub fn row_count(&self) -> i64 {
        self.row_count
    }

    /// Returns the snapshot ID for this write session.
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Returns the path where the Parquet file is being written.
    pub fn file_path(&self) -> &std::path::Path {
        &self.file_path
    }

    /// Finish the write session: close the Parquet file and register it in the catalog.
    ///
    /// Returns `WriteResult` with snapshot_id, table_id, and statistics.
    ///
    /// # Errors
    ///
    /// Returns an error if the writer was already closed or if catalog registration fails.
    pub fn finish(mut self) -> Result<WriteResult> {
        let writer = self.writer.take().ok_or_else(|| {
            crate::error::DuckLakeError::Internal("Writer already closed".to_string())
        })?;

        // Close writer to flush all data
        let _file_metadata = writer.close()?;

        // Get actual file size
        let file_size = std::fs::metadata(&self.file_path)?.len() as i64;

        // Calculate actual footer size from Parquet metadata
        // Footer = metadata length + 4 bytes (footer length) + 4 bytes (magic "PAR1")
        let footer_size = calculate_footer_size(&self.file_path)?;

        // Register data file in catalog with appropriate path
        let mut file_info = DataFileInfo::new(&self.catalog_path, file_size, self.row_count)
            .with_footer_size(footer_size);
        if !self.path_is_relative {
            file_info = file_info.with_absolute_path();
        }
        self.metadata
            .register_data_file(self.table_id, self.snapshot_id, &file_info)?;

        Ok(WriteResult {
            snapshot_id: self.snapshot_id,
            table_id: self.table_id,
            schema_id: self.schema_id,
            files_written: 1,
            records_written: self.row_count,
        })
    }
}

/// Convert an Arrow schema to a list of ColumnDefs.
fn arrow_schema_to_column_defs(schema: &Schema) -> Result<Vec<ColumnDef>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let ducklake_type = arrow_to_ducklake_type(field.data_type())?;
            Ok(ColumnDef::new(
                field.name().clone(),
                ducklake_type,
                field.is_nullable(),
            ))
        })
        .collect()
}

/// Build an Arrow schema with PARQUET:field_id metadata for DuckLake compatibility.
///
/// The parquet crate reads `PARQUET:field_id` from Arrow field metadata and uses it
/// to set the Parquet schema field_id.
fn build_schema_with_field_ids(schema: &Schema, column_ids: &[i64]) -> Schema {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .zip(column_ids.iter())
        .map(|(field, &col_id)| {
            let mut metadata: HashMap<String, String> = field.metadata().clone();
            metadata.insert("PARQUET:field_id".to_string(), col_id.to_string());
            Field::new(field.name(), field.data_type().clone(), field.is_nullable())
                .with_metadata(metadata)
        })
        .collect();

    Schema::new_with_metadata(fields, schema.metadata().clone())
}

/// Calculate the actual footer size of a Parquet file.
///
/// Parquet footer structure:
/// - Metadata (variable length)
/// - 4 bytes: metadata length (little-endian i32)
/// - 4 bytes: magic bytes "PAR1"
///
/// Footer size = metadata length + 8 bytes
fn calculate_footer_size(path: &std::path::Path) -> Result<i64> {
    let mut file = File::open(path)?;
    let file_size = file.metadata()?.len();

    if file_size < 8 {
        return Err(crate::error::DuckLakeError::Internal(
            "Invalid Parquet file: too small".to_string(),
        ));
    }

    // Read the last 8 bytes: 4 bytes metadata length + 4 bytes magic
    file.seek(SeekFrom::End(-8))?;
    let mut footer_bytes = [0u8; 8];
    std::io::Read::read_exact(&mut file, &mut footer_bytes)?;

    // Verify magic bytes "PAR1"
    if &footer_bytes[4..8] != b"PAR1" {
        return Err(crate::error::DuckLakeError::Internal(
            "Invalid Parquet file: missing PAR1 magic".to_string(),
        ));
    }

    // Read metadata length (little-endian i32)
    let metadata_len =
        i32::from_le_bytes([footer_bytes[0], footer_bytes[1], footer_bytes[2], footer_bytes[3]])
            as i64;

    // Footer size = metadata + 4 bytes (length) + 4 bytes (magic)
    Ok(metadata_len + 8)
}

/// Write batches to a Parquet file with field_id metadata.
///
/// Returns (file_size_bytes, footer_size_bytes, record_count).
#[allow(dead_code)]
fn write_parquet_with_field_ids(
    path: &std::path::Path,
    batches: &[RecordBatch],
    column_ids: &[i64],
) -> Result<(i64, i64, i64)> {
    let original_schema = batches[0].schema();

    // Build schema with field_id metadata
    let schema_with_ids = build_schema_with_field_ids(&original_schema, column_ids);
    let schema_ref: SchemaRef = Arc::new(schema_with_ids);

    // Create writer properties
    let props = WriterProperties::builder()
        .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
        .build();

    // Create file and writer
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema_ref.clone(), Some(props))?;

    // Write all batches
    let mut total_rows = 0i64;
    for batch in batches {
        // Remap batch to use schema with field_ids
        let batch_with_ids = RecordBatch::try_new(schema_ref.clone(), batch.columns().to_vec())?;
        writer.write(&batch_with_ids)?;
        total_rows += batch.num_rows() as i64;
    }

    // Close writer
    let _ = writer.close()?;

    // Get file size
    let file_size = std::fs::metadata(path)?.len() as i64;

    // Calculate actual footer size
    let footer_size = calculate_footer_size(path)?;

    Ok((file_size, footer_size, total_rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::DataType;

    #[test]
    fn test_arrow_schema_to_column_defs() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let columns = arrow_schema_to_column_defs(&schema).unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].ducklake_type, "int32");
        assert!(!columns[0].is_nullable);
        assert_eq!(columns[1].name, "name");
        assert_eq!(columns[1].ducklake_type, "varchar");
        assert!(columns[1].is_nullable);
    }

    #[test]
    fn test_build_schema_with_field_ids() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let column_ids = vec![1, 2];
        let schema_with_ids = build_schema_with_field_ids(&schema, &column_ids);

        // Check that field_ids are embedded in metadata
        let field0_metadata = schema_with_ids.field(0).metadata();
        assert_eq!(
            field0_metadata.get("PARQUET:field_id"),
            Some(&"1".to_string())
        );

        let field1_metadata = schema_with_ids.field(1).metadata();
        assert_eq!(
            field1_metadata.get("PARQUET:field_id"),
            Some(&"2".to_string())
        );
    }

    #[test]
    fn test_write_parquet_with_field_ids() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let temp_dir = tempfile::TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.parquet");

        let column_ids = vec![10, 20];
        let (file_size, footer_size, record_count) =
            write_parquet_with_field_ids(&file_path, &[batch], &column_ids).unwrap();

        assert!(file_size > 0);
        assert!(footer_size > 0);
        assert!(footer_size < file_size); // Footer should be smaller than file
        assert_eq!(record_count, 3);

        // Verify field_ids by reading back
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let file = File::open(&file_path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let metadata = reader.metadata();

        let schema_descr = metadata.file_metadata().schema_descr();
        for i in 0..schema_descr.num_columns() {
            let column = schema_descr.column(i);
            let basic_info = column.self_type().get_basic_info();
            assert!(basic_info.has_id(), "Column {} should have field_id", i);
        }
    }

    #[test]
    fn test_calculate_footer_size() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();

        let temp_dir = tempfile::TempDir::new().unwrap();
        let file_path = temp_dir.path().join("footer_test.parquet");

        let column_ids = vec![1];
        write_parquet_with_field_ids(&file_path, &[batch], &column_ids).unwrap();

        let footer_size = calculate_footer_size(&file_path).unwrap();

        // Footer should be reasonable size (metadata + 8 bytes)
        assert!(footer_size >= 8);
        assert!(footer_size < 10000); // Shouldn't be huge for a simple file
    }
}
