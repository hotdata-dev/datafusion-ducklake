//! Metadata writer trait and common types for DuckLake catalog writes.
//!
//! This module provides the `MetadataWriter` trait for writing metadata to DuckLake catalogs,
//! along with helper types for column definitions and data file registration.

use crate::Result;
use crate::types::arrow_to_ducklake_type;
use arrow::datatypes::DataType;

/// Column definition for creating or updating a table's schema.
///
/// Unlike `DuckLakeTableColumn` (used for reading), this struct doesn't have a `column_id`
/// field since IDs are assigned by the catalog during write operations.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// DuckLake type string (e.g., "varchar", "int64", "decimal(10,2)")
    pub ducklake_type: String,
    /// Whether this column allows NULL values
    pub is_nullable: bool,
}

impl ColumnDef {
    /// Create a new column definition.
    pub fn new(
        name: impl Into<String>,
        ducklake_type: impl Into<String>,
        is_nullable: bool,
    ) -> Self {
        Self {
            name: name.into(),
            ducklake_type: ducklake_type.into(),
            is_nullable,
        }
    }

    /// Create a column definition from an Arrow DataType.
    ///
    /// This is a convenience constructor that converts the Arrow type to a DuckLake type string.
    pub fn from_arrow(
        name: impl Into<String>,
        data_type: &DataType,
        is_nullable: bool,
    ) -> Result<Self> {
        let ducklake_type = arrow_to_ducklake_type(data_type)?;
        Ok(Self::new(name, ducklake_type, is_nullable))
    }
}

/// Information about a data file to register in the catalog.
///
/// This struct contains the metadata needed to register a Parquet file in the DuckLake catalog.
#[derive(Debug, Clone)]
pub struct DataFileInfo {
    /// Path to the file (relative to table path or absolute)
    pub path: String,
    /// Whether the path is relative to the table's path
    pub path_is_relative: bool,
    /// Size of the file in bytes
    pub file_size_bytes: i64,
    /// Size of the Parquet footer in bytes (optimization hint for reads)
    pub footer_size: Option<i64>,
    /// Number of records in the file
    pub record_count: i64,
}

impl DataFileInfo {
    /// Create a new data file info with relative path.
    pub fn new(path: impl Into<String>, file_size_bytes: i64, record_count: i64) -> Self {
        Self {
            path: path.into(),
            path_is_relative: true,
            file_size_bytes,
            footer_size: None,
            record_count,
        }
    }

    /// Set the footer size for read optimization.
    pub fn with_footer_size(mut self, footer_size: i64) -> Self {
        self.footer_size = Some(footer_size);
        self
    }

    /// Mark this file as having an absolute path.
    pub fn with_absolute_path(mut self) -> Self {
        self.path_is_relative = false;
        self
    }
}

/// Result of a write operation.
#[derive(Debug)]
pub struct WriteResult {
    /// Snapshot ID of the write operation
    pub snapshot_id: i64,
    /// Table ID (may be newly created)
    pub table_id: i64,
    /// Schema ID (may be newly created)
    pub schema_id: i64,
    /// Number of files written
    pub files_written: usize,
    /// Total records written
    pub records_written: i64,
}

/// Result of a transactional write setup operation.
#[derive(Debug)]
pub struct WriteSetupResult {
    /// Snapshot ID created for this write
    pub snapshot_id: i64,
    /// Schema ID (may be newly created)
    pub schema_id: i64,
    /// Table ID (may be newly created)
    pub table_id: i64,
    /// Column IDs in order
    pub column_ids: Vec<i64>,
}

/// Trait for writing metadata to DuckLake catalogs.
///
/// Implementations must be thread-safe (`Send + Sync`) to support concurrent writes
/// from different sessions. Each write operation should be atomic within a transaction.
///
/// # Example
///
/// ```ignore
/// use datafusion_ducklake::metadata_writer::{MetadataWriter, ColumnDef, DataFileInfo};
///
/// // Use transactional write setup for atomicity
/// let result = writer.begin_write_transaction("main", "users", &columns, true)?;
///
/// // Register new data file
/// let file_info = DataFileInfo::new("data.parquet", 1024, 100);
/// writer.register_data_file(result.table_id, result.snapshot_id, &file_info)?;
/// ```
pub trait MetadataWriter: Send + Sync + std::fmt::Debug {
    /// Create a new snapshot and return its ID.
    ///
    /// Each write operation should create a new snapshot for isolation and versioning.
    fn create_snapshot(&self) -> Result<i64>;

    /// Get or create a schema, returning `(schema_id, was_created)`.
    ///
    /// If the schema already exists (by name), returns its ID with `false`.
    /// If created, returns the new ID with `true`.
    ///
    /// # Arguments
    /// * `name` - Schema name
    /// * `path` - Optional path (defaults to schema name if `None`)
    /// * `snapshot_id` - Snapshot ID to use for `begin_snapshot`
    fn get_or_create_schema(
        &self,
        name: &str,
        path: Option<&str>,
        snapshot_id: i64,
    ) -> Result<(i64, bool)>;

    /// Get or create a table, returning `(table_id, was_created)`.
    ///
    /// If the table already exists (by name in schema), returns its ID with `false`.
    /// If created, returns the new ID with `true`.
    ///
    /// # Arguments
    /// * `schema_id` - Parent schema ID
    /// * `name` - Table name
    /// * `path` - Optional path (defaults to table name if `None`)
    /// * `snapshot_id` - Snapshot ID to use for `begin_snapshot`
    fn get_or_create_table(
        &self,
        schema_id: i64,
        name: &str,
        path: Option<&str>,
        snapshot_id: i64,
    ) -> Result<(i64, bool)>;

    /// Set or update columns for a table, returning assigned column IDs.
    ///
    /// This ends existing columns (using end_snapshot pattern for time travel)
    /// and creates new column definitions. Column IDs are assigned atomically
    /// and returned in the same order as the input columns.
    ///
    /// # Arguments
    /// * `table_id` - Table to set columns for
    /// * `columns` - Column definitions in order
    /// * `snapshot_id` - Snapshot ID for begin_snapshot/end_snapshot tracking
    fn set_columns(
        &self,
        table_id: i64,
        columns: &[ColumnDef],
        snapshot_id: i64,
    ) -> Result<Vec<i64>>;

    /// Register a new data file for a table.
    ///
    /// Returns the assigned `data_file_id`.
    ///
    /// # Arguments
    /// * `table_id` - Table the file belongs to
    /// * `snapshot_id` - Snapshot ID for `begin_snapshot`
    /// * `file` - File information
    fn register_data_file(
        &self,
        table_id: i64,
        snapshot_id: i64,
        file: &DataFileInfo,
    ) -> Result<i64>;

    /// End all existing data files for a table at the given snapshot.
    ///
    /// This sets `end_snapshot = snapshot_id` for all files where `end_snapshot IS NULL`.
    /// Used for replace semantics (truncate + insert).
    ///
    /// Returns the number of files ended.
    fn end_table_files(&self, table_id: i64, snapshot_id: i64) -> Result<u64>;

    /// Get the data path from catalog metadata.
    fn get_data_path(&self) -> Result<String>;

    /// Set the data path in catalog metadata.
    ///
    /// This should only be called during catalog initialization.
    fn set_data_path(&self, path: &str) -> Result<()>;

    /// Initialize DuckLake schema tables if they don't exist.
    ///
    /// This creates the `ducklake_*` tables needed for catalog operations.
    fn initialize_schema(&self) -> Result<()>;

    /// Begin a write transaction that atomically sets up all catalog metadata.
    ///
    /// This method performs the following operations in a single transaction:
    /// 1. Creates a new snapshot
    /// 2. Gets or creates the schema
    /// 3. Gets or creates the table
    /// 4. Sets column definitions (ends existing columns, creates new ones)
    /// 5. Optionally ends existing data files (for replace semantics)
    ///
    /// If any step fails, the entire transaction is rolled back, ensuring
    /// no orphaned catalog entries remain.
    ///
    /// # Arguments
    /// * `schema_name` - Target schema name (created if not exists)
    /// * `table_name` - Target table name (created if not exists)
    /// * `columns` - Column definitions for the table
    /// * `replace` - If true, ends existing data files (replace semantics)
    ///
    /// # Returns
    /// `WriteSetupResult` containing the snapshot_id, schema_id, table_id, and column_ids
    fn begin_write_transaction(
        &self,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        replace: bool,
    ) -> Result<WriteSetupResult>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_def_new() {
        let col = ColumnDef::new("test_col", "int32", true);
        assert_eq!(col.name, "test_col");
        assert_eq!(col.ducklake_type, "int32");
        assert!(col.is_nullable);
    }

    #[test]
    fn test_column_def_from_arrow() {
        let col = ColumnDef::from_arrow("id", &DataType::Int64, false).unwrap();
        assert_eq!(col.name, "id");
        assert_eq!(col.ducklake_type, "int64");
        assert!(!col.is_nullable);
    }

    #[test]
    fn test_data_file_info_new() {
        let file = DataFileInfo::new("test.parquet", 1024, 100);
        assert_eq!(file.path, "test.parquet");
        assert!(file.path_is_relative);
        assert_eq!(file.file_size_bytes, 1024);
        assert_eq!(file.record_count, 100);
        assert!(file.footer_size.is_none());
    }

    #[test]
    fn test_data_file_info_with_footer_size() {
        let file = DataFileInfo::new("test.parquet", 1024, 100).with_footer_size(256);
        assert_eq!(file.footer_size, Some(256));
    }

    #[test]
    fn test_data_file_info_with_absolute_path() {
        let file = DataFileInfo::new("/absolute/path.parquet", 1024, 100).with_absolute_path();
        assert!(!file.path_is_relative);
    }
}
