use crate::Result;

// SQL queries for DuckLake catalog tables
// These queries are database-agnostic and work with DuckDB, SQLite, PostgreSQL, MySQL
pub const SQL_GET_LATEST_SNAPSHOT: &str =
    "SELECT COALESCE(MAX(snapshot_id), 0) FROM ducklake_snapshot";

pub const SQL_LIST_SNAPSHOTS: &str = "SELECT snapshot_id, CAST(snapshot_time AS VARCHAR) as timestamp FROM ducklake_snapshot ORDER BY snapshot_id";

pub const SQL_LIST_SCHEMAS: &str =
    "SELECT schema_id, schema_name, path, path_is_relative FROM ducklake_schema
     WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

pub const SQL_LIST_TABLES: &str =
    "SELECT table_id, table_name, path, path_is_relative FROM ducklake_table
     WHERE schema_id = ?
       AND ? >= begin_snapshot
       AND (? < end_snapshot OR end_snapshot IS NULL)";

pub const SQL_GET_TABLE_COLUMNS: &str = "SELECT column_id, column_name, column_type
     FROM ducklake_column
     WHERE table_id = ?
     ORDER BY column_order";

pub const SQL_GET_DATA_FILES: &str = "
    SELECT
        data.data_file_id,
        data.path AS data_file_path,
        data.path_is_relative AS data_path_is_relative,
        data.file_size_bytes AS data_file_size,
        data.footer_size AS data_footer_size,
        del.delete_file_id,
        del.path AS delete_file_path,
        del.path_is_relative AS delete_path_is_relative,
        del.file_size_bytes AS delete_file_size,
        del.footer_size AS delete_footer_size,
        del.delete_count
    FROM ducklake_data_file AS data
    LEFT JOIN ducklake_delete_file AS del
        ON data.data_file_id = del.data_file_id
        AND del.table_id = ?
        AND ? >= del.begin_snapshot
        AND (? < del.end_snapshot OR del.end_snapshot IS NULL)
    WHERE data.table_id = ?";

pub const SQL_GET_DATA_PATH: &str =
    "SELECT value FROM ducklake_metadata WHERE key = 'data_path' AND scope IS NULL";

pub const SQL_GET_SCHEMA_BY_NAME: &str =
    "SELECT schema_id, schema_name, path, path_is_relative FROM ducklake_schema
     WHERE schema_name = ?
       AND ? >= begin_snapshot
       AND (? < end_snapshot OR end_snapshot IS NULL)";

pub const SQL_GET_TABLE_BY_NAME: &str =
    "SELECT table_id, table_name, path, path_is_relative FROM ducklake_table
     WHERE schema_id = ?
       AND table_name = ?
       AND ? >= begin_snapshot
       AND (? < end_snapshot OR end_snapshot IS NULL)";

pub const SQL_TABLE_EXISTS: &str = "SELECT EXISTS(
       SELECT 1 FROM ducklake_table
       WHERE schema_id = ?
         AND table_name = ?
         AND ? >= begin_snapshot
         AND (? < end_snapshot OR end_snapshot IS NULL)
     )";

// Bulk queries for information_schema (avoids N+1 query problem)

pub const SQL_LIST_ALL_TABLES: &str = "
    SELECT
        s.schema_name,
        t.table_id,
        t.table_name,
        t.path,
        t.path_is_relative
    FROM ducklake_schema s
    JOIN ducklake_table t ON s.schema_id = t.schema_id
    WHERE ? >= s.begin_snapshot
      AND (? < s.end_snapshot OR s.end_snapshot IS NULL)
      AND ? >= t.begin_snapshot
      AND (? < t.end_snapshot OR t.end_snapshot IS NULL)
    ORDER BY s.schema_name, t.table_name";

pub const SQL_LIST_ALL_COLUMNS: &str = "
    SELECT
        s.schema_name,
        t.table_name,
        c.column_id,
        c.column_name,
        c.column_type
    FROM ducklake_schema s
    JOIN ducklake_table t ON s.schema_id = t.schema_id
    JOIN ducklake_column c ON t.table_id = c.table_id
    WHERE ? >= s.begin_snapshot
      AND (? < s.end_snapshot OR s.end_snapshot IS NULL)
      AND ? >= t.begin_snapshot
      AND (? < t.end_snapshot OR t.end_snapshot IS NULL)
    ORDER BY s.schema_name, t.table_name, c.column_order";

pub const SQL_LIST_ALL_FILES: &str = "
    SELECT
        s.schema_name,
        t.table_name,
        data.data_file_id,
        data.path AS data_file_path,
        data.path_is_relative AS data_path_is_relative,
        data.file_size_bytes AS data_file_size,
        data.footer_size AS data_footer_size,
        del.delete_file_id,
        del.path AS delete_file_path,
        del.path_is_relative AS delete_path_is_relative,
        del.file_size_bytes AS delete_file_size,
        del.footer_size AS delete_footer_size,
        del.delete_count
    FROM ducklake_schema s
    JOIN ducklake_table t ON s.schema_id = t.schema_id
    JOIN ducklake_data_file data ON t.table_id = data.table_id
    LEFT JOIN ducklake_delete_file del
        ON data.data_file_id = del.data_file_id
        AND del.table_id = t.table_id
        AND ? >= del.begin_snapshot
        AND (? < del.end_snapshot OR del.end_snapshot IS NULL)
    WHERE ? >= s.begin_snapshot
      AND (? < s.end_snapshot OR s.end_snapshot IS NULL)
      AND ? >= t.begin_snapshot
      AND (? < t.end_snapshot OR t.end_snapshot IS NULL)
    ORDER BY s.schema_name, t.table_name, data.path";

/// Metadata for a snapshot in the DuckLake catalog
#[derive(Debug, Clone)]
pub struct SnapshotMetadata {
    /// Unique identifier for this snapshot
    pub snapshot_id: i64,
    /// Timestamp when the snapshot was created (optional)
    pub timestamp: Option<String>,
}

/// Metadata for a schema in the DuckLake catalog
#[derive(Debug, Clone)]
pub struct SchemaMetadata {
    /// Unique identifier for this schema in the catalog
    pub schema_id: i64,
    /// Name of the schema as it appears in SQL queries
    pub schema_name: String,
    /// Path to the schema's data directory (may be relative or absolute)
    pub path: String,
    /// Whether the path is relative to the catalog's data_path
    pub path_is_relative: bool,
}

/// Metadata for a table in the DuckLake catalog
#[derive(Debug, Clone)]
pub struct TableMetadata {
    /// Unique identifier for this table in the catalog
    pub table_id: i64,
    /// Name of the table as it appears in SQL queries
    pub table_name: String,
    /// Path to the table's data directory (may be relative or absolute)
    pub path: String,
    /// Whether the path is relative to the schema's path
    pub path_is_relative: bool,
}

/// Table metadata with its schema name (for bulk queries)
#[derive(Debug, Clone)]
pub struct TableWithSchema {
    /// Name of the schema this table belongs to
    pub schema_name: String,
    /// Table metadata
    pub table: TableMetadata,
}

/// Column metadata with its schema and table names (for bulk queries)
#[derive(Debug, Clone)]
pub struct ColumnWithTable {
    /// Name of the schema this column's table belongs to
    pub schema_name: String,
    /// Name of the table this column belongs to
    pub table_name: String,
    /// Column metadata
    pub column: DuckLakeTableColumn,
}

/// File metadata with its schema and table names (for bulk queries)
#[derive(Debug, Clone)]
pub struct FileWithTable {
    /// Name of the schema this file's table belongs to
    pub schema_name: String,
    /// Name of the table this file belongs to
    pub table_name: String,
    /// File metadata
    pub file: DuckLakeTableFile,
}

/// Column definition for a DuckLake table
#[derive(Debug, Clone)]
pub struct DuckLakeTableColumn {
    /// Unique identifier for this column in the catalog
    pub column_id: i64,
    /// Name of the column
    pub column_name: String,
    /// DuckLake type string (e.g., "varchar", "int64", "decimal(10,2)")
    pub column_type: String,
}

impl DuckLakeTableColumn {
    pub fn new(column_id: i64, column_name: String, column_type: String) -> Self {
        Self {
            column_id,
            column_name,
            column_type,
        }
    }
}

/// Metadata for a data file or delete file in DuckLake
#[derive(Debug, Clone)]
pub struct DuckLakeFileData {
    /// Path to the file (may be relative or absolute)
    pub path: String,
    /// Whether the path is relative to the table's path
    pub path_is_relative: bool,
    /// Encryption key for the file (currently unused, reserved for future use)
    pub encryption_key: String,
    /// Size of the file in bytes
    pub file_size_bytes: i64,
    /// Size of the Parquet footer in bytes (optional optimization hint)
    pub footer_size: Option<i64>,
}

impl DuckLakeFileData {
    pub fn new(path: String, path_is_relative: bool, file_size_bytes: i64) -> Self {
        Self {
            path,
            path_is_relative,
            encryption_key: String::new(),
            file_size_bytes,
            footer_size: None,
        }
    }
}

/// Represents a data file and its associated delete file (if any) for a DuckLake table
#[derive(Debug, Clone)]
pub struct DuckLakeTableFile {
    /// Metadata for the data file
    pub file: DuckLakeFileData,
    /// Optional associated delete file containing deleted row positions
    pub delete_file: Option<DuckLakeFileData>,
    /// Starting row ID for this file (reserved for future use)
    pub row_id_start: Option<i64>,
    /// Snapshot ID when this file was created (reserved for future use)
    pub snapshot_id: Option<i64>,
    /// Maximum number of rows in this file (reserved for future use)
    pub max_row_count: Option<i64>,
}

impl DuckLakeTableFile {
    pub fn new(file: DuckLakeFileData) -> Self {
        Self {
            file,
            delete_file: None,
            row_id_start: None,
            snapshot_id: None,
            max_row_count: None,
        }
    }
}

pub trait MetadataProvider: Send + Sync + std::fmt::Debug {
    /// Get the current snapshot ID (dynamic, not cached)
    fn get_current_snapshot(&self) -> Result<i64>;

    /// Get the data path from catalog metadata (not snapshot-dependent)
    fn get_data_path(&self) -> Result<String>;

    /// List all snapshots in the catalog
    fn list_snapshots(&self) -> Result<Vec<SnapshotMetadata>>;

    /// List schemas for a specific snapshot
    fn list_schemas(&self, snapshot_id: i64) -> Result<Vec<SchemaMetadata>>;

    /// List tables for a specific snapshot
    fn list_tables(&self, schema_id: i64, snapshot_id: i64) -> Result<Vec<TableMetadata>>;

    /// Get table structure (columns) - not snapshot-dependent as column definitions don't change
    fn get_table_structure(&self, table_id: i64) -> Result<Vec<DuckLakeTableColumn>>;

    /// Get table files for a specific snapshot
    fn get_table_files_for_select(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<DuckLakeTableFile>>;
    //     todo: support select with file pruning

    // Dynamic lookup methods for on-demand metadata retrieval

    /// Get schema by name for a specific snapshot
    fn get_schema_by_name(&self, name: &str, snapshot_id: i64) -> Result<Option<SchemaMetadata>>;

    /// Get table by name for a specific snapshot
    fn get_table_by_name(
        &self,
        schema_id: i64,
        name: &str,
        snapshot_id: i64,
    ) -> Result<Option<TableMetadata>>;

    /// Check if table exists for a specific snapshot
    fn table_exists(&self, schema_id: i64, name: &str, snapshot_id: i64) -> Result<bool>;

    // Bulk query methods for information_schema

    /// List all tables across all schemas for a snapshot
    fn list_all_tables(&self, snapshot_id: i64) -> Result<Vec<TableWithSchema>>;

    /// List all columns across all tables for a snapshot
    fn list_all_columns(&self, snapshot_id: i64) -> Result<Vec<ColumnWithTable>>;

    /// List all files across all tables for a snapshot
    fn list_all_files(&self, snapshot_id: i64) -> Result<Vec<FileWithTable>>;
}

#[cfg(feature = "metadata-postgres")]
/// Helper function to bridge async sqlx operations to sync MetadataProvider trait
pub(crate) fn block_on<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}
