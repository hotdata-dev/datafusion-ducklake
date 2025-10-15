use crate::Result;

// SQL queries for DuckLake catalog tables
// These queries are database-agnostic and work with DuckDB, SQLite, PostgreSQL, MySQL
pub const SQL_GET_LATEST_SNAPSHOT: &str =
    "SELECT COALESCE(MAX(snapshot_id), 0) FROM ducklake_snapshot";

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

/// Simple schema metadata returned by MetadataProvider
#[derive(Debug, Clone)]
pub struct SchemaMetadata {
    pub schema_id: i64,
    pub schema_name: String,
    pub path: String,
    pub path_is_relative: bool,
}

/// Simple table metadata returned by MetadataProvider
#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub table_id: i64,
    pub table_name: String,
    pub path: String,
    pub path_is_relative: bool,
}

pub struct DuckLakeTableColumn {
    pub column_id: i64,
    pub column_name: String,
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

#[derive(Debug)]
pub struct DuckLakeFileData {
    pub path: String,
    pub path_is_relative: bool,
    pub encryption_key: String,
    pub file_size_bytes: i64,
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

#[derive(Debug)]
pub struct DuckLakeTableFile {
    pub file: DuckLakeFileData,
    pub delete_file: Option<DuckLakeFileData>,
    pub row_id_start: Option<i64>,
    pub snapshot_id: Option<i64>,
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
    fn get_current_snapshot(&self) -> Result<i64>;
    fn get_data_path(&self) -> Result<String>;
    fn list_schemas(&self) -> Result<Vec<SchemaMetadata>>;
    fn list_tables(&self, schema_id: i64) -> Result<Vec<TableMetadata>>;
    fn get_table_structure(&self, table_id: i64) -> Result<Vec<DuckLakeTableColumn>>;
    fn get_table_files_for_select(&self, table_id: i64) -> Result<Vec<DuckLakeTableFile>>;
    //     todo: support select with file pruning
}
