use crate::{DuckLakeSchema, DuckLakeTable, Result};

// SQL queries for DuckLake catalog tables
// These queries are database-agnostic and work with DuckDB, SQLite, PostgreSQL, MySQL
pub const SQL_GET_LATEST_SNAPSHOT: &str =
    "SELECT COALESCE(MAX(snapshot_id), 0) FROM ducklake_snapshot";

pub const SQL_LIST_SCHEMAS: &str =
    "SELECT schema_id, schema_name FROM ducklake_schema
     WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

pub const SQL_LIST_TABLES: &str =
    "SELECT table_id, table_name FROM ducklake_table
     WHERE schema_id = ?
       AND ? >= begin_snapshot
       AND (? < end_snapshot OR end_snapshot IS NULL)";

pub const SQL_GET_TABLE_COLUMNS: &str =
    "SELECT column_id, column_name, column_type
     FROM ducklake_column
     WHERE table_id = ?
     ORDER BY column_order";

pub const SQL_GET_DATA_FILES: &str =
    "SELECT path, file_size_bytes
     FROM ducklake_data_file
     WHERE table_id = ?";

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

pub struct DuckLakeFileData {
    pub path: String,
    pub encryption_key: String,
    pub file_size_bytes: i64,
    pub footer_size: Option<i64>,
}

impl DuckLakeFileData {
    pub fn new(path: String, file_size_bytes: i64) -> Self {
        Self {
            path,
            encryption_key: String::new(),
            file_size_bytes,
            footer_size: None,
        }
    }
}

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

pub trait MetadataProvider {
    fn get_current_snapshot(&self) -> Result<i64>;
    fn list_schemas(&self) -> Result<Vec<DuckLakeSchema>>;
    fn list_tables(&self, schema_id: i64) -> Result<Vec<DuckLakeTable>>;
    fn get_table_structure(&self, table_id: i64) -> Result<Vec<DuckLakeTableColumn>>;
    fn get_table_files_for_select(&self, table_id: i64) -> Result<Vec<DuckLakeTableFile>>;
//     todo: support select with file pruning
}
