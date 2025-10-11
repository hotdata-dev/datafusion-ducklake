use crate::{DuckLakeSchema, DuckLakeTable, Result};

pub struct DuckLakeTableColumn {
    column_id: i64,
    column_name: String,
    column_type: String
}

pub struct DuckLakeFileData {
    path: String,
    encryption_key: String,
    file_size_bytes: i64,
    footer_size: Option<i64>,
}

pub struct DuckLakeTableFile {
    file: DuckLakeFileData,
    delete_file: Option<DuckLakeFileData>,
    row_id_start: Option<i64>,
    snapshot_id: Option<i64>,
    max_row_count: Option<i64>,
}

pub trait MetadataProvider {
    fn get_current_snapshot() -> Result<i64>;
    fn list_schemas() -> Result<Vec<DuckLakeSchema>>;
    fn list_tables() -> Result<Vec<DuckLakeTable>>;
    fn get_table_structure(table_id: i64) -> Result<Vec<DuckLakeTableColumn>>;
    fn get_table_files_for_select(table_id: i64) -> Result<Vec<DuckLakeTableFile>>;
//     todo: support select with file pruning
}
