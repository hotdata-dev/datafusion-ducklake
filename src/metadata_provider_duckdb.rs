use crate::DuckLakeError;
use crate::metadata_provider::{
    DuckLakeFileData, DuckLakeTableColumn, DuckLakeTableFile, MetadataProvider, SQL_GET_DATA_FILES,
    SQL_GET_DATA_PATH, SQL_GET_LATEST_SNAPSHOT, SQL_GET_TABLE_COLUMNS, SQL_LIST_SCHEMAS,
    SQL_LIST_TABLES, SchemaMetadata, TableMetadata,
};
use duckdb::AccessMode::ReadOnly;
use duckdb::{Config, Connection};

/// DuckDB metadata provider
///
/// Opens a new connection for each query to avoid thread-safety issues.
/// This is acceptable for read-only operations.
#[derive(Debug, Clone)]
pub struct DuckdbMetadataProvider {
    catalog_path: String,
    snapshot_id: i64,
}

impl DuckdbMetadataProvider {
    /// Create a new DuckDB metadata provider
    pub fn new(catalog_path: impl Into<String>) -> crate::Result<Self> {
        let catalog_path = catalog_path.into();
        let conn = DuckdbMetadataProvider::open_connection_with_path(&catalog_path)?;

        // Query latest snapshot
        let snapshot_id: i64 = conn.query_row(SQL_GET_LATEST_SNAPSHOT, [], |row| row.get(0))?;

        Ok(Self {
            catalog_path,
            snapshot_id,
        })
    }

    fn open_connection(&self) -> crate::Result<Connection> {
        DuckdbMetadataProvider::open_connection_with_path(&self.catalog_path)
    }

    /// Open a connection to the catalog database
    fn open_connection_with_path(catalog_path: &str) -> crate::Result<Connection> {
        let config = Config::default().access_mode(ReadOnly)?;
        match Connection::open_with_flags(catalog_path, config) {
            Ok(con) => Ok(con),
            Err(msg)
                if msg
                    .to_string()
                    .starts_with("IO Error: Could not set lock on file") =>
            {
                println!("Duckdb file likely already open in write mode. Cannot connect");
                Err(DuckLakeError::DuckDb(msg))
            }
            Err(msg) => {
                println!("Failed to open duckdb");
                Err(DuckLakeError::DuckDb(msg))
            }
        }
    }
}

impl MetadataProvider for DuckdbMetadataProvider {
    fn get_current_snapshot(&self) -> crate::Result<i64> {
        Ok(self.snapshot_id)
    }

    fn get_data_path(&self) -> crate::Result<String> {
        let conn = self.open_connection()?;
        let data_path: String = conn.query_row(SQL_GET_DATA_PATH, [], |row| row.get(0))?;
        Ok(data_path)
    }

    fn list_schemas(&self) -> crate::Result<Vec<SchemaMetadata>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(SQL_LIST_SCHEMAS)?;

        let schemas = stmt
            .query_map([self.snapshot_id, self.snapshot_id], |row| {
                let schema_id: i64 = row.get(0)?;
                let schema_name: String = row.get(1)?;
                let path: String = row.get(2)?;
                let path_is_relative: bool = row.get(3)?;
                Ok(SchemaMetadata {
                    schema_id,
                    schema_name,
                    path,
                    path_is_relative,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(schemas)
    }

    fn list_tables(&self, schema_id: i64) -> crate::Result<Vec<TableMetadata>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(SQL_LIST_TABLES)?;

        let tables = stmt
            .query_map([schema_id, self.snapshot_id, self.snapshot_id], |row| {
                let table_id: i64 = row.get(0)?;
                let table_name: String = row.get(1)?;
                let path: String = row.get(2)?;
                let path_is_relative: bool = row.get(3)?;
                Ok(TableMetadata {
                    table_id,
                    table_name,
                    path,
                    path_is_relative,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(tables)
    }

    fn get_table_structure(&self, table_id: i64) -> crate::Result<Vec<DuckLakeTableColumn>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(SQL_GET_TABLE_COLUMNS)?;

        let columns = stmt
            .query_map([table_id], |row| {
                let column_id: i64 = row.get(0)?;
                let column_name: String = row.get(1)?;
                let column_type: String = row.get(2)?;
                Ok(DuckLakeTableColumn::new(
                    column_id,
                    column_name,
                    column_type,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(columns)
    }

    fn get_table_files_for_select(&self, table_id: i64) -> crate::Result<Vec<DuckLakeTableFile>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(SQL_GET_DATA_FILES)?;

        let files = stmt
            .query_map([table_id], |row| {
                let path: String = row.get(0)?;
                let path_is_relative: bool = row.get(1)?;
                let file_size_bytes: i64 = row.get(2)?;
                Ok(DuckLakeTableFile::new(DuckLakeFileData::new(
                    path,
                    path_is_relative,
                    file_size_bytes,
                )))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }
}
