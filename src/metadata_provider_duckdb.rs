use crate::DuckLakeError;
use crate::metadata_provider::{
    ColumnWithTable, DataFileChange, DeleteFileChange, DuckLakeFileData, DuckLakeTableColumn,
    DuckLakeTableFile, FileWithTable, MetadataProvider, SQL_GET_DATA_FILES,
    SQL_GET_DATA_FILES_ADDED_BETWEEN_SNAPSHOTS, SQL_GET_DATA_PATH,
    SQL_GET_DELETE_FILES_ADDED_BETWEEN_SNAPSHOTS, SQL_GET_LATEST_SNAPSHOT, SQL_GET_SCHEMA_BY_NAME,
    SQL_GET_TABLE_BY_NAME, SQL_GET_TABLE_COLUMNS, SQL_LIST_ALL_COLUMNS, SQL_LIST_ALL_FILES,
    SQL_LIST_ALL_TABLES, SQL_LIST_SCHEMAS, SQL_LIST_SNAPSHOTS, SQL_LIST_TABLES, SQL_TABLE_EXISTS,
    SchemaMetadata, SnapshotMetadata, TableMetadata, TableWithSchema,
};
use duckdb::AccessMode::ReadOnly;
use duckdb::{Config, Connection, params};

/// DuckDB metadata provider
///
/// Opens a new connection for each query to avoid thread-safety issues.
/// This is acceptable for read-only operations.
#[derive(Debug, Clone)]
pub struct DuckdbMetadataProvider {
    catalog_path: String,
}

impl DuckdbMetadataProvider {
    /// Create a new DuckDB metadata provider
    pub fn new(catalog_path: impl Into<String>) -> crate::Result<Self> {
        let catalog_path = catalog_path.into();

        // Validate connection works
        let _conn = DuckdbMetadataProvider::open_connection_with_path(&catalog_path)?;

        Ok(Self {
            catalog_path,
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
                tracing::warn!(
                    error = %msg,
                    "DuckDB file likely already open in write mode. Cannot connect"
                );
                Err(DuckLakeError::DuckDb(msg))
            },
            Err(msg) => {
                tracing::error!(error = %msg, "Failed to open DuckDB catalog");
                Err(DuckLakeError::DuckDb(msg))
            },
        }
    }
}

impl MetadataProvider for DuckdbMetadataProvider {
    fn get_current_snapshot(&self) -> crate::Result<i64> {
        let conn = self.open_connection()?;
        let snapshot_id: i64 = conn.query_row(SQL_GET_LATEST_SNAPSHOT, [], |row| row.get(0))?;
        Ok(snapshot_id)
    }

    fn get_data_path(&self) -> crate::Result<String> {
        let conn = self.open_connection()?;
        let data_path: String = conn.query_row(SQL_GET_DATA_PATH, [], |row| row.get(0))?;
        Ok(data_path)
    }

    fn list_snapshots(&self) -> crate::Result<Vec<SnapshotMetadata>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(SQL_LIST_SNAPSHOTS)?;

        let snapshots = stmt
            .query_map([], |row| {
                let snapshot_id: i64 = row.get(0)?;
                let timestamp: Option<String> = row.get(1)?;
                Ok(SnapshotMetadata {
                    snapshot_id,
                    timestamp,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(snapshots)
    }

    fn list_schemas(&self, snapshot_id: i64) -> crate::Result<Vec<SchemaMetadata>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(SQL_LIST_SCHEMAS)?;

        let schemas = stmt
            .query_map([snapshot_id, snapshot_id], |row| {
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

    fn list_tables(&self, schema_id: i64, snapshot_id: i64) -> crate::Result<Vec<TableMetadata>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(SQL_LIST_TABLES)?;

        let tables = stmt
            .query_map([schema_id, snapshot_id, snapshot_id], |row| {
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

    fn get_table_files_for_select(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> crate::Result<Vec<DuckLakeTableFile>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(SQL_GET_DATA_FILES)?;

        let files = stmt
            .query_map([table_id, snapshot_id, snapshot_id, table_id], |row| {
                // Parse data file (columns 0-4)
                let _data_file_id: i64 = row.get(0)?;
                let data_file = DuckLakeFileData {
                    path: row.get(1)?,
                    path_is_relative: row.get(2)?,
                    file_size_bytes: row.get(3)?,
                    footer_size: row.get(4)?,
                    encryption_key: String::new(), // TODO: handle encryption
                };

                // Parse delete file (columns 5-10) if exists
                let delete_file = if let Ok(Some(_)) = row.get::<_, Option<i64>>(5) {
                    Some(DuckLakeFileData {
                        path: row.get(6)?,
                        path_is_relative: row.get(7)?,
                        file_size_bytes: row.get(8)?,
                        footer_size: row.get(9)?,
                        encryption_key: String::new(),
                    })
                } else {
                    None
                };

                let _delete_count: Option<i64> = row.get(10)?;

                Ok(DuckLakeTableFile {
                    file: data_file,
                    delete_file,
                    row_id_start: None,
                    snapshot_id: Some(snapshot_id),
                    max_row_count: None, // Set to None until we have actual row count from data file metadata
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    fn get_schema_by_name(
        &self,
        name: &str,
        snapshot_id: i64,
    ) -> crate::Result<Option<SchemaMetadata>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(SQL_GET_SCHEMA_BY_NAME)?;

        let mut rows = stmt.query(params![name, snapshot_id, snapshot_id])?;

        if let Some(row) = rows.next()? {
            let schema_id: i64 = row.get(0)?;
            let schema_name: String = row.get(1)?;
            let path: String = row.get(2)?;
            let path_is_relative: bool = row.get(3)?;
            Ok(Some(SchemaMetadata {
                schema_id,
                schema_name,
                path,
                path_is_relative,
            }))
        } else {
            Ok(None)
        }
    }

    fn get_table_by_name(
        &self,
        schema_id: i64,
        name: &str,
        snapshot_id: i64,
    ) -> crate::Result<Option<TableMetadata>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(SQL_GET_TABLE_BY_NAME)?;

        let mut rows = stmt.query(params![&schema_id, &name, &snapshot_id, &snapshot_id])?;

        if let Some(row) = rows.next()? {
            let table_id: i64 = row.get(0)?;
            let table_name: String = row.get(1)?;
            let path: String = row.get(2)?;
            let path_is_relative: bool = row.get(3)?;
            Ok(Some(TableMetadata {
                table_id,
                table_name,
                path,
                path_is_relative,
            }))
        } else {
            Ok(None)
        }
    }

    fn table_exists(&self, schema_id: i64, name: &str, snapshot_id: i64) -> crate::Result<bool> {
        let conn = self.open_connection()?;
        let exists: bool = conn.query_row(
            SQL_TABLE_EXISTS,
            params![schema_id, &name, &snapshot_id, &snapshot_id],
            |row| row.get(0),
        )?;
        Ok(exists)
    }

    fn list_all_tables(&self, snapshot_id: i64) -> crate::Result<Vec<TableWithSchema>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(SQL_LIST_ALL_TABLES)?;

        let tables = stmt
            .query_map(
                params![snapshot_id, snapshot_id, snapshot_id, snapshot_id],
                |row| {
                    let schema_name: String = row.get(0)?;
                    let table = TableMetadata {
                        table_id: row.get(1)?,
                        table_name: row.get(2)?,
                        path: row.get(3)?,
                        path_is_relative: row.get(4)?,
                    };
                    Ok(TableWithSchema {
                        schema_name,
                        table,
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(tables)
    }

    fn list_all_columns(&self, snapshot_id: i64) -> crate::Result<Vec<ColumnWithTable>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(SQL_LIST_ALL_COLUMNS)?;

        let columns = stmt
            .query_map(
                params![snapshot_id, snapshot_id, snapshot_id, snapshot_id],
                |row| {
                    let schema_name: String = row.get(0)?;
                    let table_name: String = row.get(1)?;
                    let column = DuckLakeTableColumn {
                        column_id: row.get(2)?,
                        column_name: row.get(3)?,
                        column_type: row.get(4)?,
                    };
                    Ok(ColumnWithTable {
                        schema_name,
                        table_name,
                        column,
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(columns)
    }

    fn list_all_files(&self, snapshot_id: i64) -> crate::Result<Vec<FileWithTable>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(SQL_LIST_ALL_FILES)?;

        let files = stmt
            .query_map(
                params![
                    snapshot_id,
                    snapshot_id,
                    snapshot_id,
                    snapshot_id,
                    snapshot_id,
                    snapshot_id
                ],
                |row| {
                    let schema_name: String = row.get(0)?;
                    let table_name: String = row.get(1)?;

                    // Parse data file (skip column 2: data_file_id, only used for JOIN)
                    let data_file = DuckLakeFileData {
                        path: row.get(3)?,
                        path_is_relative: row.get(4)?,
                        file_size_bytes: row.get(5)?,
                        footer_size: row.get(6)?,
                        encryption_key: String::new(),
                    };

                    // Parse optional delete file (column 7: delete_file_id, check if exists but don't store)
                    let delete_file =
                        if let Ok(Some(_delete_file_id)) = row.get::<_, Option<i64>>(7) {
                            Some(DuckLakeFileData {
                                path: row.get(8)?,
                                path_is_relative: row.get(9)?,
                                file_size_bytes: row.get(10)?,
                                footer_size: row.get(11)?,
                                encryption_key: String::new(),
                            })
                        } else {
                            None
                        };

                    let max_row_count = row.get::<_, Option<i64>>(12)?;

                    Ok(FileWithTable {
                        schema_name,
                        table_name,
                        file: DuckLakeTableFile {
                            file: data_file,
                            delete_file,
                            row_id_start: None,
                            snapshot_id: None,
                            max_row_count,
                        },
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    fn get_data_files_added_between_snapshots(
        &self,
        table_id: i64,
        start_snapshot: i64,
        end_snapshot: i64,
    ) -> crate::Result<Vec<DataFileChange>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(SQL_GET_DATA_FILES_ADDED_BETWEEN_SNAPSHOTS)?;

        let files = stmt
            .query_map(params![table_id, start_snapshot, end_snapshot], |row| {
                Ok(DataFileChange {
                    begin_snapshot: row.get(0)?,
                    path: row.get(1)?,
                    path_is_relative: row.get(2)?,
                    file_size_bytes: row.get(3)?,
                    footer_size: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    fn get_delete_files_added_between_snapshots(
        &self,
        table_id: i64,
        start_snapshot: i64,
        end_snapshot: i64,
    ) -> crate::Result<Vec<DeleteFileChange>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(SQL_GET_DELETE_FILES_ADDED_BETWEEN_SNAPSHOTS)?;

        let files = stmt
            .query_map(params![table_id, start_snapshot, end_snapshot], |row| {
                Ok(DeleteFileChange {
                    begin_snapshot: row.get(0)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }
}
