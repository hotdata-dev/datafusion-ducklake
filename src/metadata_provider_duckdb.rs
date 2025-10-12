use duckdb::Connection;
use crate::{DuckLakeSchema, DuckLakeTable};
use crate::metadata_provider::{
    DuckLakeTableColumn, DuckLakeTableFile, DuckLakeFileData, MetadataProvider,
    SQL_GET_LATEST_SNAPSHOT, SQL_LIST_SCHEMAS, SQL_LIST_TABLES,
    SQL_GET_TABLE_COLUMNS, SQL_GET_DATA_FILES,
};

pub struct DuckdbMetadataProvider {
    conn: Connection,
    catalog_path: String,
    snapshot_id: i64,
}

impl DuckdbMetadataProvider {
    /// Create a new DuckDB metadata provider
    pub fn new(catalog_path: impl Into<String>) -> crate::Result<Self> {
        let catalog_path = catalog_path.into();
        let conn = Connection::open(&catalog_path)?;

        // Query latest snapshot
        let snapshot_id: i64 = conn.query_row(SQL_GET_LATEST_SNAPSHOT, [], |row| row.get(0))?;

        Ok(Self {
            conn,
            catalog_path,
            snapshot_id,
        })
    }
}

impl MetadataProvider for DuckdbMetadataProvider {
    fn get_current_snapshot(&self) -> crate::Result<i64> {
        Ok(self.snapshot_id)
    }

    fn list_schemas(&self) -> crate::Result<Vec<DuckLakeSchema>> {
        let mut stmt = self.conn.prepare(SQL_LIST_SCHEMAS)?;

        let schemas = stmt.query_map([self.snapshot_id, self.snapshot_id], |row| {
            let schema_id: i64 = row.get(0)?;
            let schema_name: String = row.get(1)?;
            Ok((schema_id, schema_name))
        })?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|(schema_id, schema_name)| {
            DuckLakeSchema::new(
                schema_id,
                schema_name,
                self.catalog_path.clone(),
                self.snapshot_id,
            )
        })
        .collect();

        Ok(schemas)
    }

    fn list_tables(&self, schema_id: i64) -> crate::Result<Vec<DuckLakeTable>> {
        let mut stmt = self.conn.prepare(SQL_LIST_TABLES)?;

        let tables = stmt.query_map([schema_id, self.snapshot_id, self.snapshot_id], |row| {
            let table_id: i64 = row.get(0)?;
            let table_name: String = row.get(1)?;
            Ok((table_id, table_name))
        })?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|(table_id, table_name)| {
            DuckLakeTable::new(
                table_id,
                table_name,
                self.catalog_path.clone(),
                self.snapshot_id,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

        Ok(tables)
    }

    fn get_table_structure(&self, table_id: i64) -> crate::Result<Vec<DuckLakeTableColumn>> {
        let mut stmt = self.conn.prepare(SQL_GET_TABLE_COLUMNS)?;

        let columns = stmt.query_map([table_id], |row| {
            let column_id: i64 = row.get(0)?;
            let column_name: String = row.get(1)?;
            let column_type: String = row.get(2)?;
            Ok(DuckLakeTableColumn::new(column_id, column_name, column_type))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(columns)
    }

    fn get_table_files_for_select(&self, table_id: i64) -> crate::Result<Vec<DuckLakeTableFile>> {
        let mut stmt = self.conn.prepare(SQL_GET_DATA_FILES)?;

        let files = stmt.query_map([table_id], |row| {
            let path: String = row.get(0)?;
            let file_size_bytes: i64 = row.get(1)?;
            Ok(DuckLakeTableFile::new(DuckLakeFileData::new(path, file_size_bytes)))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }
}