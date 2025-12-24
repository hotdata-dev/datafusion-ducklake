//! PostgreSQL metadata provider for DuckLake catalogs.

use crate::Result;
use crate::metadata_provider::{
    ColumnWithTable, DuckLakeFileData, DuckLakeTableColumn, DuckLakeTableFile, FileWithTable,
    MetadataProvider, SchemaMetadata, SnapshotMetadata, TableMetadata, TableWithSchema, block_on,
};
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::types::chrono::NaiveDateTime;

macro_rules! bind_repeat {
    ($query:expr, $value:expr, 1) => {
        $query.bind($value)
    };
    ($query:expr, $value:expr, 2) => {
        $query.bind($value).bind($value)
    };
    ($query:expr, $value:expr, 3) => {
        $query.bind($value).bind($value).bind($value)
    };
    ($query:expr, $value:expr, 4) => {
        $query.bind($value).bind($value).bind($value).bind($value)
    };
    ($query:expr, $value:expr, 6) => {
        $query
            .bind($value)
            .bind($value)
            .bind($value)
            .bind($value)
            .bind($value)
            .bind($value)
    };
}

/// PostgreSQL-based metadata provider for DuckLake catalogs.
#[derive(Debug, Clone)]
pub struct PostgresMetadataProvider {
    pub pool: PgPool,
}

impl PostgresMetadataProvider {
    /// Creates a new provider for an existing DuckLake catalog.
    pub async fn new(connection_string: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(connection_string)
            .await?;

        Ok(Self {
            pool,
        })
    }
}

impl MetadataProvider for PostgresMetadataProvider {
    fn get_current_snapshot(&self) -> Result<i64> {
        block_on(async {
            let row = sqlx::query("SELECT COALESCE(MAX(snapshot_id), 0) FROM ducklake_snapshot")
                .fetch_one(&self.pool)
                .await?;
            Ok(row.try_get(0)?)
        })
    }

    fn get_data_path(&self) -> Result<String> {
        block_on(async {
            let row =
                sqlx::query("SELECT value FROM ducklake_metadata WHERE key = $1 AND scope = $2")
                    .bind("data_path")
                    .bind("")
                    .fetch_optional(&self.pool)
                    .await?;

            match row {
                Some(r) => Ok(r.try_get(0)?),
                None => Err(crate::error::DuckLakeError::InvalidConfig(
                    "Missing required catalog metadata: 'data_path' not configured. \
                     The catalog may be uninitialized or corrupted."
                        .to_string(),
                )),
            }
        })
    }

    fn list_snapshots(&self) -> Result<Vec<SnapshotMetadata>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT snapshot_id, snapshot_time
                 FROM ducklake_snapshot ORDER BY snapshot_id",
            )
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    let snapshot_id: i64 = row.try_get(0)?;
                    let timestamp: Option<NaiveDateTime> = row.try_get(1)?;
                    let timestamp_str = timestamp
                        .map(|ts: NaiveDateTime| ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string());

                    Ok(SnapshotMetadata {
                        snapshot_id,
                        timestamp: timestamp_str,
                    })
                })
                .collect()
        })
    }

    fn list_schemas(&self, snapshot_id: i64) -> Result<Vec<SchemaMetadata>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT schema_id, schema_name, path, path_is_relative FROM ducklake_schema
                 WHERE $1 >= begin_snapshot AND ($2 < end_snapshot OR end_snapshot IS NULL)",
            )
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    Ok(SchemaMetadata {
                        schema_id: row.try_get(0)?,
                        schema_name: row.try_get(1)?,
                        path: row.try_get(2)?,
                        path_is_relative: row.try_get(3)?,
                    })
                })
                .collect()
        })
    }

    fn list_tables(&self, schema_id: i64, snapshot_id: i64) -> Result<Vec<TableMetadata>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT table_id, table_name, path, path_is_relative FROM ducklake_table
                 WHERE schema_id = $1
                   AND $2 >= begin_snapshot
                   AND ($3 < end_snapshot OR end_snapshot IS NULL)",
            )
            .bind(schema_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    Ok(TableMetadata {
                        table_id: row.try_get(0)?,
                        table_name: row.try_get(1)?,
                        path: row.try_get(2)?,
                        path_is_relative: row.try_get(3)?,
                    })
                })
                .collect()
        })
    }

    fn get_table_structure(&self, table_id: i64) -> Result<Vec<DuckLakeTableColumn>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT column_id, column_name, column_type
                 FROM ducklake_column
                 WHERE table_id = $1
                 ORDER BY column_order",
            )
            .bind(table_id)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    Ok(DuckLakeTableColumn {
                        column_id: row.try_get(0)?,
                        column_name: row.try_get(1)?,
                        column_type: row.try_get(2)?,
                    })
                })
                .collect()
        })
    }

    fn get_table_files_for_select(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<DuckLakeTableFile>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT
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
                    AND del.table_id = $1
                    AND $2 >= del.begin_snapshot
                    AND ($3 < del.end_snapshot OR del.end_snapshot IS NULL)
                WHERE data.table_id = $4",
            )
            .bind(table_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(table_id)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    let data_file = DuckLakeFileData {
                        path: row.try_get(1)?,
                        path_is_relative: row.try_get(2)?,
                        file_size_bytes: row.try_get(3)?,
                        footer_size: row.try_get(4)?,
                        encryption_key: String::new(),
                    };

                    let delete_file = if row.try_get::<Option<i64>, _>(5)?.is_some() {
                        Some(DuckLakeFileData {
                            path: row.try_get(6)?,
                            path_is_relative: row.try_get(7)?,
                            file_size_bytes: row.try_get(8)?,
                            footer_size: row.try_get(9)?,
                            encryption_key: String::new(),
                        })
                    } else {
                        None
                    };

                    Ok(DuckLakeTableFile {
                        file: data_file,
                        delete_file,
                        row_id_start: None,
                        snapshot_id: None,
                        max_row_count: None,
                    })
                })
                .collect()
        })
    }

    fn get_schema_by_name(&self, name: &str, snapshot_id: i64) -> Result<Option<SchemaMetadata>> {
        block_on(async {
            let row = sqlx::query(
                "SELECT schema_id, schema_name, path, path_is_relative FROM ducklake_schema
                 WHERE schema_name = $1
                   AND $2 >= begin_snapshot
                   AND ($3 < end_snapshot OR end_snapshot IS NULL)",
            )
            .bind(name)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_optional(&self.pool)
            .await?;

            match row {
                Some(r) => Ok(Some(SchemaMetadata {
                    schema_id: r.try_get(0)?,
                    schema_name: r.try_get(1)?,
                    path: r.try_get(2)?,
                    path_is_relative: r.try_get(3)?,
                })),
                None => Ok(None),
            }
        })
    }

    fn get_table_by_name(
        &self,
        schema_id: i64,
        name: &str,
        snapshot_id: i64,
    ) -> Result<Option<TableMetadata>> {
        block_on(async {
            let row = sqlx::query(
                "SELECT table_id, table_name, path, path_is_relative FROM ducklake_table
                 WHERE schema_id = $1
                   AND table_name = $2
                   AND $3 >= begin_snapshot
                   AND ($4 < end_snapshot OR end_snapshot IS NULL)",
            )
            .bind(schema_id)
            .bind(name)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_optional(&self.pool)
            .await?;

            match row {
                Some(r) => Ok(Some(TableMetadata {
                    table_id: r.try_get(0)?,
                    table_name: r.try_get(1)?,
                    path: r.try_get(2)?,
                    path_is_relative: r.try_get(3)?,
                })),
                None => Ok(None),
            }
        })
    }

    fn table_exists(&self, schema_id: i64, name: &str, snapshot_id: i64) -> Result<bool> {
        block_on(async {
            let row = sqlx::query(
                "SELECT EXISTS(
                    SELECT 1 FROM ducklake_table
                    WHERE schema_id = $1
                      AND table_name = $2
                      AND $3 >= begin_snapshot
                      AND ($4 < end_snapshot OR end_snapshot IS NULL)
                )",
            )
            .bind(schema_id)
            .bind(name)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_one(&self.pool)
            .await?;

            Ok(row.try_get(0)?)
        })
    }

    fn list_all_tables(&self, snapshot_id: i64) -> Result<Vec<TableWithSchema>> {
        block_on(async {
            let rows = bind_repeat!(
                sqlx::query(
                    "SELECT s.schema_name, t.table_id, t.table_name, t.path, t.path_is_relative
                     FROM ducklake_schema s
                     JOIN ducklake_table t ON s.schema_id = t.schema_id
                     WHERE $1 >= s.begin_snapshot
                       AND ($2 < s.end_snapshot OR s.end_snapshot IS NULL)
                       AND $3 >= t.begin_snapshot
                       AND ($4 < t.end_snapshot OR t.end_snapshot IS NULL)
                     ORDER BY s.schema_name, t.table_name"
                ),
                snapshot_id,
                4
            )
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    let schema_name: String = row.try_get(0)?;
                    let table = TableMetadata {
                        table_id: row.try_get(1)?,
                        table_name: row.try_get(2)?,
                        path: row.try_get(3)?,
                        path_is_relative: row.try_get(4)?,
                    };
                    Ok(TableWithSchema {
                        schema_name,
                        table,
                    })
                })
                .collect()
        })
    }

    fn list_all_columns(&self, snapshot_id: i64) -> Result<Vec<ColumnWithTable>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT s.schema_name, t.table_name, c.column_id, c.column_name, c.column_type
                 FROM ducklake_schema s
                 JOIN ducklake_table t ON s.schema_id = t.schema_id
                 JOIN ducklake_column c ON t.table_id = c.table_id
                 WHERE $1 >= s.begin_snapshot
                   AND ($2 < s.end_snapshot OR s.end_snapshot IS NULL)
                   AND $3 >= t.begin_snapshot
                   AND ($4 < t.end_snapshot OR t.end_snapshot IS NULL)
                 ORDER BY s.schema_name, t.table_name, c.column_order",
            )
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    let schema_name: String = row.try_get(0)?;
                    let table_name: String = row.try_get(1)?;
                    let column = DuckLakeTableColumn {
                        column_id: row.try_get(2)?,
                        column_name: row.try_get(3)?,
                        column_type: row.try_get(4)?,
                    };
                    Ok(ColumnWithTable {
                        schema_name,
                        table_name,
                        column,
                    })
                })
                .collect()
        })
    }

    fn list_all_files(&self, snapshot_id: i64) -> Result<Vec<FileWithTable>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT
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
                    AND $1 >= del.begin_snapshot
                    AND ($2 < del.end_snapshot OR del.end_snapshot IS NULL)
                WHERE $3 >= s.begin_snapshot
                  AND ($4 < s.end_snapshot OR s.end_snapshot IS NULL)
                  AND $5 >= t.begin_snapshot
                  AND ($6 < t.end_snapshot OR t.end_snapshot IS NULL)
                ORDER BY s.schema_name, t.table_name, data.path",
            )
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    let data_file = DuckLakeFileData {
                        path: row.try_get(3)?,
                        path_is_relative: row.try_get(4)?,
                        file_size_bytes: row.try_get(5)?,
                        footer_size: row.try_get(6)?,
                        encryption_key: String::new(),
                    };

                    let delete_file = if row.try_get::<Option<i64>, _>(7)?.is_some() {
                        Some(DuckLakeFileData {
                            path: row.try_get(8)?,
                            path_is_relative: row.try_get(9)?,
                            file_size_bytes: row.try_get(10)?,
                            footer_size: row.try_get(11)?,
                            encryption_key: String::new(),
                        })
                    } else {
                        None
                    };

                    Ok(FileWithTable {
                        schema_name: row.try_get(0)?,
                        table_name: row.try_get(1)?,
                        file: DuckLakeTableFile {
                            file: data_file,
                            delete_file,
                            row_id_start: None,
                            snapshot_id: None,
                            max_row_count: row.try_get(12)?,
                        },
                    })
                })
                .collect()
        })
    }
}
