//! PostgreSQL metadata provider for DuckLake catalogs.

use crate::Result;
use crate::inlining::{
    CatalogInliningReader, InlineRow, InlinedFileDelete, InlinedTableRef,
};
use crate::metadata_provider::{
    ColumnWithTable, DataFileChange, DeleteFileChange, DuckLakeFileData, DuckLakeTableColumn,
    DuckLakeTableFile, FileWithTable, MetadataProvider, SchemaMetadata, SnapshotMetadata,
    TableMetadata, TableWithSchema, reconstruct_list_columns, reconstruct_list_columns_with_table,
};
use async_trait::async_trait;
use crate::types::ducklake_to_arrow_type;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

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
    ($query:expr, $value:expr, 8) => {
        $query
            .bind($value)
            .bind($value)
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

    fn quote_ident(identifier: &str) -> String {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }

    fn build_inline_projection(columns: &[DuckLakeTableColumn]) -> Result<String> {
        let mut projection = vec!["row_id".to_string()];

        for (index, column) in columns.iter().enumerate() {
            let alias = format!("col{}", index + 1);
            let quoted = Self::quote_ident(&column.column_name);
            let expr = match ducklake_to_arrow_type(&column.column_type)? {
                arrow::datatypes::DataType::Utf8
                | arrow::datatypes::DataType::LargeUtf8
                | arrow::datatypes::DataType::Binary
                | arrow::datatypes::DataType::LargeBinary
                | arrow::datatypes::DataType::FixedSizeBinary(_) => {
                    format!(
                        "CASE WHEN {quoted} IS NULL THEN NULL ELSE encode({quoted}, 'hex') END AS {alias}"
                    )
                },
                _ => format!("CAST({quoted} AS TEXT) AS {alias}"),
            };
            projection.push(expr);
        }

        Ok(projection.join(", "))
    }
}

#[async_trait]
impl MetadataProvider for PostgresMetadataProvider {
    fn catalog_inlining_reader(&self) -> Option<&dyn CatalogInliningReader> {
        Some(self)
    }

    async fn get_current_snapshot(&self) -> Result<i64> {
        let row = sqlx::query("SELECT COALESCE(MAX(snapshot_id), 0) FROM ducklake_snapshot")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.try_get(0)?)
    }

    async fn get_data_path(&self) -> Result<String> {
        let row =
            sqlx::query("SELECT value FROM ducklake_metadata WHERE key = $1 AND scope IS NULL")
                .bind("data_path")
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
    }

    async fn list_snapshots(&self) -> Result<Vec<SnapshotMetadata>> {
        let rows = sqlx::query(
            "SELECT snapshot_id, CAST(snapshot_time AS VARCHAR)
             FROM ducklake_snapshot ORDER BY snapshot_id",
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(SnapshotMetadata {
                    snapshot_id: row.try_get(0)?,
                    timestamp: row.try_get(1)?,
                })
            })
            .collect()
    }

    async fn list_schemas(&self, snapshot_id: i64) -> Result<Vec<SchemaMetadata>> {
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
    }

    async fn list_tables(&self, schema_id: i64, snapshot_id: i64) -> Result<Vec<TableMetadata>> {
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
    }

    async fn get_table_structure(&self, table_id: i64) -> Result<Vec<DuckLakeTableColumn>> {
        let rows = sqlx::query(
            "SELECT column_id, column_name, column_type, nulls_allowed, parent_column
             FROM ducklake_column
             WHERE table_id = $1 AND end_snapshot IS NULL
             ORDER BY column_order",
        )
        .bind(table_id)
        .fetch_all(&self.pool)
        .await?;

        let raw: Result<Vec<(DuckLakeTableColumn, Option<i64>)>> = rows
            .into_iter()
            .map(|row| {
                let nulls_allowed: Option<bool> = row.try_get(3)?;
                let parent_column: Option<i64> = row.try_get(4)?;
                Ok((
                    DuckLakeTableColumn {
                        column_id: row.try_get(0)?,
                        column_name: row.try_get(1)?,
                        column_type: row.try_get(2)?,
                        is_nullable: nulls_allowed.unwrap_or(true),
                    },
                    parent_column,
                ))
            })
            .collect();
        Ok(reconstruct_list_columns(raw?))
    }

    async fn get_table_files_for_select(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<DuckLakeTableFile>> {
        let rows = sqlx::query(
            "SELECT
                data.data_file_id,
                data.path AS data_file_path,
                data.path_is_relative AS data_path_is_relative,
                data.file_size_bytes AS data_file_size,
                data.footer_size AS data_footer_size,
                data.encryption_key AS data_encryption_key,
                data.row_id_start,
                data.begin_snapshot,
                data.record_count,
                del.delete_file_id,
                del.path AS delete_file_path,
                del.path_is_relative AS delete_path_is_relative,
                del.file_size_bytes AS delete_file_size,
                del.footer_size AS delete_footer_size,
                del.encryption_key AS delete_encryption_key,
                del.delete_count
            FROM ducklake_data_file AS data
            LEFT JOIN ducklake_delete_file AS del
                ON data.data_file_id = del.data_file_id
                AND del.table_id = $1
                AND $2 >= del.begin_snapshot
                AND ($3 < del.end_snapshot OR del.end_snapshot IS NULL)
            WHERE data.table_id = $4
              AND $5 >= data.begin_snapshot
              AND ($6 < data.end_snapshot OR data.end_snapshot IS NULL)",
        )
        .bind(table_id)
        .bind(snapshot_id)
        .bind(snapshot_id)
        .bind(table_id)
        .bind(snapshot_id)
        .bind(snapshot_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let data_file = DuckLakeFileData {
                    path: row.try_get(1)?,
                    path_is_relative: row.try_get(2)?,
                    file_size_bytes: row.try_get(3)?,
                    footer_size: row.try_get(4)?,
                    encryption_key: row.try_get(5)?,
                };

                let delete_file = if row.try_get::<Option<i64>, _>(9)?.is_some() {
                    Some(DuckLakeFileData {
                        path: row.try_get(10)?,
                        path_is_relative: row.try_get(11)?,
                        file_size_bytes: row.try_get(12)?,
                        footer_size: row.try_get(13)?,
                        encryption_key: row.try_get(14)?,
                    })
                } else {
                    None
                };

                Ok(DuckLakeTableFile {
                    data_file_id: row.try_get(0)?,
                    file: data_file,
                    delete_file,
                    row_id_start: row.try_get(6)?,
                    snapshot_id: row.try_get(7)?,
                    max_row_count: row.try_get(8)?,
                })
            })
            .collect()
    }

    async fn get_schema_by_name(
        &self,
        name: &str,
        snapshot_id: i64,
    ) -> Result<Option<SchemaMetadata>> {
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
    }

    async fn get_table_by_name(
        &self,
        schema_id: i64,
        name: &str,
        snapshot_id: i64,
    ) -> Result<Option<TableMetadata>> {
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
    }

    async fn table_exists(&self, schema_id: i64, name: &str, snapshot_id: i64) -> Result<bool> {
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
    }

    async fn list_all_tables(&self, snapshot_id: i64) -> Result<Vec<TableWithSchema>> {
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
    }

    async fn list_all_columns(&self, snapshot_id: i64) -> Result<Vec<ColumnWithTable>> {
        let rows = sqlx::query(
            "SELECT s.schema_name, t.table_name, c.column_id, c.column_name, c.column_type, c.nulls_allowed, c.parent_column
             FROM ducklake_schema s
             JOIN ducklake_table t ON s.schema_id = t.schema_id
             JOIN ducklake_column c ON t.table_id = c.table_id
             WHERE $1 >= s.begin_snapshot
               AND ($2 < s.end_snapshot OR s.end_snapshot IS NULL)
               AND $3 >= t.begin_snapshot
               AND ($4 < t.end_snapshot OR t.end_snapshot IS NULL)
               AND $5 >= c.begin_snapshot
               AND ($6 < c.end_snapshot OR c.end_snapshot IS NULL)
             ORDER BY s.schema_name, t.table_name, c.column_order",
        )
        .bind(snapshot_id)
        .bind(snapshot_id)
        .bind(snapshot_id)
        .bind(snapshot_id)
        .bind(snapshot_id)
        .bind(snapshot_id)
        .fetch_all(&self.pool)
        .await?;

        let raw: Result<Vec<(ColumnWithTable, Option<i64>)>> = rows
            .into_iter()
            .map(|row| {
                let schema_name: String = row.try_get(0)?;
                let table_name: String = row.try_get(1)?;
                let nulls_allowed: Option<bool> = row.try_get(5)?;
                let parent_column: Option<i64> = row.try_get(6)?;
                let column = DuckLakeTableColumn {
                    column_id: row.try_get(2)?,
                    column_name: row.try_get(3)?,
                    column_type: row.try_get(4)?,
                    is_nullable: nulls_allowed.unwrap_or(true),
                };
                Ok((
                    ColumnWithTable {
                        schema_name,
                        table_name,
                        column,
                    },
                    parent_column,
                ))
            })
            .collect();
        Ok(reconstruct_list_columns_with_table(raw?))
    }

    async fn list_all_files(&self, snapshot_id: i64) -> Result<Vec<FileWithTable>> {
        let rows = sqlx::query(
            "SELECT
                    s.schema_name,
                    t.table_name,
                    data.data_file_id,
                    data.path AS data_file_path,
                    data.path_is_relative AS data_path_is_relative,
                    data.file_size_bytes AS data_file_size,
                    data.footer_size AS data_footer_size,
                    data.encryption_key AS data_encryption_key,
                    data.row_id_start,
                    data.begin_snapshot,
                    data.record_count,
                    del.delete_file_id,
                    del.path AS delete_file_path,
                    del.path_is_relative AS delete_path_is_relative,
                    del.file_size_bytes AS delete_file_size,
                    del.footer_size AS delete_footer_size,
                    del.encryption_key AS delete_encryption_key,
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
                  AND $7 >= data.begin_snapshot
                  AND ($8 < data.end_snapshot OR data.end_snapshot IS NULL)
                ORDER BY s.schema_name, t.table_name, data.path",
        )
        .bind(snapshot_id)
        .bind(snapshot_id)
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
                    encryption_key: row.try_get(7)?,
                };

                let delete_file = if row.try_get::<Option<i64>, _>(11)?.is_some() {
                    Some(DuckLakeFileData {
                        path: row.try_get(12)?,
                        path_is_relative: row.try_get(13)?,
                        file_size_bytes: row.try_get(14)?,
                        footer_size: row.try_get(15)?,
                        encryption_key: row.try_get(16)?,
                    })
                } else {
                    None
                };

                Ok(FileWithTable {
                    schema_name: row.try_get(0)?,
                    table_name: row.try_get(1)?,
                    file: DuckLakeTableFile {
                        data_file_id: row.try_get(2)?,
                        file: data_file,
                        delete_file,
                        row_id_start: row.try_get(8)?,
                        snapshot_id: row.try_get(9)?,
                        max_row_count: row.try_get(10)?,
                    },
                })
            })
            .collect()
    }

    async fn get_data_files_added_between_snapshots(
        &self,
        table_id: i64,
        start_snapshot: i64,
        end_snapshot: i64,
    ) -> Result<Vec<DataFileChange>> {
        let rows = sqlx::query(
            "SELECT
                data.begin_snapshot,
                data.path,
                data.path_is_relative,
                data.file_size_bytes,
                data.footer_size,
                data.encryption_key
            FROM ducklake_data_file AS data
            WHERE data.table_id = $1
              AND data.begin_snapshot > $2
              AND data.begin_snapshot <= $3
            ORDER BY data.begin_snapshot",
        )
        .bind(table_id)
        .bind(start_snapshot)
        .bind(end_snapshot)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(DataFileChange {
                    begin_snapshot: row.try_get(0)?,
                    path: row.try_get(1)?,
                    path_is_relative: row.try_get(2)?,
                    file_size_bytes: row.try_get(3)?,
                    footer_size: row.try_get(4)?,
                    encryption_key: row.try_get(5)?,
                })
            })
            .collect()
    }

    async fn get_delete_files_added_between_snapshots(
        &self,
        table_id: i64,
        start_snapshot: i64,
        end_snapshot: i64,
    ) -> Result<Vec<DeleteFileChange>> {
        // PostgreSQL equivalent of DuckDB's SQL_GET_DELETE_FILES_ADDED_BETWEEN_SNAPSHOTS
        // Uses LATERAL joins instead of MAX_BY/COLUMNS
        let rows = sqlx::query(
            r#"
WITH current_delete AS (
    SELECT
        ddf.data_file_id,
        ddf.begin_snapshot,
        ddf.path,
        ddf.path_is_relative,
        ddf.file_size_bytes,
        ddf.footer_size,
        ddf.encryption_key
    FROM ducklake_delete_file ddf
    WHERE ddf.table_id = $1
      AND ddf.begin_snapshot > $2
      AND ddf.begin_snapshot <= $3
),

data_files AS (
    SELECT df.*
    FROM ducklake_data_file df
    WHERE df.table_id = $1
)

-- Part 1: Incremental deletes
SELECT
    data.path,
    data.path_is_relative,
    data.file_size_bytes,
    data.footer_size,
    data.row_id_start,
    data.record_count,
    data.mapping_id,
    current_delete.path,
    current_delete.path_is_relative,
    current_delete.file_size_bytes,
    current_delete.footer_size,
    prev.path,
    prev.path_is_relative,
    prev.file_size_bytes,
    prev.footer_size,
    current_delete.begin_snapshot
FROM current_delete
JOIN data_files data USING (data_file_id)
LEFT JOIN LATERAL (
    SELECT
        ddf.path,
        ddf.path_is_relative,
        ddf.file_size_bytes,
        ddf.footer_size
    FROM ducklake_delete_file ddf
    WHERE ddf.table_id = $1
      AND ddf.data_file_id = current_delete.data_file_id
      AND ddf.begin_snapshot < current_delete.begin_snapshot
    ORDER BY ddf.begin_snapshot DESC
    LIMIT 1
) prev ON true

UNION ALL

-- Part 2: Full file deletes
SELECT
    data.path,
    data.path_is_relative,
    data.file_size_bytes,
    data.footer_size,
    data.row_id_start,
    data.record_count,
    data.mapping_id,
    NULL::VARCHAR,
    NULL::BOOLEAN,
    NULL::BIGINT,
    NULL::BIGINT,
    prev.path,
    prev.path_is_relative,
    prev.file_size_bytes,
    prev.footer_size,
    data.end_snapshot
FROM ducklake_data_file data
LEFT JOIN LATERAL (
    SELECT
        ddf.path,
        ddf.path_is_relative,
        ddf.file_size_bytes,
        ddf.footer_size
    FROM ducklake_delete_file ddf
    WHERE ddf.table_id = $1
      AND ddf.data_file_id = data.data_file_id
      AND ddf.begin_snapshot < data.end_snapshot
    ORDER BY ddf.begin_snapshot DESC
    LIMIT 1
) prev ON true
WHERE data.table_id = $1
  AND data.end_snapshot > $2
  AND data.end_snapshot <= $3
"#,
        )
        .bind(table_id)
        .bind(start_snapshot)
        .bind(end_snapshot)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(DeleteFileChange {
                    data_file_path: row.try_get(0)?,
                    data_file_path_is_relative: row.try_get(1)?,
                    data_file_size_bytes: row.try_get(2)?,
                    data_file_footer_size: row.try_get(3)?,
                    data_row_id_start: row.try_get(4)?,
                    data_record_count: row.try_get(5)?,
                    data_mapping_id: row.try_get(6)?,
                    current_delete_path: row.try_get(7)?,
                    current_delete_path_is_relative: row.try_get(8)?,
                    current_delete_file_size_bytes: row.try_get(9)?,
                    current_delete_footer_size: row.try_get(10)?,
                    previous_delete_path: row.try_get(11)?,
                    previous_delete_path_is_relative: row.try_get(12)?,
                    previous_delete_file_size_bytes: row.try_get(13)?,
                    previous_delete_footer_size: row.try_get(14)?,
                    snapshot_id: row.try_get(15)?,
                })
            })
            .collect()
    }
}

#[async_trait]
impl CatalogInliningReader for PostgresMetadataProvider {
    async fn get_inlined_table_refs(&self, table_id: i64) -> Result<Vec<InlinedTableRef>> {
        let rows = sqlx::query(
            "SELECT table_name, schema_version
             FROM ducklake_inlined_data_tables
             WHERE table_id = $1
             ORDER BY schema_version",
        )
        .bind(table_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(InlinedTableRef {
                    table_name: row.try_get(0)?,
                    schema_version: row.try_get(1)?,
                })
            })
            .collect()
    }

    async fn get_historical_schema(
        &self,
        table_id: i64,
        schema_version: i64,
    ) -> Result<Vec<DuckLakeTableColumn>> {
        let begin_snapshot_row = sqlx::query(
            "SELECT begin_snapshot
             FROM ducklake_schema_versions
             WHERE table_id = $1 AND schema_version = $2",
        )
        .bind(table_id)
        .bind(schema_version)
        .fetch_optional(&self.pool)
        .await?;

        let begin_snapshot: i64 = begin_snapshot_row
            .ok_or_else(|| {
                crate::DuckLakeError::InvalidSnapshot(format!(
                    "Missing schema version {} for table {}",
                    schema_version, table_id
                ))
            })?
            .try_get(0)?;

        let rows = sqlx::query(
            "SELECT column_id, column_name, column_type, nulls_allowed, parent_column
             FROM ducklake_column
             WHERE table_id = $1
               AND $2 >= begin_snapshot
               AND ($3 < end_snapshot OR end_snapshot IS NULL)
             ORDER BY column_order",
        )
        .bind(table_id)
        .bind(begin_snapshot)
        .bind(begin_snapshot)
        .fetch_all(&self.pool)
        .await?;

        let raw: Result<Vec<(DuckLakeTableColumn, Option<i64>)>> = rows
            .into_iter()
            .map(|row| {
                Ok((
                    DuckLakeTableColumn::new(
                        row.try_get(0)?,
                        row.try_get(1)?,
                        row.try_get(2)?,
                        row.try_get::<Option<bool>, _>(3)?.unwrap_or(true),
                    ),
                    row.try_get(4)?,
                ))
            })
            .collect();

        Ok(reconstruct_list_columns(raw?))
    }

    async fn read_visible_inlined_rows(
        &self,
        table_name: &str,
        columns: &[DuckLakeTableColumn],
        snapshot_id: i64,
    ) -> Result<Vec<InlineRow>> {
        let projection = Self::build_inline_projection(columns)?;
        let query = format!(
            "SELECT {projection}
             FROM {} AS inlined_data
             WHERE $1 >= begin_snapshot
               AND ($2 < end_snapshot OR end_snapshot IS NULL)
             ORDER BY row_id",
            Self::quote_ident(table_name)
        );

        let rows = sqlx::query(&query)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter()
            .map(|row| {
                let row_id: i64 = row.try_get(0)?;
                let mut values = Vec::with_capacity(columns.len());
                for index in 0..columns.len() {
                    values.push(row.try_get::<Option<String>, _>(index + 1)?);
                }
                Ok(InlineRow {
                    row_id,
                    values,
                })
            })
            .collect()
    }

    async fn get_inlined_file_deletes(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<InlinedFileDelete>> {
        let delete_table_name = format!("ducklake_inlined_delete_{}", table_id);
        let exists = sqlx::query("SELECT to_regclass($1)")
            .bind(&delete_table_name)
            .fetch_one(&self.pool)
            .await?;

        if exists.try_get::<Option<String>, _>(0)?.is_none() {
            return Ok(Vec::new());
        }

        let query = format!(
            "SELECT file_id, row_id, begin_snapshot
             FROM {}
             WHERE begin_snapshot <= $1
             ORDER BY file_id, row_id",
            Self::quote_ident(&delete_table_name)
        );

        let rows = sqlx::query(&query)
            .bind(snapshot_id)
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter()
            .map(|row| {
                Ok(InlinedFileDelete {
                    file_id: row.try_get(0)?,
                    row_id: row.try_get(1)?,
                    begin_snapshot: row.try_get(2)?,
                })
            })
            .collect()
    }
}
