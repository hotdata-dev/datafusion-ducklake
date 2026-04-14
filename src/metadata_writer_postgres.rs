//! PostgreSQL implementation of [`MetadataWriter`] with DuckLake inline support.

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    UInt8Array, UInt16Array, UInt32Array,
};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Postgres, QueryBuilder, Row, Transaction};

use crate::DuckLakeError;
use crate::Result;
use crate::inlining::{
    CatalogInliningWriter, has_reserved_inline_column_names, is_reserved_inline_column_name,
};
use crate::metadata_provider::DuckLakeTableColumn;
use crate::metadata_provider::sync_call as block_on;
use crate::metadata_writer::{
    ColumnDef, DataFileInfo, MetadataWriter, WriteMode, WriteResult, WriteSetupResult,
    validate_name,
};
use crate::types::{ducklake_to_arrow_type, types_compatible};

const DEFAULT_MAX_CONNECTIONS: u32 = 5;
const DEFAULT_DATA_INLINING_ROW_LIMIT: usize = 10;

const SQL_CREATE_SCHEMA: &str = include_str!("sql/metadata_writer_postgres_create_schema.sql");

#[derive(Debug, Clone)]
pub struct PostgresMetadataWriter {
    pool: PgPool,
}

#[derive(Debug)]
struct PreparedWrite {
    snapshot_id: i64,
    schema_id: i64,
    table_id: i64,
    column_ids: Vec<i64>,
    schema_version: i64,
    next_row_id: i64,
}

#[derive(Debug)]
struct TableStats {
    record_count: i64,
    next_row_id: i64,
    file_size_bytes: i64,
}

impl PostgresMetadataWriter {
    pub async fn new(connection_string: &str) -> Result<Self> {
        Self::with_max_connections(connection_string, DEFAULT_MAX_CONNECTIONS).await
    }

    pub async fn with_max_connections(
        connection_string: &str,
        max_connections: u32,
    ) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(connection_string)
            .await?;
        Ok(Self {
            pool,
        })
    }

    pub async fn new_with_init(connection_string: &str) -> Result<Self> {
        let writer = Self::new(connection_string).await?;
        writer.initialize_schema()?;
        Ok(writer)
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn create_snapshot_tx(
        tx: &mut Transaction<'_, Postgres>,
        schema_version: i64,
    ) -> Result<i64> {
        let row = sqlx::query(
            "INSERT INTO ducklake_snapshot (snapshot_time, schema_version, next_catalog_id, next_file_id)
             VALUES (NOW(), $1, 0, 0)
             RETURNING snapshot_id",
        )
        .bind(schema_version)
        .fetch_one(&mut **tx)
        .await?;
        Ok(row.try_get(0)?)
    }

    async fn current_schema_version_tx(tx: &mut Transaction<'_, Postgres>) -> Result<i64> {
        let row = sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
            .fetch_one(&mut **tx)
            .await?;
        Ok(row.try_get(0)?)
    }

    async fn get_or_create_schema_tx(
        tx: &mut Transaction<'_, Postgres>,
        name: &str,
        path: Option<&str>,
        snapshot_id: i64,
    ) -> Result<(i64, bool)> {
        let existing = sqlx::query(
            "SELECT schema_id FROM ducklake_schema
             WHERE schema_name = $1 AND end_snapshot IS NULL",
        )
        .bind(name)
        .fetch_optional(&mut **tx)
        .await?;

        if let Some(row) = existing {
            return Ok((row.try_get(0)?, false));
        }

        let schema_path = path.unwrap_or(name);
        let row = sqlx::query(
            "INSERT INTO ducklake_schema (schema_name, path, path_is_relative, begin_snapshot)
             VALUES ($1, $2, TRUE, $3)
             RETURNING schema_id",
        )
        .bind(name)
        .bind(schema_path)
        .bind(snapshot_id)
        .fetch_one(&mut **tx)
        .await?;
        Ok((row.try_get(0)?, true))
    }

    async fn get_or_create_table_tx(
        tx: &mut Transaction<'_, Postgres>,
        schema_id: i64,
        name: &str,
        path: Option<&str>,
        snapshot_id: i64,
    ) -> Result<(i64, bool)> {
        let existing = sqlx::query(
            "SELECT table_id FROM ducklake_table
             WHERE schema_id = $1 AND table_name = $2 AND end_snapshot IS NULL",
        )
        .bind(schema_id)
        .bind(name)
        .fetch_optional(&mut **tx)
        .await?;

        if let Some(row) = existing {
            return Ok((row.try_get(0)?, false));
        }

        let table_path = path.unwrap_or(name);
        let row = sqlx::query(
            "INSERT INTO ducklake_table (schema_id, table_name, path, path_is_relative, begin_snapshot)
             VALUES ($1, $2, $3, TRUE, $4)
             RETURNING table_id",
        )
        .bind(schema_id)
        .bind(name)
        .bind(table_path)
        .bind(snapshot_id)
        .fetch_one(&mut **tx)
        .await?;
        Ok((row.try_get(0)?, true))
    }

    async fn load_active_columns_tx(
        tx: &mut Transaction<'_, Postgres>,
        table_id: i64,
    ) -> Result<Vec<DuckLakeTableColumn>> {
        let rows = sqlx::query(
            "SELECT column_id, column_name, column_type, nulls_allowed
             FROM ducklake_column
             WHERE table_id = $1 AND end_snapshot IS NULL
             ORDER BY column_order",
        )
        .bind(table_id)
        .fetch_all(&mut **tx)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(DuckLakeTableColumn::new(
                    row.try_get(0)?,
                    row.try_get(1)?,
                    row.try_get(2)?,
                    row.try_get::<Option<bool>, _>(3)?.unwrap_or(true),
                ))
            })
            .collect()
    }

    fn validate_append_schema(
        existing_columns: &[DuckLakeTableColumn],
        new_columns: &[ColumnDef],
    ) -> Result<()> {
        use std::collections::HashMap;

        if existing_columns.is_empty() {
            return Ok(());
        }

        let existing_map: HashMap<&str, (&str, bool)> = existing_columns
            .iter()
            .map(|column| {
                (
                    column.column_name.as_str(),
                    (column.column_type.as_str(), column.is_nullable),
                )
            })
            .collect();

        for new_col in new_columns {
            if let Some((existing_type, _)) = existing_map.get(new_col.name.as_str()) {
                if !types_compatible(existing_type, &new_col.ducklake_type) {
                    return Err(DuckLakeError::InvalidConfig(format!(
                        "Schema evolution error: column '{}' has type '{}' in existing table but '{}' in new schema. Type changes are not allowed.",
                        new_col.name, existing_type, new_col.ducklake_type
                    )));
                }
            } else if !new_col.is_nullable {
                return Err(DuckLakeError::InvalidConfig(format!(
                    "Schema evolution error: new column '{}' must be nullable. Adding non-nullable columns is not allowed.",
                    new_col.name
                )));
            }
        }

        Ok(())
    }

    async fn replace_columns_tx(
        tx: &mut Transaction<'_, Postgres>,
        table_id: i64,
        columns: &[ColumnDef],
        snapshot_id: i64,
    ) -> Result<Vec<i64>> {
        sqlx::query(
            "UPDATE ducklake_column
             SET end_snapshot = $1
             WHERE table_id = $2 AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(table_id)
        .execute(&mut **tx)
        .await?;

        let mut column_ids = Vec::with_capacity(columns.len());
        for (order, column) in columns.iter().enumerate() {
            let row = sqlx::query(
                "INSERT INTO ducklake_column (
                    table_id,
                    column_name,
                    column_type,
                    column_order,
                    nulls_allowed,
                    begin_snapshot
                 ) VALUES ($1, $2, $3, $4, $5, $6)
                 RETURNING column_id",
            )
            .bind(table_id)
            .bind(column.name())
            .bind(column.ducklake_type())
            .bind(order as i64)
            .bind(column.is_nullable())
            .bind(snapshot_id)
            .fetch_one(&mut **tx)
            .await?;
            column_ids.push(row.try_get(0)?);
        }

        Ok(column_ids)
    }

    async fn ensure_table_stats_tx(
        tx: &mut Transaction<'_, Postgres>,
        table_id: i64,
    ) -> Result<TableStats> {
        sqlx::query(
            "INSERT INTO ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes)
             VALUES ($1, 0, 0, 0)
             ON CONFLICT (table_id) DO NOTHING",
        )
        .bind(table_id)
        .execute(&mut **tx)
        .await?;

        let row = sqlx::query(
            "SELECT record_count, next_row_id, file_size_bytes
             FROM ducklake_table_stats
             WHERE table_id = $1
             FOR UPDATE",
        )
        .bind(table_id)
        .fetch_one(&mut **tx)
        .await?;

        Ok(TableStats {
            record_count: row.try_get(0)?,
            next_row_id: row.try_get(1)?,
            file_size_bytes: row.try_get(2)?,
        })
    }

    async fn end_active_inlined_rows_tx(
        tx: &mut Transaction<'_, Postgres>,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<()> {
        let rows = sqlx::query(
            "SELECT table_name
             FROM ducklake_inlined_data_tables
             WHERE table_id = $1
             ORDER BY schema_version",
        )
        .bind(table_id)
        .fetch_all(&mut **tx)
        .await?;

        for row in rows {
            let table_name: String = row.try_get(0)?;
            let query = format!(
                "UPDATE {} SET end_snapshot = $1 WHERE end_snapshot IS NULL",
                quote_ident(&table_name)
            );
            sqlx::query(&query)
                .bind(snapshot_id)
                .execute(&mut **tx)
                .await?;
        }

        Ok(())
    }

    async fn prepare_write_tx(
        tx: &mut Transaction<'_, Postgres>,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        mode: WriteMode,
    ) -> Result<PreparedWrite> {
        validate_name(schema_name, "Schema")?;
        validate_name(table_name, "Table")?;
        if columns.is_empty() {
            return Err(DuckLakeError::InvalidConfig(
                "Table must have at least one column".to_string(),
            ));
        }

        let schema_version = Self::current_schema_version_tx(tx).await? + 1;
        let snapshot_id = Self::create_snapshot_tx(tx, schema_version).await?;
        let (schema_id, _) =
            Self::get_or_create_schema_tx(tx, schema_name, None, snapshot_id).await?;
        let (table_id, _) =
            Self::get_or_create_table_tx(tx, schema_id, table_name, None, snapshot_id).await?;

        let existing_columns = Self::load_active_columns_tx(tx, table_id).await?;
        if mode == WriteMode::Append {
            Self::validate_append_schema(&existing_columns, columns)?;
        }

        let column_ids = Self::replace_columns_tx(tx, table_id, columns, snapshot_id).await?;
        sqlx::query(
            "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version, table_id)
             VALUES ($1, $2, $3)",
        )
        .bind(snapshot_id)
        .bind(schema_version)
        .bind(table_id)
        .execute(&mut **tx)
        .await?;

        let stats = Self::ensure_table_stats_tx(tx, table_id).await?;
        if mode == WriteMode::Replace {
            sqlx::query(
                "UPDATE ducklake_data_file
                 SET end_snapshot = $1
                 WHERE table_id = $2 AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut **tx)
            .await?;

            Self::end_active_inlined_rows_tx(tx, table_id, snapshot_id).await?;

            sqlx::query(
                "UPDATE ducklake_table_stats
                 SET record_count = 0, file_size_bytes = 0
                 WHERE table_id = $1",
            )
            .bind(table_id)
            .execute(&mut **tx)
            .await?;
        }

        Ok(PreparedWrite {
            snapshot_id,
            schema_id,
            table_id,
            column_ids,
            schema_version,
            next_row_id: stats.next_row_id,
        })
    }

    async fn create_inline_table_tx(
        tx: &mut Transaction<'_, Postgres>,
        table_id: i64,
        schema_version: i64,
        columns: &[ColumnDef],
    ) -> Result<String> {
        let existing = sqlx::query(
            "SELECT table_name
             FROM ducklake_inlined_data_tables
             WHERE table_id = $1 AND schema_version = $2",
        )
        .bind(table_id)
        .bind(schema_version)
        .fetch_optional(&mut **tx)
        .await?;

        if let Some(row) = existing {
            return Ok(row.try_get(0)?);
        }

        let inline_table_name = format!("ducklake_inlined_data_{}_{}", table_id, schema_version);
        let mut ddl = format!(
            "CREATE TABLE IF NOT EXISTS {} (row_id BIGINT NOT NULL, begin_snapshot BIGINT NOT NULL, end_snapshot BIGINT",
            quote_ident(&inline_table_name)
        );
        for column in columns {
            ddl.push_str(", ");
            ddl.push_str(&quote_ident(column.name()));
            ddl.push(' ');
            ddl.push_str(inline_sql_type(column.ducklake_type())?);
        }
        ddl.push(')');
        sqlx::query(&ddl).execute(&mut **tx).await?;

        sqlx::query(
            "INSERT INTO ducklake_inlined_data_tables (table_id, table_name, schema_version)
             VALUES ($1, $2, $3)",
        )
        .bind(table_id)
        .bind(&inline_table_name)
        .bind(schema_version)
        .execute(&mut **tx)
        .await?;

        Ok(inline_table_name)
    }

    async fn update_table_stats_tx(
        tx: &mut Transaction<'_, Postgres>,
        table_id: i64,
        record_count: i64,
        next_row_id: i64,
        file_size_bytes: Option<i64>,
    ) -> Result<()> {
        match file_size_bytes {
            Some(file_size_bytes) => {
                sqlx::query(
                    "UPDATE ducklake_table_stats
                     SET record_count = $1, next_row_id = $2, file_size_bytes = $3
                     WHERE table_id = $4",
                )
                .bind(record_count)
                .bind(next_row_id)
                .bind(file_size_bytes)
                .bind(table_id)
                .execute(&mut **tx)
                .await?;
            },
            None => {
                sqlx::query(
                    "UPDATE ducklake_table_stats
                     SET record_count = $1, next_row_id = $2
                     WHERE table_id = $3",
                )
                .bind(record_count)
                .bind(next_row_id)
                .bind(table_id)
                .execute(&mut **tx)
                .await?;
            },
        }

        Ok(())
    }
}

impl MetadataWriter for PostgresMetadataWriter {
    fn catalog_inlining_writer(&self) -> Option<&dyn CatalogInliningWriter> {
        Some(self)
    }

    fn create_snapshot(&self) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;
            let schema_version = Self::current_schema_version_tx(&mut tx).await?;
            let snapshot_id = Self::create_snapshot_tx(&mut tx, schema_version).await?;
            tx.commit().await?;
            Ok(snapshot_id)
        })
    }

    fn get_or_create_schema(
        &self,
        name: &str,
        path: Option<&str>,
        snapshot_id: i64,
    ) -> Result<(i64, bool)> {
        validate_name(name, "Schema")?;
        block_on(async {
            let mut tx = self.pool.begin().await?;
            let result = Self::get_or_create_schema_tx(&mut tx, name, path, snapshot_id).await?;
            tx.commit().await?;
            Ok(result)
        })
    }

    fn get_or_create_table(
        &self,
        schema_id: i64,
        name: &str,
        path: Option<&str>,
        snapshot_id: i64,
    ) -> Result<(i64, bool)> {
        validate_name(name, "Table")?;
        block_on(async {
            let mut tx = self.pool.begin().await?;
            let result =
                Self::get_or_create_table_tx(&mut tx, schema_id, name, path, snapshot_id).await?;
            tx.commit().await?;
            Ok(result)
        })
    }

    fn set_columns(
        &self,
        table_id: i64,
        columns: &[ColumnDef],
        snapshot_id: i64,
    ) -> Result<Vec<i64>> {
        if columns.is_empty() {
            return Err(DuckLakeError::InvalidConfig(
                "Table must have at least one column".to_string(),
            ));
        }
        block_on(async {
            let mut tx = self.pool.begin().await?;
            let ids = Self::replace_columns_tx(&mut tx, table_id, columns, snapshot_id).await?;
            tx.commit().await?;
            Ok(ids)
        })
    }

    fn register_data_file(
        &self,
        table_id: i64,
        snapshot_id: i64,
        file: &DataFileInfo,
    ) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;
            let stats = Self::ensure_table_stats_tx(&mut tx, table_id).await?;

            let row = sqlx::query(
                "INSERT INTO ducklake_data_file (
                    table_id,
                    begin_snapshot,
                    end_snapshot,
                    path,
                    path_is_relative,
                    file_size_bytes,
                    footer_size,
                    encryption_key,
                    record_count,
                    row_id_start,
                    mapping_id
                 ) VALUES ($1, $2, NULL, $3, $4, $5, $6, NULL, $7, $8, NULL)
                 RETURNING data_file_id",
            )
            .bind(table_id)
            .bind(snapshot_id)
            .bind(&file.path)
            .bind(file.path_is_relative)
            .bind(file.file_size_bytes)
            .bind(file.footer_size)
            .bind(file.record_count)
            .bind(stats.next_row_id)
            .fetch_one(&mut *tx)
            .await?;
            let data_file_id: i64 = row.try_get(0)?;

            Self::update_table_stats_tx(
                &mut tx,
                table_id,
                stats.record_count + file.record_count,
                stats.next_row_id + file.record_count,
                Some(stats.file_size_bytes + file.file_size_bytes),
            )
            .await?;

            tx.commit().await?;
            Ok(data_file_id)
        })
    }

    fn end_table_files(&self, table_id: i64, snapshot_id: i64) -> Result<u64> {
        block_on(async {
            let result = sqlx::query(
                "UPDATE ducklake_data_file
                 SET end_snapshot = $1
                 WHERE table_id = $2 AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(self.pool())
            .await?;
            Ok(result.rows_affected())
        })
    }

    fn get_data_path(&self) -> Result<String> {
        block_on(async {
            let row = sqlx::query(
                "SELECT value
                 FROM ducklake_metadata
                 WHERE key = $1 AND scope IS NULL",
            )
            .bind("data_path")
            .fetch_optional(self.pool())
            .await?;

            match row {
                Some(row) => Ok(row.try_get(0)?),
                None => Err(DuckLakeError::InvalidConfig(
                    "Missing required catalog metadata: 'data_path' not configured.".to_string(),
                )),
            }
        })
    }

    fn set_data_path(&self, path: &str) -> Result<()> {
        block_on(async {
            sqlx::query(
                "DELETE FROM ducklake_metadata
                 WHERE key = 'data_path' AND scope IS NULL",
            )
            .execute(self.pool())
            .await?;

            sqlx::query(
                "INSERT INTO ducklake_metadata (key, value, scope, scope_id)
                 VALUES ('data_path', $1, NULL, NULL)",
            )
            .bind(path)
            .execute(self.pool())
            .await?;
            Ok(())
        })
    }

    fn initialize_schema(&self) -> Result<()> {
        block_on(async {
            sqlx::raw_sql(SQL_CREATE_SCHEMA)
                .execute(self.pool())
                .await?;
            Ok(())
        })
    }

    fn begin_write_transaction(
        &self,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        mode: WriteMode,
    ) -> Result<WriteSetupResult> {
        block_on(async {
            let mut tx = self.pool.begin().await?;
            let prepared =
                Self::prepare_write_tx(&mut tx, schema_name, table_name, columns, mode).await?;
            tx.commit().await?;
            Ok(WriteSetupResult {
                snapshot_id: prepared.snapshot_id,
                schema_id: prepared.schema_id,
                table_id: prepared.table_id,
                column_ids: prepared.column_ids,
                schema_version: Some(prepared.schema_version),
                next_row_id: Some(prepared.next_row_id),
            })
        })
    }
}

impl CatalogInliningWriter for PostgresMetadataWriter {
    fn data_inlining_row_limit(&self) -> Result<usize> {
        block_on(async {
            let row = sqlx::query(
                "SELECT value
                 FROM ducklake_metadata
                 WHERE key = $1 AND scope IS NULL",
            )
            .bind("data_inlining_row_limit")
            .fetch_optional(self.pool())
            .await?;

            match row {
                Some(row) => {
                    let value: String = row.try_get(0)?;
                    value.parse::<usize>().map_err(|e| {
                        DuckLakeError::InvalidConfig(format!(
                            "Invalid data_inlining_row_limit value '{}': {}",
                            value, e
                        ))
                    })
                },
                None => Ok(DEFAULT_DATA_INLINING_ROW_LIMIT),
            }
        })
    }

    fn supports_inline_columns(&self, columns: &[ColumnDef]) -> Result<bool> {
        for column in columns {
            if is_reserved_inline_column_name(column.name()) {
                return Ok(false);
            }
            let arrow_type = ducklake_to_arrow_type(column.ducklake_type())?;
            if !supports_inline_arrow_type(&arrow_type) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn write_inlined_batches(
        &self,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        batches: &[RecordBatch],
        mode: WriteMode,
    ) -> Result<WriteResult> {
        if columns.is_empty() {
            return Err(DuckLakeError::InvalidConfig(
                "Table must have at least one column".to_string(),
            ));
        }
        if has_reserved_inline_column_names(columns) {
            return Err(DuckLakeError::InvalidConfig(
                "Table schema uses reserved DuckLake inline column names".to_string(),
            ));
        }

        block_on(async {
            let mut tx = self.pool.begin().await?;
            let prepared =
                Self::prepare_write_tx(&mut tx, schema_name, table_name, columns, mode).await?;
            let inline_table_name = Self::create_inline_table_tx(
                &mut tx,
                prepared.table_id,
                prepared.schema_version,
                columns,
            )
            .await?;

            let stats = Self::ensure_table_stats_tx(&mut tx, prepared.table_id).await?;
            let starting_record_count = if mode == WriteMode::Replace {
                0
            } else {
                stats.record_count
            };
            let starting_next_row_id = prepared.next_row_id;

            let quoted_table = quote_ident(&inline_table_name);
            let mut builder =
                QueryBuilder::<Postgres>::new(format!("INSERT INTO {} VALUES ", quoted_table));
            let mut row_id = starting_next_row_id;
            let mut total_rows = 0i64;
            let mut first_row = true;

            for batch in batches {
                for row_idx in 0..batch.num_rows() {
                    if !first_row {
                        builder.push(", ");
                    }
                    first_row = false;

                    builder.push("(");
                    builder.push_bind(row_id);
                    builder.push(", ");
                    builder.push_bind(prepared.snapshot_id);
                    builder.push(", ");
                    builder.push_bind(None::<i64>);

                    for (column_idx, column) in columns.iter().enumerate() {
                        builder.push(", ");
                        push_inline_bind(
                            &mut builder,
                            batch.column(column_idx),
                            row_idx,
                            column.ducklake_type(),
                        )?;
                    }

                    builder.push(")");
                    row_id += 1;
                    total_rows += 1;
                }
            }

            if total_rows > 0 {
                builder.build().execute(&mut *tx).await?;
            }

            Self::update_table_stats_tx(
                &mut tx,
                prepared.table_id,
                starting_record_count + total_rows,
                starting_next_row_id + total_rows,
                if mode == WriteMode::Replace {
                    Some(0)
                } else {
                    Some(stats.file_size_bytes)
                },
            )
            .await?;

            tx.commit().await?;

            Ok(WriteResult {
                snapshot_id: prepared.snapshot_id,
                table_id: prepared.table_id,
                schema_id: prepared.schema_id,
                files_written: 0,
                records_written: total_rows,
                schema_version: Some(prepared.schema_version),
                next_row_id: Some(starting_next_row_id + total_rows),
            })
        })
    }
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn inline_sql_type(ducklake_type: &str) -> Result<&'static str> {
    let arrow_type = ducklake_to_arrow_type(ducklake_type)?;
    Ok(match arrow_type {
        arrow::datatypes::DataType::Boolean => "BOOLEAN",
        arrow::datatypes::DataType::Int8 | arrow::datatypes::DataType::Int16 => "SMALLINT",
        arrow::datatypes::DataType::Int32 => "INTEGER",
        arrow::datatypes::DataType::Int64 => "BIGINT",
        arrow::datatypes::DataType::UInt8 | arrow::datatypes::DataType::UInt16 => "INTEGER",
        arrow::datatypes::DataType::UInt32 => "BIGINT",
        arrow::datatypes::DataType::Float32 => "REAL",
        arrow::datatypes::DataType::Float64 => "DOUBLE PRECISION",
        arrow::datatypes::DataType::Utf8
        | arrow::datatypes::DataType::LargeUtf8
        | arrow::datatypes::DataType::Binary
        | arrow::datatypes::DataType::LargeBinary
        | arrow::datatypes::DataType::FixedSizeBinary(_) => "BYTEA",
        _ => "TEXT",
    })
}

fn supports_inline_arrow_type(data_type: &arrow::datatypes::DataType) -> bool {
    !matches!(
        data_type,
        arrow::datatypes::DataType::Struct(_) | arrow::datatypes::DataType::Map(_, _)
    )
}

fn push_inline_bind(
    builder: &mut QueryBuilder<'_, Postgres>,
    array: &ArrayRef,
    row_idx: usize,
    ducklake_type: &str,
) -> Result<()> {
    if array.is_null(row_idx) {
        builder.push_bind(None::<String>);
        return Ok(());
    }

    match ducklake_to_arrow_type(ducklake_type)? {
        arrow::datatypes::DataType::Boolean => {
            let array = downcast_array::<BooleanArray>(array, ducklake_type)?;
            builder.push_bind(array.value(row_idx));
        },
        arrow::datatypes::DataType::Int8 => {
            let array = downcast_array::<Int8Array>(array, ducklake_type)?;
            builder.push_bind(array.value(row_idx) as i16);
        },
        arrow::datatypes::DataType::Int16 => {
            let array = downcast_array::<Int16Array>(array, ducklake_type)?;
            builder.push_bind(array.value(row_idx));
        },
        arrow::datatypes::DataType::Int32 => {
            let array = downcast_array::<Int32Array>(array, ducklake_type)?;
            builder.push_bind(array.value(row_idx));
        },
        arrow::datatypes::DataType::Int64 => {
            let array = downcast_array::<Int64Array>(array, ducklake_type)?;
            builder.push_bind(array.value(row_idx));
        },
        arrow::datatypes::DataType::UInt8 => {
            let array = downcast_array::<UInt8Array>(array, ducklake_type)?;
            builder.push_bind(i32::from(array.value(row_idx)));
        },
        arrow::datatypes::DataType::UInt16 => {
            let array = downcast_array::<UInt16Array>(array, ducklake_type)?;
            builder.push_bind(i32::from(array.value(row_idx)));
        },
        arrow::datatypes::DataType::UInt32 => {
            let array = downcast_array::<UInt32Array>(array, ducklake_type)?;
            builder.push_bind(i64::from(array.value(row_idx)));
        },
        arrow::datatypes::DataType::Float32 => {
            let array = downcast_array::<Float32Array>(array, ducklake_type)?;
            builder.push_bind(array.value(row_idx));
        },
        arrow::datatypes::DataType::Float64 => {
            let array = downcast_array::<Float64Array>(array, ducklake_type)?;
            builder.push_bind(array.value(row_idx));
        },
        arrow::datatypes::DataType::Utf8 => {
            let array = downcast_array::<StringArray>(array, ducklake_type)?;
            builder.push_bind(array.value(row_idx).as_bytes().to_vec());
        },
        arrow::datatypes::DataType::LargeUtf8 => {
            let array = downcast_array::<LargeStringArray>(array, ducklake_type)?;
            builder.push_bind(array.value(row_idx).as_bytes().to_vec());
        },
        arrow::datatypes::DataType::Binary => {
            let array = downcast_array::<BinaryArray>(array, ducklake_type)?;
            builder.push_bind(array.value(row_idx).to_vec());
        },
        arrow::datatypes::DataType::LargeBinary => {
            let array = downcast_array::<LargeBinaryArray>(array, ducklake_type)?;
            builder.push_bind(array.value(row_idx).to_vec());
        },
        arrow::datatypes::DataType::FixedSizeBinary(16) => {
            let array = downcast_array::<FixedSizeBinaryArray>(array, ducklake_type)?;
            builder.push_bind(array.value(row_idx).to_vec());
        },
        arrow::datatypes::DataType::FixedSizeBinary(_) => {
            let array = downcast_array::<FixedSizeBinaryArray>(array, ducklake_type)?;
            builder.push_bind(array.value(row_idx).to_vec());
        },
        _ => {
            builder.push_bind(Some(array_value_to_string(array.as_ref(), row_idx)?));
        },
    }

    Ok(())
}

fn downcast_array<'a, T: 'static>(array: &'a ArrayRef, ducklake_type: &str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        DuckLakeError::Internal(format!(
            "Unexpected Arrow array type while encoding inline DuckLake value '{}'",
            ducklake_type
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn create_test_writer() -> (
        PostgresMetadataWriter,
        testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
    ) {
        use testcontainers::runners::AsyncRunner;
        use testcontainers_modules::postgres::Postgres;

        let container = Postgres::default().start().await.unwrap();
        let host = "127.0.0.1";
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let conn_str = format!("postgresql://postgres:postgres@{}:{}/postgres", host, port);
        let writer = PostgresMetadataWriter::new_with_init(&conn_str)
            .await
            .unwrap();
        (writer, container)
    }

    #[tokio::test]
    async fn postgres_writer_initializes_default_inlining_limit() {
        let (writer, _container) = create_test_writer().await;
        assert_eq!(writer.data_inlining_row_limit().unwrap(), 10);
    }

    #[test]
    fn inline_reserved_name_detection_matches_ducklake() {
        assert!(is_reserved_inline_column_name("row_id"));
        assert!(is_reserved_inline_column_name("_ducklake_internal_row_id"));
        assert!(!is_reserved_inline_column_name("user_id"));
    }
}
