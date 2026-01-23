//! SQLite metadata writer for DuckLake catalogs.

use crate::Result;
use crate::metadata_provider::block_on;
use crate::metadata_writer::{ColumnDef, DataFileInfo, MetadataWriter};
use sqlx::Row;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

/// SQL to create DuckLake schema tables
const SQL_CREATE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_metadata (
    key VARCHAR NOT NULL,
    value VARCHAR NOT NULL,
    scope VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_snapshot (
    snapshot_id INTEGER PRIMARY KEY,
    snapshot_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ducklake_schema (
    schema_id INTEGER PRIMARY KEY,
    schema_name VARCHAR NOT NULL,
    path VARCHAR NOT NULL DEFAULT '',
    path_is_relative BOOLEAN NOT NULL DEFAULT 1,
    begin_snapshot INTEGER NOT NULL,
    end_snapshot INTEGER
);

CREATE TABLE IF NOT EXISTS ducklake_table (
    table_id INTEGER PRIMARY KEY,
    schema_id INTEGER NOT NULL,
    table_name VARCHAR NOT NULL,
    path VARCHAR NOT NULL DEFAULT '',
    path_is_relative BOOLEAN NOT NULL DEFAULT 1,
    begin_snapshot INTEGER NOT NULL,
    end_snapshot INTEGER
);

CREATE TABLE IF NOT EXISTS ducklake_column (
    column_id INTEGER PRIMARY KEY,
    table_id INTEGER NOT NULL,
    column_name VARCHAR NOT NULL,
    column_type VARCHAR NOT NULL,
    column_order INTEGER NOT NULL,
    nulls_allowed BOOLEAN DEFAULT 1
);

CREATE TABLE IF NOT EXISTS ducklake_data_file (
    data_file_id INTEGER PRIMARY KEY,
    table_id INTEGER NOT NULL,
    path VARCHAR NOT NULL,
    path_is_relative BOOLEAN NOT NULL DEFAULT 1,
    file_size_bytes INTEGER NOT NULL,
    footer_size INTEGER,
    encryption_key VARCHAR,
    record_count INTEGER,
    row_id_start INTEGER,
    mapping_id INTEGER,
    begin_snapshot INTEGER NOT NULL,
    end_snapshot INTEGER
);

CREATE TABLE IF NOT EXISTS ducklake_delete_file (
    delete_file_id INTEGER PRIMARY KEY,
    data_file_id INTEGER NOT NULL,
    table_id INTEGER NOT NULL,
    path VARCHAR NOT NULL,
    path_is_relative BOOLEAN NOT NULL DEFAULT 1,
    file_size_bytes INTEGER NOT NULL,
    footer_size INTEGER,
    encryption_key VARCHAR,
    delete_count INTEGER,
    begin_snapshot INTEGER NOT NULL,
    end_snapshot INTEGER
);
"#;

/// SQLite-based metadata writer for DuckLake catalogs.
#[derive(Debug, Clone)]
pub struct SqliteMetadataWriter {
    pool: SqlitePool,
}

impl SqliteMetadataWriter {
    /// Creates a new writer for a DuckLake catalog.
    ///
    /// Connection string format: `sqlite:///path/to/catalog.db?mode=rwc`
    /// Use `?mode=rwc` to create the file if it doesn't exist.
    pub async fn new(connection_string: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(connection_string)
            .await?;

        Ok(Self {
            pool,
        })
    }

    /// Creates a new writer and initializes the schema if needed.
    pub async fn new_with_init(connection_string: &str) -> Result<Self> {
        let writer = Self::new(connection_string).await?;
        writer.initialize_schema()?;
        Ok(writer)
    }
}

impl MetadataWriter for SqliteMetadataWriter {
    fn create_snapshot(&self) -> Result<i64> {
        block_on(async {
            // Get next snapshot ID
            let row =
                sqlx::query("SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM ducklake_snapshot")
                    .fetch_one(&self.pool)
                    .await?;
            let snapshot_id: i64 = row.try_get(0)?;

            // Insert new snapshot
            sqlx::query("INSERT INTO ducklake_snapshot (snapshot_id) VALUES (?)")
                .bind(snapshot_id)
                .execute(&self.pool)
                .await?;

            Ok(snapshot_id)
        })
    }

    fn get_or_create_schema(
        &self,
        name: &str,
        path: Option<&str>,
        snapshot_id: i64,
    ) -> Result<(i64, bool)> {
        block_on(async {
            // Check if schema exists
            let existing = sqlx::query(
                "SELECT schema_id FROM ducklake_schema
                 WHERE schema_name = ? AND end_snapshot IS NULL",
            )
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;

            if let Some(row) = existing {
                let schema_id: i64 = row.try_get(0)?;
                return Ok((schema_id, false));
            }

            // Get next schema ID
            let row = sqlx::query("SELECT COALESCE(MAX(schema_id), 0) + 1 FROM ducklake_schema")
                .fetch_one(&self.pool)
                .await?;
            let schema_id: i64 = row.try_get(0)?;

            // Create schema
            let schema_path = path.unwrap_or(name);
            sqlx::query(
                "INSERT INTO ducklake_schema (schema_id, schema_name, path, path_is_relative, begin_snapshot)
                 VALUES (?, ?, ?, 1, ?)",
            )
            .bind(schema_id)
            .bind(name)
            .bind(schema_path)
            .bind(snapshot_id)
            .execute(&self.pool)
            .await?;

            Ok((schema_id, true))
        })
    }

    fn get_or_create_table(
        &self,
        schema_id: i64,
        name: &str,
        path: Option<&str>,
        snapshot_id: i64,
    ) -> Result<(i64, bool)> {
        block_on(async {
            // Check if table exists
            let existing = sqlx::query(
                "SELECT table_id FROM ducklake_table
                 WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL",
            )
            .bind(schema_id)
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;

            if let Some(row) = existing {
                let table_id: i64 = row.try_get(0)?;
                return Ok((table_id, false));
            }

            // Get next table ID
            let row = sqlx::query("SELECT COALESCE(MAX(table_id), 0) + 1 FROM ducklake_table")
                .fetch_one(&self.pool)
                .await?;
            let table_id: i64 = row.try_get(0)?;

            // Create table
            let table_path = path.unwrap_or(name);
            sqlx::query(
                "INSERT INTO ducklake_table (table_id, schema_id, table_name, path, path_is_relative, begin_snapshot)
                 VALUES (?, ?, ?, ?, 1, ?)",
            )
            .bind(table_id)
            .bind(schema_id)
            .bind(name)
            .bind(table_path)
            .bind(snapshot_id)
            .execute(&self.pool)
            .await?;

            Ok((table_id, true))
        })
    }

    fn set_columns(&self, table_id: i64, columns: &[ColumnDef]) -> Result<Vec<i64>> {
        block_on(async {
            // Delete existing columns for this table
            sqlx::query("DELETE FROM ducklake_column WHERE table_id = ?")
                .bind(table_id)
                .execute(&self.pool)
                .await?;

            // Get next column ID base
            let row = sqlx::query("SELECT COALESCE(MAX(column_id), 0) FROM ducklake_column")
                .fetch_one(&self.pool)
                .await?;
            let mut column_id: i64 = row.try_get(0)?;

            // Insert columns
            let mut column_ids = Vec::with_capacity(columns.len());
            for (order, col) in columns.iter().enumerate() {
                column_id += 1;
                sqlx::query(
                    "INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, column_order, nulls_allowed)
                     VALUES (?, ?, ?, ?, ?, ?)",
                )
                .bind(column_id)
                .bind(table_id)
                .bind(&col.name)
                .bind(&col.ducklake_type)
                .bind(order as i64)
                .bind(col.is_nullable)
                .execute(&self.pool)
                .await?;

                column_ids.push(column_id);
            }

            Ok(column_ids)
        })
    }

    fn register_data_file(
        &self,
        table_id: i64,
        snapshot_id: i64,
        file: &DataFileInfo,
    ) -> Result<i64> {
        block_on(async {
            // Get next data file ID
            let row =
                sqlx::query("SELECT COALESCE(MAX(data_file_id), 0) + 1 FROM ducklake_data_file")
                    .fetch_one(&self.pool)
                    .await?;
            let data_file_id: i64 = row.try_get(0)?;

            // Insert data file
            sqlx::query(
                "INSERT INTO ducklake_data_file (data_file_id, table_id, path, path_is_relative, file_size_bytes, footer_size, record_count, begin_snapshot)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(data_file_id)
            .bind(table_id)
            .bind(&file.path)
            .bind(file.path_is_relative)
            .bind(file.file_size_bytes)
            .bind(file.footer_size)
            .bind(file.record_count)
            .bind(snapshot_id)
            .execute(&self.pool)
            .await?;

            Ok(data_file_id)
        })
    }

    fn end_table_files(&self, table_id: i64, snapshot_id: i64) -> Result<u64> {
        block_on(async {
            let result = sqlx::query(
                "UPDATE ducklake_data_file SET end_snapshot = ?
                 WHERE table_id = ? AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&self.pool)
            .await?;

            Ok(result.rows_affected())
        })
    }

    fn get_data_path(&self) -> Result<String> {
        block_on(async {
            let row =
                sqlx::query("SELECT value FROM ducklake_metadata WHERE key = ? AND scope IS NULL")
                    .bind("data_path")
                    .fetch_optional(&self.pool)
                    .await?;

            match row {
                Some(r) => Ok(r.try_get(0)?),
                None => Err(crate::error::DuckLakeError::InvalidConfig(
                    "Missing required catalog metadata: 'data_path' not configured.".to_string(),
                )),
            }
        })
    }

    fn set_data_path(&self, path: &str) -> Result<()> {
        block_on(async {
            // Delete existing data_path if present, then insert
            sqlx::query("DELETE FROM ducklake_metadata WHERE key = 'data_path' AND scope IS NULL")
                .execute(&self.pool)
                .await?;

            sqlx::query(
                "INSERT INTO ducklake_metadata (key, value, scope)
                 VALUES ('data_path', ?, NULL)",
            )
            .bind(path)
            .execute(&self.pool)
            .await?;

            Ok(())
        })
    }

    fn initialize_schema(&self) -> Result<()> {
        block_on(async {
            sqlx::query(SQL_CREATE_SCHEMA).execute(&self.pool).await?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_writer() -> (SqliteMetadataWriter, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
        let writer = SqliteMetadataWriter::new_with_init(&conn_str)
            .await
            .unwrap();
        (writer, temp_dir)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_snapshot() {
        let (writer, _temp) = create_test_writer().await;

        let snap1 = writer.create_snapshot().unwrap();
        assert_eq!(snap1, 1);

        let snap2 = writer.create_snapshot().unwrap();
        assert_eq!(snap2, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_or_create_schema() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();

        // Create new schema
        let (schema_id, created) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        assert!(created);
        assert_eq!(schema_id, 1);

        // Get existing schema
        let (schema_id2, created2) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        assert!(!created2);
        assert_eq!(schema_id2, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_or_create_table() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();

        // Create new table
        let (table_id, created) = writer
            .get_or_create_table(schema_id, "users", None, snapshot_id)
            .unwrap();
        assert!(created);
        assert_eq!(table_id, 1);

        // Get existing table
        let (table_id2, created2) = writer
            .get_or_create_table(schema_id, "users", None, snapshot_id)
            .unwrap();
        assert!(!created2);
        assert_eq!(table_id2, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_set_columns() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "users", None, snapshot_id)
            .unwrap();

        let columns =
            vec![ColumnDef::new("id", "int64", false), ColumnDef::new("name", "varchar", true)];

        let column_ids = writer.set_columns(table_id, &columns).unwrap();
        assert_eq!(column_ids.len(), 2);
        assert_eq!(column_ids[0], 1);
        assert_eq!(column_ids[1], 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_data_file() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "users", None, snapshot_id)
            .unwrap();

        let file = DataFileInfo::new("data.parquet", 1024, 100).with_footer_size(256);

        let file_id = writer
            .register_data_file(table_id, snapshot_id, &file)
            .unwrap();
        assert_eq!(file_id, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_end_table_files() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot1 = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot1)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "users", None, snapshot1)
            .unwrap();

        // Register a file
        let file = DataFileInfo::new("data1.parquet", 1024, 100);
        writer
            .register_data_file(table_id, snapshot1, &file)
            .unwrap();

        // End files at new snapshot
        let snapshot2 = writer.create_snapshot().unwrap();
        let ended = writer.end_table_files(table_id, snapshot2).unwrap();
        assert_eq!(ended, 1);

        // End again should affect 0 files
        let ended2 = writer.end_table_files(table_id, snapshot2).unwrap();
        assert_eq!(ended2, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_data_path() {
        let (writer, _temp) = create_test_writer().await;

        // Set data path
        writer.set_data_path("/data/path").unwrap();

        // Get data path
        let path = writer.get_data_path().unwrap();
        assert_eq!(path, "/data/path");

        // Update data path
        writer.set_data_path("/new/path").unwrap();
        let path2 = writer.get_data_path().unwrap();
        assert_eq!(path2, "/new/path");
    }
}
