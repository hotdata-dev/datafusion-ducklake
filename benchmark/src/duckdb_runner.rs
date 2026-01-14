use crate::runner::QueryResult;
use anyhow::{Context, Result};
use duckdb::Connection;
use std::path::Path;
use walkdir::WalkDir;

use std::path::PathBuf;

pub struct DuckDbRunner {
    conn: Connection,
    catalog_path: PathBuf,
}

impl DuckDbRunner {
    pub fn new(catalog_path: &Path) -> Result<Self> {
        let conn =
            Connection::open_in_memory().context("Failed to open in-memory DuckDB connection")?;

        // Install and load DuckLake extension
        conn.execute_batch(
            r#"
            INSTALL ducklake;
            LOAD ducklake;
            "#,
        )
        .context("Failed to install/load DuckLake extension")?;

        // Attach the DuckLake catalog
        let attach_sql = format!(
            "ATTACH '{}' AS ducklake (TYPE ducklake)",
            catalog_path.display()
        );
        conn.execute(&attach_sql, [])
            .with_context(|| format!("Failed to attach catalog: {:?}", catalog_path))?;

        // Set as default catalog
        conn.execute("USE ducklake", [])
            .context("Failed to set default catalog")?;

        Ok(Self {
            conn,
            catalog_path: catalog_path.to_path_buf(),
        })
    }

    /// Get total data size in bytes by scanning Parquet files in the data directory
    pub fn get_data_size_bytes(&self) -> Result<u64> {
        // DuckLake stores data files relative to the catalog location
        let data_dir = self.catalog_path.parent().unwrap_or(Path::new("."));

        let mut total_size = 0u64;
        for entry in WalkDir::new(data_dir).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "parquet")
                && let Ok(metadata) = std::fs::metadata(path)
            {
                total_size += metadata.len();
            }
        }
        Ok(total_size)
    }

    /// Get row counts for specified tables
    pub fn get_table_sizes(&self, tables: &[&str]) -> Result<Vec<(String, usize)>> {
        let mut sizes = Vec::new();

        for table in tables {
            let sql = format!("SELECT COUNT(*) FROM main.{}", table);
            let mut stmt = self.conn.prepare(&sql)?;
            let mut rows = stmt.query([])?;
            if let Some(row) = rows.next()? {
                let count: i64 = row.get(0)?;
                sizes.push((table.to_string(), count as usize));
            }
        }

        Ok(sizes)
    }

    pub fn execute(&self, sql: &str) -> Result<QueryResult> {
        let mut stmt = self
            .conn
            .prepare(sql)
            .with_context(|| format!("Failed to prepare SQL: {}", &sql[..sql.len().min(100)]))?;

        let mut rows = stmt.query([]).context("Failed to execute query")?;

        let mut row_count = 0;
        while let Some(_row) = rows.next()? {
            row_count += 1;
        }

        Ok(QueryResult {
            row_count,
            phases: None, // DuckDB doesn't provide phase breakdown
        })
    }
}
