use anyhow::Result;
use duckdb::Connection;
use std::path::Path;

pub struct DuckDbRunner {
    conn: Connection,
}

impl DuckDbRunner {
    pub fn new(catalog_path: &Path) -> Result<Self> {
        let conn = Connection::open_in_memory()?;

        // Install and load DuckLake extension
        conn.execute_batch(
            r#"
            INSTALL ducklake;
            LOAD ducklake;
            "#,
        )?;

        // Attach the DuckLake catalog
        let attach_sql = format!(
            "ATTACH '{}' AS ducklake (TYPE ducklake)",
            catalog_path.display()
        );
        conn.execute(&attach_sql, [])?;

        // Set as default catalog
        conn.execute("USE ducklake", [])?;

        Ok(Self { conn })
    }

    pub fn execute(&self, sql: &str) -> Result<QueryResult> {
        let mut stmt = self.conn.prepare(sql)?;
        let mut rows = stmt.query([])?;

        let mut row_count = 0;
        while let Some(_row) = rows.next()? {
            row_count += 1;
        }

        Ok(QueryResult { row_count })
    }
}

pub struct QueryResult {
    pub row_count: usize,
}
