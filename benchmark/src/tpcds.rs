use anyhow::Result;
use duckdb::Connection;

/// TPC-DS query with metadata
#[derive(Clone)]
pub struct TpcdsQuery {
    pub name: String,
    pub sql: String,
}

/// TPC-DS tables (24 tables)
pub const TPCDS_TABLES: &[&str] = &[
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
];

/// Fetch TPC-DS queries from DuckDB extension
pub fn get_tpcds_queries() -> Result<Vec<TpcdsQuery>> {
    let conn = Connection::open_in_memory()?;

    // Load TPC-DS extension
    conn.execute_batch(
        r#"
        INSTALL tpcds;
        LOAD tpcds;
        "#,
    )?;

    // Fetch all 99 queries
    let mut stmt = conn.prepare("SELECT query_nr, query FROM tpcds_queries() ORDER BY query_nr")?;
    let mut rows = stmt.query([])?;

    let mut queries = Vec::new();
    while let Some(row) = rows.next()? {
        let query_nr: i32 = row.get(0)?;
        let query: String = row.get(1)?;

        // Prefix table names with main. for DuckLake schema
        let sql = prefix_table_names(&query);

        queries.push(TpcdsQuery {
            name: format!("ds{:02}", query_nr),
            sql,
        });
    }

    Ok(queries)
}

/// Add main. prefix to TPC-DS table names for DuckLake schema
// TODO: Replace regex-based table rewriting with a proper SQL parser for robustness
fn prefix_table_names(query: &str) -> String {
    let mut result = query.to_string();

    for table in TPCDS_TABLES {
        // Match table name followed by whitespace, comma, or end - indicating it's a table reference
        // Capture the trailing character to preserve it in replacement
        let patterns = vec![
            // FROM table (followed by whitespace or newline)
            (
                regex::Regex::new(&format!(r"(?i)(FROM\s+){}(\s)", table)).unwrap(),
                format!("${{1}}main.{}${{2}}", table),
            ),
            // JOIN table (followed by whitespace or newline)
            (
                regex::Regex::new(&format!(r"(?i)(JOIN\s+){}(\s)", table)).unwrap(),
                format!("${{1}}main.{}${{2}}", table),
            ),
            // , table (followed by whitespace)
            (
                regex::Regex::new(&format!(r"(,\s*){}(\s)", table)).unwrap(),
                format!("${{1}}main.{}${{2}}", table),
            ),
            // FROM table, (followed by comma - for multi-table FROM)
            (
                regex::Regex::new(&format!(r"(?i)(FROM\s+)({})(,)", table)).unwrap(),
                format!("${{1}}main.{}${{3}}", table),
            ),
        ];

        for (re, replacement) in patterns {
            result = re.replace_all(&result, replacement.as_str()).to_string();
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fetch_tpcds_queries() {
        let queries = get_tpcds_queries().unwrap();
        assert_eq!(queries.len(), 99);
        assert_eq!(queries[0].name, "ds01");
    }
}
