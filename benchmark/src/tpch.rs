use anyhow::Result;
use duckdb::Connection;

/// TPC-H tables (8 tables)
pub const TPCH_TABLES: &[&str] =
    &["lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region"];

/// TPC-H query with metadata
#[derive(Clone)]
pub struct TpchQuery {
    pub name: String,
    pub category: String,
    pub description: String,
    pub sql: String,
}

/// TPC-H query categories and descriptions
fn get_query_metadata(query_nr: i32) -> (&'static str, &'static str) {
    match query_nr {
        1 => ("Aggregation", "Pricing Summary Report"),
        2 => ("Subquery", "Minimum Cost Supplier"),
        3 => ("Join+Aggregation", "Shipping Priority"),
        4 => ("Subquery", "Order Priority Checking"),
        5 => ("Join+Aggregation", "Local Supplier Volume"),
        6 => ("Scan+Filter", "Forecasting Revenue Change"),
        7 => ("Join+Aggregation", "Volume Shipping"),
        8 => ("Join+Aggregation", "National Market Share"),
        9 => ("Join+Aggregation", "Product Type Profit"),
        10 => ("Join+Aggregation", "Returned Item Reporting"),
        11 => ("Subquery+Aggregation", "Important Stock Identification"),
        12 => ("Join+Aggregation", "Shipping Modes and Order Priority"),
        13 => ("Outer Join", "Customer Distribution"),
        14 => ("Join+Aggregation", "Promotion Effect"),
        15 => ("CTE+Subquery", "Top Supplier"),
        16 => ("Subquery+Aggregation", "Parts/Supplier Relationship"),
        17 => ("Correlated Subquery", "Small-Quantity-Order Revenue"),
        18 => ("Subquery+Aggregation", "Large Volume Customer"),
        19 => ("Join+Filter", "Discounted Revenue"),
        20 => ("Nested Subquery", "Potential Part Promotion"),
        21 => ("Correlated Subquery", "Suppliers Who Kept Orders Waiting"),
        22 => ("Subquery+Aggregation", "Global Sales Opportunity"),
        _ => ("Unknown", "Unknown Query"),
    }
}

/// Fetch TPC-H queries with full metadata (category, description)
pub fn get_tpch_queries_with_metadata() -> Result<Vec<TpchQuery>> {
    let conn = Connection::open_in_memory()?;

    // Load TPC-H extension
    conn.execute_batch(
        r#"
        INSTALL tpch;
        LOAD tpch;
        "#,
    )?;

    // Fetch all 22 queries
    let mut stmt = conn.prepare("SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr")?;
    let mut rows = stmt.query([])?;

    let mut queries = Vec::new();
    while let Some(row) = rows.next()? {
        let query_nr: i32 = row.get(0)?;
        let query: String = row.get(1)?;

        // Prefix table names with main. for DuckLake schema
        let sql = prefix_table_names(&query);

        let (category, description) = get_query_metadata(query_nr);

        queries.push(TpchQuery {
            name: format!("q{:02}", query_nr),
            category: category.to_string(),
            description: description.to_string(),
            sql,
        });
    }

    Ok(queries)
}

/// Add main. prefix to TPC-H table names for DuckLake schema
// TODO: Replace regex-based table rewriting with a proper SQL parser for robustness
fn prefix_table_names(query: &str) -> String {
    let mut result = query.to_string();
    for table in TPCH_TABLES {
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
    fn test_fetch_queries() {
        let queries = get_tpch_queries_with_metadata().unwrap();
        assert_eq!(queries.len(), 22);
        assert_eq!(queries[0].name, "q01");
        assert!(queries[0].sql.contains("main.lineitem"));
    }
}
