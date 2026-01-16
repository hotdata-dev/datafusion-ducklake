use anyhow::Result;
use clap::Parser;
use duckdb::Connection;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "generate-tpcds")]
#[command(about = "Generate TPC-DS data in DuckLake format")]
struct Args {
    /// Path for the DuckLake catalog database
    #[arg(short, long, default_value = "benchmark/data/tpcds.ducklake")]
    catalog: PathBuf,

    /// Path for data files (Parquet storage)
    #[arg(short, long, default_value = "benchmark/data/tpcds_files")]
    data_path: PathBuf,

    /// TPC-DS scale factor (1 = 1GB, 10 = 10GB, etc.)
    #[arg(short, long, default_value = "1")]
    scale_factor: f64,
}

/// TPC-DS tables (24 tables)
const TPCDS_TABLES: &[&str] = &[
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

fn main() -> Result<()> {
    let args = Args::parse();

    println!("TPC-DS DuckLake Data Generator");
    println!("==============================");
    println!("Catalog: {:?}", args.catalog);
    println!("Data path: {:?}", args.data_path);
    println!(
        "Scale factor: {} (~{}GB)",
        args.scale_factor, args.scale_factor
    );
    println!();

    // Ensure directories exist
    std::fs::create_dir_all(&args.data_path)?;
    if let Some(parent) = args.catalog.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Remove existing catalog if present
    if args.catalog.exists() {
        std::fs::remove_file(&args.catalog)?;
    }

    let conn = Connection::open_in_memory()?;

    // Install and load extensions
    println!("Installing extensions...");
    conn.execute_batch(
        r#"
        INSTALL tpcds;
        LOAD tpcds;
        INSTALL ducklake;
        LOAD ducklake;
        "#,
    )?;

    // Generate TPC-DS data in memory
    println!("Generating TPC-DS data (SF={})...", args.scale_factor);
    println!("  This may take a while for large scale factors...");
    conn.execute_batch(&format!("CALL dsdgen(sf={})", args.scale_factor))?;

    // Create DuckLake catalog
    println!("Creating DuckLake catalog...");
    let attach_sql = format!(
        "ATTACH '{}' AS tpcds_lake (TYPE ducklake, DATA_PATH '{}')",
        args.catalog.display(),
        args.data_path.display()
    );
    conn.execute(&attach_sql, [])?;

    // Create schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS tpcds_lake.main", [])?;

    // Copy TPC-DS tables to DuckLake
    for table in TPCDS_TABLES {
        print!("  Copying {} to DuckLake... ", table);
        conn.execute_batch(&format!(
            "CREATE TABLE tpcds_lake.main.{} AS SELECT * FROM {}",
            table, table
        ))?;

        // Get row count
        let count: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM tpcds_lake.main.{}", table),
            [],
            |row| row.get(0),
        )?;
        println!("{} rows", count);
    }

    println!("\nData generation complete!");
    println!("Catalog saved to: {:?}", args.catalog);

    // Print data size
    let total_size = dir_size(&args.data_path)?;
    println!(
        "\nTotal data size: {:.2} GB",
        total_size as f64 / 1_000_000_000.0
    );

    Ok(())
}

fn dir_size(path: &PathBuf) -> Result<u64> {
    let mut size = 0;
    for entry in walkdir::WalkDir::new(path) {
        let entry = entry?;
        if entry.file_type().is_file() {
            size += entry.metadata()?.len();
        }
    }
    Ok(size)
}
