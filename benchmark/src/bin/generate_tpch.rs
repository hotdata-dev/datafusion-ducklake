use anyhow::Result;
use clap::Parser;
use duckdb::Connection;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "generate-tpch")]
#[command(about = "Generate TPC-H data in DuckLake format")]
struct Args {
    /// Path for the DuckLake catalog database
    #[arg(short, long, default_value = "benchmark/data/tpch.ducklake")]
    catalog: PathBuf,

    /// Path for data files (Parquet storage)
    #[arg(short, long, default_value = "benchmark/data/tpch_files")]
    data_path: PathBuf,

    /// TPC-H scale factor (1 = 1GB, 10 = 10GB, etc.)
    #[arg(short, long, default_value = "1")]
    scale_factor: f64,
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("TPC-H DuckLake Data Generator");
    println!("=============================");
    println!("Catalog: {:?}", args.catalog);
    println!("Data path: {:?}", args.data_path);
    println!("Scale factor: {} (~{}GB)", args.scale_factor, args.scale_factor);
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
        INSTALL tpch;
        LOAD tpch;
        INSTALL ducklake;
        LOAD ducklake;
        "#,
    )?;

    // Generate TPC-H data in memory
    println!("Generating TPC-H data (SF={})...", args.scale_factor);
    conn.execute_batch(&format!("CALL dbgen(sf={})", args.scale_factor))?;

    // Create DuckLake catalog
    println!("Creating DuckLake catalog...");
    let attach_sql = format!(
        "ATTACH '{}' AS tpch_lake (TYPE ducklake, DATA_PATH '{}')",
        args.catalog.display(),
        args.data_path.display()
    );
    conn.execute(&attach_sql, [])?;

    // Create schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS tpch_lake.main", [])?;

    // Copy TPC-H tables to DuckLake
    let tables = [
        "customer", "lineitem", "nation", "orders",
        "part", "partsupp", "region", "supplier"
    ];

    for table in &tables {
        println!("  Copying {} to DuckLake...", table);
        conn.execute_batch(&format!(
            "CREATE TABLE tpch_lake.main.{} AS SELECT * FROM {}",
            table, table
        ))?;
    }

    println!("\nData generation complete!");
    println!("Catalog saved to: {:?}", args.catalog);

    // Print table statistics
    println!("\nTable Statistics:");
    println!("-----------------");
    for table in &tables {
        let count: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM tpch_lake.main.{}", table),
            [],
            |row| row.get(0),
        )?;
        println!("  {}: {} rows", table, count);
    }

    // Print data size
    let total_size = dir_size(&args.data_path)?;
    println!("\nTotal data size: {:.2} MB", total_size as f64 / 1_000_000.0);

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
