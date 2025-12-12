//! Basic DuckLake query example
//!
//! This example demonstrates how to:
//! 1. Create a DuckLake catalog from a DuckDB catalog file
//! 2. Register it with DataFusion
//! 3. Execute a simple SELECT query
//!
//! To run this example, you need:
//! - A DuckDB database file with DuckLake tables
//! - Parquet data files referenced by the catalog
//!
//! Usage: cargo run --example basic_query

use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider, register_ducklake_functions};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use std::env;
use std::process::exit;
use std::sync::Arc;
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: cargo run --example basic_query catalog.db sql");
        exit(1);
    }
    let catalog_path = &args[1];
    let sql = &args[2];

    // // Path to your DuckLake catalog database
    // let catalog_path = "test_catalog.db";

    println!("Connecting to DuckLake catalog: {}", catalog_path);

    // Create the metadata provider
    let provider = DuckdbMetadataProvider::new(catalog_path)?;

    // Create runtime and register object stores
    // For MinIO or S3, register the object store with the runtime
    let runtime = Arc::new(RuntimeEnv::default());

    // Example: Register S3/MinIO object store
    let s3: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint("http://localhost:9000") // Your MinIO endpoint
            .with_bucket_name("ducklake-data") // Your bucket name
            .with_access_key_id("minioadmin") // Your credentials
            .with_secret_access_key("minioadmin") // Your credentials
            .with_region("us-west-2") // Any region works for MinIO
            .with_allow_http(true) // Required for http:// endpoints
            .build()?,
    );
    runtime.register_object_store(&Url::parse("s3://ducklake-data/")?, s3);

    // Create the DuckLake catalog
    let ducklake_catalog = DuckLakeCatalog::new(provider)?;

    println!("✓ Connected to DuckLake catalog");

    let config = SessionConfig::new().with_default_catalog_and_schema("ducklake", "main");

    // Create DataFusion session context
    let ctx = SessionContext::new_with_config_rt(config, runtime.clone());

    // Get the provider before moving the catalog
    let provider = ducklake_catalog.provider();

    // Register the DuckLake catalog (standard DataFusion pattern)
    ctx.register_catalog("ducklake", Arc::new(ducklake_catalog));

    // Register table functions (ducklake_snapshots, ducklake_table_info, ducklake_list_files)
    register_ducklake_functions(&ctx, provider);

    println!("✓ Registered DuckLake catalog with DataFusion");
    println!(
        "✓ Registered table functions (ducklake_snapshots, ducklake_table_info, ducklake_list_files)"
    );

    // List available schemas
    let catalogs = ctx.catalog_names();
    println!("\nAvailable catalogs: {:?}", catalogs);

    if let Some(catalog) = ctx.catalog("ducklake") {
        let schemas = catalog.schema_names();
        println!("Available schemas in 'ducklake' catalog: {:?}", schemas);

        // List tables in each schema
        for schema_name in &schemas {
            if let Some(schema) = catalog.schema(schema_name) {
                let tables = schema.table_names();
                println!("Available tables in schema '{}': {:?}", schema_name, tables);
            }
        }
    }

    // Example query (adjust schema and table names to match your data)
    // Uncomment and modify this once you have actual DuckLake data:

    println!("\nExecuting query...");
    let df = ctx.sql(sql).await?;

    // Show the query results
    df.show().await?;

    println!("\n✓ Example completed successfully!");
    println!("\nTo run a query, create a DuckLake database and uncomment the query section.");

    Ok(())
}
