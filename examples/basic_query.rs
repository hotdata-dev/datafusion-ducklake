//! Basic DuckLake query example with snapshot isolation
//!
//! This example demonstrates how to:
//! 1. Create a DuckLake catalog from a DuckDB catalog file
//! 2. Bind the catalog to a specific snapshot for query consistency
//! 3. Register it with DataFusion
//! 4. Execute a simple SELECT query
//!
//! ## Snapshot Isolation
//!
//! Each DuckLake catalog is bound to a specific snapshot ID at creation time.
//! This guarantees that all queries through that catalog see a consistent view
//! of the data, even if multiple schema/table lookups happen during query planning
//! or if the underlying data changes.
//!
//! To query data at different points in time, create separate catalogs bound to
//! different snapshot IDs.
//!
//! To run this example, you need:
//! - A DuckDB database file with DuckLake tables
//! - Parquet data files referenced by the catalog
//!
//! Usage: cargo run --example basic_query <catalog.db> <sql>

use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider, MetadataProvider};
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
    let provider = Arc::new(DuckdbMetadataProvider::new(catalog_path)?);

    // Get the current snapshot ID
    // This ensures query consistency - all metadata lookups will use this snapshot
    let snapshot_id = provider.get_current_snapshot()?;
    println!("Current snapshot ID: {}", snapshot_id);

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

    // Create the DuckLake catalog bound to the snapshot
    // This ensures all queries through this catalog see consistent data
    // from this specific snapshot, even if the underlying data changes
    let ducklake_catalog = DuckLakeCatalog::with_snapshot(provider, snapshot_id)?;

    // Alternative: Use the backward-compatible constructor that automatically
    // binds to the current snapshot:
    // let ducklake_catalog = DuckLakeCatalog::new(DuckdbMetadataProvider::new(catalog_path)?)?;

    println!("✓ Connected to DuckLake catalog");

    let config = SessionConfig::new().with_default_catalog_and_schema("ducklake", "main");

    // Create DataFusion session context
    let ctx = SessionContext::new_with_config_rt(config, runtime.clone());

    // Register the DuckLake catalog
    ctx.register_catalog("ducklake", Arc::new(ducklake_catalog));

    println!("✓ Registered DuckLake catalog with DataFusion");

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
