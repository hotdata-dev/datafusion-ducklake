//! PostgreSQL catalog example
//!
//! ## Usage
//!
//! Default (connects to localhost:5432):
//! ```bash
//! cargo run --example postgres_catalog --features metadata-postgres
//! ```
//!
//! Custom PostgreSQL connection:
//! ```bash
//! POSTGRES_URL="postgresql://user:pass@host:5432/dbname" \
//!   cargo run --example postgres_catalog --features metadata-postgres
//! ```

use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, PostgresMetadataProvider};
use std::sync::Arc;

#[cfg(feature = "metadata-postgres")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::env;

    println!("==> PostgreSQL catalog example\n");

    let conn_str = env::var("POSTGRES_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/ducklake_catalog".to_string()
    });
    println!("Connecting to: {}", conn_str);

    let provider = PostgresMetadataProvider::new(&conn_str).await?;
    println!("✓ Connected\n");

    let catalog = DuckLakeCatalog::new(provider)?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    println!("==> Schemas:");
    ctx.sql("SELECT * FROM ducklake.information_schema.schemata")
        .await?
        .show()
        .await?;

    println!("\n==> Tables:");
    ctx.sql("SELECT * FROM ducklake.information_schema.tables")
        .await?
        .show()
        .await?;

    println!("\n==> Snapshots:");
    ctx.sql("SELECT * FROM ducklake.information_schema.snapshots")
        .await?
        .show()
        .await?;

    println!("\n✓ Example completed successfully");
    println!("\nNext steps:");
    println!("  1. Populate the catalog with schemas, tables, and data files");
    println!("  2. Query your DuckLake tables using SQL");
    println!("  3. Enjoy PostgreSQL metadata catalog support!");

    Ok(())
}

#[cfg(not(feature = "metadata-postgres"))]
fn main() {
    eprintln!("Error: This example requires the 'metadata-postgres' feature");
    eprintln!("Run with: cargo run --example postgres_catalog --features metadata-postgres");
    std::process::exit(1);
}
