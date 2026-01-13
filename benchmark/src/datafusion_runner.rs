use anyhow::Result;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
use std::path::Path;
use std::sync::Arc;

pub struct DataFusionRunner {
    ctx: SessionContext,
}

impl DataFusionRunner {
    pub async fn new(catalog_path: &Path) -> Result<Self> {
        // Create metadata provider
        let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap())?;

        // Create runtime (can register object stores here for S3/MinIO)
        let runtime = Arc::new(RuntimeEnv::default());

        // Create DuckLake catalog
        let catalog = DuckLakeCatalog::new(provider)?;

        // Create session with default catalog and schema configured
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new().with_default_catalog_and_schema("ducklake", "main"),
            runtime,
        );
        ctx.register_catalog("ducklake", Arc::new(catalog));

        Ok(Self { ctx })
    }

    pub async fn execute(&self, sql: &str) -> Result<QueryResult> {
        let df = self.ctx.sql(sql).await?;
        let batches = df.collect().await?;

        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        Ok(QueryResult { row_count })
    }
}

pub struct QueryResult {
    pub row_count: usize,
}
