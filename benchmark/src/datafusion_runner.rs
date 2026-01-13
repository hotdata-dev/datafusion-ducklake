use anyhow::{Context, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
use futures::StreamExt;
use std::path::Path;
use std::sync::Arc;

pub struct DataFusionRunner {
    ctx: SessionContext,
}

impl DataFusionRunner {
    pub async fn new(catalog_path: &Path) -> Result<Self> {
        // Create metadata provider
        let provider =
            DuckdbMetadataProvider::new(catalog_path.to_str().unwrap()).with_context(|| {
                format!("Failed to create metadata provider for {:?}", catalog_path)
            })?;

        // Create runtime (can register object stores here for S3/MinIO)
        let runtime = Arc::new(RuntimeEnv::default());

        // Create DuckLake catalog
        let catalog =
            DuckLakeCatalog::new(provider).context("Failed to create DuckLake catalog")?;

        // Create session with default catalog and schema configured
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new().with_default_catalog_and_schema("ducklake", "main"),
            runtime,
        );
        ctx.register_catalog("ducklake", Arc::new(catalog));

        Ok(Self {
            ctx,
        })
    }

    pub async fn execute(&self, sql: &str) -> Result<QueryResult> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .with_context(|| format!("Failed to parse SQL: {}", &sql[..sql.len().min(100)]))?;

        // Use streaming to count rows without loading all data into memory
        let mut stream = df
            .execute_stream()
            .await
            .context("Failed to execute query stream")?;

        let mut row_count = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            row_count += batch.num_rows();
        }

        Ok(QueryResult {
            row_count,
        })
    }
}

pub struct QueryResult {
    pub row_count: usize,
}
