use crate::runner::{PhaseTiming, QueryResult};
use anyhow::{Context, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
use futures::StreamExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

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
        // Phase 1: SQL parsing + logical planning + optimization
        let plan_start = Instant::now();
        let df = self
            .ctx
            .sql(sql)
            .await
            .with_context(|| format!("Failed to parse SQL: {}", &sql[..sql.len().min(100)]))?;
        let plan_ms = plan_start.elapsed().as_secs_f64() * 1000.0;

        // Phase 2: Physical plan creation
        let physical_start = Instant::now();
        let task_ctx = self.ctx.task_ctx();
        let physical_plan = df
            .create_physical_plan()
            .await
            .context("Failed to create physical plan")?;
        let physical_ms = physical_start.elapsed().as_secs_f64() * 1000.0;

        // Phase 3: Execution
        let exec_start = Instant::now();
        let mut stream = datafusion::physical_plan::execute_stream(physical_plan, task_ctx)
            .context("Failed to execute query stream")?;

        let mut row_count = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            row_count += batch.num_rows();
        }
        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;

        Ok(QueryResult {
            row_count,
            phases: Some(PhaseTiming {
                plan_ms,
                physical_ms,
                exec_ms,
            }),
        })
    }
}
