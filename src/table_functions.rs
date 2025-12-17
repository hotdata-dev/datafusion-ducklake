//! User-Defined Table Functions (UDTFs) for DuckLake catalog metadata
//!
//! This module provides DuckDB-style table functions for accessing catalog metadata:
//! - ducklake_snapshots() - List snapshots
//! - ducklake_table_info() - Table metadata with file statistics
//! - ducklake_list_files() - File enumeration

use datafusion::catalog::TableFunctionImpl;
use datafusion::common::{Result as DataFusionResult, plan_err};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::Expr;
use std::sync::Arc;

use crate::information_schema::{FilesTable, SnapshotsTable, TableInfoTable};
use crate::metadata_provider::MetadataProvider;

/// Table function for querying snapshots: `SELECT * FROM ducklake_snapshots()`
#[derive(Debug)]
pub struct DucklakeSnapshotsFunction {
    provider: Arc<dyn MetadataProvider>,
}

impl DucklakeSnapshotsFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>) -> Self {
        Self {
            provider,
        }
    }
}

impl TableFunctionImpl for DucklakeSnapshotsFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("ducklake_snapshots() takes no arguments");
        }

        Ok(Arc::new(SnapshotsTable::new(self.provider.clone())))
    }
}

/// Table function for querying table info: `SELECT * FROM ducklake_table_info()`
#[derive(Debug)]
pub struct DucklakeTableInfoFunction {
    provider: Arc<dyn MetadataProvider>,
}

impl DucklakeTableInfoFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>) -> Self {
        Self {
            provider,
        }
    }
}

impl TableFunctionImpl for DucklakeTableInfoFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("ducklake_table_info() takes no arguments");
        }

        Ok(Arc::new(TableInfoTable::new(self.provider.clone())))
    }
}

/// Table function for querying files: `SELECT * FROM ducklake_list_files()`
#[derive(Debug)]
pub struct DucklakeListFilesFunction {
    provider: Arc<dyn MetadataProvider>,
}

impl DucklakeListFilesFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>) -> Self {
        Self {
            provider,
        }
    }
}

impl TableFunctionImpl for DucklakeListFilesFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("ducklake_list_files() takes no arguments");
        }

        Ok(Arc::new(FilesTable::new(self.provider.clone())))
    }
}

/// Helper function to register all DuckLake table functions with a SessionContext
///
/// Registers three table functions:
/// - `ducklake_snapshots()` - List all snapshots
/// - `ducklake_table_info()` - Table metadata with file statistics
/// - `ducklake_list_files()` - File enumeration
///
/// # Example
///
/// ```no_run
/// use datafusion::prelude::*;
/// use datafusion_ducklake::{DuckdbMetadataProvider, register_ducklake_functions};
/// use std::sync::Arc;
///
/// # async fn example() -> datafusion_ducklake::Result<()> {
/// let ctx = SessionContext::new();
/// let provider = DuckdbMetadataProvider::new("catalog.db")?;
///
/// // Register all ducklake_*() functions
/// register_ducklake_functions(&ctx, Arc::new(provider));
///
/// // Now you can use them
/// let df = ctx.sql("SELECT * FROM ducklake_snapshots()").await?;
/// let df = ctx.sql("SELECT * FROM ducklake_table_info()").await?;
/// let df = ctx.sql("SELECT * FROM ducklake_list_files()").await?;
/// # Ok(())
/// # }
/// ```
pub fn register_ducklake_functions(
    ctx: &datafusion::execution::context::SessionContext,
    provider: Arc<dyn MetadataProvider>,
) {
    ctx.register_udtf(
        "ducklake_snapshots",
        Arc::new(DucklakeSnapshotsFunction::new(provider.clone())),
    );
    ctx.register_udtf(
        "ducklake_table_info",
        Arc::new(DucklakeTableInfoFunction::new(provider.clone())),
    );
    ctx.register_udtf(
        "ducklake_list_files",
        Arc::new(DucklakeListFilesFunction::new(provider.clone())),
    );
}
