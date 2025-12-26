//! User-Defined Table Functions (UDTFs) for DuckLake catalog metadata
//!
//! This module provides DuckDB-style table functions for accessing catalog metadata:
//! - ducklake_snapshots() - List snapshots
//! - ducklake_table_info() - Table metadata with file statistics
//! - ducklake_list_files() - File enumeration
//! - ducklake_table_changes() - CDC: files added/removed between snapshots

use datafusion::catalog::TableFunctionImpl;
use datafusion::common::{Result as DataFusionResult, ScalarValue, plan_err};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::Expr;
use std::sync::Arc;

use crate::information_schema::{FilesTable, SnapshotsTable, TableChangesTable, TableInfoTable};
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

/// Table function for querying table changes (CDC):
/// `SELECT * FROM ducklake_table_changes('schema.table', start_snapshot, end_snapshot)`
///
/// Returns files added between two snapshots:
/// - INSERT changes: data files added (begin_snapshot in range)
/// - DELETE changes: delete files added (begin_snapshot in range)
#[derive(Debug)]
pub struct DucklakeTableChangesFunction {
    provider: Arc<dyn MetadataProvider>,
}

impl DucklakeTableChangesFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>) -> Self {
        Self { provider }
    }

    /// Parse table name in format "schema.table" or just "table" (defaults to "main" schema)
    fn parse_table_name(table_name: &str) -> (&str, &str) {
        if let Some(dot_pos) = table_name.find('.') {
            let schema = &table_name[..dot_pos];
            let table = &table_name[dot_pos + 1..];
            (schema, table)
        } else {
            ("main", table_name)
        }
    }
}

impl TableFunctionImpl for DucklakeTableChangesFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if exprs.len() != 3 {
            return plan_err!(
                "ducklake_table_changes() requires 3 arguments: \
                 ducklake_table_changes('schema.table', start_snapshot, end_snapshot)"
            );
        }

        // Parse table name argument
        let table_name = match &exprs[0] {
            Expr::Literal(ScalarValue::Utf8(Some(name)), _) => name.clone(),
            _ => {
                return plan_err!(
                    "First argument to ducklake_table_changes() must be a string literal \
                     (e.g., 'main.users' or 'users')"
                );
            }
        };

        // Parse start_snapshot argument
        let start_snapshot = match &exprs[1] {
            Expr::Literal(ScalarValue::Int64(Some(v)), _) => *v,
            Expr::Literal(ScalarValue::Int32(Some(v)), _) => *v as i64,
            _ => {
                return plan_err!(
                    "Second argument to ducklake_table_changes() must be an integer (start_snapshot)"
                );
            }
        };

        // Parse end_snapshot argument
        let end_snapshot = match &exprs[2] {
            Expr::Literal(ScalarValue::Int64(Some(v)), _) => *v,
            Expr::Literal(ScalarValue::Int32(Some(v)), _) => *v as i64,
            _ => {
                return plan_err!(
                    "Third argument to ducklake_table_changes() must be an integer (end_snapshot)"
                );
            }
        };

        // Look up the table to get table_id
        let (schema_name, table_name_only) = Self::parse_table_name(&table_name);

        let snapshot_id = self
            .provider
            .get_current_snapshot()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let schema = self
            .provider
            .get_schema_by_name(schema_name, snapshot_id)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Schema '{}' not found in catalog",
                    schema_name
                ))
            })?;

        let table = self
            .provider
            .get_table_by_name(schema.schema_id, table_name_only, snapshot_id)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Table '{}.{}' not found in catalog",
                    schema_name, table_name_only
                ))
            })?;

        Ok(Arc::new(TableChangesTable::new(
            self.provider.clone(),
            table.table_id,
            start_snapshot,
            end_snapshot,
        )))
    }
}

/// Helper function to register all DuckLake table functions with a SessionContext
///
/// Registers four table functions:
/// - `ducklake_snapshots()` - List all snapshots
/// - `ducklake_table_info()` - Table metadata with file statistics
/// - `ducklake_list_files()` - File enumeration
/// - `ducklake_table_changes('table', start, end)` - CDC: files added between snapshots
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
/// let df = ctx.sql("SELECT * FROM ducklake_table_changes('main.users', 0, 5)").await?;
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
    ctx.register_udtf(
        "ducklake_table_changes",
        Arc::new(DucklakeTableChangesFunction::new(provider.clone())),
    );
}
