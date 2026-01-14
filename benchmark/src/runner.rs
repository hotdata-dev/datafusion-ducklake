use anyhow::Result;

/// Phase timing breakdown (in milliseconds)
#[derive(Debug, Clone, Default)]
pub struct PhaseTiming {
    /// SQL parsing + logical planning + optimization
    pub plan_ms: f64,
    /// Physical plan creation
    pub physical_ms: f64,
    /// Query execution
    pub exec_ms: f64,
}

/// Result of executing a query
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub row_count: usize,
    /// Optional phase timing (only available for DataFusion)
    pub phases: Option<PhaseTiming>,
}

/// Verify that two query results match (same row count)
pub fn assert_results_match(
    query_name: &str,
    result1: &QueryResult,
    runner1_name: &str,
    result2: &QueryResult,
    runner2_name: &str,
) -> Result<()> {
    if result1.row_count != result2.row_count {
        anyhow::bail!(
            "Row count mismatch for '{}': {} returned {} rows, {} returned {} rows",
            query_name,
            runner1_name,
            result1.row_count,
            runner2_name,
            result2.row_count
        );
    }
    Ok(())
}
