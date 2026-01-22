use crate::metrics::BenchmarkMetrics;
use anyhow::Result;
use chrono::Utc;
use serde::Serialize;
use std::fs;
use std::path::Path;

#[derive(Serialize)]
pub struct QueryResult {
    pub query_name: String,
    pub duckdb: BenchmarkMetrics,
    pub datafusion: BenchmarkMetrics,
}

#[derive(Serialize)]
struct Report {
    timestamp: String,
    results: Vec<QueryResult>,
    summary: Summary,
}

#[derive(Serialize)]
struct Summary {
    total_queries: usize,
    duckdb_faster_count: usize,
    datafusion_faster_count: usize,
    avg_speedup_ratio: f64,
}

pub fn generate(output_dir: &Path, results: &[QueryResult]) -> Result<()> {
    fs::create_dir_all(output_dir)?;

    let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();

    // Compute summary
    let mut duckdb_faster = 0;
    let mut datafusion_faster = 0;
    let mut speedup_ratios = Vec::new();

    for r in results {
        if r.duckdb.avg_ms < r.datafusion.avg_ms {
            duckdb_faster += 1;
        } else {
            datafusion_faster += 1;
        }
        speedup_ratios.push(r.datafusion.avg_ms / r.duckdb.avg_ms);
    }

    let avg_speedup = speedup_ratios.iter().sum::<f64>() / speedup_ratios.len() as f64;

    let report = Report {
        timestamp: timestamp.clone(),
        results: results.to_vec(),
        summary: Summary {
            total_queries: results.len(),
            duckdb_faster_count: duckdb_faster,
            datafusion_faster_count: datafusion_faster,
            avg_speedup_ratio: avg_speedup,
        },
    };

    // Write JSON report
    let json_path = output_dir.join(format!("report_{}.json", timestamp));
    let json = serde_json::to_string_pretty(&report)?;
    fs::write(&json_path, json)?;

    // Write CSV report
    let csv_path = output_dir.join(format!("report_{}.csv", timestamp));
    let mut csv_writer = csv::Writer::from_path(&csv_path)?;

    csv_writer.write_record([
        "query",
        "duckdb_avg_ms",
        "duckdb_min_ms",
        "duckdb_max_ms",
        "datafusion_avg_ms",
        "datafusion_min_ms",
        "datafusion_max_ms",
        "df_plan_ms",
        "df_physical_ms",
        "df_exec_ms",
        "speedup_ratio",
        "row_count",
    ])?;

    for r in results {
        let (plan_ms, physical_ms, exec_ms) = r
            .datafusion
            .phases
            .as_ref()
            .map(|p| (p.plan_ms, p.physical_ms, p.exec_ms))
            .unwrap_or((0.0, 0.0, 0.0));

        csv_writer.write_record([
            &r.query_name,
            &format!("{:.2}", r.duckdb.avg_ms),
            &format!("{:.2}", r.duckdb.min_ms),
            &format!("{:.2}", r.duckdb.max_ms),
            &format!("{:.2}", r.datafusion.avg_ms),
            &format!("{:.2}", r.datafusion.min_ms),
            &format!("{:.2}", r.datafusion.max_ms),
            &format!("{:.2}", plan_ms),
            &format!("{:.2}", physical_ms),
            &format!("{:.2}", exec_ms),
            &format!("{:.2}", r.datafusion.avg_ms / r.duckdb.avg_ms),
            &r.duckdb.row_count.to_string(),
        ])?;
    }

    csv_writer.flush()?;

    // Print summary
    println!("\n=== Summary ===");
    println!("Total queries: {}", results.len());
    println!("DuckDB faster: {}", duckdb_faster);
    println!("DataFusion faster: {}", datafusion_faster);
    println!("Avg speedup ratio (DF/DuckDB): {:.2}x", avg_speedup);

    Ok(())
}

impl Clone for QueryResult {
    fn clone(&self) -> Self {
        Self {
            query_name: self.query_name.clone(),
            duckdb: self.duckdb.clone(),
            datafusion: self.datafusion.clone(),
        }
    }
}
