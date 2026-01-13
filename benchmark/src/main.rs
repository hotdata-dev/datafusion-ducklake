mod datafusion_runner;
mod duckdb_runner;
mod metrics;
mod report;
mod runner;
mod tpcds;
mod tpch;

use anyhow::{Context, Result};
use clap::Parser;
use runner::assert_results_match;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "ducklake-benchmark")]
#[command(about = "Benchmark comparing DuckDB-DuckLake vs DataFusion-DuckLake performance")]
struct Args {
    /// Path to the DuckLake catalog database
    #[arg(short, long)]
    catalog: PathBuf,

    /// Use TPC-H queries from DuckDB's tpch extension (22 queries)
    #[arg(long)]
    tpch: bool,

    /// Use TPC-DS queries from DuckDB's tpcds extension (99 queries)
    #[arg(long)]
    tpcds: bool,

    /// Directory containing custom SQL query files
    #[arg(short, long)]
    queries: Option<PathBuf>,

    /// Number of iterations per query
    #[arg(short, long, default_value = "5")]
    iterations: usize,

    /// Output directory for results
    #[arg(short, long, default_value = "benchmark/results")]
    output: PathBuf,

    /// Skip warmup iteration
    #[arg(long)]
    no_warmup: bool,

    /// Skip queries that fail and continue (useful for TPC-DS where some queries may not be supported)
    #[arg(long)]
    skip_errors: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║   DuckLake Query Engine Comparison                            ║");
    println!("║   DuckDB-DuckLake vs DataFusion-DuckLake                      ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();
    println!("Catalog:    {:?}", args.catalog);
    println!("Iterations: {}", args.iterations);
    println!("Warmup:     {}", !args.no_warmup);

    // Initialize runners
    let duckdb_runner = duckdb_runner::DuckDbRunner::new(&args.catalog)?;
    let datafusion_runner = datafusion_runner::DataFusionRunner::new(&args.catalog).await?;

    let mut all_results = Vec::new();

    if args.tpch {
        println!("Queries:    TPC-H (from DuckDB extension)");
        println!();

        // Show dataset sizes
        println!("Dataset:");
        let table_sizes = duckdb_runner.get_table_sizes(tpch::TPCH_TABLES)?;
        let total_rows: usize = table_sizes.iter().map(|(_, count)| count).sum();
        let data_size_bytes = duckdb_runner.get_data_size_bytes()?;
        let data_size_gb = data_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        for (table, count) in &table_sizes {
            println!("  {:>12}: {:>12} rows", table, count);
        }
        println!("  {:>12}: {:>12} rows", "TOTAL", total_rows);
        println!("  {:>12}: {:>12.2} GB (Parquet)", "Size", data_size_gb);
        println!();

        let queries = tpch::get_tpch_queries_with_metadata()?;
        println!("Loaded {} TPC-H queries\n", queries.len());

        // Separate warmup phase - run all queries once before timing and verify row counts match
        if !args.no_warmup {
            println!("───────────────────────────────────────────────────────────────");
            println!("Warmup phase: running all queries once and verifying row counts...");
            for (i, query) in queries.iter().enumerate() {
                print!("  [{:>2}/{}] {}... ", i + 1, queries.len(), query.name);
                let duckdb_result = duckdb_runner
                    .execute(&query.sql)
                    .with_context(|| format!("DuckDB failed on {}", query.name))?;
                let datafusion_result = datafusion_runner
                    .execute(&query.sql)
                    .await
                    .with_context(|| format!("DataFusion failed on {}", query.name))?;
                assert_results_match(
                    &query.name,
                    &duckdb_result,
                    "DuckDB",
                    &datafusion_result,
                    "DataFusion",
                )?;
                println!("done ({} rows)", duckdb_result.row_count);
            }
            println!("Warmup complete.\n");
        }

        for query in &queries {
            println!("═══════════════════════════════════════════════════════════════");
            println!(
                "{} [{}] - {}",
                query.name, query.category, query.description
            );
            println!("───────────────────────────────────────────────────────────────");

            // Show query (truncate long lines)
            for line in query.sql.lines().take(12) {
                println!("  {}", line.chars().take(65).collect::<String>());
            }
            if query.sql.lines().count() > 12 {
                println!("  ... ({} more lines)", query.sql.lines().count() - 12);
            }
            println!("───────────────────────────────────────────────────────────────");

            // Benchmark DuckDB
            print!("  DuckDB:     ");
            let duckdb_metrics =
                metrics::benchmark(|| duckdb_runner.execute(&query.sql), args.iterations)?;
            println!(
                "avg={:>8.2}ms  min={:>8.2}ms  max={:>8.2}ms",
                duckdb_metrics.avg_ms, duckdb_metrics.min_ms, duckdb_metrics.max_ms
            );

            // Benchmark DataFusion
            print!("  DataFusion: ");
            let datafusion_metrics =
                metrics::benchmark_async(|| datafusion_runner.execute(&query.sql), args.iterations)
                    .await?;
            println!(
                "avg={:>8.2}ms  min={:>8.2}ms  max={:>8.2}ms",
                datafusion_metrics.avg_ms, datafusion_metrics.min_ms, datafusion_metrics.max_ms
            );

            // Show ratio
            let ratio = datafusion_metrics.avg_ms / duckdb_metrics.avg_ms;
            let indicator = if ratio < 1.5 {
                "★★★"
            } else if ratio < 3.0 {
                "★★"
            } else if ratio < 5.0 {
                "★"
            } else {
                ""
            };
            println!("  Ratio:      {:.2}x {}", ratio, indicator);

            all_results.push(report::QueryResult {
                query_name: format!("{} [{}]", query.name, query.category),
                duckdb: duckdb_metrics,
                datafusion: datafusion_metrics,
            });
        }
    } else if args.tpcds {
        println!("Queries:    TPC-DS (from DuckDB extension)");
        println!();

        // Show dataset sizes
        println!("Dataset:");
        let table_sizes = duckdb_runner.get_table_sizes(tpcds::TPCDS_TABLES)?;
        let total_rows: usize = table_sizes.iter().map(|(_, count)| count).sum();
        let data_size_bytes = duckdb_runner.get_data_size_bytes()?;
        let data_size_gb = data_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        for (table, count) in &table_sizes {
            println!("  {:>20}: {:>12} rows", table, count);
        }
        println!("  {:>20}: {:>12} rows", "TOTAL", total_rows);
        println!("  {:>20}: {:>12.2} GB (Parquet)", "Size", data_size_gb);
        println!();

        let queries = tpcds::get_tpcds_queries()?;
        println!("Loaded {} TPC-DS queries\n", queries.len());

        // Track which queries to skip (failed during warmup)
        let mut skipped_queries: Vec<(String, String)> = Vec::new();

        // Separate warmup phase
        if !args.no_warmup {
            println!("───────────────────────────────────────────────────────────────");
            println!("Warmup phase: running all queries once and verifying row counts...");
            for (i, query) in queries.iter().enumerate() {
                print!("  [{:>2}/{}] {}... ", i + 1, queries.len(), query.name);

                let duckdb_result = duckdb_runner.execute(&query.sql);
                let datafusion_result = datafusion_runner.execute(&query.sql).await;

                match (&duckdb_result, &datafusion_result) {
                    (Ok(duck), Ok(df)) => {
                        if let Err(e) = assert_results_match(
                            &query.name,
                            duck,
                            "DuckDB",
                            df,
                            "DataFusion",
                        ) {
                            if args.skip_errors {
                                println!("SKIP (row mismatch)");
                                skipped_queries.push((query.name.clone(), e.to_string()));
                            } else {
                                return Err(e);
                            }
                        } else {
                            println!("done ({} rows)", duck.row_count);
                        }
                    }
                    (Err(e), _) => {
                        if args.skip_errors {
                            println!("SKIP (DuckDB error)");
                            skipped_queries.push((query.name.clone(), format!("DuckDB: {}", e)));
                        } else {
                            return Err(anyhow::anyhow!("DuckDB failed on {}: {}", query.name, e));
                        }
                    }
                    (_, Err(e)) => {
                        if args.skip_errors {
                            println!("SKIP (DataFusion error)");
                            skipped_queries.push((query.name.clone(), format!("DataFusion: {}", e)));
                        } else {
                            return Err(anyhow::anyhow!("DataFusion failed on {}: {}", query.name, e));
                        }
                    }
                }
            }
            if !skipped_queries.is_empty() {
                println!("\nSkipped {} queries due to errors", skipped_queries.len());
            }
            println!("Warmup complete.\n");
        }

        for query in &queries {
            // Skip queries that failed during warmup
            if skipped_queries.iter().any(|(name, _)| name == &query.name) {
                continue;
            }

            println!("═══════════════════════════════════════════════════════════════");
            println!("{}", query.name);
            println!("───────────────────────────────────────────────────────────────");

            // Show query (truncate long lines)
            for line in query.sql.lines().take(8) {
                println!("  {}", line.chars().take(65).collect::<String>());
            }
            if query.sql.lines().count() > 8 {
                println!("  ... ({} more lines)", query.sql.lines().count() - 8);
            }
            println!("───────────────────────────────────────────────────────────────");

            // Benchmark DuckDB
            print!("  DuckDB:     ");
            let duckdb_metrics =
                metrics::benchmark(|| duckdb_runner.execute(&query.sql), args.iterations)?;
            println!(
                "avg={:>8.2}ms  min={:>8.2}ms  max={:>8.2}ms",
                duckdb_metrics.avg_ms, duckdb_metrics.min_ms, duckdb_metrics.max_ms
            );

            // Benchmark DataFusion
            print!("  DataFusion: ");
            let datafusion_metrics =
                metrics::benchmark_async(|| datafusion_runner.execute(&query.sql), args.iterations)
                    .await?;
            println!(
                "avg={:>8.2}ms  min={:>8.2}ms  max={:>8.2}ms",
                datafusion_metrics.avg_ms, datafusion_metrics.min_ms, datafusion_metrics.max_ms
            );

            // Show ratio
            let ratio = datafusion_metrics.avg_ms / duckdb_metrics.avg_ms;
            let indicator = if ratio < 1.5 {
                "★★★"
            } else if ratio < 3.0 {
                "★★"
            } else if ratio < 5.0 {
                "★"
            } else {
                ""
            };
            println!("  Ratio:      {:.2}x {}", ratio, indicator);

            all_results.push(report::QueryResult {
                query_name: query.name.clone(),
                duckdb: duckdb_metrics,
                datafusion: datafusion_metrics,
            });
        }

        // Print skipped queries summary at the end
        if !skipped_queries.is_empty() {
            println!("\n═══════════════════════════════════════════════════════════════");
            println!("SKIPPED QUERIES ({}):", skipped_queries.len());
            println!("───────────────────────────────────────────────────────────────");
            for (name, reason) in &skipped_queries {
                println!("  {}: {}", name, reason.lines().next().unwrap_or("Unknown"));
            }
        }
    } else if let Some(queries_dir) = &args.queries {
        println!("Queries:    {:?}", queries_dir);
        println!();

        let queries = load_sql_queries(queries_dir)?;
        println!("Loaded {} queries\n", queries.len());

        // Separate warmup phase - run all queries once before timing and verify row counts match
        if !args.no_warmup {
            println!("───────────────────────────────────────────────────────────────");
            println!("Warmup phase: running all queries once and verifying row counts...");
            for (i, (name, sql)) in queries.iter().enumerate() {
                print!("  [{:>2}/{}] {}... ", i + 1, queries.len(), name);
                let duckdb_result = duckdb_runner
                    .execute(sql)
                    .with_context(|| format!("DuckDB failed on {}", name))?;
                let datafusion_result = datafusion_runner
                    .execute(sql)
                    .await
                    .with_context(|| format!("DataFusion failed on {}", name))?;
                assert_results_match(
                    name,
                    &duckdb_result,
                    "DuckDB",
                    &datafusion_result,
                    "DataFusion",
                )?;
                println!("done ({} rows)", duckdb_result.row_count);
            }
            println!("Warmup complete.\n");
        }

        for (name, sql) in &queries {
            println!("═══════════════════════════════════════════════════════════════");
            println!("{}", name);
            println!("───────────────────────────────────────────────────────────────");

            // Benchmark DuckDB
            print!("  DuckDB:     ");
            let duckdb_metrics =
                metrics::benchmark(|| duckdb_runner.execute(sql), args.iterations)?;
            println!(
                "avg={:>8.2}ms  min={:>8.2}ms  max={:>8.2}ms",
                duckdb_metrics.avg_ms, duckdb_metrics.min_ms, duckdb_metrics.max_ms
            );

            // Benchmark DataFusion
            print!("  DataFusion: ");
            let datafusion_metrics =
                metrics::benchmark_async(|| datafusion_runner.execute(sql), args.iterations)
                    .await?;
            println!(
                "avg={:>8.2}ms  min={:>8.2}ms  max={:>8.2}ms",
                datafusion_metrics.avg_ms, datafusion_metrics.min_ms, datafusion_metrics.max_ms
            );

            // Show ratio
            let ratio = datafusion_metrics.avg_ms / duckdb_metrics.avg_ms;
            println!("  Ratio:      {:.2}x", ratio);

            all_results.push(report::QueryResult {
                query_name: name.clone(),
                duckdb: duckdb_metrics,
                datafusion: datafusion_metrics,
            });
        }
    } else {
        println!("\nError: Specify --tpch, --tpcds, or --queries <dir>");
        std::process::exit(1);
    }

    // Print comparison table
    println!("\n═══════════════════════════════════════════════════════════════════════════");
    println!("                            COMPARISON TABLE");
    println!("═══════════════════════════════════════════════════════════════════════════");
    println!(
        "{:<25} {:>10} {:>12} {:>12} {:>10}",
        "Query", "Rows", "DuckDB (ms)", "DataFusion", "Ratio"
    );
    println!("───────────────────────────────────────────────────────────────────────────");

    for r in &all_results {
        let ratio = r.datafusion.avg_ms / r.duckdb.avg_ms;
        let winner = if ratio < 1.0 {
            "◀"
        } else {
            ""
        };
        println!(
            "{:<25} {:>10} {:>12.2} {:>12.2} {:>8.2}x {}",
            &r.query_name[..r.query_name.len().min(25)],
            r.duckdb.row_count,
            r.duckdb.avg_ms,
            r.datafusion.avg_ms,
            ratio,
            winner
        );
    }

    println!("───────────────────────────────────────────────────────────────────────────");

    // Calculate totals
    let total_duckdb: f64 = all_results.iter().map(|r| r.duckdb.avg_ms).sum();
    let total_datafusion: f64 = all_results.iter().map(|r| r.datafusion.avg_ms).sum();
    let total_ratio = total_datafusion / total_duckdb;

    println!(
        "{:<25} {:>10} {:>12.2} {:>12.2} {:>8.2}x",
        "TOTAL", "", total_duckdb, total_datafusion, total_ratio
    );

    // Generate report
    report::generate(&args.output, &all_results)?;
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("Results written to {:?}", args.output);

    Ok(())
}

fn load_sql_queries(dir: &PathBuf) -> Result<Vec<(String, String)>> {
    let mut queries = Vec::new();

    if dir.is_dir() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "sql") {
                let name = path.file_stem().unwrap().to_string_lossy().to_string();
                let sql = std::fs::read_to_string(&path)?;
                queries.push((name, sql));
            }
        }
    }

    queries.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(queries)
}
