mod datafusion_runner;
mod duckdb_runner;
mod metrics;
mod report;
mod tpch;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "ducklake-benchmark")]
#[command(about = "Benchmark comparing DuckDB-DuckLake vs DataFusion-DuckLake performance")]
struct Args {
    /// Path to the DuckLake catalog database
    #[arg(short, long)]
    catalog: PathBuf,

    /// Use TPC-H queries from DuckDB's tpch extension
    #[arg(long)]
    tpch: bool,

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

        let queries = tpch::get_tpch_queries_with_metadata()?;
        println!("Loaded {} TPC-H queries\n", queries.len());

        // Separate warmup phase - run all queries once before timing
        if !args.no_warmup {
            println!("───────────────────────────────────────────────────────────────");
            println!("Warmup phase: running all queries once...");
            for (i, query) in queries.iter().enumerate() {
                print!("  [{:>2}/{}] {}... ", i + 1, queries.len(), query.name);
                let _ = duckdb_runner.execute(&query.sql);
                let _ = datafusion_runner.execute(&query.sql).await;
                println!("done");
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
    } else if let Some(queries_dir) = &args.queries {
        println!("Queries:    {:?}", queries_dir);
        println!();

        let queries = load_sql_queries(queries_dir)?;
        println!("Loaded {} queries\n", queries.len());

        // Separate warmup phase - run all queries once before timing
        if !args.no_warmup {
            println!("───────────────────────────────────────────────────────────────");
            println!("Warmup phase: running all queries once...");
            for (i, (name, sql)) in queries.iter().enumerate() {
                print!("  [{:>2}/{}] {}... ", i + 1, queries.len(), name);
                let _ = duckdb_runner.execute(sql);
                let _ = datafusion_runner.execute(sql).await;
                println!("done");
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
        println!("\nError: Specify --tpch or --queries <dir>");
        std::process::exit(1);
    }

    // Print comparison table
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("                      COMPARISON TABLE");
    println!("═══════════════════════════════════════════════════════════════");
    println!(
        "{:<25} {:>12} {:>12} {:>10}",
        "Query", "DuckDB (ms)", "DataFusion", "Ratio"
    );
    println!("───────────────────────────────────────────────────────────────");

    for r in &all_results {
        let ratio = r.datafusion.avg_ms / r.duckdb.avg_ms;
        let winner = if ratio < 1.0 {
            "◀"
        } else {
            ""
        };
        println!(
            "{:<25} {:>12.2} {:>12.2} {:>8.2}x {}",
            &r.query_name[..r.query_name.len().min(25)],
            r.duckdb.avg_ms,
            r.datafusion.avg_ms,
            ratio,
            winner
        );
    }

    println!("───────────────────────────────────────────────────────────────");

    // Calculate totals
    let total_duckdb: f64 = all_results.iter().map(|r| r.duckdb.avg_ms).sum();
    let total_datafusion: f64 = all_results.iter().map(|r| r.datafusion.avg_ms).sum();
    let total_ratio = total_datafusion / total_duckdb;

    println!(
        "{:<25} {:>12.2} {:>12.2} {:>8.2}x",
        "TOTAL", total_duckdb, total_datafusion, total_ratio
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
