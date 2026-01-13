use anyhow::Result;
use serde::Serialize;
use std::future::Future;
use std::time::Instant;

#[derive(Clone, Serialize)]
pub struct BenchmarkMetrics {
    pub avg_ms: f64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub std_dev_ms: f64,
    pub iterations: usize,
    pub row_count: usize,
}

pub fn benchmark<F, T>(mut f: F, iterations: usize) -> Result<BenchmarkMetrics>
where
    F: FnMut() -> Result<T>,
    T: HasRowCount,
{
    let mut durations = Vec::with_capacity(iterations);
    let mut row_count = 0;

    for _ in 0..iterations {
        let start = Instant::now();
        let result = f()?;
        let elapsed = start.elapsed();
        durations.push(elapsed.as_secs_f64() * 1000.0);
        row_count = result.row_count();
    }

    Ok(compute_metrics(durations, row_count, iterations))
}

pub async fn benchmark_async<F, Fut, T>(mut f: F, iterations: usize) -> Result<BenchmarkMetrics>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
    T: HasRowCount,
{
    let mut durations = Vec::with_capacity(iterations);
    let mut row_count = 0;

    for _ in 0..iterations {
        let start = Instant::now();
        let result = f().await?;
        let elapsed = start.elapsed();
        durations.push(elapsed.as_secs_f64() * 1000.0);
        row_count = result.row_count();
    }

    Ok(compute_metrics(durations, row_count, iterations))
}

fn compute_metrics(durations: Vec<f64>, row_count: usize, iterations: usize) -> BenchmarkMetrics {
    let sum: f64 = durations.iter().sum();
    let avg_ms = sum / iterations as f64;
    let min_ms = durations.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_ms = durations.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

    let variance: f64 =
        durations.iter().map(|d| (d - avg_ms).powi(2)).sum::<f64>() / iterations as f64;
    let std_dev_ms = variance.sqrt();

    BenchmarkMetrics {
        avg_ms,
        min_ms,
        max_ms,
        std_dev_ms,
        iterations,
        row_count,
    }
}

pub trait HasRowCount {
    fn row_count(&self) -> usize;
}

impl HasRowCount for crate::runner::QueryResult {
    fn row_count(&self) -> usize {
        self.row_count
    }
}
