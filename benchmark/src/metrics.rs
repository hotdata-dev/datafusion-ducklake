use crate::runner::PhaseTiming;
use anyhow::Result;
use serde::Serialize;
use std::future::Future;
use std::time::Instant;

/// Average phase timing breakdown (in milliseconds)
#[derive(Clone, Serialize, Default)]
pub struct PhaseMetrics {
    pub plan_ms: f64,
    pub physical_ms: f64,
    pub exec_ms: f64,
}

#[derive(Clone, Serialize)]
pub struct BenchmarkMetrics {
    pub avg_ms: f64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub std_dev_ms: f64,
    pub iterations: usize,
    pub row_count: usize,
    /// Average phase timing (only available for DataFusion)
    pub phases: Option<PhaseMetrics>,
}

pub fn benchmark<F, T>(mut f: F, iterations: usize) -> Result<BenchmarkMetrics>
where
    F: FnMut() -> Result<T>,
    T: HasRowCount + HasPhases,
{
    let mut durations = Vec::with_capacity(iterations);
    let mut phase_timings: Vec<PhaseTiming> = Vec::new();
    let mut row_count = 0;

    for _ in 0..iterations {
        let start = Instant::now();
        let result = f()?;
        let elapsed = start.elapsed();
        durations.push(elapsed.as_secs_f64() * 1000.0);
        row_count = result.row_count();
        if let Some(phases) = result.phases() {
            phase_timings.push(phases);
        }
    }

    Ok(compute_metrics(durations, row_count, iterations, phase_timings))
}

pub async fn benchmark_async<F, Fut, T>(mut f: F, iterations: usize) -> Result<BenchmarkMetrics>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
    T: HasRowCount + HasPhases,
{
    let mut durations = Vec::with_capacity(iterations);
    let mut phase_timings: Vec<PhaseTiming> = Vec::new();
    let mut row_count = 0;

    for _ in 0..iterations {
        let start = Instant::now();
        let result = f().await?;
        let elapsed = start.elapsed();
        durations.push(elapsed.as_secs_f64() * 1000.0);
        row_count = result.row_count();
        if let Some(phases) = result.phases() {
            phase_timings.push(phases);
        }
    }

    Ok(compute_metrics(durations, row_count, iterations, phase_timings))
}

fn compute_metrics(
    durations: Vec<f64>,
    row_count: usize,
    iterations: usize,
    phase_timings: Vec<PhaseTiming>,
) -> BenchmarkMetrics {
    let sum: f64 = durations.iter().sum();
    let avg_ms = sum / iterations as f64;
    let min_ms = durations.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_ms = durations.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

    let variance: f64 =
        durations.iter().map(|d| (d - avg_ms).powi(2)).sum::<f64>() / iterations as f64;
    let std_dev_ms = variance.sqrt();

    // Compute average phase timings if available
    let phases = if !phase_timings.is_empty() {
        let n = phase_timings.len() as f64;
        Some(PhaseMetrics {
            plan_ms: phase_timings.iter().map(|p| p.plan_ms).sum::<f64>() / n,
            physical_ms: phase_timings.iter().map(|p| p.physical_ms).sum::<f64>() / n,
            exec_ms: phase_timings.iter().map(|p| p.exec_ms).sum::<f64>() / n,
        })
    } else {
        None
    };

    BenchmarkMetrics {
        avg_ms,
        min_ms,
        max_ms,
        std_dev_ms,
        iterations,
        row_count,
        phases,
    }
}

pub trait HasRowCount {
    fn row_count(&self) -> usize;
}

pub trait HasPhases {
    fn phases(&self) -> Option<PhaseTiming>;
}

impl HasRowCount for crate::runner::QueryResult {
    fn row_count(&self) -> usize {
        self.row_count
    }
}

impl HasPhases for crate::runner::QueryResult {
    fn phases(&self) -> Option<PhaseTiming> {
        self.phases.clone()
    }
}
