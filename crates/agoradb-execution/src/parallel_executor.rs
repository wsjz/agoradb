use crate::chunk::DataChunk;
use crate::morsel_scheduler::MorselScheduler;
use agoradb_core::{ExecutionError, Morsel};
use std::sync::Arc;

/// Execute a pipeline in parallel across multiple worker threads.
///
/// Each worker pulls morsels from the scheduler, builds its own operator pipeline,
/// reads the morsel data, and pushes it through the pipeline.
pub struct ParallelExecutor;

impl ParallelExecutor {
    /// Run `num_threads` workers in parallel. Each worker repeatedly pulls a morsel
    /// from the scheduler and runs the provided `work` function.
    ///
    /// Returns a flattened Vec of all DataChunks produced by all workers.
    pub async fn execute<F, Fut>(
        scheduler: Arc<MorselScheduler>,
        num_threads: usize,
        work: F,
    ) -> Result<Vec<DataChunk>, ExecutionError>
    where
        F: Fn(Morsel) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Vec<DataChunk>, ExecutionError>> + Send,
    {
        let work = Arc::new(work);
        let mut handles = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            let sched = scheduler.clone();
            let w = work.clone();
            handles.push(tokio::spawn(async move {
                let mut results = Vec::new();
                while let Some(morsel) = sched.next() {
                    match w(morsel).await {
                        Ok(chunks) => results.extend(chunks),
                        Err(e) => return Err(e),
                    }
                }
                Ok(results)
            }));
        }

        let mut all_results = Vec::new();
        for h in handles {
            let worker_results = h
                .await
                .map_err(|e| ExecutionError::OperatorError(format!("Worker panicked: {e}")))?;
            all_results.extend(worker_results?);
        }
        Ok(all_results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::{ColumnVector, DataChunk};
    use agoradb_core::DataType;

    #[tokio::test]
    async fn test_parallel_executor() {
        let scheduler = Arc::new(MorselScheduler::new(vec![
            Morsel {
                file_path: "a.parquet".to_string(),
                row_start: 0,
                row_count: 10,
            },
            Morsel {
                file_path: "b.parquet".to_string(),
                row_start: 0,
                row_count: 20,
            },
        ]));

        let results = ParallelExecutor::execute(scheduler, 2, |morsel| async move {
            // Simulate work: return a DataChunk with row_count rows
            let mut col = ColumnVector::new(DataType::Int64, morsel.row_count);
            for i in 0..morsel.row_count {
                col.push_i64(i as i64);
            }
            Ok(vec![DataChunk::new(vec![col])])
        })
        .await
        .unwrap();

        assert_eq!(results.len(), 2);
        let total_rows: usize = results.iter().map(|c| c.len).sum();
        assert_eq!(total_rows, 30);
    }
}
