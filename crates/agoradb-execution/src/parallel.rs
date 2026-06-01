use crate::chunk::DataChunk;
use agoradb_core::{ExecutionError, Morsel};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A work-stealing scheduler that distributes morsels to worker threads.
/// Each thread calls `next()` to get the next unit of work.
pub struct MorselScheduler {
    morsels: Vec<Morsel>,
    next_index: AtomicUsize,
}

impl MorselScheduler {
    pub fn new(morsels: Vec<Morsel>) -> Self {
        Self {
            morsels,
            next_index: AtomicUsize::new(0),
        }
    }

    /// Pull the next morsel. Returns None when all work is consumed.
    pub fn next(&self) -> Option<Morsel> {
        let idx = self.next_index.fetch_add(1, Ordering::Relaxed);
        self.morsels.get(idx).cloned()
    }

    pub fn total(&self) -> usize {
        self.morsels.len()
    }
}

/// Execute a pipeline in parallel across multiple worker threads.
///
/// Each worker pulls morsels from the scheduler, builds its own operator chain,
/// reads the morsel data, and pushes it through the chain.
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

    #[test]
    fn test_scheduler_basic() {
        let scheduler = MorselScheduler::new(vec![
            Morsel {
                file_path: "a.parquet".to_string(),
                row_start: 0,
                row_count: 10000,
            },
            Morsel {
                file_path: "b.parquet".to_string(),
                row_start: 0,
                row_count: 10000,
            },
            Morsel {
                file_path: "c.parquet".to_string(),
                row_start: 0,
                row_count: 10000,
            },
        ]);

        assert_eq!(scheduler.total(), 3);
        assert!(scheduler.next().is_some());
        assert!(scheduler.next().is_some());
        assert!(scheduler.next().is_some());
        assert!(scheduler.next().is_none());
    }

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
            let mut col =
                crate::chunk::ColumnVector::new(crate::chunk::DataType::Int64, morsel.row_count);
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
