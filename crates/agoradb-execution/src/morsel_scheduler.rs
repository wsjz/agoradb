use agoradb_core::Morsel;
use std::sync::atomic::{AtomicUsize, Ordering};

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
}
