// Copyright 2025 The AgoraDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::chunk::DataChunk;
use crate::source::{Source, SourceResult};
use agoradb_core::{ExecutionError, PipelineId, StageId};
use std::sync::atomic::{AtomicUsize, Ordering};

// ------------------------------------------------------------------
// PipelineOperator trait
// ------------------------------------------------------------------

/// A pull-based operator that transforms one `DataChunk` into another.
pub trait PipelineOperator: Send {
    fn execute(&mut self, input: &DataChunk, output: &mut DataChunk) -> Result<(), ExecutionError>;
    fn as_any(&self) -> &dyn std::any::Any;
}

// ------------------------------------------------------------------
// Sink trait
// ------------------------------------------------------------------

/// A sink consumes `DataChunk`s and produces a final result on `finalize`.
pub trait Sink: Send {
    fn consume(&mut self, chunk: DataChunk) -> Result<(), ExecutionError>;
    fn finalize(&mut self) -> Result<(), ExecutionError>;
    fn as_any(&self) -> &dyn std::any::Any;
}

// ------------------------------------------------------------------
// Clone helpers for trait objects
// ------------------------------------------------------------------

pub trait CloneOperator {
    fn clone_box(&self) -> Box<dyn PipelineOperator>;
}

pub trait CloneSink {
    fn clone_box(&self) -> Box<dyn Sink>;
}

/// Clone a PipelineOperator by downcasting to known types.
/// Each concrete operator type implements CloneOperator.
pub fn clone_operator(op: &dyn PipelineOperator) -> Box<dyn PipelineOperator> {
    if let Some(f) = op.as_any().downcast_ref::<crate::filter::FilterOperator>() {
        return f.clone_box();
    }
    if let Some(p) = op
        .as_any()
        .downcast_ref::<crate::project::ProjectOperator>()
    {
        return p.clone_box();
    }
    if let Some(l) = op.as_any().downcast_ref::<crate::limit::LimitOperator>() {
        return l.clone_box();
    }
    if let Some(j) = op
        .as_any()
        .downcast_ref::<crate::hash_join::HashJoinProbeOperator>()
    {
        return j.clone_box();
    }
    if let Some(a) = op
        .as_any()
        .downcast_ref::<crate::hash_aggregate::HashAggregateEmitOperator>()
    {
        return a.clone_box();
    }
    if let Some(s) = op.as_any().downcast_ref::<crate::sort::SortEmitOperator>() {
        return s.clone_box();
    }
    panic!("Unknown PipelineOperator type — cannot clone");
}

/// Clone a Sink by downcasting to known types.
pub fn clone_sink(sink: &dyn Sink) -> Box<dyn Sink> {
    if let Some(c) = sink.as_any().downcast_ref::<crate::adapters::CollectSink>() {
        return c.clone_box();
    }
    if let Some(h) = sink
        .as_any()
        .downcast_ref::<crate::hash_join::HashJoinBuildSink>()
    {
        return h.clone_box();
    }
    if let Some(a) = sink
        .as_any()
        .downcast_ref::<crate::hash_aggregate::HashAggregateAccumulateSink>()
    {
        return a.clone_box();
    }
    if let Some(s) = sink.as_any().downcast_ref::<crate::sort::SortCollectSink>() {
        return s.clone_box();
    }
    if let Some(e) = sink
        .as_any()
        .downcast_ref::<crate::local_exchange::LocalExchangeSink>()
    {
        return e.clone_box();
    }
    panic!("Unknown Sink type — cannot clone");
}

// ------------------------------------------------------------------
// Pipeline (static template)
// ------------------------------------------------------------------

/// A Pipeline is a static template within a Stage: Source -> [Operators] -> Sink.
/// It is cloned into `parallelism` runtime `PipelineTask`s.
pub struct Pipeline {
    pub id: PipelineId,
    pub stage_id: StageId,
    pub source_factory: Box<dyn Fn(usize) -> Box<dyn Source> + Send + Sync>,
    pub operators: Vec<Box<dyn PipelineOperator>>,
    pub sink: Box<dyn Sink>,
    pub parallelism: usize,
    /// Upstream Pipeline IDs that must complete before this pipeline starts.
    pub dependencies: Vec<PipelineId>,
}

impl Pipeline {
    /// Create a `PipelineTask` for the given task index.
    pub fn create_task(&self, task_id: usize) -> PipelineTask {
        let source = (self.source_factory)(task_id);
        PipelineTask {
            task_id,
            pipeline_id: self.id,
            stage_id: self.stage_id,
            source,
            operators: self
                .operators
                .iter()
                .map(|op| clone_operator(op.as_ref()))
                .collect(),
            sink: clone_sink(self.sink.as_ref()),
            pending_chunk: None,
            scheduler: None,
        }
    }

    /// Create a `PipelineTask` with scheduler injection for event-driven wake.
    pub fn create_task_with_scheduler(
        &self,
        task_id: usize,
        scheduler: std::sync::Arc<crate::scheduler::TaskScheduler>,
    ) -> PipelineTask {
        let mut source = (self.source_factory)(task_id);
        source.set_scheduler(scheduler.clone());
        let waker = crate::scheduler::TaskWaker::new(scheduler.clone(), self.id, task_id);
        source.set_waker(waker);
        PipelineTask {
            task_id,
            pipeline_id: self.id,
            stage_id: self.stage_id,
            source,
            operators: self
                .operators
                .iter()
                .map(|op| clone_operator(op.as_ref()))
                .collect(),
            sink: clone_sink(self.sink.as_ref()),
            pending_chunk: None,
            scheduler: Some(scheduler),
        }
    }
}

// ------------------------------------------------------------------
// PipelineState (atomic)
// ------------------------------------------------------------------

/// Atomic state machine for a Pipeline.
pub struct PipelineState {
    state: AtomicUsize,
}

impl PipelineState {
    const NOT_STARTED: usize = 0;
    const RUNNING: usize = 1;
    const COMPLETED: usize = 2;

    pub fn new() -> Self {
        Self {
            state: AtomicUsize::new(Self::NOT_STARTED),
        }
    }

    /// CAS: NotStarted -> Running. Returns true if transition succeeded.
    pub fn start(&self) -> bool {
        self.state
            .compare_exchange(
                Self::NOT_STARTED,
                Self::RUNNING,
                Ordering::SeqCst,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// CAS: Running -> Completed. Returns true if transition succeeded.
    pub fn complete(&self) -> bool {
        self.state
            .compare_exchange(
                Self::RUNNING,
                Self::COMPLETED,
                Ordering::SeqCst,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    pub fn is_completed(&self) -> bool {
        self.state.load(Ordering::SeqCst) == Self::COMPLETED
    }
}

impl Default for PipelineState {
    fn default() -> Self {
        Self::new()
    }
}

// ------------------------------------------------------------------
// PipelineTask (runtime instance)
// ------------------------------------------------------------------

/// Result of executing a `PipelineTask`.
pub enum TaskStatus {
    /// Task finished all data.
    Finished,
    /// Task yielded — should be requeued when data is ready.
    Yielded(PipelineTask),
    /// Task encountered an error.
    Error(ExecutionError),
}

/// A runtime instance of a Pipeline.
pub struct PipelineTask {
    pub task_id: usize,
    pub pipeline_id: PipelineId,
    pub stage_id: StageId,
    pub source: Box<dyn Source>,
    pub operators: Vec<Box<dyn PipelineOperator>>,
    pub sink: Box<dyn Sink>,
    pub pending_chunk: Option<DataChunk>,
    /// Reference to the query-level scheduler. Used by the worker thread
    /// to notify pipeline completion / blocked tasks without needing
    /// the scheduler reference passed externally.
    pub scheduler: Option<std::sync::Arc<crate::scheduler::TaskScheduler>>,
}

impl PipelineTask {
    /// Run the task until it finishes, yields, or errors.
    pub fn run(mut self) -> TaskStatus {
        loop {
            // 1. Get data from source
            let chunk = match self.pending_chunk.take() {
                Some(chunk) => chunk,
                None => match self.source.try_next() {
                    Ok(SourceResult::Ready(chunk)) => chunk,
                    Ok(SourceResult::Done) => {
                        if let Err(e) = self.sink.finalize() {
                            return TaskStatus::Error(e);
                        }
                        return TaskStatus::Finished;
                    }
                    Ok(SourceResult::NotReady) => {
                        return TaskStatus::Yielded(self);
                    }
                    Err(e) => return TaskStatus::Error(e),
                },
            };

            // 2. Run operator chain
            let mut current = chunk;
            for op in &mut self.operators {
                let mut output = DataChunk::new(vec![]);
                if let Err(e) = op.execute(&current, &mut output) {
                    return TaskStatus::Error(e);
                }
                current = output;
            }

            // 3. Sink consume
            if let Err(e) = self.sink.consume(current) {
                return TaskStatus::Error(e);
            }
        }
    }
}
