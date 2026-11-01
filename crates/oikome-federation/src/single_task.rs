// Copyright 2025 The Oikome Authors
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

//! Running a plan without spawning tasks, where there is no Tokio runtime
//! to spawn them on (the browser node, architecture v3 §10).
//!
//! With one target partition DataFusion plans no repartitioning, but a
//! `UNION` still has a partition per input, and `CoalescePartitionsExec`
//! merges partitions by spawning a task for each. [`MergePartitionsExec`]
//! merges them by polling every partition from the one task that polls it,
//! and [`MergeInOneTask`] puts it in place of each `CoalescePartitionsExec`
//! and above a root that still has several partitions. The partitions'
//! remote queries still run concurrently: their streams are polled together.

use std::fmt;
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};

/// Merges the partitions of its input into one, in the polling task.
#[derive(Debug)]
pub struct MergePartitionsExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl MergePartitionsExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        // The same output as coalescing: one partition, no ordering.
        let properties = CoalescePartitionsExec::new(input.clone())
            .properties()
            .clone();
        Self { input, properties }
    }
}

impl DisplayAs for MergePartitionsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MergePartitionsExec")
    }
}

impl ExecutionPlan for MergePartitionsExec {
    fn name(&self) -> &'static str {
        "MergePartitionsExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children.swap_remove(0))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "MergePartitionsExec has one partition, not {partition}"
            )));
        }
        let streams = (0..self.input.output_partitioning().partition_count())
            .map(|i| self.input.execute(i, context.clone()))
            .collect::<DfResult<Vec<_>>>()?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::select_all(streams),
        )))
    }
}

/// Replaces partition merges that spawn tasks with [`MergePartitionsExec`];
/// runs after every other physical optimizer rule.
#[derive(Debug, Default)]
pub struct MergeInOneTask;

impl PhysicalOptimizerRule for MergeInOneTask {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let plan = plan
            .transform_up(|node| {
                let Some(coalesce) = node.downcast_ref::<CoalescePartitionsExec>() else {
                    return Ok(Transformed::no(node));
                };
                let merged: Arc<dyn ExecutionPlan> =
                    Arc::new(MergePartitionsExec::new(coalesce.input().clone()));
                Ok(Transformed::yes(match coalesce.fetch() {
                    Some(fetch) => Arc::new(GlobalLimitExec::new(merged, 0, Some(fetch))),
                    None => merged,
                }))
            })?
            .data;
        // Executing a plan with several partitions coalesces them too.
        Ok(if plan.output_partitioning().partition_count() > 1 {
            Arc::new(MergePartitionsExec::new(plan))
        } else {
            plan
        })
    }

    fn name(&self) -> &str {
        "merge_in_one_task"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
