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

//! A single-task coordinator runs without a Tokio runtime, as in the
//! browser: these tests drive it with `futures::executor::block_on`, where
//! any spawned task would panic.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::executor::block_on;
use futures::TryStreamExt;
use oikome_core::EngineKind;
use oikome_engine::{
    Capabilities, EngineError, QualifiedName, QueryEngine, RecordBatchStream, SqlDialect,
    TableSource, TxHandle, Value,
};
use oikome_federation::{BoundSpace, BoundTable, Coordinator};

/// An engine with one table `t (name)`; every query returns its rows.
#[derive(Debug)]
struct Names {
    id: String,
    batch: RecordBatch,
}

impl Names {
    fn new(id: &str, names: &[&str]) -> Arc<Self> {
        let batch =
            RecordBatch::try_new(schema(), vec![Arc::new(StringArray::from(names.to_vec()))])
                .unwrap();
        Arc::new(Self {
            id: id.to_string(),
            batch,
        })
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]))
}

fn unsupported() -> EngineError {
    EngineError::Unsupported(EngineKind::Host, "test engine".to_string())
}

#[async_trait]
impl QueryEngine for Names {
    fn kind(&self) -> EngineKind {
        EngineKind::Host
    }

    fn dialect(&self) -> SqlDialect {
        SqlDialect::Generic
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::ARROW_OUT
    }

    fn instance_id(&self) -> &str {
        &self.id
    }

    async fn attach(&self, _name: &QualifiedName, _source: TableSource) -> Result<(), EngineError> {
        Err(unsupported())
    }

    async fn detach(&self, _name: &QualifiedName) -> Result<(), EngineError> {
        Err(unsupported())
    }

    async fn table_names(&self, _space: &str) -> Result<Vec<String>, EngineError> {
        Ok(vec!["t".to_string()])
    }

    async fn table_schema(&self, _name: &QualifiedName) -> Result<SchemaRef, EngineError> {
        Ok(schema())
    }

    async fn query(
        &self,
        _sql: &str,
        _params: &[Value],
    ) -> Result<(SchemaRef, RecordBatchStream), EngineError> {
        let batch = self.batch.clone();
        Ok((
            schema(),
            Box::pin(futures::stream::iter(std::iter::once(Ok(batch)))),
        ))
    }

    async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<u64, EngineError> {
        Err(unsupported())
    }

    async fn begin(&self) -> Result<TxHandle, EngineError> {
        Err(unsupported())
    }

    async fn commit(&self, _tx: TxHandle) -> Result<(), EngineError> {
        Err(unsupported())
    }

    async fn rollback(&self, _tx: TxHandle) -> Result<(), EngineError> {
        Err(unsupported())
    }
}

fn coordinator() -> Coordinator {
    let bound: Vec<BoundSpace> = [
        ("a", Names::new("engine-a", &["alice", "carol"])),
        ("b", Names::new("engine-b", &["bob", "alice"])),
    ]
    .into_iter()
    .map(|(space, engine)| {
        BoundSpace::new(
            space,
            engine,
            vec![BoundTable {
                name: "t".to_string(),
                schema: schema(),
                snapshot_id: None,
            }],
        )
    })
    .collect();
    Coordinator::single_task(&bound).unwrap()
}

fn names(coordinator: &Coordinator, sql: &str) -> Vec<String> {
    block_on(async {
        let (_, stream) = coordinator.run(sql).await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut names: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let a = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                (0..a.len())
                    .map(|i| a.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        names.sort();
        names
    })
}

#[test]
fn partitions_are_merged_without_spawning_tasks() {
    let c = coordinator();
    let plan = block_on(c.explain("SELECT name FROM a.t UNION SELECT name FROM b.t")).unwrap();
    assert!(plan.contains("MergePartitionsExec"), "{plan}");
    assert!(!plan.contains("CoalescePartitionsExec"), "{plan}");
    assert!(!plan.contains("RepartitionExec"), "{plan}");

    // A coalesce inside the plan (UNION's distinct aggregate) ...
    assert_eq!(
        names(&c, "SELECT name FROM a.t UNION SELECT name FROM b.t"),
        vec!["alice", "bob", "carol"]
    );
    // ... a root with one partition per input ...
    assert_eq!(
        names(&c, "SELECT name FROM a.t UNION ALL SELECT name FROM b.t"),
        vec!["alice", "alice", "bob", "carol"]
    );
    // ... and a coalesce with a limit.
    assert_eq!(
        names(
            &c,
            "SELECT name FROM a.t UNION ALL SELECT name FROM b.t LIMIT 3"
        )
        .len(),
        3
    );
    let joined = names(&c, "SELECT x.name FROM a.t x JOIN b.t y ON x.name = y.name");
    assert_eq!(joined, vec!["alice"]);
}
