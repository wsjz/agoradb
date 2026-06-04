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

//! Tests for LocalExchange in the Pipeline architecture.
//!
//! These tests verify that two Pipelines can exchange data through
//! `LocalExchangeBuffer` — producer writes via `LocalExchangeSink`,
//! consumer reads via `ExchangeSource`.

use agoradb_core::{DataType, ExchangeType};
use agoradb_execution::adapters::CollectSink;
use agoradb_execution::chunk::{ColumnVector, DataChunk};
use agoradb_execution::pipeline::{Pipeline, PipelineTask, TaskStatus};
use agoradb_execution::pipeline_builder::PipelineBuilder;
use agoradb_execution::source::{ExchangeSource, InMemorySource};
use agoradb_execution::{LocalExchangeBuffer, LocalExchangeSink, LocalExchangeSource};
use std::sync::Arc;

fn make_chunk(values: &[i64]) -> DataChunk {
    let mut col = ColumnVector::new(DataType::Int64, values.len());
    for &v in values {
        col.push_i64(v);
    }
    DataChunk::new(vec![col])
}

/// Test: a single producer task writes to LocalExchange, a single consumer task reads.
#[test]
fn test_local_exchange_single_producer_single_consumer() {
    // 1. Create exchange buffer (Gather: all data goes to partition 0)
    let buffer = Arc::new(LocalExchangeBuffer::new(ExchangeType::Gather, 1));

    // 2. Build producer PipelineTask manually
    let chunks = vec![make_chunk(&[1, 2, 3]), make_chunk(&[4, 5, 6])];
    let producer_task = PipelineTask {
        task_id: 0,
        pipeline_id: 0,
        stage_id: 0,
        source: Box::new(InMemorySource::new(chunks)),
        operators: vec![],
        sink: Box::new(LocalExchangeSink::new(buffer.clone(), 0)),
        pending_chunk: None,
        scheduler: None,
    };

    // 3. Build consumer PipelineTask manually
    let collect_sink = CollectSink::new();
    let results_arc = collect_sink.get_results_arc();
    let consumer_task = PipelineTask {
        task_id: 0,
        pipeline_id: 1,
        stage_id: 0,
        source: Box::new(ExchangeSource::new(LocalExchangeSource::new(
            buffer.clone(),
            0,
        ))),
        operators: vec![],
        sink: Box::new(collect_sink),
        pending_chunk: None,
        scheduler: None,
    };

    // 4. Run producer
    let status = producer_task.run();
    assert!(
        matches!(status, TaskStatus::Finished),
        "Producer should finish"
    );

    // 5. Run consumer
    let status = consumer_task.run();
    assert!(
        matches!(status, TaskStatus::Finished),
        "Consumer should finish"
    );

    // 6. Verify: consumer received all chunks
    let results = results_arc.lock().unwrap();
    assert_eq!(results.len(), 2, "Expected 2 chunks");
    assert_eq!(results[0].len, 3, "First chunk should have 3 rows");
    assert_eq!(results[1].len, 3, "Second chunk should have 3 rows");
}

/// Test: multiple producers write to LocalExchange, single consumer gathers all.
#[test]
fn test_local_exchange_multi_producer_gather() {
    let buffer = Arc::new(LocalExchangeBuffer::new(ExchangeType::Gather, 2));

    // Producer 0
    let producer0 = PipelineTask {
        task_id: 0,
        pipeline_id: 0,
        stage_id: 0,
        source: Box::new(InMemorySource::new(vec![make_chunk(&[1, 2])])),
        operators: vec![],
        sink: Box::new(LocalExchangeSink::new(buffer.clone(), 0)),
        pending_chunk: None,
        scheduler: None,
    };

    // Producer 1
    let producer1 = PipelineTask {
        task_id: 1,
        pipeline_id: 0,
        stage_id: 0,
        source: Box::new(InMemorySource::new(vec![make_chunk(&[3, 4])])),
        operators: vec![],
        sink: Box::new(LocalExchangeSink::new(buffer.clone(), 1)),
        pending_chunk: None,
        scheduler: None,
    };

    // Consumer
    let collect_sink = CollectSink::new();
    let results_arc = collect_sink.get_results_arc();
    let consumer = PipelineTask {
        task_id: 0,
        pipeline_id: 1,
        stage_id: 0,
        source: Box::new(ExchangeSource::new(LocalExchangeSource::new(
            buffer.clone(),
            0,
        ))),
        operators: vec![],
        sink: Box::new(collect_sink),
        pending_chunk: None,
        scheduler: None,
    };

    // Run both producers
    assert!(matches!(producer0.run(), TaskStatus::Finished));
    assert!(matches!(producer1.run(), TaskStatus::Finished));

    // Run consumer
    assert!(matches!(consumer.run(), TaskStatus::Finished));

    // Verify: 2 chunks total
    let results = results_arc.lock().unwrap();
    assert_eq!(results.len(), 2, "Expected 2 chunks from 2 producers");
    let total_rows: usize = results.iter().map(|c| c.len).sum();
    assert_eq!(total_rows, 4, "Expected 4 rows total");
}

/// Test: Broadcast — one producer replicates data to two consumers.
#[test]
fn test_local_exchange_broadcast() {
    // Broadcast: 1 producer (num_sinks=1), 2 consumers (num_partitions=2)
    let buffer = Arc::new(LocalExchangeBuffer::new_with_partitions(
        ExchangeType::Broadcast,
        1,
        2,
    ));

    // Producer
    let producer = PipelineTask {
        task_id: 0,
        pipeline_id: 0,
        stage_id: 0,
        source: Box::new(InMemorySource::new(vec![make_chunk(&[1, 2, 3])])),
        operators: vec![],
        sink: Box::new(LocalExchangeSink::new(buffer.clone(), 0)),
        pending_chunk: None,
        scheduler: None,
    };

    // Consumer 0
    let collect0 = CollectSink::new();
    let results0 = collect0.get_results_arc();
    let consumer0 = PipelineTask {
        task_id: 0,
        pipeline_id: 1,
        stage_id: 0,
        source: Box::new(ExchangeSource::new(LocalExchangeSource::new(
            buffer.clone(),
            0,
        ))),
        operators: vec![],
        sink: Box::new(collect0),
        pending_chunk: None,
        scheduler: None,
    };

    // Consumer 1
    let collect1 = CollectSink::new();
    let results1 = collect1.get_results_arc();
    let consumer1 = PipelineTask {
        task_id: 1,
        pipeline_id: 1,
        stage_id: 0,
        source: Box::new(ExchangeSource::new(LocalExchangeSource::new(
            buffer.clone(),
            1,
        ))),
        operators: vec![],
        sink: Box::new(collect1),
        pending_chunk: None,
        scheduler: None,
    };

    // Run producer
    assert!(matches!(producer.run(), TaskStatus::Finished));

    // Run both consumers
    assert!(matches!(consumer0.run(), TaskStatus::Finished));
    assert!(matches!(consumer1.run(), TaskStatus::Finished));

    // Both consumers should get the same data
    let r0 = results0.lock().unwrap();
    let r1 = results1.lock().unwrap();
    assert_eq!(r0.len(), 1, "Consumer 0 should get 1 chunk");
    assert_eq!(r1.len(), 1, "Consumer 1 should get 1 chunk");
    assert_eq!(r0[0].len, 3, "Consumer 0 chunk should have 3 rows");
    assert_eq!(r1[0].len, 3, "Consumer 1 chunk should have 3 rows");
}

/// Test: `connect_local_exchange` helper on a Vec<Pipeline>.
#[test]
fn test_pipeline_builder_connect_local_exchange() {
    // Build two simple pipelines manually
    let chunks = vec![make_chunk(&[10, 20, 30])];

    let mut pipelines = vec![
        Pipeline {
            id: 0,
            stage_id: 0,
            source_factory: Box::new(move |_task_id: usize| {
                Box::new(InMemorySource::new(chunks.clone()))
            }),
            operators: vec![],
            sink: Box::new(CollectSink::new()), // placeholder, will be replaced
            parallelism: 1,
            dependencies: vec![],
        },
        Pipeline {
            id: 1,
            stage_id: 0,
            source_factory: Box::new(|_task_id: usize| Box::new(agoradb_execution::source::EmptySource)),
            operators: vec![],
            sink: Box::new(CollectSink::new()),
            parallelism: 1,
            dependencies: vec![0],
        },
    ];

    // Connect them with LocalExchange
    PipelineBuilder::connect_local_exchange(
        &mut pipelines,
        0,
        1,
        ExchangeType::Gather,
    );

    // Create tasks and run
    let producer_task = pipelines[0].create_task(0);
    let consumer_task = pipelines[1].create_task(0);

    // Verify producer sink is now LocalExchangeSink
    assert!(
        producer_task
            .sink
            .as_any()
            .downcast_ref::<LocalExchangeSink>()
            .is_some(),
        "Producer sink should be LocalExchangeSink"
    );

    // Run producer then consumer
    assert!(matches!(producer_task.run(), TaskStatus::Finished));
    assert!(matches!(consumer_task.run(), TaskStatus::Finished));
}
