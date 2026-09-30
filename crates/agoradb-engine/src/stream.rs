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

//! Helpers for exposing blocking database drivers as async Arrow streams.

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::Stream;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

use crate::error::EngineError;

/// A stream of Arrow batches produced by an engine.
pub type RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, EngineError>> + Send>>;

/// Default number of batches buffered between the producing thread and the consumer.
pub const DEFAULT_STREAM_CAPACITY: usize = 4;

/// Where a blocking producer pushes the schema and the batches of a result set.
pub struct BatchSink {
    schema_tx: Option<oneshot::Sender<Result<SchemaRef, EngineError>>>,
    batch_tx: mpsc::Sender<Result<RecordBatch, EngineError>>,
}

impl BatchSink {
    /// Announce the result schema. Must be called exactly once, before any batch.
    pub fn start(&mut self, schema: SchemaRef) -> Result<(), EngineError> {
        let tx = self
            .schema_tx
            .take()
            .ok_or_else(|| EngineError::Sql("result schema announced twice".to_string()))?;
        tx.send(Ok(schema))
            .map_err(|_| EngineError::Cancelled("consumer dropped before schema".to_string()))
    }

    /// Push one batch. Blocks while the consumer is behind (back-pressure).
    pub fn send(&mut self, batch: RecordBatch) -> Result<(), EngineError> {
        if self.schema_tx.is_some() {
            return Err(EngineError::Sql(
                "batch sent before result schema".to_string(),
            ));
        }
        self.batch_tx
            .blocking_send(Ok(batch))
            .map_err(|_| EngineError::Cancelled("consumer dropped".to_string()))
    }
}

/// Runs closures against a blocking connection on the tokio blocking pool.
///
/// The connection is owned by a `std::sync::Mutex` that is only ever locked
/// from the blocking thread, so an async caller is never blocked on it.
/// Statements on one worker are serialised; parallelism comes from the engine
/// itself (e.g. DuckDB's thread pool).
pub struct BlockingWorker<C> {
    conn: Arc<Mutex<C>>,
}

impl<C> std::fmt::Debug for BlockingWorker<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BlockingWorker")
    }
}

impl<C: Send + 'static> BlockingWorker<C> {
    /// Wrap a connection.
    pub fn new(conn: C) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
        }
    }

    /// Run `f` with exclusive access to the connection and return its result.
    pub async fn run<T, F>(&self, f: F) -> Result<T, EngineError>
    where
        T: Send + 'static,
        F: FnOnce(&mut C) -> Result<T, EngineError> + Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            // A panic in an earlier closure poisons the mutex; the connection
            // itself is still usable, so recover instead of failing forever.
            let mut guard = conn.lock().unwrap_or_else(|p| p.into_inner());
            f(&mut guard)
        })
        .await
        .map_err(|e| EngineError::Cancelled(e.to_string()))?
    }

    /// Run `producer` on the blocking pool and expose what it pushes into the
    /// [`BatchSink`] as an async stream.
    ///
    /// Resolves once the producer has announced the schema. A producer error
    /// raised before that is returned directly; one raised afterwards becomes
    /// the last item of the stream.
    pub async fn query_stream<F>(
        &self,
        capacity: usize,
        producer: F,
    ) -> Result<(SchemaRef, RecordBatchStream), EngineError>
    where
        F: FnOnce(&mut C, &mut BatchSink) -> Result<(), EngineError> + Send + 'static,
    {
        let (schema_tx, schema_rx) = oneshot::channel();
        let (batch_tx, batch_rx) = mpsc::channel(capacity.max(1));
        let conn = Arc::clone(&self.conn);

        tokio::task::spawn_blocking(move || {
            let mut sink = BatchSink {
                schema_tx: Some(schema_tx),
                batch_tx: batch_tx.clone(),
            };
            let mut guard = conn.lock().unwrap_or_else(|p| p.into_inner());
            let result = producer(&mut guard, &mut sink);
            match (result, sink.schema_tx.take()) {
                (Err(e), Some(tx)) => {
                    let _ = tx.send(Err(e));
                }
                (Err(e), None) => {
                    let _ = batch_tx.blocking_send(Err(e));
                }
                (Ok(()), Some(tx)) => {
                    let _ = tx.send(Err(EngineError::Sql(
                        "producer finished without announcing a schema".to_string(),
                    )));
                }
                (Ok(()), None) => {}
            }
        });

        let schema = schema_rx
            .await
            .map_err(|_| EngineError::Cancelled("engine worker panicked".to_string()))??;
        Ok((schema, Box::pin(ReceiverStream::new(batch_rx))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batch(values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    #[tokio::test]
    async fn run_returns_closure_result() {
        let worker = BlockingWorker::new(41u32);
        let v = worker
            .run(|c| {
                *c += 1;
                Ok(*c)
            })
            .await
            .unwrap();
        assert_eq!(v, 42);
    }

    #[tokio::test]
    async fn blocking_worker_stream_propagates_error() {
        let worker = BlockingWorker::new(());
        let (s, mut stream) = worker
            .query_stream(2, |_, sink| {
                sink.start(schema())?;
                sink.send(batch(vec![1, 2]))?;
                Err(EngineError::Sql("boom".to_string()))
            })
            .await
            .unwrap();
        assert_eq!(s.fields().len(), 1);
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 2);
        let err = stream.next().await.unwrap().unwrap_err();
        assert!(matches!(err, EngineError::Sql(m) if m == "boom"));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn error_before_schema_is_returned_directly() {
        let worker = BlockingWorker::new(());
        let Err(err) = worker
            .query_stream(2, |_, _| Err(EngineError::TableNotFound("x".to_string())))
            .await
        else {
            panic!("expected an error");
        };
        assert!(matches!(err, EngineError::TableNotFound(t) if t == "x"));
    }

    #[tokio::test]
    async fn blocking_worker_cancelled_maps_to_error() {
        let worker = BlockingWorker::new(());
        let err = worker
            .run(|_| -> Result<(), EngineError> { panic!("driver crashed") })
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::Cancelled(_)));

        let Err(err) = worker
            .query_stream(1, |_, _| -> Result<(), EngineError> {
                panic!("driver crashed")
            })
            .await
        else {
            panic!("expected an error");
        };
        assert!(matches!(err, EngineError::Cancelled(_)));
    }

    #[tokio::test]
    async fn stream_applies_back_pressure_and_finishes() {
        let worker = BlockingWorker::new(());
        let (_, stream) = worker
            .query_stream(1, |_, sink| {
                sink.start(schema())?;
                for i in 0..10 {
                    sink.send(batch(vec![i]))?;
                }
                Ok(())
            })
            .await
            .unwrap();
        let batches: Vec<_> = stream.map(|b| b.unwrap()).collect().await;
        assert_eq!(batches.len(), 10);
    }
}
