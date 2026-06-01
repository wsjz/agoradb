use crate::chunk::DataChunk;
use crate::operator::Operator;
use agoradb_catalog::{AgoraCatalog, StorageScanProvider};
use agoradb_core::{ExecutionError, SpaceUri};
use futures::StreamExt;
use std::sync::Arc;

/// Scan operator — reads from storage and pushes DataChunks downstream.
pub struct ScanOperator {
    catalog: Arc<AgoraCatalog>,
    space: SpaceUri,
    snapshot_id: i64,
    output: Option<Box<dyn Operator>>,
}

impl ScanOperator {
    pub fn new(catalog: Arc<AgoraCatalog>, space: SpaceUri, snapshot_id: i64) -> Self {
        Self {
            catalog,
            space,
            snapshot_id,
            output: None,
        }
    }

    pub fn set_output(&mut self, output: Box<dyn Operator>) {
        self.output = Some(output);
    }

    /// Execute the scan — read all data and push downstream.
    pub async fn execute(&mut self) -> Result<(), ExecutionError> {
        let stream = self
            .catalog
            .scan_table(&self.space, self.snapshot_id, None)
            .await
            .map_err(ExecutionError::Catalog)?;

        let mut stream = stream;
        while let Some(result) = stream.next().await {
            let record_batch = result
                .map_err(|e| ExecutionError::OperatorError(format!("Scan read error: {e}")))?;
            let chunk = DataChunk::from_record_batch(&record_batch)?;
            if let Some(ref mut output) = self.output {
                output.push(chunk)?;
            }
        }

        if let Some(ref mut output) = self.output {
            output.finalize()?;
        }

        Ok(())
    }
}
