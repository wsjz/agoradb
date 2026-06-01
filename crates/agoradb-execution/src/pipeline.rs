use crate::chunk::DataChunk;
use crate::operator::Operator;
use agoradb_core::ExecutionError;

/// A linear pipeline segment (chain of operators).
pub struct Pipeline {
    operators: Vec<Box<dyn Operator>>,
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            operators: Vec::new(),
        }
    }

    pub fn add_operator(&mut self, op: Box<dyn Operator>) {
        self.operators.push(op);
    }

    pub fn execute(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        if let Some(first) = self.operators.first_mut() {
            first.push(chunk)?;
        }
        Ok(())
    }

    pub fn finalize(&mut self) -> Result<(), ExecutionError> {
        if let Some(first) = self.operators.first_mut() {
            first.finalize()?;
        }
        Ok(())
    }
}

/// Query executor — runs all pipelines in dependency order.
pub struct QueryExecutor {
    pipelines: Vec<Pipeline>,
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryExecutor {
    pub fn new() -> Self {
        Self {
            pipelines: Vec::new(),
        }
    }

    pub fn add_pipeline(&mut self, pipeline: Pipeline) {
        self.pipelines.push(pipeline);
    }
}
