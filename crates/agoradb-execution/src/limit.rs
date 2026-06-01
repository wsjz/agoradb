use crate::chunk::DataChunk;
use crate::operator::Operator;
use agoradb_core::ExecutionError;

pub struct LimitOperator {
    skip: usize,
    fetch: usize,
    seen: usize,
    emitted: usize,
    output: Option<Box<dyn Operator>>,
}

impl LimitOperator {
    pub fn new(skip: usize, fetch: usize) -> Self {
        Self {
            skip,
            fetch,
            seen: 0,
            emitted: 0,
            output: None,
        }
    }
}

impl Operator for LimitOperator {
    fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        if self.emitted >= self.fetch {
            return Ok(());
        }

        let chunk_len = chunk.len;
        let start = self.seen.saturating_sub(self.skip);
        let end = ((self.seen + chunk_len).saturating_sub(self.skip)).min(self.fetch);

        if end > start {
            if let Some(ref mut output) = self.output {
                output.push(chunk)?;
            }
            self.emitted += end - start;
        }

        self.seen += chunk_len;
        Ok(())
    }

    fn finalize(&mut self) -> Result<(), ExecutionError> {
        if let Some(ref mut output) = self.output {
            output.finalize()?;
        }
        Ok(())
    }

    fn set_output(&mut self, output: Box<dyn Operator>) {
        self.output = Some(output);
    }
}
