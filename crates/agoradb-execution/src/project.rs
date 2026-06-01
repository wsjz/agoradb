use crate::chunk::{ColumnVector, DataChunk};
use crate::operator::Operator;
use agoradb_core::ExecutionError;

pub struct ProjectOperator {
    column_indices: Vec<usize>,
    output: Option<Box<dyn Operator>>,
}

impl ProjectOperator {
    pub fn new(column_indices: Vec<usize>) -> Self {
        Self {
            column_indices,
            output: None,
        }
    }
}

impl Operator for ProjectOperator {
    fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        let mut columns = Vec::with_capacity(self.column_indices.len());
        for &idx in &self.column_indices {
            if idx >= chunk.columns.len() {
                return Err(ExecutionError::ColumnNotFound(format!(
                    "Column index {} out of bounds",
                    idx
                )));
            }
            columns.push(ColumnVector {
                data_type: chunk.columns[idx].data_type.clone(),
                validity: chunk.columns[idx].validity.clone(),
                data: chunk.columns[idx].data.clone(),
                strings: chunk.columns[idx].strings.clone(),
                len: chunk.columns[idx].len,
                capacity: chunk.columns[idx].capacity,
            });
        }
        if let Some(ref mut output) = self.output {
            output.push(DataChunk::new(columns))?;
        }
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
