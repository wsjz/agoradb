use crate::chunk::{ColumnVector, DataChunk, DataType};
use crate::operator::Operator;
use agoradb_core::ExecutionError;

pub type PredicateFn = Box<dyn Fn(&DataChunk, usize) -> bool + Send>;

pub struct FilterOperator {
    predicate: PredicateFn,
    output: Option<Box<dyn Operator>>,
}

impl FilterOperator {
    pub fn new(predicate: PredicateFn) -> Self {
        Self {
            predicate,
            output: None,
        }
    }
}

impl Operator for FilterOperator {
    fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        let selected: Vec<usize> = (0..chunk.len)
            .filter(|&i| (self.predicate)(&chunk, i))
            .collect();

        if selected.is_empty() {
            return Ok(());
        }

        let mut output_columns = Vec::with_capacity(chunk.columns.len());
        for col in &chunk.columns {
            let mut new_col = ColumnVector::new(col.data_type.clone(), selected.len());
            for &idx in &selected {
                match col.data_type {
                    DataType::Int64 => new_col.push_i64(col.as_i64_slice()[idx]),
                    DataType::Float64 => {
                        let slice = unsafe {
                            std::slice::from_raw_parts(col.data.as_ptr() as *const f64, col.len)
                        };
                        new_col.push_f64(slice[idx]);
                    }
                    DataType::Boolean => new_col.push_bool(col.data[idx] != 0),
                    _ => {
                        return Err(ExecutionError::OperatorError(
                            "Unsupported type in filter".to_string(),
                        ))
                    }
                }
                new_col.validity[new_col.len - 1] = col.validity[idx];
            }
            output_columns.push(new_col);
        }

        if let Some(ref mut output) = self.output {
            output.push(DataChunk::new(output_columns))?;
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
