use crate::chunk::{ColumnVector, DataChunk, DataType};
use crate::operator::Operator;
use agoradb_core::ExecutionError;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Clone)]
struct AggregateState {
    count: i64,
    sum: f64,
    min: i64,
    max: i64,
}

impl AggregateState {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: i64::MAX,
            max: i64::MIN,
        }
    }

    fn update(&mut self, value: i64) {
        self.count += 1;
        self.sum += value as f64;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum GroupKey {
    Int64(i64),
    Utf8(String),
}

pub struct HashAggregateOperator {
    group_indices: Vec<usize>,
    agg_indices: Vec<(usize, AggFunc)>,
    groups: HashMap<Vec<GroupKey>, Vec<AggregateState>>,
    output: Option<Box<dyn Operator>>,
}

impl HashAggregateOperator {
    pub fn new(group_indices: Vec<usize>, agg_indices: Vec<(usize, AggFunc)>) -> Self {
        Self {
            group_indices,
            agg_indices,
            groups: HashMap::new(),
            output: None,
        }
    }

    pub fn emit_results(&mut self) -> Result<DataChunk, ExecutionError> {
        let num_cols = self.group_indices.len() + self.agg_indices.len();
        let mut columns: Vec<ColumnVector> = (0..num_cols)
            .map(|i| {
                let dt = if i < self.group_indices.len() {
                    // Infer type from first group's key (if any groups exist)
                    if let Some((first_key, _)) = self.groups.iter().next() {
                        match first_key.get(i) {
                            Some(GroupKey::Int64(_)) => DataType::Int64,
                            Some(GroupKey::Utf8(_)) => DataType::Utf8,
                            None => DataType::Int64, // fallback
                        }
                    } else {
                        DataType::Int64 // fallback for empty result
                    }
                } else {
                    match self.agg_indices[i - self.group_indices.len()].1 {
                        AggFunc::Count | AggFunc::Sum | AggFunc::Min | AggFunc::Max => {
                            DataType::Int64
                        }
                        AggFunc::Avg => DataType::Float64,
                    }
                };
                ColumnVector::new(dt, self.groups.len())
            })
            .collect();

        for (key, states) in &self.groups {
            for (i, k) in key.iter().enumerate() {
                match k {
                    GroupKey::Int64(v) => columns[i].push_i64(*v),
                    GroupKey::Utf8(s) => columns[i].push_utf8(s),
                }
            }
            for (j, state) in states.iter().enumerate() {
                let col_idx = self.group_indices.len() + j;
                match self.agg_indices[j].1 {
                    AggFunc::Count => columns[col_idx].push_i64(state.count),
                    AggFunc::Sum => columns[col_idx].push_i64(state.sum as i64),
                    AggFunc::Avg => columns[col_idx].push_f64(state.sum / state.count as f64),
                    AggFunc::Min => columns[col_idx].push_i64(state.min),
                    AggFunc::Max => columns[col_idx].push_i64(state.max),
                }
            }
        }

        Ok(DataChunk::new(columns))
    }
}

impl Operator for HashAggregateOperator {
    fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        for row in 0..chunk.len {
            let key: Vec<GroupKey> = self
                .group_indices
                .iter()
                .map(|&idx| {
                    let col = &chunk.columns[idx];
                    match col.data_type {
                        DataType::Int64 => GroupKey::Int64(col.as_i64_slice()[row]),
                        DataType::Utf8 => GroupKey::Utf8(col.as_utf8_slice()[row].to_string()),
                        _ => panic!("Unsupported group-by type: {:?}", col.data_type),
                    }
                })
                .collect();

            let states = self.groups.entry(key).or_insert_with(|| {
                self.agg_indices
                    .iter()
                    .map(|_| AggregateState::new())
                    .collect()
            });

            for (j, &(col_idx, _)) in self.agg_indices.iter().enumerate() {
                let value = chunk.columns[col_idx].as_i64_slice()[row];
                states[j].update(value);
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_aggregate_int64_group_by() {
        let mut agg = HashAggregateOperator::new(
            vec![0],                 // group by column 0
            vec![(1, AggFunc::Sum)], // sum column 1
        );

        // Create chunk: [(A=1, B=10), (A=1, B=20), (A=2, B=30)]
        let mut col_a = ColumnVector::new(DataType::Int64, 3);
        col_a.push_i64(1);
        col_a.push_i64(1);
        col_a.push_i64(2);
        let mut col_b = ColumnVector::new(DataType::Int64, 3);
        col_b.push_i64(10);
        col_b.push_i64(20);
        col_b.push_i64(30);

        let chunk = DataChunk::new(vec![col_a, col_b]);
        agg.push(chunk).unwrap();

        let result = agg.emit_results().unwrap();
        assert_eq!(result.len, 2); // 2 groups
                                   // HashMap iteration order is not deterministic, so check values independently
        let keys: Vec<i64> = result.columns[0].as_i64_slice().to_vec();
        let sums: Vec<i64> = result.columns[1].as_i64_slice().to_vec();
        assert!(keys.contains(&1));
        assert!(keys.contains(&2));
        let idx_1 = keys.iter().position(|&k| k == 1).unwrap();
        let idx_2 = keys.iter().position(|&k| k == 2).unwrap();
        assert_eq!(sums[idx_1], 30); // group 1: 10 + 20 = 30
        assert_eq!(sums[idx_2], 30); // group 2: 30
    }

    #[test]
    fn test_hash_aggregate_utf8_group_by() {
        let mut agg = HashAggregateOperator::new(
            vec![0],                   // group by column 0 (Utf8)
            vec![(1, AggFunc::Count)], // count column 1
        );

        let mut col_a = ColumnVector::new(DataType::Utf8, 3);
        col_a.push_utf8("US");
        col_a.push_utf8("US");
        col_a.push_utf8("EU");
        let mut col_b = ColumnVector::new(DataType::Int64, 3);
        col_b.push_i64(100);
        col_b.push_i64(200);
        col_b.push_i64(150);

        let chunk = DataChunk::new(vec![col_a, col_b]);
        agg.push(chunk).unwrap();

        let result = agg.emit_results().unwrap();
        assert_eq!(result.len, 2); // 2 groups: US, EU
                                   // HashMap iteration order is not deterministic, so check values independently
        let groups: Vec<&str> = result.columns[0].as_utf8_slice();
        let counts: Vec<i64> = result.columns[1].as_i64_slice().to_vec();
        let us_idx = groups.iter().position(|&g| g == "US").unwrap();
        let eu_idx = groups.iter().position(|&g| g == "EU").unwrap();
        assert_eq!(counts[us_idx], 2); // US appears twice
        assert_eq!(counts[eu_idx], 1); // EU appears once
    }
}
