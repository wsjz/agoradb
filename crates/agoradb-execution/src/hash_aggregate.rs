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

use crate::chunk::{ColumnVector, DataChunk};
use crate::operator::Operator;
use agoradb_core::{AggFunction, DataType, ExecutionError};
use std::collections::HashMap;

#[derive(Debug, Clone)]
enum AggValue {
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    Utf8(String),
}

impl AggValue {
    fn from_column(col: &ColumnVector, row: usize) -> Self {
        match col.data_type {
            DataType::Int64 => AggValue::Int64(col.as_i64_slice()[row]),
            DataType::Float64 => {
                let slice = unsafe {
                    std::slice::from_raw_parts(col.data.as_ptr() as *const f64, col.len)
                };
                AggValue::Float64(slice[row])
            }
            DataType::Boolean => AggValue::Boolean(col.data[row] != 0),
            DataType::Utf8 => AggValue::Utf8(col.as_utf8_slice()[row].to_string()),
        }
    }

    fn lt(&self, other: &Self) -> bool {
        match (self, other) {
            (AggValue::Int64(a), AggValue::Int64(b)) => a < b,
            (AggValue::Float64(a), AggValue::Float64(b)) => a < b,
            (AggValue::Boolean(a), AggValue::Boolean(b)) => a < b,
            (AggValue::Utf8(a), AggValue::Utf8(b)) => a < b,
            _ => false,
        }
    }

    fn gt(&self, other: &Self) -> bool {
        match (self, other) {
            (AggValue::Int64(a), AggValue::Int64(b)) => a > b,
            (AggValue::Float64(a), AggValue::Float64(b)) => a > b,
            (AggValue::Boolean(a), AggValue::Boolean(b)) => a > b,
            (AggValue::Utf8(a), AggValue::Utf8(b)) => a > b,
            _ => false,
        }
    }
}

#[derive(Clone)]
struct AggregateState {
    count: i64,
    sum_i64: i64,
    sum_f64: f64,
    min: Option<AggValue>,
    max: Option<AggValue>,
}

impl AggregateState {
    fn new() -> Self {
        Self {
            count: 0,
            sum_i64: 0,
            sum_f64: 0.0,
            min: None,
            max: None,
        }
    }

    fn update(&mut self, value: AggValue, func: &AggFunction) {
        self.count += 1;
        match (&value, func) {
            (AggValue::Int64(v), AggFunction::Sum) => self.sum_i64 += *v,
            (AggValue::Float64(v), AggFunction::Sum) => self.sum_f64 += *v,
            (AggValue::Int64(v), AggFunction::Avg) => {
                self.sum_i64 += *v;
                self.sum_f64 += *v as f64;
            }
            (AggValue::Float64(v), AggFunction::Avg) => self.sum_f64 += *v,
            _ => {}
        }
        match func {
            AggFunction::Min => {
                if let Some(ref current) = self.min {
                    if value.lt(current) {
                        self.min = Some(value);
                    }
                } else {
                    self.min = Some(value);
                }
            }
            AggFunction::Max => {
                if let Some(ref current) = self.max {
                    if value.gt(current) {
                        self.max = Some(value);
                    }
                } else {
                    self.max = Some(value);
                }
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum GroupKey {
    Int64(i64),
    Float64(u64), // bit-pattern for deterministic hashing
    Boolean(bool),
    Utf8(String),
}

pub struct HashAggregateOperator {
    group_indices: Vec<usize>,
    agg_indices: Vec<(usize, AggFunction)>,
    agg_input_types: Vec<DataType>,
    groups: HashMap<Vec<GroupKey>, Vec<AggregateState>>,
    output: Option<Box<dyn Operator>>,
}

impl HashAggregateOperator {
    pub fn new(group_indices: Vec<usize>, agg_indices: Vec<(usize, AggFunction)>) -> Self {
        Self {
            group_indices,
            agg_indices,
            agg_input_types: Vec::new(),
            groups: HashMap::new(),
            output: None,
        }
    }

    fn infer_agg_output_type(func: &AggFunction, input_type: &DataType) -> DataType {
        match func {
            AggFunction::Count => DataType::Int64,
            AggFunction::Sum => match input_type {
                DataType::Int64 => DataType::Int64,
                DataType::Float64 => DataType::Float64,
                _ => DataType::Int64,
            },
            AggFunction::Avg => DataType::Float64,
            AggFunction::Min | AggFunction::Max => input_type.clone(),
        }
    }

    pub fn emit_results(&mut self) -> Result<DataChunk, ExecutionError> {
        let num_cols = self.group_indices.len() + self.agg_indices.len();
        let mut columns: Vec<ColumnVector> = (0..num_cols)
            .map(|i| {
                let dt = if i < self.group_indices.len() {
                    if let Some((first_key, _)) = self.groups.iter().next() {
                        match first_key.get(i) {
                            Some(GroupKey::Int64(_)) => DataType::Int64,
                            Some(GroupKey::Float64(_)) => DataType::Float64,
                            Some(GroupKey::Boolean(_)) => DataType::Boolean,
                            Some(GroupKey::Utf8(_)) => DataType::Utf8,
                            None => DataType::Int64,
                        }
                    } else {
                        DataType::Int64
                    }
                } else {
                    let agg_idx = i - self.group_indices.len();
                    let input_type = self.agg_input_types.get(agg_idx).unwrap_or(&DataType::Int64);
                    Self::infer_agg_output_type(&self.agg_indices[agg_idx].1, input_type)
                };
                ColumnVector::new(dt, self.groups.len())
            })
            .collect();

        for (key, states) in &self.groups {
            for (i, k) in key.iter().enumerate() {
                match k {
                    GroupKey::Int64(v) => columns[i].push_i64(*v),
                    GroupKey::Float64(bits) => columns[i].push_f64(f64::from_bits(*bits)),
                    GroupKey::Boolean(v) => columns[i].push_bool(*v),
                    GroupKey::Utf8(s) => columns[i].push_utf8(s),
                }
            }
            for (j, state) in states.iter().enumerate() {
                let col_idx = self.group_indices.len() + j;
                let func = &self.agg_indices[j].1;
                let input_type = self.agg_input_types.get(j).unwrap_or(&DataType::Int64);
                match func {
                    AggFunction::Count => columns[col_idx].push_i64(state.count),
                    AggFunction::Sum => match input_type {
                        DataType::Int64 => columns[col_idx].push_i64(state.sum_i64),
                        DataType::Float64 => columns[col_idx].push_f64(state.sum_f64),
                        _ => columns[col_idx].push_i64(state.count),
                    },
                    AggFunction::Avg => columns[col_idx].push_f64(state.sum_f64 / state.count as f64),
                    AggFunction::Min => {
                        if let Some(ref v) = state.min {
                            Self::push_value(&mut columns[col_idx], v);
                        } else {
                            columns[col_idx].push_i64(0);
                        }
                    }
                    AggFunction::Max => {
                        if let Some(ref v) = state.max {
                            Self::push_value(&mut columns[col_idx], v);
                        } else {
                            columns[col_idx].push_i64(0);
                        }
                    }
                }
            }
        }

        Ok(DataChunk::new(columns))
    }

    fn push_value(col: &mut ColumnVector, value: &AggValue) {
        match value {
            AggValue::Int64(v) => col.push_i64(*v),
            AggValue::Float64(v) => col.push_f64(*v),
            AggValue::Boolean(v) => col.push_bool(*v),
            AggValue::Utf8(v) => col.push_utf8(v),
        }
    }
}

impl Operator for HashAggregateOperator {
    fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        // Record input types on first chunk
        if self.agg_input_types.is_empty() {
            self.agg_input_types = self
                .agg_indices
                .iter()
                .map(|(col_idx, _)| chunk.columns[*col_idx].data_type.clone())
                .collect();
        }

        for row in 0..chunk.len {
            let key: Vec<GroupKey> = self
                .group_indices
                .iter()
                .map(|&idx| {
                    let col = &chunk.columns[idx];
                    match col.data_type {
                        DataType::Int64 => GroupKey::Int64(col.as_i64_slice()[row]),
                        DataType::Float64 => {
                            let slice = unsafe {
                                std::slice::from_raw_parts(col.data.as_ptr() as *const f64, col.len)
                            };
                            GroupKey::Float64(slice[row].to_bits())
                        }
                        DataType::Boolean => GroupKey::Boolean(col.data[row] != 0),
                        DataType::Utf8 => GroupKey::Utf8(col.as_utf8_slice()[row].to_string()),
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
                let value = AggValue::from_column(&chunk.columns[col_idx], row);
                states[j].update(value, &self.agg_indices[j].1);
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
            vec![(1, AggFunction::Sum)], // sum column 1
        );

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
        assert_eq!(result.len, 2);
        let keys: Vec<i64> = result.columns[0].as_i64_slice().to_vec();
        let sums: Vec<i64> = result.columns[1].as_i64_slice().to_vec();
        assert!(keys.contains(&1));
        assert!(keys.contains(&2));
        let idx_1 = keys.iter().position(|&k| k == 1).unwrap();
        let idx_2 = keys.iter().position(|&k| k == 2).unwrap();
        assert_eq!(sums[idx_1], 30);
        assert_eq!(sums[idx_2], 30);
    }

    #[test]
    fn test_hash_aggregate_utf8_group_by() {
        let mut agg = HashAggregateOperator::new(
            vec![0],
            vec![(1, AggFunction::Count)],
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
        assert_eq!(result.len, 2);
        let groups: Vec<&str> = result.columns[0].as_utf8_slice();
        let counts: Vec<i64> = result.columns[1].as_i64_slice().to_vec();
        let us_idx = groups.iter().position(|&g| g == "US").unwrap();
        let eu_idx = groups.iter().position(|&g| g == "EU").unwrap();
        assert_eq!(counts[us_idx], 2);
        assert_eq!(counts[eu_idx], 1);
    }

    #[test]
    fn test_hash_aggregate_float64_sum() {
        let mut agg = HashAggregateOperator::new(
            vec![0],
            vec![(1, AggFunction::Sum)],
        );

        let mut col_a = ColumnVector::new(DataType::Int64, 3);
        col_a.push_i64(1);
        col_a.push_i64(1);
        col_a.push_i64(2);
        let mut col_b = ColumnVector::new(DataType::Float64, 3);
        col_b.push_f64(1.5);
        col_b.push_f64(2.5);
        col_b.push_f64(3.0);

        let chunk = DataChunk::new(vec![col_a, col_b]);
        agg.push(chunk).unwrap();

        let result = agg.emit_results().unwrap();
        assert_eq!(result.len, 2);
        assert_eq!(result.columns[1].data_type, DataType::Float64);
        let keys: Vec<i64> = result.columns[0].as_i64_slice().to_vec();
        let sums_slice = unsafe {
            std::slice::from_raw_parts(result.columns[1].data.as_ptr() as *const f64, result.columns[1].len)
        };
        let sums: Vec<f64> = sums_slice.to_vec();
        let idx_1 = keys.iter().position(|&k| k == 1).unwrap();
        let idx_2 = keys.iter().position(|&k| k == 2).unwrap();
        assert!((sums[idx_1] - 4.0).abs() < f64::EPSILON);
        assert!((sums[idx_2] - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_hash_aggregate_float64_avg() {
        let mut agg = HashAggregateOperator::new(
            vec![0],
            vec![(1, AggFunction::Avg)],
        );

        let mut col_a = ColumnVector::new(DataType::Int64, 2);
        col_a.push_i64(1);
        col_a.push_i64(1);
        let mut col_b = ColumnVector::new(DataType::Float64, 2);
        col_b.push_f64(10.0);
        col_b.push_f64(20.0);

        let chunk = DataChunk::new(vec![col_a, col_b]);
        agg.push(chunk).unwrap();

        let result = agg.emit_results().unwrap();
        assert_eq!(result.len, 1);
        let avg_slice = unsafe {
            std::slice::from_raw_parts(result.columns[1].data.as_ptr() as *const f64, result.columns[1].len)
        };
        assert!((avg_slice[0] - 15.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_hash_aggregate_min_max_utf8() {
        let mut agg = HashAggregateOperator::new(
            vec![0],
            vec![(1, AggFunction::Min), (1, AggFunction::Max)],
        );

        let mut col_a = ColumnVector::new(DataType::Int64, 3);
        col_a.push_i64(1);
        col_a.push_i64(1);
        col_a.push_i64(1);
        let mut col_b = ColumnVector::new(DataType::Utf8, 3);
        col_b.push_utf8("charlie");
        col_b.push_utf8("alice");
        col_b.push_utf8("bob");

        let chunk = DataChunk::new(vec![col_a, col_b]);
        agg.push(chunk).unwrap();

        let result = agg.emit_results().unwrap();
        assert_eq!(result.len, 1);
        let min_vals: Vec<&str> = result.columns[1].as_utf8_slice();
        let max_vals: Vec<&str> = result.columns[2].as_utf8_slice();
        assert_eq!(min_vals[0], "alice");
        assert_eq!(max_vals[0], "charlie");
    }

    #[test]
    fn test_hash_aggregate_float64_group_by() {
        let mut agg = HashAggregateOperator::new(
            vec![0],
            vec![(1, AggFunction::Count)],
        );

        let mut col_a = ColumnVector::new(DataType::Float64, 4);
        col_a.push_f64(1.5);
        col_a.push_f64(1.5);
        col_a.push_f64(2.5);
        col_a.push_f64(1.5);
        let mut col_b = ColumnVector::new(DataType::Int64, 4);
        col_b.push_i64(10);
        col_b.push_i64(20);
        col_b.push_i64(30);
        col_b.push_i64(40);

        let chunk = DataChunk::new(vec![col_a, col_b]);
        agg.push(chunk).unwrap();

        let result = agg.emit_results().unwrap();
        assert_eq!(result.len, 2);
        assert_eq!(result.columns[0].data_type, DataType::Float64);
        let keys_slice = unsafe {
            std::slice::from_raw_parts(result.columns[0].data.as_ptr() as *const f64, result.columns[0].len)
        };
        let keys: Vec<f64> = keys_slice.to_vec();
        let counts: Vec<i64> = result.columns[1].as_i64_slice().to_vec();
        let idx_1_5 = keys.iter().position(|&k| (k - 1.5).abs() < f64::EPSILON).unwrap();
        let idx_2_5 = keys.iter().position(|&k| (k - 2.5).abs() < f64::EPSILON).unwrap();
        assert_eq!(counts[idx_1_5], 3);
        assert_eq!(counts[idx_2_5], 1);
    }

    #[test]
    fn test_hash_aggregate_boolean_group_by() {
        let mut agg = HashAggregateOperator::new(
            vec![0],
            vec![(1, AggFunction::Sum)],
        );

        let mut col_a = ColumnVector::new(DataType::Boolean, 4);
        col_a.push_bool(true);
        col_a.push_bool(false);
        col_a.push_bool(true);
        col_a.push_bool(false);
        let mut col_b = ColumnVector::new(DataType::Int64, 4);
        col_b.push_i64(10);
        col_b.push_i64(20);
        col_b.push_i64(30);
        col_b.push_i64(40);

        let chunk = DataChunk::new(vec![col_a, col_b]);
        agg.push(chunk).unwrap();

        let result = agg.emit_results().unwrap();
        assert_eq!(result.len, 2);
        assert_eq!(result.columns[0].data_type, DataType::Boolean);
        let keys: Vec<bool> = result.columns[0].data[..result.columns[0].len]
            .iter()
            .map(|&v| v != 0)
            .collect();
        let sums: Vec<i64> = result.columns[1].as_i64_slice().to_vec();
        let true_idx = keys.iter().position(|&k| k).unwrap();
        let false_idx = keys.iter().position(|&k| !k).unwrap();
        assert_eq!(sums[true_idx], 40); // 10 + 30
        assert_eq!(sums[false_idx], 60); // 20 + 40
    }
}
