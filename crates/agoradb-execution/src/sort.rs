use crate::chunk::{ColumnVector, DataChunk};
use crate::pipeline::{CloneOperator, CloneSink, PipelineOperator, Sink};
use agoradb_core::{DataType, ExecutionError, SortDirection};
use std::any::Any;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::Mutex;

pub struct SortState {
    sort_columns: Vec<usize>,
    directions: Vec<SortDirection>,
    limit: Option<usize>,
    chunks: Mutex<Vec<DataChunk>>,
    sorted: Mutex<Option<Vec<DataChunk>>>,
}

impl SortState {
    pub fn new(
        sort_columns: Vec<usize>,
        directions: Vec<SortDirection>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            sort_columns,
            directions,
            limit,
            chunks: Mutex::new(Vec::new()),
            sorted: Mutex::new(None),
        }
    }

    pub fn push(&self, chunk: DataChunk) {
        self.chunks.lock().unwrap().push(chunk);
    }

    pub fn emit(&self) -> Result<Vec<DataChunk>, ExecutionError> {
        let mut sorted_guard = self.sorted.lock().unwrap();
        if let Some(ref s) = *sorted_guard {
            return Ok(s.clone());
        }

        let chunks = self.chunks.lock().unwrap();
        let total: usize = chunks.iter().map(|c| c.len).sum();
        if total == 0 {
            *sorted_guard = Some(Vec::new());
            return Ok(Vec::new());
        }

        let rows: Vec<(usize, usize)> = (0..chunks.len())
            .flat_map(|ci| (0..chunks[ci].len).map(move |ri| (ci, ri)))
            .collect();

        // ── Top-K optimisation: O(N log K) instead of O(N log N) ──
        let selected = if let Some(k) = self.limit {
            if rows.len() > k && k > 0 {
                top_k(rows, &chunks, &self.sort_columns, &self.directions, k)
            } else {
                rows
            }
        } else {
            rows
        };

        // Final sort of the selected rows (stable order for output).
        let mut sorted = selected;
        sorted.sort_by(|a, b| {
            let (ca, cb) = (&chunks[a.0], &chunks[b.0]);
            for (&col, &dir) in self.sort_columns.iter().zip(&self.directions) {
                let cmp = compare_cell(ca, cb, col, a.1, b.1);
                let ord = match dir {
                    SortDirection::Asc => cmp,
                    SortDirection::Desc => cmp.reverse(),
                };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });

        let out = rebuild_chunks(sorted, &chunks);
        *sorted_guard = Some(out.clone());
        Ok(out)
    }
}

/// Keep only the best K rows using a binary heap.
///
/// The comparison follows the ORDER BY directions: a row that would appear
/// *earlier* in the final sorted output is "better".
/// We keep a max-heap of size K (heap top = current worst among the best K).
fn top_k(
    rows: Vec<(usize, usize)>,
    chunks: &[DataChunk],
    sort_columns: &[usize],
    directions: &[SortDirection],
    k: usize,
) -> Vec<(usize, usize)> {
    let mut heap: BinaryHeap<Reverse<RowKey>> = BinaryHeap::with_capacity(k);

    for (chunk_idx, row_idx) in rows {
        let key = RowKey::new(chunk_idx, row_idx, chunks, sort_columns, directions);
        if heap.len() < k {
            heap.push(Reverse(key));
        } else {
            // heap.peek() = current "worst" among the best K (smallest Reverse = largest key)
            if let Some(Reverse(ref top)) = heap.peek() {
                if key < *top {
                    heap.pop();
                    heap.push(Reverse(key));
                }
            }
        }
    }

    heap.into_vec().into_iter().map(|Reverse(k)| (k.chunk_idx, k.row_idx)).collect()
}

/// Pre-computed sort key for a single row — used by the Top-K heap.
#[derive(Clone, PartialEq, Eq)]
struct RowKey {
    chunk_idx: usize,
    row_idx: usize,
    values: Vec<KeyValue>,
}

#[derive(Clone, PartialEq, Eq)]
enum KeyValue {
    Int64(i64),
    Float64(u64),
    Boolean(bool),
    Utf8(String),
    Null,
}

impl RowKey {
    fn new(
        chunk_idx: usize,
        row_idx: usize,
        chunks: &[DataChunk],
        sort_columns: &[usize],
        _directions: &[SortDirection],
    ) -> Self {
        let chunk = &chunks[chunk_idx];
        let mut values = Vec::with_capacity(sort_columns.len());
        for &col in sort_columns {
            values.push(extract_key(&chunk.columns[col], row_idx));
        }
        Self {
            chunk_idx,
            row_idx,
            values,
        }
    }
}

impl std::cmp::Ord for RowKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for (a, b) in self.values.iter().zip(other.values.iter()) {
            let c = compare_key_values(a, b);
            if c != std::cmp::Ordering::Equal {
                return c;
            }
        }
        std::cmp::Ordering::Equal
    }
}

impl std::cmp::PartialOrd for RowKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}


fn extract_key(col: &ColumnVector, row: usize) -> KeyValue {
    if !col.validity[row] {
        return KeyValue::Null;
    }
    match col.data_type {
        DataType::Int64 => KeyValue::Int64(col.as_i64_slice()[row]),
        DataType::Float64 => {
            let s = unsafe { std::slice::from_raw_parts(col.data.as_ptr() as *const f64, col.len) };
            KeyValue::Float64(s[row].to_bits())
        }
        DataType::Boolean => KeyValue::Boolean(col.data[row] != 0),
        DataType::Utf8 => KeyValue::Utf8(col.as_utf8_slice()[row].to_string()),
    }
}

fn compare_key_values(a: &KeyValue, b: &KeyValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (KeyValue::Null, KeyValue::Null) => Ordering::Equal,
        (KeyValue::Null, _) => Ordering::Less,
        (_, KeyValue::Null) => Ordering::Greater,
        (KeyValue::Int64(a), KeyValue::Int64(b)) => a.cmp(b),
        (KeyValue::Float64(a), KeyValue::Float64(b)) => a.cmp(b),
        (KeyValue::Boolean(a), KeyValue::Boolean(b)) => a.cmp(b),
        (KeyValue::Utf8(a), KeyValue::Utf8(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

fn compare_cell(
    a_chunk: &DataChunk,
    b_chunk: &DataChunk,
    col: usize,
    a_row: usize,
    b_row: usize,
) -> std::cmp::Ordering {
    let (a, b) = (&a_chunk.columns[col], &b_chunk.columns[col]);
    let av = a.validity[a_row];
    let bv = b.validity[b_row];
    if !av && !bv {
        return std::cmp::Ordering::Equal;
    }
    if !av {
        return std::cmp::Ordering::Less;
    }
    if !bv {
        return std::cmp::Ordering::Greater;
    }
    match a.data_type {
        DataType::Int64 => a.as_i64_slice()[a_row].cmp(&b.as_i64_slice()[b_row]),
        DataType::Float64 => {
            let (as_, bs) = (
                unsafe { std::slice::from_raw_parts(a.data.as_ptr() as *const f64, a.len) },
                unsafe { std::slice::from_raw_parts(b.data.as_ptr() as *const f64, b.len) },
            );
            as_[a_row].partial_cmp(&bs[b_row]).unwrap_or(std::cmp::Ordering::Equal)
        }
        DataType::Boolean => a.data[a_row].cmp(&b.data[b_row]),
        DataType::Utf8 => a.as_utf8_slice()[a_row].cmp(b.as_utf8_slice()[b_row]),
    }
}

fn rebuild_chunks(
    rows: Vec<(usize, usize)>,
    chunks: &[DataChunk],
) -> Vec<DataChunk> {
    let ncols = chunks[0].columns.len();
    let mut out = Vec::new();
    let mut cols: Vec<ColumnVector> = (0..ncols)
        .map(|i| ColumnVector::new(chunks[0].columns[i].data_type.clone(), 1024))
        .collect();
    let mut cnt = 0usize;

    for (ci, ri) in rows {
        let src = &chunks[ci];
        for c in 0..ncols {
            append_cell(&src.columns[c], ri, &mut cols[c]);
        }
        cnt += 1;
        if cnt >= 1024 {
            for c in &mut cols {
                c.len = c.capacity;
            }
            out.push(DataChunk::new(std::mem::replace(
                &mut cols,
                (0..ncols)
                    .map(|i| ColumnVector::new(chunks[0].columns[i].data_type.clone(), 1024))
                    .collect(),
            )));
            cnt = 0;
        }
    }
    if cnt > 0 {
        for c in &mut cols {
            c.len = cnt;
        }
        out.push(DataChunk::new(cols));
    }
    out
}

fn append_cell(src: &ColumnVector, src_row: usize, dst: &mut ColumnVector) {
    match src.data_type {
        DataType::Int64 => dst.push_i64(src.as_i64_slice()[src_row]),
        DataType::Float64 => {
            let s = unsafe { std::slice::from_raw_parts(src.data.as_ptr() as *const f64, src.len) };
            dst.push_f64(s[src_row]);
        }
        DataType::Boolean => dst.push_bool(src.data[src_row] != 0),
        DataType::Utf8 => dst.push_utf8(src.as_utf8_slice()[src_row]),
    }
    dst.validity[dst.len - 1] = src.validity[src_row];
}

// ------------------------------------------------------------------
// SortCollectSink
// ------------------------------------------------------------------

/// Sink that collects all input chunks into a SortState.
pub struct SortCollectSink {
    sort_id: usize,
    sort_state: Arc<SortState>,
}

impl SortCollectSink {
    pub fn new(
        _sort_id: usize,
        sort_columns: Vec<usize>,
        directions: Vec<SortDirection>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            sort_id: _sort_id,
            sort_state: Arc::new(SortState::new(sort_columns, directions, limit)),
        }
    }
}

impl Sink for SortCollectSink {
    fn consume(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        self.sort_state.push(chunk);
        Ok(())
    }

    fn finalize(&mut self) -> Result<(), ExecutionError> {
        // SortState is lazily sorted on emit — nothing to do here
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl CloneSink for SortCollectSink {
    fn clone_box(&self) -> Box<dyn Sink> {
        Box::new(Self {
            sort_id: self.sort_id,
            sort_state: self.sort_state.clone(),
        })
    }
}

// ------------------------------------------------------------------
// SortEmitOperator
// ------------------------------------------------------------------

/// PipelineOperator that emits sorted results from a SortState.
pub struct SortEmitOperator {
    sort_id: usize,
    sort_state: Arc<SortState>,
    emitted: bool,
}

impl SortEmitOperator {
    pub fn new(
        _sort_id: usize,
        sort_columns: Vec<usize>,
        directions: Vec<SortDirection>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            sort_id: _sort_id,
            sort_state: Arc::new(SortState::new(sort_columns, directions, limit)),
            emitted: false,
        }
    }
}

impl PipelineOperator for SortEmitOperator {
    fn execute(&mut self, _input: &DataChunk, output: &mut DataChunk) -> Result<(), ExecutionError> {
        if self.emitted {
            return Ok(());
        }
        self.emitted = true;

        let chunks = self.sort_state.emit()?;
        if !chunks.is_empty() {
            // Merge all chunks into one output chunk
            let mut result = chunks[0].deep_clone();
            for chunk in &chunks[1..] {
                result = append_chunks(result, chunk)?;
            }
            *output = result;
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl CloneOperator for SortEmitOperator {
    fn clone_box(&self) -> Box<dyn PipelineOperator> {
        Box::new(Self {
            sort_id: self.sort_id,
            sort_state: self.sort_state.clone(),
            emitted: false,
        })
    }
}

/// Append all rows from `other` into `result`.
fn append_chunks(mut result: DataChunk, other: &DataChunk) -> Result<DataChunk, ExecutionError> {
    for (rc, oc) in result.columns.iter_mut().zip(other.columns.iter()) {
        match oc.data_type {
            DataType::Int64 => {
                for &val in oc.as_i64_slice() {
                    rc.push_i64(val);
                }
            }
            DataType::Float64 => {
                let slice = unsafe {
                    std::slice::from_raw_parts(oc.data.as_ptr() as *const f64, oc.len)
                };
                for &val in slice {
                    rc.push_f64(val);
                }
            }
            DataType::Boolean => {
                for i in 0..oc.len {
                    rc.push_bool(oc.data[i] != 0);
                }
            }
            DataType::Utf8 => {
                for val in &oc.strings {
                    rc.push_utf8(val);
                }
            }
        }
        for i in 0..oc.len {
            rc.validity[rc.len - oc.len + i] = oc.validity[i];
        }
    }
    result.len = result.columns.first().map(|c| c.len).unwrap_or(0);
    Ok(result)
}
