// Copyright 2025 The Oikome Authors
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

//! Arrow IPC streams: how batches cross the boundary to JavaScript.

use std::io::Cursor;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};

/// Encode a result set as an Arrow IPC stream.
pub fn encode(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>, ArrowError> {
    let mut buf = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    drop(writer);
    Ok(buf)
}

/// Decode an Arrow IPC stream into its schema and batches.
pub fn decode(bytes: Vec<u8>) -> Result<(SchemaRef, Vec<RecordBatch>), ArrowError> {
    let reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn batches_round_trip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .unwrap();
        let (decoded_schema, batches) =
            decode(encode(&schema, std::slice::from_ref(&batch)).unwrap()).unwrap();
        assert_eq!(decoded_schema, schema);
        assert_eq!(batches, vec![batch]);

        // A schema with no batches is a valid empty result.
        let (_, none) = decode(encode(&schema, &[]).unwrap()).unwrap();
        assert!(none.is_empty());
        assert!(decode(b"not arrow".to_vec()).is_err());
    }
}
