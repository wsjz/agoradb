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

//! The Oikome peer protocol on top of Arrow Flight (architecture v3 §9).
//!
//! | Flight call | Request | Response |
//! |-------------|---------|----------|
//! | `DoAction("oikome.list_tables")` | [`ListTablesRequest`] (JSON) | [`ListTablesResponse`] (JSON) |
//! | `GetSchema` | descriptor path `[space, table]` | the relation's schema as the caller sees it |
//! | `DoGet` | ticket [`QueryTicket`] (JSON) | Arrow record batches |
//! | `DoAction("oikome.snapshot")` | [`SnapshotRequest`] (JSON) | [`SnapshotResponse`] (JSON) |
//! | `DoAction("oikome.fetch_file")` | [`FetchFileRequest`] (JSON) | the file's bytes, in chunks of [`FILE_CHUNK_BYTES`] |
//!
//! `DoGet`, `GetSchema` and `oikome.list_tables` need `space/query`;
//! `oikome.snapshot` and `oikome.fetch_file` (subscription, §5.3) need
//! `space/read`. Data files are named by their BLAKE3 hash, never by a path
//! on the owner.
//!
//! Every call carries two headers: [`HEADER_UCAN`], the capability token,
//! and [`HEADER_AUTH`], an [`AuthRequest`](oikome_identity::AuthRequest)
//! signing the call's [`digest`]. The SQL in a ticket is in the *owner's*
//! namespace (`<space>.<table>` on the owner node); the owner parses and
//! rewrites it with its own semantic layer before any engine sees it.

use std::io::Cursor;

use arrow_schema::{ArrowError, Schema, SchemaRef};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};

pub mod webrtc;

/// Header carrying the signed request (base64url JSON `AuthRequest`).
pub const HEADER_AUTH: &str = "x-oikome-auth";
/// Header carrying the UCAN.
pub const HEADER_UCAN: &str = "x-oikome-ucan";

/// `DoAction` type listing the relations of a Space the caller may read.
pub const ACTION_LIST_TABLES: &str = "oikome.list_tables";
/// `DoAction` type returning the current snapshot of a Space's tables.
pub const ACTION_SNAPSHOT: &str = "oikome.snapshot";
/// `DoAction` type streaming one data file of a snapshot.
pub const ACTION_FETCH_FILE: &str = "oikome.fetch_file";

/// Method names bound into the request digest.
pub const METHOD_DO_GET: &str = "do_get";
pub const METHOD_GET_SCHEMA: &str = "get_schema";
pub const METHOD_LIST_TABLES: &str = "do_action:oikome.list_tables";
pub const METHOD_SNAPSHOT: &str = "do_action:oikome.snapshot";
pub const METHOD_FETCH_FILE: &str = "do_action:oikome.fetch_file";

/// Size of the chunks `oikome.fetch_file` streams a file in.
pub const FILE_CHUNK_BYTES: usize = 1 << 20;

/// Largest SQL text accepted in a ticket.
pub const MAX_SQL_BYTES: usize = 1 << 20;
/// Largest identifier (Space or table name) accepted.
pub const MAX_IDENTIFIER_BYTES: usize = 128;

/// `DoGet` ticket: one query in the owner's namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryTicket {
    pub sql: String,
}

/// `oikome.list_tables` request body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListTablesRequest {
    pub space: String,
}

/// `oikome.list_tables` response body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListTablesResponse {
    pub tables: Vec<String>,
}

/// `oikome.snapshot` request body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotRequest {
    pub space: String,
}

/// `oikome.snapshot` response body: the tables the caller may copy in full,
/// and the names of those it may only query (column grants or row policies).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SnapshotResponse {
    pub tables: Vec<TableSnapshot>,
    pub restricted: Vec<String>,
}

/// One table pinned to one snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSnapshot {
    pub name: String,
    /// `None` for a table without data yet.
    pub snapshot_id: Option<i64>,
    /// Arrow schema, see [`encode_schema`].
    pub schema: String,
    pub files: Vec<FileEntry>,
}

/// A data file of a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEntry {
    /// BLAKE3 hash of the file, 64 lowercase hex digits.
    pub hash: String,
    pub size: u64,
}

/// `oikome.fetch_file` request body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FetchFileRequest {
    pub space: String,
    pub table: String,
    pub snapshot_id: i64,
    pub hash: String,
}

/// Whether `hash` is a BLAKE3 hash as files are named by: 64 lowercase hex
/// digits. Safe to use as a file name once this holds.
pub fn is_file_hash(hash: &str) -> bool {
    hash.len() == 64 && hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Encode a schema for [`TableSnapshot::schema`]: an Arrow IPC stream with
/// no batches, base64url without padding.
pub fn encode_schema(schema: &Schema) -> Result<String, ArrowError> {
    let mut buf = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, schema)?;
    writer.finish()?;
    drop(writer);
    Ok(URL_SAFE_NO_PAD.encode(buf))
}

/// Decode a schema written by [`encode_schema`].
pub fn decode_schema(text: &str) -> Result<SchemaRef, ArrowError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(text)
        .map_err(|e| ArrowError::ParseError(format!("schema: {e}")))?;
    let reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes), None)?;
    Ok(reader.schema())
}

/// The bytes a `GetSchema` request is identified by in its digest.
pub fn schema_payload(space: &str, table: &str) -> Vec<u8> {
    format!("{}\n{space}\n{table}", space.len()).into_bytes()
}

/// The Space and table of a [`schema_payload`].
pub fn parse_schema_payload(payload: &[u8]) -> Option<(String, String)> {
    let text = std::str::from_utf8(payload).ok()?;
    let (len, rest) = text.split_once('\n')?;
    let len: usize = len.parse().ok()?;
    let space = rest.get(..len)?;
    let table = rest.get(len..)?.strip_prefix('\n')?;
    Some((space.to_string(), table.to_string()))
}

/// A result set as an Arrow IPC stream.
pub fn encode_stream(
    schema: &SchemaRef,
    batches: &[arrow_array::RecordBatch],
) -> Result<Vec<u8>, ArrowError> {
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
pub fn decode_stream(
    bytes: Vec<u8>,
) -> Result<(SchemaRef, Vec<arrow_array::RecordBatch>), ArrowError> {
    let reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
}

/// Digest of a call, as signed by the caller: `method`, its payload and the
/// UCAN presented with it.
pub fn digest(method: &str, payload: &[u8], ucan: &str) -> [u8; 32] {
    oikome_identity::request_digest(method, payload, ucan)
}

/// Whether `name` is a plain identifier usable as a Space or table name in
/// a request: ASCII letters, digits, `_` or `-`, not starting with a digit.
pub fn is_identifier(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_IDENTIFIER_BYTES
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        && !name.starts_with(|c: char| c.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_plain() {
        assert!(is_identifier("blog"));
        assert!(is_identifier("orders_2024-q1"));
        for bad in ["", "1blog", "a.b", "a\"b", "a b", "a;b", "é"] {
            assert!(!is_identifier(bad), "{bad}");
        }
        assert!(!is_identifier(&"a".repeat(MAX_IDENTIFIER_BYTES + 1)));
    }

    #[test]
    fn schema_payloads_parse_back() {
        for (space, table) in [("blog", "posts"), ("a", "b\nc"), ("a\nb", "c")] {
            assert_eq!(
                parse_schema_payload(&schema_payload(space, table)),
                Some((space.to_string(), table.to_string()))
            );
        }
        for bad in [&b""[..], b"x\nab", b"9\nab\ncd", b"\xff"] {
            assert_eq!(parse_schema_payload(bad), None);
        }
    }

    #[test]
    fn schema_payload_is_unambiguous() {
        assert_ne!(schema_payload("a", "b\nc"), schema_payload("a\nb", "c"));
    }

    #[test]
    fn tickets_reject_unknown_fields() {
        assert!(serde_json::from_str::<QueryTicket>(r#"{"sql":"SELECT 1"}"#).is_ok());
        assert!(serde_json::from_str::<QueryTicket>(r#"{"sql":"x","as":"owner"}"#).is_err());
    }

    #[test]
    fn subscription_requests_reject_unknown_fields() {
        assert!(serde_json::from_str::<SnapshotRequest>(r#"{"space":"blog"}"#).is_ok());
        assert!(serde_json::from_str::<SnapshotRequest>(r#"{"space":"blog","path":"/"}"#).is_err());
        let fetch = r#"{"space":"blog","table":"posts","snapshot_id":7,"hash":"00"}"#;
        assert!(serde_json::from_str::<FetchFileRequest>(fetch).is_ok());
        let with_path =
            r#"{"space":"blog","table":"posts","snapshot_id":7,"hash":"00","path":"/etc"}"#;
        assert!(serde_json::from_str::<FetchFileRequest>(with_path).is_err());
    }

    #[test]
    fn file_hashes_are_64_lowercase_hex_digits() {
        let hash = blake3::hash(b"parquet").to_hex().to_string();
        assert!(is_file_hash(&hash));
        assert!(!is_file_hash(&hash.to_uppercase()));
        assert!(!is_file_hash(&hash[..63]));
        for bad in ["", "../../etc/passwd", &format!("{}/", &hash[..63])] {
            assert!(!is_file_hash(bad), "{bad}");
        }
    }

    #[test]
    fn schemas_round_trip() {
        use arrow_schema::{DataType, Field};
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, true),
        ]);
        let decoded = decode_schema(&encode_schema(&schema).unwrap()).unwrap();
        assert_eq!(decoded.as_ref(), &schema);
        assert!(decode_schema("not base64!").is_err());
        assert!(decode_schema(&URL_SAFE_NO_PAD.encode(b"not ipc")).is_err());
    }
}
