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

//! The AgoraDB peer protocol on top of Arrow Flight (architecture v3 §9).
//!
//! | Flight call | Request | Response |
//! |-------------|---------|----------|
//! | `DoAction("agora.list_tables")` | [`ListTablesRequest`] (JSON) | [`ListTablesResponse`] (JSON) |
//! | `GetSchema` | descriptor path `[space, table]` | the relation's schema as the caller sees it |
//! | `DoGet` | ticket [`QueryTicket`] (JSON) | Arrow record batches |
//!
//! Every call carries two headers: [`HEADER_UCAN`], the capability token,
//! and [`HEADER_AUTH`], an [`AuthRequest`](agoradb_identity::AuthRequest)
//! signing the call's [`digest`]. The SQL in a ticket is in the *owner's*
//! namespace (`<space>.<table>` on the owner node); the owner parses and
//! rewrites it with its own semantic layer before any engine sees it.

use serde::{Deserialize, Serialize};

/// Header carrying the signed request (base64url JSON `AuthRequest`).
pub const HEADER_AUTH: &str = "x-agora-auth";
/// Header carrying the UCAN.
pub const HEADER_UCAN: &str = "x-agora-ucan";

/// `DoAction` type listing the relations of a Space the caller may read.
pub const ACTION_LIST_TABLES: &str = "agora.list_tables";

/// Method names bound into the request digest.
pub const METHOD_DO_GET: &str = "do_get";
pub const METHOD_GET_SCHEMA: &str = "get_schema";
pub const METHOD_LIST_TABLES: &str = "do_action:agora.list_tables";

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

/// `agora.list_tables` request body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListTablesRequest {
    pub space: String,
}

/// `agora.list_tables` response body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListTablesResponse {
    pub tables: Vec<String>,
}

/// The bytes a `GetSchema` request is identified by in its digest.
pub fn schema_payload(space: &str, table: &str) -> Vec<u8> {
    format!("{}\n{space}\n{table}", space.len()).into_bytes()
}

/// Digest of a call, as signed by the caller: `method`, its payload and the
/// UCAN presented with it.
pub fn digest(method: &str, payload: &[u8], ucan: &str) -> [u8; 32] {
    agoradb_identity::request_digest(method, payload, ucan)
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
    fn schema_payload_is_unambiguous() {
        assert_ne!(schema_payload("a", "b\nc"), schema_payload("a\nb", "c"));
    }

    #[test]
    fn tickets_reject_unknown_fields() {
        assert!(serde_json::from_str::<QueryTicket>(r#"{"sql":"SELECT 1"}"#).is_ok());
        assert!(serde_json::from_str::<QueryTicket>(r#"{"sql":"x","as":"owner"}"#).is_err());
    }
}
