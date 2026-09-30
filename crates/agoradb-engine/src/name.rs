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

use std::fmt;

/// A `<space>.<table>` name as the engine sees it.
///
/// Every engine is configured so that this two-part name resolves natively:
/// DuckDB maps a Space to a schema, SQLite to an attached database alias.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedName {
    pub space: String,
    pub table: String,
}

impl QualifiedName {
    /// Create a qualified name.
    pub fn new(space: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            space: space.into(),
            table: table.into(),
        }
    }

    /// The name with both parts double-quoted, safe to splice into SQL.
    pub fn quoted(&self) -> String {
        format!("{}.{}", quote_ident(&self.space), quote_ident(&self.table))
    }
}

impl fmt::Display for QualifiedName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.space, self.table)
    }
}

/// Double-quote an SQL identifier, escaping embedded quotes.
pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Single-quote an SQL string literal, escaping embedded quotes.
pub fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualified_name_quoting() {
        let name = QualifiedName::new("blog", "posts");
        assert_eq!(name.quoted(), "\"blog\".\"posts\"");
        assert_eq!(name.to_string(), "blog.posts");

        let odd = QualifiedName::new("we\"ird", "t");
        assert_eq!(odd.quoted(), "\"we\"\"ird\".\"t\"");
        assert_eq!(quote_literal("it's"), "'it''s'");
    }
}
