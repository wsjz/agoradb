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

//! SQL/PGQ statements of DuckPGQ (architecture v3 §8), which sqlparser
//! cannot read:
//!
//! ```sql
//! CREATE [OR REPLACE] PROPERTY GRAPH [IF NOT EXISTS] [<space>.]<graph>
//!     VERTEX TABLES (people LABEL person)
//!     EDGE TABLES (knows SOURCE KEY (src) REFERENCES people (id)
//!                        DESTINATION KEY (dst) REFERENCES people (id) LABEL knows);
//! DROP PROPERTY GRAPH [IF EXISTS] [<space>.]<graph>;
//! FROM GRAPH_TABLE (<space>.<graph> MATCH (a:person)-[k:knows]->(b:person)
//!                   COLUMNS (a.name AS a, b.name AS b));
//! ```
//!
//! They are tokenized, not parsed: only what names a table or a graph is
//! picked out, so the session can qualify it, check that it belongs to the
//! graph's Space and name the graph as DuckDB knows it. The rest is passed
//! on as written.

use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::error::SemanticError;
use crate::sql_parser::validate_name;

/// A piece of a graph statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphPart {
    /// Passed on as written.
    Text(String),
    /// A table of the graph; `space` is `None` when not qualified.
    Table {
        space: Option<String>,
        table: String,
    },
    /// The graph a `GRAPH_TABLE` reads.
    Graph {
        space: Option<String>,
        graph: String,
    },
}

/// `CREATE PROPERTY GRAPH ...`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateGraphRequest {
    pub space: Option<String>,
    pub graph: String,
    pub or_replace: bool,
    pub if_not_exists: bool,
    /// `VERTEX TABLES (...) EDGE TABLES (...)`.
    pub body: Vec<GraphPart>,
}

/// `DROP PROPERTY GRAPH ...`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropGraphRequest {
    pub space: Option<String>,
    pub graph: String,
    pub if_exists: bool,
}

/// A query using `GRAPH_TABLE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphQuery {
    pub parts: Vec<GraphPart>,
    /// Every `<a>.<b>` name outside the graph names, so the session can
    /// check that the query reads no other Space.
    pub qualified: Vec<(String, String)>,
}

impl GraphQuery {
    /// The `(space, graph)` pairs the query reads.
    pub fn graphs(&self) -> impl Iterator<Item = (Option<&str>, &str)> {
        self.parts.iter().filter_map(|p| match p {
            GraphPart::Graph { space, graph } => Some((space.as_deref(), graph.as_str())),
            _ => None,
        })
    }
}

/// Render `parts`, with tables and graphs named by `name`.
pub fn render_graph_parts<E>(
    parts: &[GraphPart],
    mut name: impl FnMut(&GraphPart) -> Result<String, E>,
) -> Result<String, E> {
    let mut out = String::new();
    for part in parts {
        match part {
            GraphPart::Text(text) => out.push_str(text),
            other => out.push_str(&name(other)?),
        }
    }
    Ok(out.trim().to_string())
}

fn tokens(stmt: &str) -> Result<Vec<Token>, SemanticError> {
    let mut tokens = Tokenizer::new(&GenericDialect {}, stmt)
        .tokenize()
        .map_err(|e| SemanticError::Parse(e.to_string()))?;
    while matches!(tokens.last(), Some(Token::Whitespace(_) | Token::SemiColon)) {
        tokens.pop();
    }
    Ok(tokens)
}

/// An unquoted word, upper-cased.
fn keyword(token: &Token) -> Option<String> {
    match token {
        Token::Word(w) if w.quote_style.is_none() => Some(w.value.to_ascii_uppercase()),
        _ => None,
    }
}

/// Whether `stmt` uses `GRAPH_TABLE`.
pub(crate) fn mentions_graph_table(stmt: &str) -> bool {
    tokens(stmt).is_ok_and(|t| {
        t.iter()
            .any(|t| keyword(t).as_deref() == Some("GRAPH_TABLE"))
    })
}

/// A cursor over the tokens of a statement that skips whitespace.
struct Cursor {
    tokens: Vec<Token>,
    pos: usize,
}

impl Cursor {
    fn skip_space(&mut self) {
        while matches!(self.tokens.get(self.pos), Some(Token::Whitespace(_))) {
            self.pos += 1;
        }
    }

    fn peek(&mut self) -> Option<&Token> {
        self.skip_space();
        self.tokens.get(self.pos)
    }

    fn eat_keyword(&mut self, word: &str) -> bool {
        if self.peek().and_then(keyword).as_deref() == Some(word) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect_keywords(&mut self, words: &[&str], usage: &str) -> Result<(), SemanticError> {
        for word in words {
            if !self.eat_keyword(word) {
                return Err(SemanticError::Parse(usage.to_string()));
            }
        }
        Ok(())
    }

    /// `<a>` or `<a>.<b>`, as `(Some(a), b)` or `(None, a)`.
    fn name(&mut self) -> Option<(Option<String>, String)> {
        let first = match self.peek() {
            Some(Token::Word(w)) => w.value.clone(),
            _ => return None,
        };
        self.pos += 1;
        if matches!(self.tokens.get(self.pos), Some(Token::Period)) {
            if let Some(Token::Word(w)) = self.tokens.get(self.pos + 1) {
                let second = w.value.clone();
                self.pos += 2;
                return Some((Some(first), second));
            }
        }
        Some((None, first))
    }

    fn rest(&self) -> &[Token] {
        &self.tokens[self.pos..]
    }
}

const CREATE_USAGE: &str = "CREATE PROPERTY GRAPH: expected CREATE [OR REPLACE] PROPERTY GRAPH \
     [IF NOT EXISTS] <space>.<graph> VERTEX TABLES (...) [EDGE TABLES (...)]";

/// `CREATE [OR REPLACE] PROPERTY GRAPH ...`
pub(crate) fn parse_create_graph(stmt: &str) -> Result<CreateGraphRequest, SemanticError> {
    let mut c = Cursor {
        tokens: tokens(stmt)?,
        pos: 0,
    };
    c.expect_keywords(&["CREATE"], CREATE_USAGE)?;
    let or_replace = c.eat_keyword("OR");
    if or_replace {
        c.expect_keywords(&["REPLACE"], CREATE_USAGE)?;
    }
    c.expect_keywords(&["PROPERTY", "GRAPH"], CREATE_USAGE)?;
    let if_not_exists = c.eat_keyword("IF");
    if if_not_exists {
        c.expect_keywords(&["NOT", "EXISTS"], CREATE_USAGE)?;
    }
    let (space, graph) = c
        .name()
        .ok_or_else(|| SemanticError::Parse(CREATE_USAGE.to_string()))?;
    if c.peek().and_then(keyword).as_deref() != Some("VERTEX") {
        return Err(SemanticError::Parse(CREATE_USAGE.to_string()));
    }
    Ok(CreateGraphRequest {
        space: space.map(|s| validate_name(&s)).transpose()?,
        graph: validate_name(&graph)?,
        or_replace,
        if_not_exists,
        body: graph_body(c.rest())?,
    })
}

/// Split a graph definition into text and table names: the first name of
/// each element of `VERTEX TABLES (...)` and `EDGE TABLES (...)`, and the
/// name after each `REFERENCES`.
fn graph_body(tokens: &[Token]) -> Result<Vec<GraphPart>, SemanticError> {
    let mut parts = Vec::new();
    let mut text = String::new();
    let mut depth = 0usize;
    let mut list_depth: Option<usize> = None;
    let mut after_tables = false;
    let mut expect_table = false;
    let mut i = 0;
    while i < tokens.len() {
        let token = &tokens[i];
        if expect_table {
            if let Token::Word(w) = token {
                let mut table = (None, w.value.clone());
                if let (Some(Token::Period), Some(Token::Word(second))) =
                    (tokens.get(i + 1), tokens.get(i + 2))
                {
                    table = (Some(w.value.clone()), second.value.clone());
                    i += 2;
                }
                parts.push(GraphPart::Text(std::mem::take(&mut text)));
                parts.push(GraphPart::Table {
                    space: table.0,
                    table: table.1,
                });
                expect_table = false;
                i += 1;
                continue;
            }
        }
        match token {
            Token::LParen => {
                depth += 1;
                if after_tables {
                    list_depth = Some(depth);
                    expect_table = true;
                }
            }
            Token::RParen => {
                if list_depth == Some(depth) {
                    list_depth = None;
                }
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| SemanticError::Parse("unbalanced parentheses".into()))?;
            }
            Token::Comma if list_depth == Some(depth) => expect_table = true,
            _ => {}
        }
        if !matches!(token, Token::Whitespace(_)) {
            after_tables = keyword(token).as_deref() == Some("TABLES");
            if keyword(token).as_deref() == Some("REFERENCES") {
                expect_table = true;
            }
        }
        text.push_str(&token.to_string());
        i += 1;
    }
    if depth != 0 {
        return Err(SemanticError::Parse("unbalanced parentheses".into()));
    }
    parts.push(GraphPart::Text(text));
    parts.retain(|p| !matches!(p, GraphPart::Text(t) if t.is_empty()));
    Ok(parts)
}

const DROP_USAGE: &str =
    "DROP PROPERTY GRAPH: expected DROP PROPERTY GRAPH [IF EXISTS] <space>.<graph>";

/// `DROP PROPERTY GRAPH [IF EXISTS] <name>`
pub(crate) fn parse_drop_graph(stmt: &str) -> Result<DropGraphRequest, SemanticError> {
    let mut c = Cursor {
        tokens: tokens(stmt)?,
        pos: 0,
    };
    c.expect_keywords(&["DROP", "PROPERTY", "GRAPH"], DROP_USAGE)?;
    let if_exists = c.eat_keyword("IF");
    if if_exists {
        c.expect_keywords(&["EXISTS"], DROP_USAGE)?;
    }
    let (space, graph) = c
        .name()
        .ok_or_else(|| SemanticError::Parse(DROP_USAGE.to_string()))?;
    if c.peek().is_some() {
        return Err(SemanticError::Parse(DROP_USAGE.to_string()));
    }
    Ok(DropGraphRequest {
        space: space.map(|s| validate_name(&s)).transpose()?,
        graph: validate_name(&graph)?,
        if_exists,
    })
}

/// A query using `GRAPH_TABLE (<graph> MATCH ...)`.
pub(crate) fn parse_graph_query(stmt: &str) -> Result<GraphQuery, SemanticError> {
    let tokens = tokens(stmt)?;
    let mut parts = Vec::new();
    let mut qualified = Vec::new();
    let mut text = String::new();
    let mut i = 0;
    while i < tokens.len() {
        let token = &tokens[i];
        if keyword(token).as_deref() == Some("GRAPH_TABLE") {
            text.push_str(&token.to_string());
            i += 1;
            while matches!(tokens.get(i), Some(Token::Whitespace(_))) {
                text.push_str(&tokens[i].to_string());
                i += 1;
            }
            if !matches!(tokens.get(i), Some(Token::LParen)) {
                return Err(SemanticError::Parse(
                    "GRAPH_TABLE: expected GRAPH_TABLE (<space>.<graph> MATCH ...)".into(),
                ));
            }
            text.push('(');
            i += 1;
            while matches!(tokens.get(i), Some(Token::Whitespace(_))) {
                text.push_str(&tokens[i].to_string());
                i += 1;
            }
            let mut c = Cursor {
                tokens: tokens.clone(),
                pos: i,
            };
            let (space, graph) = c
                .name()
                .ok_or_else(|| SemanticError::Parse("GRAPH_TABLE: expected a graph name".into()))?;
            parts.push(GraphPart::Text(std::mem::take(&mut text)));
            parts.push(GraphPart::Graph {
                space: space.map(|s| validate_name(&s)).transpose()?,
                graph: validate_name(&graph)?,
            });
            i = c.pos;
            continue;
        }
        if let (Token::Word(a), Some(Token::Period), Some(Token::Word(b))) =
            (token, tokens.get(i + 1), tokens.get(i + 2))
        {
            qualified.push((a.value.clone(), b.value.clone()));
        }
        text.push_str(&token.to_string());
        i += 1;
    }
    parts.push(GraphPart::Text(text));
    parts.retain(|p| !matches!(p, GraphPart::Text(t) if t.is_empty()));
    if !parts.iter().any(|p| matches!(p, GraphPart::Graph { .. })) {
        return Err(SemanticError::Parse("expected GRAPH_TABLE (...)".into()));
    }
    Ok(GraphQuery { parts, qualified })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(space: Option<&str>, table: &str) -> GraphPart {
        GraphPart::Table {
            space: space.map(str::to_string),
            table: table.into(),
        }
    }

    #[test]
    fn graph_definitions_name_their_tables() {
        let req = parse_create_graph(
            "CREATE OR REPLACE PROPERTY GRAPH social.friends \
             VERTEX TABLES (people PROPERTIES (id, name) LABEL person, social.pets LABEL pet) \
             EDGE TABLES (knows SOURCE KEY (src) REFERENCES people (id) \
                          DESTINATION KEY (dst) REFERENCES social.people (id) LABEL knows);",
        )
        .unwrap();
        assert_eq!(req.space.as_deref(), Some("social"));
        assert_eq!(req.graph, "friends");
        assert!(req.or_replace && !req.if_not_exists);
        let tables: Vec<&GraphPart> = req
            .body
            .iter()
            .filter(|p| !matches!(p, GraphPart::Text(_)))
            .collect();
        assert_eq!(
            tables,
            vec![
                &table(None, "people"),
                &table(Some("social"), "pets"),
                &table(None, "knows"),
                &table(None, "people"),
                &table(Some("social"), "people"),
            ]
        );
        let rendered = render_graph_parts::<()>(&req.body, |p| match p {
            GraphPart::Table { table, .. } => Ok(format!("s.{table}")),
            _ => unreachable!(),
        })
        .unwrap();
        assert_eq!(
            rendered,
            "VERTEX TABLES (s.people PROPERTIES (id, name) LABEL person, s.pets LABEL pet) \
             EDGE TABLES (s.knows SOURCE KEY (src) REFERENCES s.people (id) \
                          DESTINATION KEY (dst) REFERENCES s.people (id) LABEL knows)"
        );
        for sql in [
            "CREATE PROPERTY GRAPH g",
            "CREATE PROPERTY GRAPH g EDGE TABLES (k)",
            "CREATE PROPERTY GRAPH g VERTEX TABLES (p",
            "CREATE GRAPH g VERTEX TABLES (p)",
        ] {
            assert!(parse_create_graph(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn graph_queries_name_their_graph() {
        let q = parse_graph_query(
            "SELECT * FROM GRAPH_TABLE (social.friends MATCH (a:person)-[k:knows]->(b:person) \
             COLUMNS (a.name AS a, b.name AS b)) g JOIN notes.items i ON i.owner = g.a",
        )
        .unwrap();
        assert_eq!(
            q.graphs().collect::<Vec<_>>(),
            vec![(Some("social"), "friends")]
        );
        assert!(q
            .qualified
            .contains(&("notes".to_string(), "items".to_string())));
        let text = render_graph_parts::<()>(&q.parts, |_| Ok("social__friends".into())).unwrap();
        assert!(
            text.starts_with("SELECT * FROM GRAPH_TABLE (social__friends MATCH"),
            "{text}"
        );

        let drop = parse_drop_graph("DROP PROPERTY GRAPH IF EXISTS friends").unwrap();
        assert_eq!(
            drop,
            DropGraphRequest {
                space: None,
                graph: "friends".into(),
                if_exists: true
            }
        );
        assert!(parse_drop_graph("DROP PROPERTY GRAPH a.b.c").is_err());
    }
}
