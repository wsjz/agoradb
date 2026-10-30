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

//! Parsing of AgoraDB statements.
//!
//! AgoraDB adds these statements on top of SQL:
//!
//! ```sql
//! CREATE SPACE <name> [WITH KIND = 'analytical' | 'transactional',
//!                           ENGINE = 'duckdb' | 'sqlite',
//!                           LOCATION = '<location id>',
//!                           ACCESS = 'writable' | 'readonly',
//!                           STORAGE = 'disk']
//! CREATE SPACE <name> WITH TOKEN = '<ucan>' [, PEER = 'https://host:port']
//!                          [, REMOTE = '<space on the peer>'] [, KIND = 'remote']
//!                          [, MODE = 'query' | 'subscribe']
//! DROP SPACE <name>
//! REFRESH SPACE <name>
//! SHOW PEERS
//! SET SPACE [=] <name>
//! PUBLISH SPACE <name> [TABLES (<table>, ...)] [TO <target space>]
//! ```
//!
//! plus the identity and capability statements of [`crate::capability`].
//!
//! Everything else is parsed by `sqlparser` with the generic dialect and
//! returned as an AST for classification and routing.

use agoradb_core::CreateSpaceRequest;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::capability::{parse_issue_ucan, parse_revoke_ucan, IssueUcanRequest};
use crate::error::SemanticError;

/// One parsed statement.
#[derive(Debug, Clone, PartialEq)]
pub enum AgoraStatement {
    /// `CREATE SPACE ...`
    CreateSpace(CreateSpaceRequest),
    /// `DROP SPACE <name>`
    DropSpace(String),
    /// `SET SPACE <name>`: the default Space for unqualified table names.
    SetSpace(String),
    /// `PUBLISH SPACE ...`: snapshot a transactional Space into an analytical one.
    PublishSpace(PublishRequest),
    /// `REFRESH SPACE <name>`: copy the owner's current snapshot of a
    /// subscribed Space.
    RefreshSpace(String),
    /// `SHOW IDENTITY`: this node's DID.
    ShowIdentity,
    /// `ISSUE UCAN ...`: grant a peer a capability on Spaces of this node.
    IssueUcan(IssueUcanRequest),
    /// `REVOKE UCAN '<id>'`
    RevokeUcan(String),
    /// `SHOW UCANS`: issued and revoked tokens.
    ShowUcans,
    /// `SHOW PEERS`: nodes found by discovery.
    ShowPeers,
    /// Any standard SQL statement.
    Sql(Box<Statement>),
}

/// What `PUBLISH SPACE` asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRequest {
    /// The transactional Space to publish.
    pub space: String,
    /// Tables to publish; `None` publishes all of them.
    pub tables: Option<Vec<String>>,
    /// Target analytical Space; `None` means `<space>_published`.
    pub target: Option<String>,
}

/// Parse `sql` into one or more statements.
pub fn parse(sql: &str) -> Result<Vec<AgoraStatement>, SemanticError> {
    split_statements(sql)
        .into_iter()
        .map(|stmt| parse_one(&stmt))
        .collect()
}

/// Parse `sql`, which must hold exactly one statement.
pub fn parse_single(sql: &str) -> Result<AgoraStatement, SemanticError> {
    let mut statements = parse(sql)?;
    match statements.len() {
        1 => Ok(statements.remove(0)),
        n => Err(SemanticError::MultipleStatements(n)),
    }
}

fn parse_one(stmt: &str) -> Result<AgoraStatement, SemanticError> {
    let words: Vec<&str> = stmt.split_whitespace().collect();
    let upper = |i: usize| words.get(i).map(|w| w.to_ascii_uppercase());
    match (upper(0).as_deref(), upper(1).as_deref()) {
        (Some("CREATE"), Some("SPACE")) => parse_create_space(stmt),
        (Some(verb @ ("DROP" | "REFRESH")), Some("SPACE")) => {
            let name = words.get(2).ok_or_else(|| {
                SemanticError::Parse(format!("{verb} SPACE: expected a space name"))
            })?;
            if words.len() > 3 {
                return Err(SemanticError::Parse(format!(
                    "{verb} SPACE: unexpected input after '{name}'"
                )));
            }
            let name = validate_name(name)?;
            Ok(if verb == "DROP" {
                AgoraStatement::DropSpace(name)
            } else {
                AgoraStatement::RefreshSpace(name)
            })
        }
        (Some("PUBLISH"), Some("SPACE")) => parse_publish(stmt),
        (Some("ISSUE"), Some("UCAN")) => Ok(AgoraStatement::IssueUcan(parse_issue_ucan(stmt)?)),
        (Some("REVOKE"), Some("UCAN")) => Ok(AgoraStatement::RevokeUcan(parse_revoke_ucan(stmt)?)),
        (Some("SHOW"), Some(second @ ("IDENTITY" | "UCANS" | "PEERS"))) => {
            if words.len() > 2 {
                return Err(SemanticError::Parse(format!(
                    "SHOW {second}: unexpected input after {second}"
                )));
            }
            Ok(match second {
                "IDENTITY" => AgoraStatement::ShowIdentity,
                "UCANS" => AgoraStatement::ShowUcans,
                _ => AgoraStatement::ShowPeers,
            })
        }
        (Some("SET"), Some("SPACE")) => {
            let rest: Vec<&str> = words[2..].iter().copied().filter(|w| *w != "=").collect();
            let raw = match rest.as_slice() {
                [name] => name.trim_start_matches('=').trim_end_matches('='),
                [] => {
                    return Err(SemanticError::Parse(
                        "SET SPACE: expected a space name".into(),
                    ))
                }
                _ => {
                    return Err(SemanticError::Parse(
                        "SET SPACE: expected a single space name".into(),
                    ))
                }
            };
            Ok(AgoraStatement::SetSpace(validate_name(unquote(raw))?))
        }
        _ => {
            let mut parsed = Parser::parse_sql(&GenericDialect {}, stmt)
                .map_err(|e| SemanticError::Parse(e.to_string()))?;
            match parsed.len() {
                1 => Ok(AgoraStatement::Sql(Box::new(parsed.remove(0)))),
                0 => Err(SemanticError::Parse("empty statement".into())),
                n => Err(SemanticError::MultipleStatements(n)),
            }
        }
    }
}

fn parse_create_space(stmt: &str) -> Result<AgoraStatement, SemanticError> {
    let mut words = stmt.split_whitespace();
    words.next(); // CREATE
    words.next(); // SPACE
    let name = words
        .next()
        .ok_or_else(|| SemanticError::Parse("CREATE SPACE: expected a space name".into()))?;
    let mut request = CreateSpaceRequest::new(validate_name(name)?);

    let rest: Vec<&str> = words.collect();
    if rest.is_empty() {
        return Ok(AgoraStatement::CreateSpace(request));
    }
    if !rest[0].eq_ignore_ascii_case("WITH") {
        return Err(SemanticError::Parse(format!(
            "CREATE SPACE: expected WITH, found '{}'",
            rest[0]
        )));
    }
    let props = rest[1..].join(" ");
    if props.trim().is_empty() {
        return Err(SemanticError::Parse(
            "CREATE SPACE: WITH must be followed by properties".into(),
        ));
    }

    for assignment in split_top_level(&props, ',') {
        let assignment = assignment.trim();
        if assignment.is_empty() {
            continue;
        }
        let (key, value) = assignment.split_once('=').ok_or_else(|| {
            SemanticError::Parse(format!(
                "CREATE SPACE: expected KEY = 'value', found '{assignment}'"
            ))
        })?;
        let key = key.trim().to_ascii_uppercase();
        let value = unquote(value.trim()).to_string();
        let invalid = |e: agoradb_core::AgoraError| SemanticError::InvalidProperty(e.to_string());
        match key.as_str() {
            "KIND" => request.kind = Some(value.parse().map_err(invalid)?),
            "ENGINE" => request.engine = Some(value.parse().map_err(invalid)?),
            "ACCESS" => request.access = Some(value.parse().map_err(invalid)?),
            "LOCATION" => request.location = Some(value),
            "STORAGE" => request.storage = Some(value),
            "PEER" => request.peer = Some(value),
            "REMOTE" => request.remote = Some(validate_name(&value)?),
            "TOKEN" => request.token = Some(value),
            "MODE" => request.mode = Some(value.parse().map_err(invalid)?),
            other => {
                return Err(SemanticError::InvalidProperty(format!(
                    "unknown CREATE SPACE property '{other}' \
                     (expected KIND, ENGINE, LOCATION, ACCESS, STORAGE, PEER, REMOTE, TOKEN or MODE)"
                )))
            }
        }
    }
    let remote_keys = request.peer.is_some()
        || request.remote.is_some()
        || request.token.is_some()
        || request.mode.is_some();
    let remote_kind = request.kind == Some(agoradb_core::SpaceKind::Remote);
    if remote_keys || remote_kind {
        if request.token.is_none() {
            return Err(SemanticError::InvalidProperty(
                "a remote space needs TOKEN = '<ucan>' (and PEER = 'https://host:port' \
                 unless its owner has been discovered)"
                    .to_string(),
            ));
        }
        if request
            .kind
            .is_some_and(|k| k != agoradb_core::SpaceKind::Remote)
            || request.engine.is_some()
            || request.location.is_some()
            || request.access.is_some()
            || request.storage.is_some()
        {
            return Err(SemanticError::InvalidProperty(
                "a remote space only takes PEER, REMOTE, TOKEN, MODE and KIND = 'remote'"
                    .to_string(),
            ));
        }
        request.kind = Some(agoradb_core::SpaceKind::Remote);
    }
    Ok(AgoraStatement::CreateSpace(request))
}

fn parse_publish(stmt: &str) -> Result<AgoraStatement, SemanticError> {
    let err = |msg: &str| SemanticError::Parse(format!("PUBLISH SPACE: {msg}"));
    let mut rest = stmt.trim();
    for keyword in ["PUBLISH", "SPACE"] {
        rest = rest[keyword.len()..].trim_start();
    }
    let name_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let space = validate_name(&rest[..name_end])?;
    rest = rest[name_end..].trim();

    let mut tables = None;
    let mut target = None;
    while !rest.is_empty() {
        let upper = rest.to_ascii_uppercase();
        if upper.starts_with("TABLES") && tables.is_none() {
            rest = rest["TABLES".len()..].trim_start();
            let list = rest
                .strip_prefix('(')
                .ok_or_else(|| err("expected ( after TABLES"))?;
            let close = list.find(')').ok_or_else(|| err("missing )"))?;
            let names = list[..close]
                .split(',')
                .map(|t| validate_name(t.trim()))
                .collect::<Result<Vec<_>, _>>()?;
            if names.is_empty() {
                return Err(err("TABLES needs at least one table"));
            }
            tables = Some(names);
            rest = list[close + 1..].trim();
        } else if upper.starts_with("TO ") && target.is_none() {
            rest = rest[2..].trim_start();
            let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
            target = Some(validate_name(&rest[..end])?);
            rest = rest[end..].trim();
        } else {
            return Err(err(&format!("unexpected input '{rest}'")));
        }
    }
    Ok(AgoraStatement::PublishSpace(PublishRequest {
        space,
        tables,
        target,
    }))
}

fn validate_name(raw: &str) -> Result<String, SemanticError> {
    let name = unquote(raw.trim_end_matches(';'));
    let ok = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        && !name.starts_with(|c: char| c.is_ascii_digit());
    if ok {
        Ok(name.to_string())
    } else {
        Err(SemanticError::Parse(format!(
            "invalid space name '{name}': use letters, digits, '_' or '-' and do not start with a digit"
        )))
    }
}

fn unquote(value: &str) -> &str {
    let v = value.trim();
    if v.len() >= 2 {
        let first = v.as_bytes()[0];
        let last = v.as_bytes()[v.len() - 1];
        if (first == b'\'' && last == b'\'') || (first == b'"' && last == b'"') {
            return &v[1..v.len() - 1];
        }
    }
    v
}

/// Split on `sep` outside single/double quotes.
fn split_top_level(input: &str, sep: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    for c in input.chars() {
        match (quote, c) {
            (None, '\'' | '"') => {
                quote = Some(c);
                current.push(c);
            }
            (Some(q), c) if q == c => {
                quote = None;
                current.push(c);
            }
            (None, c) if c == sep => parts.push(std::mem::take(&mut current)),
            (_, c) => current.push(c),
        }
    }
    parts.push(current);
    parts
}

/// Split a script into statements on top-level `;`, dropping empty ones.
pub fn split_statements(sql: &str) -> Vec<String> {
    split_top_level(sql, ';')
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use agoradb_core::{AccessMode, EngineKind, SpaceKind};

    fn create(sql: &str) -> CreateSpaceRequest {
        match parse_single(sql).unwrap() {
            AgoraStatement::CreateSpace(req) => req,
            other => panic!("expected CREATE SPACE, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_create_space_basic() {
        let req = create("CREATE SPACE blog");
        assert_eq!(req, CreateSpaceRequest::new("blog"));
        assert_eq!(create("create space blog;").name, "blog");
    }

    #[test]
    fn test_parse_create_space_with_properties() {
        let req = create(
            "CREATE SPACE orders WITH KIND = 'transactional', ENGINE = \"sqlite\", STORAGE = 'disk'",
        );
        assert_eq!(req.name, "orders");
        assert_eq!(req.kind, Some(SpaceKind::Transactional));
        assert_eq!(req.engine, Some(EngineKind::Sqlite));
        assert_eq!(req.storage.as_deref(), Some("disk"));
        assert_eq!(req.location, None);
    }

    #[test]
    fn test_parse_create_space_location_and_access() {
        let req = create(
            "CREATE SPACE orders_ro WITH KIND='analytical', LOCATION='orders', ACCESS='readonly'",
        );
        assert_eq!(req.kind, Some(SpaceKind::Analytical));
        assert_eq!(req.location.as_deref(), Some("orders"));
        assert_eq!(req.access, Some(AccessMode::ReadOnly));
    }

    #[test]
    fn test_parse_create_space_quoted_comma() {
        let req = create("CREATE SPACE s WITH LOCATION = 'a,b', STORAGE = 'disk'");
        assert_eq!(req.location.as_deref(), Some("a,b"));
        assert_eq!(req.storage.as_deref(), Some("disk"));
    }

    #[test]
    fn test_parse_create_space_errors() {
        assert!(matches!(
            parse_single("CREATE SPACE"),
            Err(SemanticError::Parse(_))
        ));
        assert!(matches!(
            parse_single("CREATE SPACE s WITH"),
            Err(SemanticError::Parse(_))
        ));
        assert!(matches!(
            parse_single("CREATE SPACE s WITH MODES = 'TABLE'"),
            Err(SemanticError::InvalidProperty(_))
        ));
        assert!(matches!(
            parse_single("CREATE SPACE s WITH KIND = 'graph'"),
            Err(SemanticError::InvalidProperty(_))
        ));
        assert!(matches!(
            parse_single("CREATE SPACE a.b"),
            Err(SemanticError::Parse(_))
        ));
        assert!(matches!(
            parse_single("CREATE SPACE s FOO = 1"),
            Err(SemanticError::Parse(_))
        ));
    }

    #[test]
    fn test_parse_create_remote_space() {
        let req = create(
            "CREATE SPACE pub WITH PEER = 'http://node-a:50051', REMOTE = 'blog', TOKEN = 'a.b.c'",
        );
        assert_eq!(req.kind, Some(SpaceKind::Remote));
        assert_eq!(req.peer.as_deref(), Some("http://node-a:50051"));
        assert_eq!(req.remote.as_deref(), Some("blog"));
        assert_eq!(req.token.as_deref(), Some("a.b.c"));
        let req = create("CREATE SPACE blog WITH KIND='remote', PEER='http://a:1', TOKEN='t'");
        assert_eq!(req.remote, None);

        for bad in [
            "CREATE SPACE pub WITH PEER = 'http://a:1'",
            "CREATE SPACE pub WITH KIND = 'remote'",
            "CREATE SPACE pub WITH PEER = 'http://a:1', TOKEN = 't', KIND = 'analytical'",
            "CREATE SPACE pub WITH PEER = 'http://a:1', TOKEN = 't', LOCATION = 'x'",
            "CREATE SPACE pub WITH PEER = 'http://a:1', TOKEN = 't', REMOTE = 'a.b'",
        ] {
            assert!(parse_single(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn test_parse_discovered_peers() {
        // Without PEER the owner is looked up among discovered peers.
        let req = create("CREATE SPACE pub WITH TOKEN = 't', REMOTE = 'blog'");
        assert_eq!(req.kind, Some(SpaceKind::Remote));
        assert_eq!(req.peer, None);
        assert_eq!(
            create("CREATE SPACE pub WITH PEER = 'https://a:1', TOKEN = 't'")
                .peer
                .as_deref(),
            Some("https://a:1")
        );
        assert_eq!(
            parse_single("show peers").unwrap(),
            AgoraStatement::ShowPeers
        );
        assert!(parse_single("SHOW PEERS now").is_err());
    }

    #[test]
    fn test_parse_subscription() {
        let req = create(
            "CREATE SPACE copy WITH PEER = 'http://a:1', TOKEN = 't', REMOTE = 'blog', MODE = 'subscribe'",
        );
        assert_eq!(req.kind, Some(SpaceKind::Remote));
        assert_eq!(req.mode, Some(agoradb_core::RemoteMode::Subscribe));
        let req = create("CREATE SPACE pub WITH PEER = 'http://a:1', TOKEN = 't'");
        assert_eq!(req.mode, None);

        for bad in [
            // MODE only applies to a remote space.
            "CREATE SPACE copy WITH MODE = 'subscribe'",
            "CREATE SPACE copy WITH KIND = 'analytical', MODE = 'query'",
            "CREATE SPACE copy WITH PEER = 'http://a:1', TOKEN = 't', MODE = 'mirror'",
        ] {
            assert!(parse_single(bad).is_err(), "{bad}");
        }

        assert_eq!(
            parse_single("refresh space copy;").unwrap(),
            AgoraStatement::RefreshSpace("copy".to_string())
        );
        for bad in ["REFRESH SPACE", "REFRESH SPACE a b", "REFRESH SPACE a.b"] {
            assert!(parse_single(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn test_parse_identity_statements() {
        assert_eq!(
            parse_single("SHOW IDENTITY").unwrap(),
            AgoraStatement::ShowIdentity
        );
        assert_eq!(
            parse_single("show ucans;").unwrap(),
            AgoraStatement::ShowUcans
        );
        assert!(parse_single("SHOW IDENTITY x").is_err());
        assert!(matches!(
            parse_single("ISSUE UCAN ON SPACE blog TO 'did:key:z'").unwrap(),
            AgoraStatement::IssueUcan(_)
        ));
        assert_eq!(
            parse_single("REVOKE UCAN 'abc'").unwrap(),
            AgoraStatement::RevokeUcan("abc".into())
        );
        // REVOKE SELECT stays standard SQL.
        assert!(matches!(
            parse_single("REVOKE SELECT ON blog.posts FROM alice").unwrap(),
            AgoraStatement::Sql(_)
        ));
        // Other SHOW statements are left to sqlparser.
        assert!(matches!(
            parse_single("SHOW TABLES").unwrap(),
            AgoraStatement::Sql(_)
        ));
    }

    #[test]
    fn test_parse_drop_and_set_space() {
        assert_eq!(
            parse_single("DROP SPACE blog").unwrap(),
            AgoraStatement::DropSpace("blog".into())
        );
        assert_eq!(
            parse_single("SET SPACE orders").unwrap(),
            AgoraStatement::SetSpace("orders".into())
        );
        assert_eq!(
            parse_single("SET SPACE = 'orders';").unwrap(),
            AgoraStatement::SetSpace("orders".into())
        );
        assert!(matches!(
            parse_single("SET SPACE"),
            Err(SemanticError::Parse(_))
        ));
        assert!(matches!(
            parse_single("DROP SPACE a b"),
            Err(SemanticError::Parse(_))
        ));
    }

    #[test]
    fn test_parse_publish_space() {
        assert_eq!(
            parse_single("PUBLISH SPACE orders").unwrap(),
            AgoraStatement::PublishSpace(PublishRequest {
                space: "orders".into(),
                tables: None,
                target: None
            })
        );
        assert_eq!(
            parse_single("publish space orders tables (orders, items) to snap;").unwrap(),
            AgoraStatement::PublishSpace(PublishRequest {
                space: "orders".into(),
                tables: Some(vec!["orders".into(), "items".into()]),
                target: Some("snap".into())
            })
        );
        assert_eq!(
            parse_single("PUBLISH SPACE orders TO snap TABLES (orders)").unwrap(),
            AgoraStatement::PublishSpace(PublishRequest {
                space: "orders".into(),
                tables: Some(vec!["orders".into()]),
                target: Some("snap".into())
            })
        );
        for bad in [
            "PUBLISH SPACE",
            "PUBLISH SPACE orders TABLES orders",
            "PUBLISH SPACE orders TABLES (a",
            "PUBLISH SPACE orders WITH x",
            "PUBLISH SPACE orders TO",
        ] {
            assert!(parse_single(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn test_parse_standard_sql() {
        let stmts = parse("SELECT id FROM blog.posts WHERE id > 1").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], AgoraStatement::Sql(_)));

        // CREATE TABLE is ordinary SQL, not an Agora statement.
        let stmts = parse("CREATE TABLE blog.posts (id BIGINT)").unwrap();
        assert!(
            matches!(&stmts[0], AgoraStatement::Sql(s) if matches!(**s, Statement::CreateTable(_)))
        );
    }

    #[test]
    fn test_parse_multiple_statements() {
        let stmts = parse("CREATE SPACE a; SELECT 1; SET SPACE a;").unwrap();
        assert_eq!(stmts.len(), 3);
        assert!(matches!(stmts[0], AgoraStatement::CreateSpace(_)));
        assert!(matches!(stmts[1], AgoraStatement::Sql(_)));
        assert!(matches!(stmts[2], AgoraStatement::SetSpace(_)));
        assert_eq!(
            parse_single("SELECT 1; SELECT 2"),
            Err(SemanticError::MultipleStatements(2))
        );
        // A ';' inside a string literal does not split.
        let stmts = parse("SELECT 'a;b'").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_parse_invalid_sql() {
        assert!(matches!(
            parse("SELEC nonsense"),
            Err(SemanticError::Parse(_))
        ));
        assert!(parse("   ;  ").unwrap().is_empty());
    }
}
