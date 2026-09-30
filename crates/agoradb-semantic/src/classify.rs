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

//! Statement classification and table-reference qualification (v3 §4.4).
//!
//! Every table reference must be `<space>.<table>`. A bare `<table>` is
//! qualified with the session's default Space; a three-part name is an error
//! because AgoraDB has no catalog level above the Space.

use std::collections::{BTreeSet, HashSet};
use std::ops::ControlFlow;

use sqlparser::ast::{
    visit_relations_mut, GrantObjects, Ident, ObjectName, ObjectNamePart, ObjectType, Query,
    Statement, TableObject, Visit, Visitor,
};

use crate::error::SemanticError;

/// Which DML verb a statement is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlKind {
    Insert,
    Update,
    Delete,
}

/// Transaction control verbs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TclKind {
    Begin,
    Commit,
    Rollback,
}

/// How the session must route a SQL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementClass {
    /// `CREATE TABLE` / `DROP TABLE` in one Space.
    TableDdl { space: String },
    /// `INSERT` / `UPDATE` / `DELETE` targeting one Space (sources may span more).
    Dml {
        space: String,
        kind: DmlKind,
        spaces: BTreeSet<String>,
    },
    /// `BEGIN` / `COMMIT` / `ROLLBACK`.
    Tcl(TclKind),
    /// A read-only statement touching these Spaces (possibly none, e.g. `SELECT 1`).
    Query { spaces: BTreeSet<String> },
    /// `CREATE VIEW` / `DROP VIEW`; names are qualified in place.
    ViewDdl,
    /// `GRANT` / `REVOKE` / `CREATE POLICY` / `DROP POLICY`; names are qualified in place.
    Acl,
}

/// Names of every CTE defined anywhere in `stmt`. References to them are not
/// tables and must be left unqualified.
pub fn cte_names(stmt: &Statement) -> HashSet<String> {
    struct Collector(HashSet<String>);
    impl Visitor for Collector {
        type Break = ();
        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
            if let Some(with) = &query.with {
                for cte in &with.cte_tables {
                    self.0.insert(cte.alias.name.value.clone());
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut collector = Collector(HashSet::new());
    let _ = stmt.visit(&mut collector);
    collector.0
}

/// Whether `name` is a single-part reference to one of `ctes`.
pub(crate) fn is_cte_ref(name: &ObjectName, ctes: &HashSet<String>) -> bool {
    name.0.len() == 1 && ctes.contains(&part_value(&name.0[0]))
}

/// Spaces referenced by the (already qualified) relations of `stmt`.
pub fn collect_spaces(stmt: &Statement) -> BTreeSet<String> {
    let ctes = cte_names(stmt);
    let mut spaces = BTreeSet::new();
    let _ = sqlparser::ast::visit_relations(stmt, |name: &ObjectName| {
        if name.0.len() == 2 && !is_cte_ref(name, &ctes) {
            spaces.insert(part_value(&name.0[0]));
        }
        ControlFlow::<()>::Continue(())
    });
    spaces
}

/// Qualify every table reference in `stmt` and collect the Spaces it touches.
///
/// Bare names are rewritten in place to `<default_space>.<name>`.
pub fn qualify_tables(
    stmt: &mut Statement,
    default_space: Option<&str>,
) -> Result<BTreeSet<String>, SemanticError> {
    let ctes = cte_names(stmt);
    let mut spaces = BTreeSet::new();
    let mut error: Option<SemanticError> = None;
    let _ = visit_relations_mut(stmt, |name: &mut ObjectName| {
        if is_cte_ref(name, &ctes) {
            return ControlFlow::Continue(());
        }
        match qualify_name(name, default_space) {
            Ok(space) => {
                spaces.insert(space);
                ControlFlow::Continue(())
            }
            Err(e) => {
                error = Some(e);
                ControlFlow::Break(())
            }
        }
    });
    match error {
        Some(e) => Err(e),
        None => Ok(spaces),
    }
}

pub(crate) fn part_value(part: &ObjectNamePart) -> String {
    match part {
        ObjectNamePart::Identifier(ident) => ident.value.clone(),
        other => other.to_string(),
    }
}

fn qualify_name(
    name: &mut ObjectName,
    default_space: Option<&str>,
) -> Result<String, SemanticError> {
    match name.0.len() {
        1 => {
            let table = part_value(&name.0[0]);
            let space = default_space
                .ok_or_else(|| SemanticError::UnqualifiedTable(table.clone()))?
                .to_string();
            name.0
                .insert(0, ObjectNamePart::Identifier(Ident::new(space.clone())));
            Ok(space)
        }
        2 => Ok(part_value(&name.0[0])),
        _ => Err(SemanticError::InvalidTableReference(name.to_string())),
    }
}

fn single_space(spaces: &BTreeSet<String>, what: &str) -> Result<String, SemanticError> {
    let mut iter = spaces.iter();
    match (iter.next(), iter.next()) {
        (Some(space), None) => Ok(space.clone()),
        (None, _) => Err(SemanticError::Unsupported(format!(
            "{what} without a table reference"
        ))),
        (Some(_), Some(_)) => Err(SemanticError::Unsupported(format!(
            "{what} across several spaces: {}",
            spaces.iter().cloned().collect::<Vec<_>>().join(", ")
        ))),
    }
}

fn object_space(name: &ObjectName) -> Result<String, SemanticError> {
    match name.0.len() {
        2 => Ok(part_value(&name.0[0])),
        1 => Err(SemanticError::UnqualifiedTable(name.to_string())),
        _ => Err(SemanticError::InvalidTableReference(name.to_string())),
    }
}

/// Qualify table references and classify `stmt` for routing.
pub fn classify(
    stmt: &mut Statement,
    default_space: Option<&str>,
) -> Result<StatementClass, SemanticError> {
    match stmt {
        Statement::StartTransaction { .. } => return Ok(StatementClass::Tcl(TclKind::Begin)),
        Statement::Commit { .. } => return Ok(StatementClass::Tcl(TclKind::Commit)),
        Statement::Rollback { .. } => return Ok(StatementClass::Tcl(TclKind::Rollback)),
        _ => {}
    }

    let spaces = qualify_tables(stmt, default_space)?;
    match stmt {
        Statement::Query(_) => Ok(StatementClass::Query { spaces }),
        Statement::Explain { statement, .. } => match statement.as_ref() {
            Statement::Query(_) => Ok(StatementClass::Query { spaces }),
            other => Err(SemanticError::Unsupported(format!(
                "EXPLAIN of {}",
                statement_kind(other)
            ))),
        },
        Statement::Insert(insert) => {
            let space = match &insert.table {
                TableObject::TableName(name) => object_space(name)?,
                other => return Err(SemanticError::Unsupported(format!("INSERT INTO {other}"))),
            };
            Ok(StatementClass::Dml {
                space,
                kind: DmlKind::Insert,
                spaces,
            })
        }
        Statement::Update { .. } => Ok(StatementClass::Dml {
            space: single_space(&spaces, "UPDATE")?,
            kind: DmlKind::Update,
            spaces,
        }),
        Statement::Delete(_) => Ok(StatementClass::Dml {
            space: single_space(&spaces, "DELETE")?,
            kind: DmlKind::Delete,
            spaces,
        }),
        // DDL object names are not "relations" for the visitor, so qualify them here.
        Statement::CreateTable(create) => Ok(StatementClass::TableDdl {
            space: qualify_name(&mut create.name, default_space)?,
        }),
        Statement::Drop {
            object_type: ObjectType::Table,
            names,
            ..
        } => {
            let targets: BTreeSet<String> = names
                .iter_mut()
                .map(|name| qualify_name(name, default_space))
                .collect::<Result<_, _>>()?;
            Ok(StatementClass::TableDdl {
                space: single_space(&targets, "DROP TABLE")?,
            })
        }
        Statement::CreateView(create) => {
            qualify_name(&mut create.name, default_space)?;
            Ok(StatementClass::ViewDdl)
        }
        Statement::Drop {
            object_type: ObjectType::View,
            names,
            ..
        } => {
            for name in names.iter_mut() {
                qualify_name(name, default_space)?;
            }
            Ok(StatementClass::ViewDdl)
        }
        Statement::Grant(sqlparser::ast::Grant { objects, .. })
        | Statement::Revoke(sqlparser::ast::Revoke { objects, .. }) => {
            match objects {
                Some(GrantObjects::Tables(names)) => {
                    for name in names.iter_mut() {
                        qualify_name(name, default_space)?;
                    }
                }
                other => {
                    return Err(SemanticError::Unsupported(format!(
                        "GRANT/REVOKE on {}; only ON <space>.<table or view> is supported",
                        other.as_ref().map(|o| o.to_string()).unwrap_or_default()
                    )))
                }
            }
            Ok(StatementClass::Acl)
        }
        // Policy table names are relations, already qualified above.
        Statement::CreatePolicy(_) | Statement::DropPolicy(_) => Ok(StatementClass::Acl),
        other => Err(SemanticError::Unsupported(statement_kind(other))),
    }
}

fn statement_kind(stmt: &Statement) -> String {
    let text = stmt.to_string();
    text.split_whitespace()
        .take(2)
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_parser::{parse_single, AgoraStatement};

    fn sql(text: &str) -> Statement {
        match parse_single(text).unwrap() {
            AgoraStatement::Sql(s) => *s,
            other => panic!("expected SQL, got {other:?}"),
        }
    }

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn query_collects_spaces() {
        let mut stmt = sql("SELECT o.customer, b.title FROM orders.orders o \
             JOIN blog.posts b ON b.author = o.customer \
             WHERE o.id IN (SELECT id FROM orders.orders)");
        let class = classify(&mut stmt, None).unwrap();
        assert_eq!(
            class,
            StatementClass::Query {
                spaces: set(&["blog", "orders"])
            }
        );
    }

    #[test]
    fn bare_table_qualified_with_default_space() {
        let mut stmt = sql("SELECT * FROM posts WHERE id = 1");
        let class = classify(&mut stmt, Some("blog")).unwrap();
        assert_eq!(
            class,
            StatementClass::Query {
                spaces: set(&["blog"])
            }
        );
        assert_eq!(stmt.to_string(), "SELECT * FROM blog.posts WHERE id = 1");
    }

    #[test]
    fn bare_table_without_default_is_error() {
        let mut stmt = sql("SELECT * FROM posts");
        assert_eq!(
            classify(&mut stmt, None),
            Err(SemanticError::UnqualifiedTable("posts".into()))
        );
    }

    #[test]
    fn three_part_name_rejected() {
        let mut stmt = sql("SELECT * FROM agora.blog.posts");
        assert!(matches!(
            classify(&mut stmt, None),
            Err(SemanticError::InvalidTableReference(_))
        ));
    }

    #[test]
    fn insert_classified_as_dml_with_space() {
        let mut stmt = sql("INSERT INTO orders.orders (id) VALUES (1)");
        assert_eq!(
            classify(&mut stmt, None).unwrap(),
            StatementClass::Dml {
                space: "orders".into(),
                kind: DmlKind::Insert,
                spaces: set(&["orders"])
            }
        );

        // INSERT ... SELECT from another Space keeps the target Space.
        let mut stmt = sql("INSERT INTO blog.archive SELECT * FROM blog.posts");
        assert_eq!(
            classify(&mut stmt, None).unwrap(),
            StatementClass::Dml {
                space: "blog".into(),
                kind: DmlKind::Insert,
                spaces: set(&["blog"])
            }
        );

        let mut stmt = sql("UPDATE items SET qty = 0 WHERE qty < 0");
        assert!(matches!(
            classify(&mut stmt, Some("inv")).unwrap(),
            StatementClass::Dml { space, kind: DmlKind::Update, .. } if space == "inv"
        ));
        let mut stmt = sql("DELETE FROM inv.items WHERE qty = 0");
        assert!(matches!(
            classify(&mut stmt, None).unwrap(),
            StatementClass::Dml {
                kind: DmlKind::Delete,
                ..
            }
        ));
    }

    #[test]
    fn begin_is_tcl() {
        for (text, kind) in [
            ("BEGIN", TclKind::Begin),
            ("START TRANSACTION", TclKind::Begin),
            ("COMMIT", TclKind::Commit),
            ("ROLLBACK", TclKind::Rollback),
        ] {
            let mut stmt = sql(text);
            assert_eq!(
                classify(&mut stmt, None).unwrap(),
                StatementClass::Tcl(kind)
            );
        }
    }

    #[test]
    fn create_table_is_table_ddl() {
        let mut stmt = sql("CREATE TABLE blog.posts (id BIGINT, title VARCHAR)");
        assert_eq!(
            classify(&mut stmt, None).unwrap(),
            StatementClass::TableDdl {
                space: "blog".into()
            }
        );
        let mut stmt = sql("DROP TABLE posts");
        assert_eq!(
            classify(&mut stmt, Some("blog")).unwrap(),
            StatementClass::TableDdl {
                space: "blog".into()
            }
        );
        let mut stmt = sql("CREATE TABLE posts (id BIGINT)");
        assert!(matches!(
            classify(&mut stmt, None),
            Err(SemanticError::UnqualifiedTable(_))
        ));
    }

    #[test]
    fn unsupported_statements_are_reported() {
        let mut stmt = sql("CREATE INDEX i ON blog.posts (id)");
        assert!(matches!(
            classify(&mut stmt, None),
            Err(SemanticError::Unsupported(_))
        ));
        let mut stmt = sql("SELECT 1");
        assert_eq!(
            classify(&mut stmt, None).unwrap(),
            StatementClass::Query {
                spaces: BTreeSet::new()
            }
        );
        let mut stmt = sql("EXPLAIN SELECT * FROM blog.posts");
        assert!(matches!(
            classify(&mut stmt, None).unwrap(),
            StatementClass::Query { .. }
        ));
    }
}
