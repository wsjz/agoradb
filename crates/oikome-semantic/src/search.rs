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

//! Search table functions of Lance Spaces (architecture v3 §7.6):
//!
//! ```sql
//! SELECT id, _distance FROM lance_vector_search(notes.items, 'embedding', [..], k => 5)
//! SELECT id, _score FROM lance_fts(notes.items, 'body', 'apple', k => 5)
//! SELECT id, _hybrid_score FROM lance_hybrid_search(notes.items, 'embedding', [..], 'body', 'apple')
//! ```
//!
//! The first argument names the table, qualified like any relation
//! (`SET SPACE` applies). The function names themselves are not tables.
//! The node replaces the table with the dataset path the `lance` extension
//! expects ([`bind_search_tables`]) before the query reaches DuckDB.

use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, Ident, ObjectName, Statement, TableFactor,
    Value, Visit, VisitMut, Visitor, VisitorMut,
};

use crate::classify::part_value;
use crate::error::SemanticError;

/// Table functions that search a Lance table named by their first argument.
pub const SEARCH_FUNCTIONS: [&str; 3] = ["lance_vector_search", "lance_fts", "lance_hybrid_search"];

/// Whether `name` is one of [`SEARCH_FUNCTIONS`].
pub(crate) fn is_search_function(name: &ObjectName) -> bool {
    name.0.len() == 1 && {
        let name = part_value(&name.0[0]).to_ascii_lowercase();
        SEARCH_FUNCTIONS.contains(&name.as_str())
    }
}

/// The table argument of a search call, if `factor` is one.
fn search_arg(factor: &mut TableFactor) -> Option<(String, &mut FunctionArg)> {
    match factor {
        TableFactor::Table {
            name,
            args: Some(args),
            ..
        } if is_search_function(name) => {
            let function = part_value(&name.0[0]).to_ascii_lowercase();
            args.args.first_mut().map(|arg| (function, arg))
        }
        _ => None,
    }
}

fn table_expr(arg: &mut FunctionArg) -> Option<&mut Expr> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
        _ => None,
    }
}

/// Qualify the table argument of every search call in `stmt`; returns the
/// `(space, table)` each call reads.
pub(crate) fn qualify_search_calls(
    stmt: &mut Statement,
    default_space: Option<&str>,
) -> Result<Vec<(String, String)>, SemanticError> {
    struct Qualify<'a> {
        default_space: Option<&'a str>,
        found: Vec<(String, String)>,
    }
    impl VisitorMut for Qualify<'_> {
        type Break = SemanticError;
        fn pre_visit_table_factor(
            &mut self,
            factor: &mut TableFactor,
        ) -> ControlFlow<SemanticError> {
            let Some((function, arg)) = search_arg(factor) else {
                return ControlFlow::Continue(());
            };
            let usage = || {
                SemanticError::InvalidTableReference(format!(
                    "the first argument of {function} names a table: \
                     {function}(<space>.<table>, ...)"
                ))
            };
            let Some(expr) = table_expr(arg) else {
                return ControlFlow::Break(usage());
            };
            let (space, table) = match expr {
                Expr::Identifier(table) => match self.default_space {
                    Some(space) => (space.to_string(), table.value.clone()),
                    None => {
                        return ControlFlow::Break(SemanticError::UnqualifiedTable(
                            table.value.clone(),
                        ))
                    }
                },
                Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                    (parts[0].value.clone(), parts[1].value.clone())
                }
                _ => return ControlFlow::Break(usage()),
            };
            *expr = Expr::CompoundIdentifier(vec![Ident::new(&space), Ident::new(&table)]);
            self.found.push((space, table));
            ControlFlow::Continue(())
        }
    }
    let mut visitor = Qualify {
        default_space,
        found: Vec::new(),
    };
    match stmt.visit(&mut visitor) {
        ControlFlow::Break(e) => Err(e),
        ControlFlow::Continue(()) => Ok(visitor.found),
    }
}

/// Whether `stmt` uses the `MATCH` operator: full-text search of an FTS5
/// table or vector search of a sqlite-vec `vec0` table.
pub fn uses_match(stmt: &Statement) -> bool {
    struct Find;
    impl Visitor for Find {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            match expr {
                Expr::BinaryOp {
                    op: BinaryOperator::Match,
                    ..
                } => ControlFlow::Break(()),
                _ => ControlFlow::Continue(()),
            }
        }
    }
    stmt.visit(&mut Find).is_break()
}

/// The `(space, table)` each search call in a classified statement reads.
pub fn search_tables(stmt: &Statement) -> Vec<(String, String)> {
    struct Collect(Vec<(String, String)>);
    impl Visitor for Collect {
        type Break = ();
        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
            if let TableFactor::Table {
                name,
                args: Some(args),
                ..
            } = factor
            {
                if is_search_function(name) {
                    if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        Expr::CompoundIdentifier(parts),
                    ))) = args.args.first()
                    {
                        if let [space, table] = parts.as_slice() {
                            self.0.push((space.value.clone(), table.value.clone()));
                        }
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut collect = Collect(Vec::new());
    let _ = stmt.visit(&mut collect);
    collect.0
}

/// Replace the table argument of every search call with the dataset path
/// `path(space, table)` returns, as a string literal.
pub fn bind_search_tables<E>(
    stmt: &mut Statement,
    path: impl FnMut(&str, &str) -> Result<String, E>,
) -> Result<(), E> {
    struct Bind<F>(F);
    impl<E, F: FnMut(&str, &str) -> Result<String, E>> VisitorMut for Bind<F> {
        type Break = E;
        fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<E> {
            let Some((_, arg)) = search_arg(factor) else {
                return ControlFlow::Continue(());
            };
            if let Some(expr) = table_expr(arg) {
                if let Expr::CompoundIdentifier(parts) = expr {
                    if let [space, table] = parts.as_slice() {
                        match (self.0)(&space.value, &table.value) {
                            Ok(path) => {
                                *expr = Expr::Value(Value::SingleQuotedString(path).into());
                            }
                            Err(e) => return ControlFlow::Break(e),
                        }
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }
    match stmt.visit(&mut Bind(path)) {
        ControlFlow::Break(e) => Err(e),
        ControlFlow::Continue(()) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{classify, parse_single, OikomeStatement, StatementClass};

    fn stmt(sql: &str) -> Statement {
        match parse_single(sql).unwrap() {
            OikomeStatement::Sql(stmt) => *stmt,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn search_calls_name_their_table_and_its_space() {
        let mut s = stmt(
            "SELECT id, _distance FROM lance_vector_search(items, 'embedding', [1, 2]::FLOAT[2], k => 3) \
             WHERE id > 1",
        );
        let class = classify(&mut s, Some("notes")).unwrap();
        assert_eq!(
            class,
            StatementClass::Query {
                spaces: ["notes".to_string()].into()
            }
        );
        assert_eq!(
            search_tables(&s),
            vec![("notes".to_string(), "items".to_string())]
        );
        bind_search_tables::<()>(&mut s, |space, table| {
            Ok(format!("/data/{space}/{table}.lance"))
        })
        .unwrap();
        assert_eq!(
            s.to_string(),
            "SELECT id, _distance FROM lance_vector_search('/data/notes/items.lance', 'embedding', \
             [1, 2]::FLOAT[2], k => 3) WHERE id > 1"
        );
    }

    #[test]
    fn a_search_call_needs_a_table_reference() {
        for sql in [
            "SELECT * FROM lance_fts('notes.items', 'body', 'apple')",
            "SELECT * FROM lance_fts(a.b.c, 'body', 'apple')",
        ] {
            let err = classify(&mut stmt(sql), Some("notes")).unwrap_err();
            assert!(err.to_string().contains("names a table"), "{sql}: {err}");
        }
        let err = classify(
            &mut stmt("SELECT * FROM lance_fts(items, 'body', 'x')"),
            None,
        )
        .unwrap_err();
        assert!(matches!(err, SemanticError::UnqualifiedTable(_)), "{err}");
    }
}
