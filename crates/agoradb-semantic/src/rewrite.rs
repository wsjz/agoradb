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

//! SQL → SQL rewrites of the semantic layer (architecture v3 §6).
//!
//! Both rewrites replace a relation reference `<space>.<rel> [AS a]` with a
//! derived table `(<query>) AS a`, so they compose and everything they add
//! ends up *inside* the SQL an engine executes:
//!
//! * [`apply_access_control`] wraps every relation a principal references in
//!   `(SELECT <granted columns> FROM <space>.<rel> WHERE <row policies>)`.
//!   `SELECT *` therefore expands to granted columns only, row filters run in
//!   the engine, and an ungranted relation is reported exactly like a missing
//!   one.
//! * [`inline_views`] replaces view references with the view body. It runs
//!   after access control, so view bodies read their base tables with the
//!   view owner's rights (PostgreSQL's default): granting a view does not
//!   expose the tables behind it.

use std::collections::HashSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    visit_expressions_mut, Expr, Ident, ObjectName, Query, Statement, TableAlias, TableFactor,
    Value, VisitMut, VisitorMut,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::classify::{cte_names, is_cte_ref, part_value};
use crate::error::SemanticError;

/// Maximum nesting depth of views referencing views.
pub const MAX_VIEW_DEPTH: usize = 16;

/// Looks up view bodies.
pub trait ViewResolver {
    /// The fully qualified body of view `space.name`, if it is a view.
    fn view_query(&self, space: &str, name: &str) -> Option<String>;
}

/// What a principal may read of one relation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationAccess {
    /// Granted columns; `None` = every column.
    pub columns: Option<Vec<String>>,
    /// Row filters combined with `OR`; `None` = every row, `Some(vec![])` = no row.
    pub row_filters: Option<Vec<String>>,
}

/// Decides what a principal may read.
pub trait AccessResolver {
    /// `None` when `principal` may not read `space.relation` (or it does not exist).
    fn access(&self, principal: &str, space: &str, relation: &str) -> Option<RelationAccess>;
}

/// Parse a standalone query.
pub fn parse_query(sql: &str) -> Result<Box<Query>, SemanticError> {
    Parser::new(&GenericDialect {})
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_query())
        .map_err(|e| SemanticError::Parse(e.to_string()))
}

/// Parse a standalone expression.
pub fn parse_expr(sql: &str) -> Result<Expr, SemanticError> {
    Parser::new(&GenericDialect {})
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_expr())
        .map_err(|e| SemanticError::Parse(e.to_string()))
}

/// Double-quote an identifier for generated SQL.
pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// A relation reference that was replaced by a derived table.
struct Replaced {
    space: String,
    relation: String,
    alias: String,
}

/// Replace every two-part relation reference for which `f` returns a query
/// with `(<query>) AS <alias or relation name>`.
fn replace_relations<F>(stmt: &mut Statement, mut f: F) -> Result<Vec<Replaced>, SemanticError>
where
    F: FnMut(&str, &str) -> Result<Option<Box<Query>>, SemanticError>,
{
    struct Replacer<'a, F> {
        ctes: HashSet<String>,
        f: &'a mut F,
        replaced: Vec<Replaced>,
        error: Option<SemanticError>,
    }

    impl<F> VisitorMut for Replacer<'_, F>
    where
        F: FnMut(&str, &str) -> Result<Option<Box<Query>>, SemanticError>,
    {
        type Break = ();

        // Post-order: a derived table inserted here is not visited again in
        // this pass, so a pass never rewrites its own output.
        fn post_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
            let TableFactor::Table {
                name, alias, args, ..
            } = factor
            else {
                return ControlFlow::Continue(());
            };
            if name.0.len() != 2 || is_cte_ref(name, &self.ctes) {
                return ControlFlow::Continue(());
            }
            let space = part_value(&name.0[0]);
            let relation = part_value(&name.0[1]);
            match (self.f)(&space, &relation) {
                Ok(None) => ControlFlow::Continue(()),
                Ok(Some(subquery)) => {
                    if args.is_some() {
                        self.error = Some(SemanticError::Unsupported(format!(
                            "table function syntax on {space}.{relation}"
                        )));
                        return ControlFlow::Break(());
                    }
                    let alias = alias.clone().unwrap_or(TableAlias {
                        explicit: true,
                        name: Ident::new(relation.clone()),
                        columns: vec![],
                        at: None,
                    });
                    self.replaced.push(Replaced {
                        space,
                        relation,
                        alias: alias.name.value.clone(),
                    });
                    *factor = TableFactor::Derived {
                        lateral: false,
                        subquery,
                        alias: Some(alias),
                        sample: None,
                    };
                    ControlFlow::Continue(())
                }
                Err(e) => {
                    self.error = Some(e);
                    ControlFlow::Break(())
                }
            }
        }
    }

    let mut replacer = Replacer {
        ctes: cte_names(stmt),
        f: &mut f,
        replaced: Vec::new(),
        error: None,
    };
    let _ = stmt.visit(&mut replacer);
    if let Some(e) = replacer.error {
        return Err(e);
    }
    let replaced = replacer.replaced;
    requalify_columns(stmt, &replaced);
    Ok(replaced)
}

/// `space.rel.col` no longer resolves once `space.rel` became `(…) AS alias`;
/// rewrite such column references to `alias.col`.
fn requalify_columns(stmt: &mut Statement, replaced: &[Replaced]) {
    if replaced.is_empty() {
        return;
    }
    let _ = visit_expressions_mut(stmt, |expr: &mut Expr| {
        if let Expr::CompoundIdentifier(parts) = expr {
            if parts.len() >= 3 {
                if let Some(r) = replaced
                    .iter()
                    .find(|r| parts[0].value == r.space && parts[1].value == r.relation)
                {
                    parts.splice(0..2, [Ident::new(r.alias.clone())]);
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Replace `current_user` with the principal as a string literal.
fn substitute_current_user(query: &mut Query, principal: &str) {
    let _ = visit_expressions_mut(query, |expr: &mut Expr| {
        let is_current_user = match expr {
            Expr::Function(f) => {
                f.name.0.len() == 1 && part_value(&f.name.0[0]).eq_ignore_ascii_case("current_user")
            }
            Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("current_user"),
            _ => false,
        };
        if is_current_user {
            *expr = Expr::value(Value::SingleQuotedString(principal.to_string()));
        }
        ControlFlow::<()>::Continue(())
    });
}

/// The guarded query a principal reads instead of `space.relation`.
pub fn guarded_query(
    space: &str,
    relation: &str,
    access: &RelationAccess,
    principal: &str,
) -> Result<Box<Query>, SemanticError> {
    let columns = match &access.columns {
        None => "*".to_string(),
        Some(cols) if cols.is_empty() => {
            return Err(SemanticError::TableNotFound(format!("{space}.{relation}")))
        }
        Some(cols) => cols
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", "),
    };
    let mut sql = format!(
        "SELECT {columns} FROM {}.{}",
        quote_ident(space),
        quote_ident(relation)
    );
    match &access.row_filters {
        None => {}
        Some(filters) if filters.is_empty() => sql.push_str(" WHERE FALSE"),
        Some(filters) => {
            let combined = filters
                .iter()
                .map(|f| format!("({f})"))
                .collect::<Vec<_>>()
                .join(" OR ");
            sql.push_str(" WHERE ");
            sql.push_str(&combined);
        }
    }
    let mut query = parse_query(&sql)?;
    substitute_current_user(&mut query, principal);
    Ok(query)
}

/// Enforce `principal`'s grants and row policies on every relation `stmt`
/// references directly. Relations without a grant are reported as not found.
pub fn apply_access_control(
    stmt: &mut Statement,
    principal: &str,
    resolver: &dyn AccessResolver,
) -> Result<(), SemanticError> {
    replace_relations(stmt, |space, relation| {
        let access = resolver
            .access(principal, space, relation)
            .ok_or_else(|| SemanticError::TableNotFound(format!("{space}.{relation}")))?;
        guarded_query(space, relation, &access, principal).map(Some)
    })
    .map(|_| ())
}

/// Replace view references with their bodies until none remain; returns the
/// number of references inlined.
pub fn inline_views(
    stmt: &mut Statement,
    resolver: &dyn ViewResolver,
) -> Result<usize, SemanticError> {
    let mut total = 0;
    for _ in 0..MAX_VIEW_DEPTH {
        let replaced = replace_relations(stmt, |space, name| {
            resolver
                .view_query(space, name)
                .map(|sql| parse_query(&sql))
                .transpose()
        })?;
        if replaced.is_empty() {
            return Ok(total);
        }
        total += replaced.len();
    }
    Err(SemanticError::ViewRecursion(
        MAX_VIEW_DEPTH,
        stmt.to_string().chars().take(200).collect(),
    ))
}

/// Parse `name` as `<space>.<relation>`.
pub fn split_qualified(name: &ObjectName) -> Result<(String, String), SemanticError> {
    match name.0.as_slice() {
        [space, relation] => Ok((part_value(space), part_value(relation))),
        _ => Err(SemanticError::InvalidTableReference(name.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classify::collect_spaces;
    use crate::sql_parser::{parse_single, AgoraStatement};
    use std::collections::{BTreeSet, HashMap};

    fn stmt(sql: &str) -> Statement {
        match parse_single(sql).unwrap() {
            AgoraStatement::Sql(s) => *s,
            other => panic!("{other:?}"),
        }
    }

    #[derive(Default)]
    struct Views(HashMap<(String, String), String>);
    impl Views {
        fn with(mut self, space: &str, name: &str, sql: &str) -> Self {
            self.0.insert((space.into(), name.into()), sql.into());
            self
        }
    }
    impl ViewResolver for Views {
        fn view_query(&self, space: &str, name: &str) -> Option<String> {
            self.0.get(&(space.to_string(), name.to_string())).cloned()
        }
    }

    #[derive(Default)]
    struct Access(HashMap<(String, String, String), RelationAccess>);
    impl Access {
        fn with(mut self, p: &str, space: &str, rel: &str, access: RelationAccess) -> Self {
            self.0.insert((p.into(), space.into(), rel.into()), access);
            self
        }
    }
    impl AccessResolver for Access {
        fn access(&self, principal: &str, space: &str, relation: &str) -> Option<RelationAccess> {
            self.0
                .get(&(principal.into(), space.into(), relation.into()))
                .cloned()
        }
    }

    fn spaces(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn view_is_inlined_with_its_name_as_alias() {
        let views = Views::default().with(
            "blog",
            "recent",
            "SELECT id, title FROM blog.posts WHERE id > 1",
        );
        let mut s = stmt("SELECT recent.title FROM blog.recent");
        assert_eq!(inline_views(&mut s, &views).unwrap(), 1);
        assert_eq!(
            s.to_string(),
            "SELECT recent.title FROM (SELECT id, title FROM blog.posts WHERE id > 1) AS recent"
        );
        assert_eq!(collect_spaces(&s), spaces(&["blog"]));
    }

    #[test]
    fn nested_views_spanning_spaces_are_inlined() {
        let views = Views::default()
            .with("sem", "buyers", "SELECT customer FROM orders.orders")
            .with("sem", "active", "SELECT b.customer, p.title FROM sem.buyers AS b JOIN blog.posts AS p ON p.author = b.customer");
        let mut s = stmt("SELECT * FROM sem.active AS a WHERE a.customer <> 'x'");
        assert_eq!(inline_views(&mut s, &views).unwrap(), 2);
        assert_eq!(collect_spaces(&s), spaces(&["blog", "orders"]));
        assert!(!s.to_string().contains("sem."), "{s}");
    }

    #[test]
    fn recursive_view_is_rejected() {
        let views = Views::default().with("s", "v", "SELECT * FROM s.v");
        let mut s = stmt("SELECT * FROM s.v");
        assert!(matches!(
            inline_views(&mut s, &views),
            Err(SemanticError::ViewRecursion(MAX_VIEW_DEPTH, _))
        ));
    }

    #[test]
    fn three_part_column_references_follow_the_alias() {
        let views = Views::default().with("blog", "recent", "SELECT id FROM blog.posts");
        let mut s = stmt("SELECT blog.recent.id FROM blog.recent");
        inline_views(&mut s, &views).unwrap();
        assert_eq!(
            s.to_string(),
            "SELECT recent.id FROM (SELECT id FROM blog.posts) AS recent"
        );
    }

    #[test]
    fn column_grant_and_row_policies_wrap_the_table() {
        let access = Access::default().with(
            "alice",
            "orders",
            "orders",
            RelationAccess {
                columns: Some(vec!["id".into(), "customer".into()]),
                row_filters: Some(vec!["customer = current_user".into(), "id < 3".into()]),
            },
        );
        let mut s = stmt("SELECT * FROM orders.orders AS o");
        apply_access_control(&mut s, "alice", &access).unwrap();
        assert_eq!(
            s.to_string(),
            "SELECT * FROM (SELECT \"id\", \"customer\" FROM \"orders\".\"orders\" \
             WHERE (customer = 'alice') OR (id < 3)) AS o"
        );
    }

    #[test]
    fn ungranted_table_is_not_found_and_empty_policy_set_hides_all_rows() {
        let access = Access::default().with(
            "bob",
            "orders",
            "orders",
            RelationAccess {
                columns: None,
                row_filters: Some(vec![]),
            },
        );
        let mut s = stmt("SELECT * FROM orders.orders JOIN blog.posts ON true");
        assert_eq!(
            apply_access_control(&mut s, "bob", &access),
            Err(SemanticError::TableNotFound("blog.posts".into()))
        );
        let mut s = stmt("SELECT count(*) FROM orders.orders");
        apply_access_control(&mut s, "bob", &access).unwrap();
        assert!(s.to_string().to_uppercase().contains("WHERE FALSE"), "{s}");
    }

    #[test]
    fn view_body_runs_with_owner_rights() {
        // alice may read the view, not the table behind it.
        let access = Access::default().with("alice", "blog", "public", RelationAccess::default());
        let views = Views::default().with("blog", "public", "SELECT id, title FROM blog.posts");
        let mut s = stmt("SELECT * FROM blog.public");
        apply_access_control(&mut s, "alice", &access).unwrap();
        inline_views(&mut s, &views).unwrap();
        assert_eq!(
            s.to_string(),
            "SELECT * FROM (SELECT * FROM (SELECT id, title FROM blog.posts) AS public) AS public"
        );

        let mut direct = stmt("SELECT * FROM blog.posts");
        assert!(matches!(
            apply_access_control(&mut direct, "alice", &access),
            Err(SemanticError::TableNotFound(_))
        ));
    }

    #[test]
    fn cte_references_are_left_alone() {
        let access = Access::default().with("alice", "blog", "posts", RelationAccess::default());
        let mut s = stmt("WITH p AS (SELECT id FROM blog.posts) SELECT * FROM p");
        apply_access_control(&mut s, "alice", &access).unwrap();
        assert_eq!(
            s.to_string(),
            "WITH p AS (SELECT id FROM (SELECT * FROM \"blog\".\"posts\") AS posts) SELECT * FROM p"
        );
        assert_eq!(collect_spaces(&s), spaces(&["blog"]));
    }

    #[test]
    fn split_qualified_requires_two_parts() {
        let s = stmt("SELECT 1 FROM a.b");
        let Statement::Query(q) = s else {
            unreachable!()
        };
        let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
            unreachable!()
        };
        let TableFactor::Table { name, .. } = &sel.from[0].relation else {
            unreachable!()
        };
        assert_eq!(split_qualified(name).unwrap(), ("a".into(), "b".into()));
    }
}
