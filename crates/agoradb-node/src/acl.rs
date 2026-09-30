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

//! Views, grants and row policies: catalog-backed resolvers for the semantic
//! rewrites, and execution of `CREATE/DROP VIEW`, `GRANT`, `REVOKE` and
//! `CREATE/DROP POLICY`.

use agoradb_catalog::{AgoraCatalog, Grant, RowPolicy, ViewDef};
use agoradb_core::CatalogError;
use agoradb_semantic::rewrite::{guarded_query, quote_ident, split_qualified};
use agoradb_semantic::{
    collect_spaces, inline_views, parse_single, AccessResolver, AgoraStatement, RelationAccess,
    ViewResolver,
};
use arrow_array::RecordBatch;
use futures::TryStreamExt;
use sqlparser::ast::{
    Action, CreatePolicyCommand, CreatePolicyType, GrantObjects, Grantee, GranteeName,
    GranteesType, ObjectName, ObjectNamePart, ObjectType, Owner, Privileges, Statement,
};

use crate::error::SessionError;
use crate::result::QueryResult;
use crate::session::{AgoraSession, Routed};

/// Views stored in the catalog.
pub(crate) struct CatalogViews<'a>(pub(crate) &'a AgoraCatalog);

impl ViewResolver for CatalogViews<'_> {
    fn view_query(&self, space: &str, name: &str) -> Option<String> {
        self.0.get_view(space, name).map(|v| v.query)
    }
}

/// The catalog's views with one definition replaced, to check a
/// `CREATE [OR REPLACE] VIEW` before it is stored.
struct ProposedView<'a> {
    base: CatalogViews<'a>,
    space: &'a str,
    name: &'a str,
    query: &'a str,
}

impl ViewResolver for ProposedView<'_> {
    fn view_query(&self, space: &str, name: &str) -> Option<String> {
        if space == self.space && name == self.name {
            Some(self.query.to_string())
        } else {
            self.base.view_query(space, name)
        }
    }
}

/// Grants and row policies stored in the catalog.
pub(crate) struct CatalogAccess<'a>(pub(crate) &'a AgoraCatalog);

impl AccessResolver for CatalogAccess<'_> {
    fn access(&self, principal: &str, space: &str, relation: &str) -> Option<RelationAccess> {
        let grant = self.0.get_grant(principal, space, relation)?;
        let policies = self.0.policies_on(space, relation);
        let row_filters = if policies.is_empty() {
            None
        } else {
            Some(
                policies
                    .into_iter()
                    .filter(|p| p.principals.iter().any(|q| q == principal))
                    .map(|p| p.predicate)
                    .collect(),
            )
        };
        Some(RelationAccess {
            columns: grant.columns,
            row_filters,
        })
    }
}

fn unsupported(msg: impl Into<String>) -> SessionError {
    SessionError::Unsupported(msg.into())
}

/// Columns of a `SELECT [(cols)]` privilege list; anything else is rejected.
fn select_columns(privileges: &Privileges) -> Result<Option<Vec<String>>, SessionError> {
    match privileges {
        Privileges::Actions(actions) => match actions.as_slice() {
            [Action::Select { columns }] => Ok(columns
                .as_ref()
                .map(|cols| cols.iter().map(|c| c.value.clone()).collect())),
            _ => Err(unsupported("only SELECT can be granted or revoked")),
        },
        Privileges::All { .. } => Err(unsupported(
            "GRANT ALL; grant SELECT (principals are read-only)",
        )),
    }
}

fn grant_targets(objects: &Option<GrantObjects>) -> Result<&[ObjectName], SessionError> {
    match objects {
        Some(GrantObjects::Tables(names)) => Ok(names),
        _ => Err(unsupported("only ON <space>.<table or view> is supported")),
    }
}

fn principal_names(grantees: &[Grantee]) -> Result<Vec<String>, SessionError> {
    grantees
        .iter()
        .map(|g| match (&g.grantee_type, &g.name) {
            (
                GranteesType::None | GranteesType::User | GranteesType::Role,
                Some(GranteeName::ObjectName(name)),
            ) => match name.0.as_slice() {
                [ObjectNamePart::Identifier(ident)] => Ok(ident.value.clone()),
                _ => Err(unsupported(format!("grantee {name}"))),
            },
            _ => Err(unsupported(format!("grantee {g}"))),
        })
        .collect()
}

impl AgoraSession {
    /// Run a read-only check query with the owner's rights and drain it.
    async fn check_query(&self, sql: &str, views: &dyn ViewResolver) -> Result<(), SessionError> {
        let AgoraStatement::Sql(stmt) = parse_single(sql)? else {
            return Err(unsupported("internal check must be SQL"));
        };
        let mut stmt = *stmt;
        inline_views(&mut stmt, views)?;
        let spaces = collect_spaces(&stmt);
        let (_, stream) = self.run_query(&spaces, &stmt.to_string()).await?;
        let _: Vec<RecordBatch> = stream.try_collect().await?;
        Ok(())
    }

    /// Whether `space.name` is a table (not a view).
    async fn table_exists(&self, space: &str, name: &str) -> Result<bool, SessionError> {
        let space = self.space(space)?;
        let engine = self.bind(&space).await?;
        Ok(engine
            .table_names(&space.name)
            .await?
            .iter()
            .any(|t| t == name))
    }

    /// Fail with "not found" unless `space.relation` is a table or a view.
    async fn require_relation(&self, space: &str, relation: &str) -> Result<(), SessionError> {
        if self.catalog.get_view(space, relation).is_some()
            || self.table_exists(space, relation).await?
        {
            Ok(())
        } else {
            Err(CatalogError::TableNotFound(format!("{space}.{relation}")).into())
        }
    }

    pub(crate) async fn run_view_ddl(&self, routed: Routed) -> Result<QueryResult, SessionError> {
        match routed.stmt {
            Statement::CreateView(create) => {
                if create.materialized {
                    return Err(unsupported("materialized views"));
                }
                if !create.columns.is_empty() {
                    return Err(unsupported(
                        "view column lists; alias the columns inside the query",
                    ));
                }
                let (space, name) = split_qualified(&create.name)?;
                self.space(&space)?;
                if self.catalog.get_view(&space, &name).is_some() && create.if_not_exists {
                    return Ok(QueryResult::Empty);
                }
                if self.table_exists(&space, &name).await? {
                    return Err(SessionError::AlreadyExists(format!(
                        "a table named {space}.{name} already exists"
                    )));
                }
                let query = create.query.to_string();
                // Expand the view as it would be stored: catches cycles and
                // references to missing tables or columns before storing it.
                let proposed = ProposedView {
                    base: CatalogViews(&self.catalog),
                    space: &space,
                    name: &name,
                    query: &query,
                };
                let probe = format!(
                    "SELECT * FROM {}.{} LIMIT 0",
                    quote_ident(&space),
                    quote_ident(&name)
                );
                self.check_query(&probe, &proposed).await?;
                self.catalog
                    .create_view(ViewDef::new(space, name, query), create.or_replace)?;
            }
            Statement::Drop {
                object_type: ObjectType::View,
                names,
                if_exists,
                ..
            } => {
                for name in &names {
                    let (space, view) = split_qualified(name)?;
                    match self.catalog.drop_view(&space, &view) {
                        Err(CatalogError::ViewNotFound(_)) if if_exists => {}
                        other => other?,
                    }
                }
            }
            other => return Err(unsupported(other.to_string())),
        }
        Ok(QueryResult::Empty)
    }

    pub(crate) async fn run_acl(&self, routed: Routed) -> Result<QueryResult, SessionError> {
        match routed.stmt {
            Statement::Grant(grant) => {
                if grant.with_grant_option {
                    return Err(unsupported("WITH GRANT OPTION"));
                }
                let columns = select_columns(&grant.privileges)?;
                let principals = principal_names(&grant.grantees)?;
                for target in grant_targets(&grant.objects)? {
                    let (space, relation) = split_qualified(target)?;
                    self.require_relation(&space, &relation).await?;
                    let projection = columns
                        .as_ref()
                        .map(|cols| {
                            cols.iter()
                                .map(|c| quote_ident(c))
                                .collect::<Vec<_>>()
                                .join(", ")
                        })
                        .unwrap_or_else(|| "*".to_string());
                    self.check_query(
                        &format!(
                            "SELECT {projection} FROM {}.{} LIMIT 0",
                            quote_ident(&space),
                            quote_ident(&relation)
                        ),
                        &CatalogViews(&self.catalog),
                    )
                    .await?;
                    for principal in &principals {
                        self.catalog.grant_select(Grant {
                            principal: principal.clone(),
                            space: space.clone(),
                            relation: relation.clone(),
                            columns: columns.clone(),
                        })?;
                    }
                }
            }
            Statement::Revoke(revoke) => {
                if select_columns(&revoke.privileges)?.is_some() {
                    return Err(unsupported(
                        "column-level REVOKE; revoke SELECT and grant the remaining columns",
                    ));
                }
                let principals = principal_names(&revoke.grantees)?;
                for target in grant_targets(&revoke.objects)? {
                    let (space, relation) = split_qualified(target)?;
                    for principal in &principals {
                        self.catalog.revoke_select(principal, &space, &relation)?;
                    }
                }
            }
            Statement::CreatePolicy(policy) => {
                match policy.command {
                    None | Some(CreatePolicyCommand::All) | Some(CreatePolicyCommand::Select) => {}
                    Some(other) => {
                        return Err(unsupported(format!(
                            "CREATE POLICY ... FOR {other}; principals are read-only"
                        )))
                    }
                }
                if matches!(policy.policy_type, Some(CreatePolicyType::Restrictive)) {
                    return Err(unsupported("RESTRICTIVE policies"));
                }
                if policy.with_check.is_some() {
                    return Err(unsupported("WITH CHECK; principals are read-only"));
                }
                let principals = policy
                    .to
                    .ok_or_else(|| unsupported("CREATE POLICY needs TO <principal>"))?
                    .into_iter()
                    .map(|owner| match owner {
                        Owner::Ident(ident) => Ok(ident.value),
                        other => Err(unsupported(format!("policy role {other}"))),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let predicate = policy
                    .using
                    .ok_or_else(|| unsupported("CREATE POLICY needs USING (<predicate>)"))?
                    .to_string();
                let (space, relation) = split_qualified(&policy.table_name)?;
                self.require_relation(&space, &relation).await?;
                let guarded = guarded_query(
                    &space,
                    &relation,
                    &RelationAccess {
                        columns: None,
                        row_filters: Some(vec![predicate.clone()]),
                    },
                    "",
                )?;
                self.check_query(
                    &format!("SELECT * FROM ({guarded}) AS agora_policy_check LIMIT 0"),
                    &CatalogViews(&self.catalog),
                )
                .await?;
                self.catalog.create_policy(RowPolicy {
                    name: policy.name.value,
                    space,
                    relation,
                    principals,
                    predicate,
                })?;
            }
            Statement::DropPolicy(drop) => {
                let (space, relation) = split_qualified(&drop.table_name)?;
                match self
                    .catalog
                    .drop_policy(&space, &relation, &drop.name.value)
                {
                    Err(CatalogError::PolicyNotFound(_)) if drop.if_exists => {}
                    other => other?,
                }
            }
            other => return Err(unsupported(other.to_string())),
        }
        Ok(QueryResult::Empty)
    }
}
