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

//! Semantic-layer metadata: views, SELECT grants and row policies
//! (architecture v3 §6). Persisted next to the Space registry.

use agoradb_core::CatalogError;
use serde::{Deserialize, Serialize};

use crate::catalog::AgoraCatalog;
use crate::registry::now_ms;

/// A named query stored in a Space; inlined wherever it is referenced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewDef {
    pub space: String,
    pub name: String,
    /// The view body, with every table reference qualified as `<space>.<table>`.
    pub query: String,
    pub created_at_ms: u64,
}

impl ViewDef {
    /// Create a view definition timestamped now.
    pub fn new(
        space: impl Into<String>,
        name: impl Into<String>,
        query: impl Into<String>,
    ) -> Self {
        Self {
            space: space.into(),
            name: name.into(),
            query: query.into(),
            created_at_ms: now_ms(),
        }
    }

    pub(crate) fn key(&self) -> (String, String) {
        (self.space.clone(), self.name.clone())
    }
}

/// `GRANT SELECT [(columns)] ON <space>.<relation> TO <principal>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub principal: String,
    pub space: String,
    /// A table or a view.
    pub relation: String,
    /// Granted columns; `None` grants every column.
    pub columns: Option<Vec<String>>,
}

impl Grant {
    pub(crate) fn key(&self) -> (String, String, String) {
        (
            self.principal.clone(),
            self.space.clone(),
            self.relation.clone(),
        )
    }
}

/// `CREATE POLICY <name> ON <space>.<relation> FOR SELECT TO <principals> USING (<predicate>)`.
///
/// Once any policy exists on a relation, a principal only sees the rows
/// matching at least one of the policies that name it (permissive policies
/// combine with `OR`, as in PostgreSQL); a principal named by none sees no rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowPolicy {
    pub name: String,
    pub space: String,
    pub relation: String,
    pub principals: Vec<String>,
    /// SQL boolean expression over the relation's columns; may use `current_user`.
    pub predicate: String,
}

impl RowPolicy {
    pub(crate) fn key(&self) -> (String, String, String) {
        (self.space.clone(), self.relation.clone(), self.name.clone())
    }
}

impl AgoraCatalog {
    fn require_space(&self, space: &str) -> Result<(), CatalogError> {
        self.get_space(space).map(|_| ())
    }

    /// Store a view. With `or_replace`, an existing view of that name is replaced.
    pub fn create_view(&self, view: ViewDef, or_replace: bool) -> Result<(), CatalogError> {
        self.require_space(&view.space)?;
        self.mutate_registry(|reg| {
            let key = view.key();
            if !or_replace && reg.views.contains_key(&key) {
                return Err(CatalogError::ViewExists(format!("{}.{}", key.0, key.1)));
            }
            reg.views.insert(key, view);
            Ok(())
        })
    }

    /// Look up a view.
    pub fn get_view(&self, space: &str, name: &str) -> Option<ViewDef> {
        self.with_registry(|reg| {
            reg.views
                .get(&(space.to_string(), name.to_string()))
                .cloned()
        })
    }

    /// Views of a Space, ordered by name.
    pub fn list_views(&self, space: &str) -> Vec<ViewDef> {
        self.with_registry(|reg| {
            reg.views
                .values()
                .filter(|v| v.space == space)
                .cloned()
                .collect()
        })
    }

    /// Remove a view, and every grant and policy on it.
    pub fn drop_view(&self, space: &str, name: &str) -> Result<(), CatalogError> {
        self.mutate_registry(|reg| {
            reg.views
                .remove(&(space.to_string(), name.to_string()))
                .ok_or_else(|| CatalogError::ViewNotFound(format!("{space}.{name}")))?;
            reg.grants.retain(|(_, s, r), _| !(s == space && r == name));
            reg.policies
                .retain(|(s, r, _), _| !(s == space && r == name));
            Ok(())
        })
    }

    /// Record a SELECT grant, replacing any previous grant for the same
    /// principal and relation.
    pub fn grant_select(&self, grant: Grant) -> Result<(), CatalogError> {
        self.require_space(&grant.space)?;
        self.mutate_registry(|reg| {
            reg.grants.insert(grant.key(), grant);
            Ok(())
        })
    }

    /// Remove a SELECT grant; returns whether one existed.
    pub fn revoke_select(
        &self,
        principal: &str,
        space: &str,
        relation: &str,
    ) -> Result<bool, CatalogError> {
        self.mutate_registry(|reg| {
            Ok(reg
                .grants
                .remove(&(
                    principal.to_string(),
                    space.to_string(),
                    relation.to_string(),
                ))
                .is_some())
        })
    }

    /// The grant `principal` holds on `space.relation`, if any.
    pub fn get_grant(&self, principal: &str, space: &str, relation: &str) -> Option<Grant> {
        self.with_registry(|reg| {
            reg.grants
                .get(&(
                    principal.to_string(),
                    space.to_string(),
                    relation.to_string(),
                ))
                .cloned()
        })
    }

    /// Whether `principal` holds any grant in `space`.
    pub fn has_grants_in(&self, principal: &str, space: &str) -> bool {
        self.with_registry(|reg| {
            reg.grants
                .keys()
                .any(|(p, s, _)| p == principal && s == space)
        })
    }

    /// Create a row policy; names are unique per relation.
    pub fn create_policy(&self, policy: RowPolicy) -> Result<(), CatalogError> {
        self.require_space(&policy.space)?;
        self.mutate_registry(|reg| {
            let key = policy.key();
            if reg.policies.contains_key(&key) {
                return Err(CatalogError::PolicyExists(format!(
                    "{} on {}.{}",
                    key.2, key.0, key.1
                )));
            }
            reg.policies.insert(key, policy);
            Ok(())
        })
    }

    /// Remove a row policy.
    pub fn drop_policy(&self, space: &str, relation: &str, name: &str) -> Result<(), CatalogError> {
        self.mutate_registry(|reg| {
            reg.policies
                .remove(&(space.to_string(), relation.to_string(), name.to_string()))
                .map(|_| ())
                .ok_or_else(|| {
                    CatalogError::PolicyNotFound(format!("{name} on {space}.{relation}"))
                })
        })
    }

    /// Every row policy on `space.relation`.
    pub fn policies_on(&self, space: &str, relation: &str) -> Vec<RowPolicy> {
        self.with_registry(|reg| {
            reg.policies
                .values()
                .filter(|p| p.space == space && p.relation == relation)
                .cloned()
                .collect()
        })
    }
}
