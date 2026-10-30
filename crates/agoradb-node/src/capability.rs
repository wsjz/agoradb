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

//! Identity and capability statements, and remote Spaces (architecture v3 §9):
//! `SHOW IDENTITY`, `ISSUE UCAN`, `REVOKE UCAN`, `SHOW UCANS` and
//! `CREATE SPACE ... WITH PEER = ..., TOKEN = ...`.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use agoradb_catalog::{validate_peer_url, HeldToken, IssuedUcan, UcanCapability};
use agoradb_core::{CatalogError, CreateSpaceRequest, SpaceKind};
use agoradb_identity::ucan::{
    self, Capability, Ucan, UcanBuilder, VerifyOptions, ABILITY_ALL, ABILITY_QUERY, ABILITY_READ,
};
use agoradb_identity::{unix_now, unix_now_ms, Did};
use agoradb_semantic::{classify, parse_single, AgoraStatement, IssueUcanRequest, StatementClass};
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use crate::error::SessionError;
use crate::result::QueryResult;
use crate::session::AgoraSession;

pub(crate) fn invalid(msg: impl Into<String>) -> SessionError {
    CatalogError::InvalidProperty(msg.into()).into()
}

fn batch(fields: Vec<Field>, columns: Vec<ArrayRef>) -> Result<QueryResult, SessionError> {
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    Ok(QueryResult::Batches {
        schema,
        batches: vec![batch],
    })
}

/// The Spaces a query names before views are expanded: those a peer must
/// hold a capability for. Anything but a query is refused, as is a bare
/// table name (peers always qualify).
pub fn query_spaces(sql: &str) -> Result<BTreeSet<String>, SessionError> {
    let AgoraStatement::Sql(mut stmt) = parse_single(sql)? else {
        return Err(SessionError::PermissionDenied(
            "peers may only run queries".to_string(),
        ));
    };
    match classify(&mut stmt, None)? {
        StatementClass::Query { spaces } => Ok(spaces),
        _ => Err(SessionError::PermissionDenied(
            "peers may only run queries".to_string(),
        )),
    }
}

impl AgoraSession {
    /// `SHOW IDENTITY`
    pub(crate) fn show_identity(&self) -> Result<QueryResult, SessionError> {
        let identity = self.engines().identity()?;
        batch(
            vec![Field::new("did", DataType::Utf8, false)],
            vec![Arc::new(StringArray::from(vec![identity
                .did()
                .to_string()]))],
        )
    }

    /// `ISSUE UCAN`: sign a capability for a peer and return it. Only the
    /// token's id and claims are recorded; the token is shown once.
    pub(crate) fn issue_ucan(
        &self,
        request: &IssueUcanRequest,
    ) -> Result<QueryResult, SessionError> {
        let identity = self.engines().identity()?;
        let audience = Did::parse(&request.audience)?;
        if ![ABILITY_QUERY, ABILITY_READ, ABILITY_ALL].contains(&request.ability.as_str()) {
            return Err(invalid(format!(
                "unknown ability '{}' (expected '{ABILITY_QUERY}', '{ABILITY_READ}' or '{ABILITY_ALL}')",
                request.ability
            )));
        }
        let mut capabilities = Vec::with_capacity(request.spaces.len());
        for name in &request.spaces {
            if name != "*" {
                let space = self.space(name)?;
                if space.kind == SpaceKind::Remote {
                    return Err(SessionError::Unsupported(format!(
                        "space '{name}' belongs to another node; only its owner can grant it"
                    )));
                }
            }
            capabilities.push(Capability::space(identity.did(), name, &request.ability));
        }

        let mut builder = UcanBuilder::new(audience.clone(), request.lifetime_secs);
        for capability in &capabilities {
            builder = builder.capability(capability.clone());
        }
        let token = builder
            .sign(&identity)
            .map_err(|e| invalid(e.to_string()))?;
        let decoded = Ucan::decode(&token).map_err(|e| invalid(e.to_string()))?;
        let id = decoded.id();
        let expires_at = decoded.payload().exp;
        self.catalog.record_issued_ucan(IssuedUcan {
            id: id.clone(),
            audience: audience.to_string(),
            capabilities: capabilities
                .into_iter()
                .map(|c| UcanCapability {
                    with: c.with,
                    can: c.can,
                })
                .collect(),
            issued_at_ms: unix_now_ms(),
            expires_at,
        })?;

        batch(
            vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("audience", DataType::Utf8, false),
                Field::new("expires_at", DataType::Int64, false),
                Field::new("token", DataType::Utf8, false),
            ],
            vec![
                Arc::new(StringArray::from(vec![id])),
                Arc::new(StringArray::from(vec![audience.to_string()])),
                Arc::new(Int64Array::from(vec![expires_at as i64])),
                Arc::new(StringArray::from(vec![token])),
            ],
        )
    }

    /// `REVOKE UCAN '<id>'`: 1 row affected if newly revoked, 0 if it already was.
    pub(crate) fn revoke_ucan(&self, id: &str) -> Result<QueryResult, SessionError> {
        let revoked = self.catalog.revoke_ucan(id)?;
        Ok(QueryResult::RowsAffected(u64::from(revoked)))
    }

    /// `SHOW UCANS`: every token this node issued or revoked.
    pub(crate) fn show_ucans(&self) -> Result<QueryResult, SessionError> {
        let issued = self.catalog.list_issued_ucans();
        let revoked: HashSet<String> = self.catalog.revoked_ucans();
        let mut ids = Vec::new();
        let mut audiences = Vec::new();
        let mut capabilities = Vec::new();
        let mut expires = Vec::new();
        let mut is_revoked = Vec::new();
        for u in &issued {
            ids.push(u.id.clone());
            audiences.push(Some(u.audience.clone()));
            capabilities.push(Some(
                u.capabilities
                    .iter()
                    .map(|c| format!("{} {}", c.can, c.with))
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
            expires.push(Some(u.expires_at as i64));
            is_revoked.push(revoked.contains(&u.id));
        }
        // Revoked ids issued elsewhere (delegations of our tokens).
        for r in self.catalog.list_revocations() {
            if !issued.iter().any(|u| u.id == r.id) {
                ids.push(r.id);
                audiences.push(None);
                capabilities.push(None);
                expires.push(None);
                is_revoked.push(true);
            }
        }
        batch(
            vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("audience", DataType::Utf8, true),
                Field::new("capabilities", DataType::Utf8, true),
                Field::new("expires_at", DataType::Int64, true),
                Field::new("revoked", DataType::Boolean, false),
            ],
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(StringArray::from(audiences)),
                Arc::new(StringArray::from(capabilities)),
                Arc::new(Int64Array::from(expires)),
                Arc::new(BooleanArray::from(is_revoked)),
            ],
        )
    }

    /// `CREATE SPACE <name> WITH PEER = ..., TOKEN = ...`: mount a Space of
    /// another node. The token must have been issued (or delegated) to this
    /// node and grant `space/query` on the remote Space; its root issuer is
    /// the owner every request will be signed for.
    pub(crate) fn create_remote_space(
        &self,
        request: CreateSpaceRequest,
    ) -> Result<QueryResult, SessionError> {
        let name = request.name.clone();
        let (peer, remote, token) = Self::peer_space_request(request)?;
        let owner = self.peer_space_owner(&token, &remote, ABILITY_QUERY)?;
        self.catalog.create_remote_space(
            &name,
            &peer,
            &remote,
            owner.as_str(),
            HeldToken::new(token),
        )?;
        Ok(QueryResult::Empty)
    }

    /// The peer, remote Space name (default: the local name) and token of a
    /// `CREATE SPACE ... WITH PEER` request.
    pub(crate) fn peer_space_request(
        request: CreateSpaceRequest,
    ) -> Result<(String, String, String), SessionError> {
        let peer = validate_peer_url(request.peer.as_deref().unwrap_or_default())?;
        let token = request
            .token
            .ok_or_else(|| invalid("a remote space needs TOKEN = '<ucan>'"))?;
        let remote = request.remote.unwrap_or(request.name);
        Ok((peer, remote, token))
    }

    /// Check that `token` was issued (or delegated) to this node and grants
    /// `ability` on `remote`; return its root issuer, the node every request
    /// will be signed for.
    pub(crate) fn peer_space_owner(
        &self,
        token: &str,
        remote: &str,
        ability: &str,
    ) -> Result<Did, SessionError> {
        let me = self.engines().identity()?;
        let decoded = Ucan::decode(token).map_err(|e| invalid(format!("TOKEN: {e}")))?;
        let owner = decoded
            .root_issuer()
            .map_err(|e| invalid(format!("TOKEN: {e}")))?;
        if &owner == me.did() {
            return Err(invalid(
                "TOKEN was issued by this node; query its spaces directly",
            ));
        }
        let verb = if ability == ABILITY_QUERY {
            "query"
        } else {
            "subscribe to"
        };
        ucan::verify(
            token,
            &VerifyOptions {
                root: &owner,
                audience: me.did(),
                required: &Capability::space(&owner, remote, ability),
                now: unix_now(),
                revoked: &HashSet::new(),
            },
        )
        .map_err(|e| {
            invalid(format!(
                "TOKEN does not let this node {verb} '{remote}': {e}"
            ))
        })?;
        Ok(owner)
    }

    /// Relations (tables and views) of `space` this session may read: all
    /// of them for the owner, the granted ones for a principal. A principal
    /// with no grant in `space` gets `Space not found`, as for `SET SPACE`.
    pub async fn readable_relations(&self, space: &str) -> Result<Vec<String>, SessionError> {
        if let Some(p) = self.principal() {
            if !self.catalog.has_grants_in(p, space) {
                return Err(CatalogError::SpaceNotFound(space.to_string()).into());
            }
        }
        let resolved = self.space(space)?;
        if resolved.kind == SpaceKind::Remote && self.principal().is_some() {
            return Err(SessionError::PermissionDenied(
                "remote spaces are not served to peers".to_string(),
            ));
        }
        let engine = self.bind(&resolved).await?;
        let mut names: BTreeSet<String> = engine.table_names(space).await?.into_iter().collect();
        names.extend(self.catalog.list_views(space).into_iter().map(|v| v.name));
        if let Some(p) = self.principal() {
            names.retain(|rel| self.catalog.get_grant(p, space, rel).is_some());
        }
        Ok(names.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_spaces_requires_qualified_queries() {
        let spaces =
            query_spaces("SELECT p.id FROM blog.posts p JOIN news.items i ON true").unwrap();
        assert_eq!(
            spaces.into_iter().collect::<Vec<_>>(),
            vec!["blog".to_string(), "news".to_string()]
        );
        assert!(query_spaces("SELECT 1").unwrap().is_empty());
        for refused in [
            "DELETE FROM blog.posts",
            "INSERT INTO blog.posts VALUES (1)",
            "DROP TABLE blog.posts",
            "CREATE SPACE x",
            "ISSUE UCAN ON SPACE blog TO 'did:key:z'",
            "GRANT SELECT ON blog.posts TO alice",
        ] {
            assert!(
                matches!(
                    query_spaces(refused),
                    Err(SessionError::PermissionDenied(_))
                ),
                "{refused}"
            );
        }
        assert!(query_spaces("SELECT * FROM posts").is_err());
    }
}
