//! The embedded client (spec §6.1, ADR-0012).
//!
//! `DirectClient` is a [`ConfigStore`] over a local [`ConfigNode`]. "Embedded" describes where
//! the code runs, not which rules apply: every call still goes through validation,
//! authorization, Raft, and the linearizable read barrier. In particular a `DirectClient` on a
//! follower returns `NotLeader` rather than reading local state, which is what lets the same
//! conformance suite run against it and against the gRPC client.
//!
//! The principal is bound at construction, never taken from a request field: an embedder
//! chooses the identity of the handle it hands out, and the caller of that handle cannot
//! change it (spec §6.2).

use async_trait::async_trait;
use config_core::{
    Capabilities, ConfigError, ConfigStore, DeleteRequest, GetRequest, GetResponse, ListRequest,
    ListResponse, MutationResponse, Principal, PutRequest,
};

use crate::node::ConfigNode;

/// A [`ConfigStore`] backed by an in-process [`ConfigNode`].
#[derive(Debug, Clone)]
pub struct DirectClient {
    node: ConfigNode,
    principal: Principal,
}

impl DirectClient {
    /// Bind `principal` to `node`.
    pub fn new(node: ConfigNode, principal: Principal) -> Self {
        Self { node, principal }
    }

    /// The node this client talks to.
    pub fn node(&self) -> &ConfigNode {
        &self.node
    }

    /// The principal every call from this handle is authorized as.
    pub fn principal(&self) -> &Principal {
        &self.principal
    }
}

impl ConfigNode {
    /// A [`ConfigStore`] handle on this node, authorized as `principal`.
    pub fn direct_client(&self, principal: Principal) -> DirectClient {
        DirectClient::new(self.clone(), principal)
    }
}

#[async_trait]
impl ConfigStore for DirectClient {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        self.node.get(&self.principal, request).await
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        self.node.list(&self.principal, request).await
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        self.node.put(&self.principal, request).await
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        self.node.delete(&self.principal, request).await
    }

    fn capabilities(&self) -> Capabilities {
        self.node.capabilities()
    }
}
