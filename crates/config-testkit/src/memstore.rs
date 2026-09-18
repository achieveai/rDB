//! An in-memory reference [`ConfigStore`] with no Raft.
//!
//! [`MemStore`] wraps a single [`KvState`] behind a mutex and applies the same validate-then-
//! apply flow a real embedded client uses: [`validate_put`]/[`validate_delete`]/[`validate_list`]
//! at the edge, then [`KvState::apply`]. It exists to test [`crate::conformance`] itself before
//! `Cluster`/`DirectClient` exist, and to give other crates' tests "some `ConfigStore`" without
//! standing up a cluster.

use std::sync::Mutex;

use async_trait::async_trait;
use config_core::{
    validate_delete, validate_list, validate_put, Capabilities, Command, CommandResponse,
    ConfigError, ConfigStore, DeleteRequest, GetRequest, GetResponse, KvState, Limits, ListRequest,
    ListResponse, MutationResponse, PutRequest,
};

/// An in-memory, single-process [`ConfigStore`] over [`KvState`] (spec/TA-10 reference impl).
///
/// Reports `Capabilities::EPHEMERAL_DEVELOPMENT` honestly: state lives only in this process's
/// memory, and every caller is treated as the `Development` principal — there is no
/// authorization and no transport.
pub struct MemStore {
    state: Mutex<KvState>,
    limits: Limits,
    /// When set, every call fails with a clone of this error instead of touching `state`.
    failing: Mutex<Option<ConfigError>>,
}

impl MemStore {
    /// A fresh, empty store using [`Limits::DEFAULT`].
    pub fn new() -> Self {
        Self::with_limits(Limits::DEFAULT)
    }

    /// A fresh, empty store using explicit caps (e.g. to exercise truncation with small
    /// limits, as [`crate::conformance`]'s C-09/C-10/C-12 scenarios do).
    pub fn with_limits(limits: Limits) -> Self {
        Self {
            state: Mutex::new(KvState::with_limits(limits)),
            limits,
            failing: Mutex::new(None),
        }
    }

    /// Make every subsequent `get`/`list`/`put`/`delete` fail with a clone of `err`, without
    /// touching the underlying state, until [`MemStore::stop_failing`].
    ///
    /// Exists so a conformance-suite test can prove the report actually captures a failure
    /// rather than always reporting a pass.
    pub fn failing_with(&self, err: ConfigError) {
        *self.failing.lock().unwrap_or_else(|e| e.into_inner()) = Some(err);
    }

    /// Stop failing calls injected by [`MemStore::failing_with`].
    pub fn stop_failing(&self) {
        *self.failing.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    fn injected_failure(&self) -> Result<(), ConfigError> {
        match self
            .failing
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    fn validate_get_key(&self, key: &[u8]) -> Result<(), ConfigError> {
        if key.is_empty() {
            return Err(ConfigError::invalid_argument("key must not be empty"));
        }
        if key.len() > self.limits.max_key_bytes {
            return Err(ConfigError::invalid_argument(format!(
                "key is {} bytes, limit is {}",
                key.len(),
                self.limits.max_key_bytes
            )));
        }
        Ok(())
    }
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Turn a total, infallible [`CommandResponse`] into the [`ConfigStore`] result shape.
///
/// [`CommandResponse::Noop`] is documented as never returned by [`KvState::apply`]; it is
/// handled without panicking anyway, because a library must not crash a caller's process on a
/// contract this crate does not itself enforce.
fn to_mutation_result(resp: CommandResponse) -> Result<MutationResponse, ConfigError> {
    match resp {
        CommandResponse::Mutation { response, .. } => Ok(response),
        CommandResponse::Rejected { reason } => Err(ConfigError::invalid_argument(reason)),
        CommandResponse::Noop => Err(ConfigError::invalid_argument(
            "apply produced Noop for a Command, which KvState::apply never does",
        )),
    }
}

#[async_trait]
impl ConfigStore for MemStore {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        self.injected_failure()?;
        self.validate_get_key(&request.key)?;
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_response(&request.key))
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        self.injected_failure()?;
        let effective = validate_list(&request, &self.limits)?;
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .list(&effective))
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        self.injected_failure()?;
        validate_put(&request, &self.limits)?;
        let cmd = Command::from(&request);
        let resp = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .apply(&cmd);
        to_mutation_result(resp)
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        self.injected_failure()?;
        validate_delete(&request, &self.limits)?;
        let cmd = Command::from(&request);
        let resp = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .apply(&cmd);
        to_mutation_result(resp)
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::EPHEMERAL_DEVELOPMENT
    }
}
