//! [`ScopedDb`]: one tenant scope's view of a [`Db`], for request code that must reach nothing
//! else (#147).
//!
//! A scope is an app and an optional sub-tenant ([`Scope`]); a workspace is a `u64` group in it.
//! The handle writes every object id as `S || group BE || id`, places it by
//! [`scope_affinity`], and derives every request id it sends from the scope, so request code
//! names only its own ids, never a key, an affinity group or a raw request id. No two scopes'
//! `S || group` are prefixes of each other, so no call of one scope reaches a record of
//! another, whichever partition they share. The argument, path by path, is in
//! `docs/rdb/embed.md`, "Scopes, groups, list and delete".
//!
//! What the handle does not stop: a holder of the `Db` itself, or an app over a network link,
//! reaches every scope. rDB's client and peer ports must never be reachable from sandbox or app
//! networks (#154 owes a node-side check).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rdb_core::contracts::ids::{AffinityId, Generation, RequestId};
use rdb_core::contracts::txn::{TxnRequest, TxnStatus};
use rdb_value::keys::{scope_affinity, scoped_request, Scope};
use rdb_value::Expected;

use super::async_db::{blocking, put_answer, read_answer};
use super::{
    app_request_id, ApiError, AsyncDb, Db, GetOk, Limit, ObjectEntry, ObjectPage,
    PrefixDeleteError, PutError, PutOk, ReadOk, TooLarge, TxnPut, MAX_SCOPED_ID,
};
use crate::host::{Change, Look};

/// The request a [`ScopedDb`] sent, for [`ScopedDb::resend_within`] and [`ScopedDb::status_of`]
/// of the same scope. Only the handle makes one, and nothing in it can be read or changed, so a
/// request built by hand, or taken from another scope or from the `Db`, cannot be sent through
/// a scope.
///
/// A token resends (this compiles, and is the control for the two below):
///
/// ```no_run
/// async fn again(scope: &rdb_api::ScopedDb, sent: rdb_api::ScopedSent) {
///     let _ = scope.resend_within(sent, std::time::Duration::from_secs(1)).await;
/// }
/// ```
///
/// A raw request does not (#147 row k):
///
/// ```compile_fail,E0308
/// async fn again(scope: &rdb_api::ScopedDb, raw: rdb_api::TxnRequest) {
///     let _ = scope.resend_within(raw, std::time::Duration::from_secs(1)).await;
/// }
/// ```
///
/// Nor can a token be built from one:
///
/// ```compile_fail,E0451
/// fn forge(raw: rdb_api::TxnRequest, scope: rdb_api::Scope) -> rdb_api::ScopedSent {
///     rdb_api::ScopedSent { request: Box::new(raw), scope, group: 0 }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedSent {
    request: Box<TxnRequest>,
    scope: Scope,
    group: u64,
}

/// A scoped write's answer: the [`PutOk`], whose `sent` is emptied of its keys and values, and
/// the token to resend it with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedOk {
    /// The answer. Its `sent` keeps the identity, and has no conditions or mutations.
    pub ok: PutOk,
    /// The request, for [`ScopedDb::resend_within`] and [`ScopedDb::status_of`].
    pub sent: ScopedSent,
}

/// A refused scoped write, with the token to resend it when one was sent.
#[derive(Debug, Clone)]
pub struct ScopedPutError {
    /// Why.
    pub error: ApiError,
    /// The request, when one was sent, for [`ScopedDb::resend_within`].
    pub sent: Option<ScopedSent>,
}

impl std::fmt::Display for ScopedPutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for ScopedPutError {}

/// One put of [`ScopedDb::txn`]: an id in the transaction's workspace, and its bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedWrite {
    /// The object id, inside the scope and workspace.
    pub id: Bytes,
    /// The bytes.
    pub value: Bytes,
    /// Write only if the object is at this version.
    pub if_version: Option<u64>,
}

/// One tenant scope of a [`Db`], async (#147). Every call takes a workspace `group` and an
/// object `id` inside the scope, and none takes a scope, a key, an affinity group or a raw
/// request id. Made by [`AsyncDb::scoped`]. Cloning is cheap.
///
/// Every object id is at most [`MAX_SCOPED_ID`] bytes, in every scope; a longer one is
/// `TOO_LARGE` ([`Limit::ObjectId`]) with that maximum.
#[derive(Clone)]
pub struct ScopedDb {
    db: Arc<Db>,
    scope: Scope,
    /// Test only: every group of the handle placed on this affinity ([`Self::__with_affinity`]).
    pinned: Option<AffinityId>,
}

impl AsyncDb {
    /// The handle for `scope`. Give request code this, never the `AsyncDb` or the `Db`.
    #[must_use]
    pub fn scoped(&self, scope: Scope) -> ScopedDb {
        ScopedDb {
            db: Arc::clone(self.shared()),
            scope,
            pinned: None,
        }
    }
}

/// The scoped id `S || group || id`, or `TOO_LARGE` for an id over [`MAX_SCOPED_ID`].
fn scoped_id(scope: &Scope, group: u64, id: &[u8]) -> Result<Bytes, ApiError> {
    if id.len() > MAX_SCOPED_ID {
        return Err(ApiError::too_large_refusal(
            TooLarge {
                limit: Limit::ObjectId,
                size: id.len(),
                max: MAX_SCOPED_ID,
            },
            format!(
                "object id {} bytes, over the {MAX_SCOPED_ID}-byte limit of a scoped id; not sent",
                id.len()
            ),
        ));
    }
    let mut out = scope.prefix(group);
    out.extend_from_slice(id);
    Ok(Bytes::from(out))
}

fn refused(error: ApiError) -> ScopedPutError {
    ScopedPutError { error, sent: None }
}

impl ScopedDb {
    /// Test only: a handle of `scope` that places every group on affinity `affinity`, so several
    /// scopes can be forced into one `(tenant, affinity)` and the id prefix is all that tells
    /// them apart, as under a real collision. Misused, it costs placement, never isolation.
    #[doc(hidden)]
    #[must_use]
    pub fn __with_affinity(db: Arc<Db>, scope: Scope, affinity: AffinityId) -> Self {
        Self {
            db,
            scope,
            pinned: Some(affinity),
        }
    }

    /// The scope.
    #[must_use]
    pub const fn scope(&self) -> &Scope {
        &self.scope
    }

    /// The affinity group workspace `group` is placed on: [`scope_affinity`].
    #[must_use]
    pub fn affinity(&self, group: u64) -> AffinityId {
        self.pinned
            .unwrap_or_else(|| scope_affinity(&self.scope, group))
    }

    /// The request id the app's request number `n` is sent under in workspace `group`.
    fn request_id(&self, group: u64, n: u64) -> RequestId {
        app_request_id(scoped_request(&self.scope, group, n))
            .expect("a scoped request number has its top bit clear")
    }

    /// `answer` with its request held as a [`ScopedSent`] of this scope and `group`.
    fn sealed(
        &self,
        group: u64,
        answer: Result<PutOk, PutError>,
    ) -> Result<ScopedOk, ScopedPutError> {
        match answer {
            Ok(mut ok) => {
                let request = ok.sent.clone();
                ok.sent.conditions.clear();
                ok.sent.mutations.clear();
                Ok(ScopedOk {
                    ok,
                    sent: ScopedSent {
                        request,
                        scope: self.scope.clone(),
                        group,
                    },
                })
            }
            Err(PutError { error, request }) => Err(ScopedPutError {
                error,
                sent: request.map(|request| ScopedSent {
                    request,
                    scope: self.scope.clone(),
                    group,
                }),
            }),
        }
    }

    fn sealed_change(
        &self,
        group: u64,
        answer: Result<Option<PutOk>, PutError>,
    ) -> Result<Option<ScopedOk>, ScopedPutError> {
        match answer {
            Ok(None) => Ok(None),
            Ok(Some(ok)) => self.sealed(group, Ok(ok)).map(Some),
            Err(error) => self.sealed(group, Err(error)).map(Some),
        }
    }

    /// Put `value` at `id` in workspace `group`; as [`Db::put_in`].
    ///
    /// # Errors
    ///
    /// As [`Db::put_in`], and `TOO_LARGE` for an id over [`MAX_SCOPED_ID`].
    pub async fn put(
        &self,
        group: u64,
        id: Bytes,
        value: Bytes,
        if_version: Option<u64>,
    ) -> Result<ScopedOk, ScopedPutError> {
        let object = scoped_id(&self.scope, group, &id).map_err(refused)?;
        let (db, affinity) = (Arc::clone(&self.db), self.affinity(group));
        let answer =
            put_answer(blocking(move || db.put_in(affinity, &object, &value, if_version)).await);
        self.sealed(group, answer)
    }

    /// Read `id` in workspace `group` as bytes; as [`Db::get_in`].
    ///
    /// # Errors
    ///
    /// As [`Db::get_in`], and `TOO_LARGE` for an id over [`MAX_SCOPED_ID`].
    pub async fn get(&self, group: u64, id: Bytes) -> Result<GetOk, ApiError> {
        let object = scoped_id(&self.scope, group, &id)?;
        let (db, affinity) = (Arc::clone(&self.db), self.affinity(group));
        read_answer(blocking(move || db.get_in(affinity, &object)).await)
    }

    /// Read `look` of `id` in workspace `group`; as [`Db::read_in`]. A [`Look::List`] is
    /// refused: use [`Self::list`].
    ///
    /// # Errors
    ///
    /// As [`Db::read_in`], `TOO_LARGE` for an id over [`MAX_SCOPED_ID`], and `INVALID_ARGUMENT`
    /// for a list look.
    pub async fn read(&self, group: u64, id: Bytes, look: Look) -> Result<ReadOk, ApiError> {
        if matches!(look, Look::List { .. }) {
            return Err(ApiError::invalid(
                "a scoped read takes no list look; use list",
            ));
        }
        let object = scoped_id(&self.scope, group, &id)?;
        let (db, affinity) = (Arc::clone(&self.db), self.affinity(group));
        read_answer(blocking(move || db.read_in(affinity, &object, look)).await)
    }

    /// Apply `change` to `id` in workspace `group`; as [`Db::write_in`]. [`Change::Delete`]
    /// deletes any kind, one batch a call: call again until `Ok(None)`.
    ///
    /// # Errors
    ///
    /// As [`Db::write_in`], and `TOO_LARGE` for an id over [`MAX_SCOPED_ID`].
    pub async fn write(
        &self,
        group: u64,
        id: Bytes,
        change: Change,
        expect: Option<Expected>,
    ) -> Result<Option<ScopedOk>, ScopedPutError> {
        let object = scoped_id(&self.scope, group, &id).map_err(refused)?;
        let (db, affinity) = (Arc::clone(&self.db), self.affinity(group));
        let answer =
            put_answer(blocking(move || db.write_in(affinity, &object, change, expect)).await);
        self.sealed_change(group, answer)
    }

    /// [`Self::write`] under the app's request number `n`, as [`Db::write_as_in`]: a fresh
    /// compile, so once `n` applied, writing it again meets the dedup rules
    /// (`REQUEST_ID_REUSE`) and writes nothing. To replay the answer, resend its token with
    /// [`Self::resend_within`]; after a crash, ask [`Self::status`] for `n`. Numbers are the
    /// app's own per scope and workspace; two scopes using the same `n` never meet.
    ///
    /// # Errors
    ///
    /// As [`Db::write_as_in`].
    pub async fn write_as(
        &self,
        group: u64,
        n: u64,
        id: Bytes,
        change: Change,
        expect: Option<Expected>,
    ) -> Result<Option<ScopedOk>, ScopedPutError> {
        let object = scoped_id(&self.scope, group, &id).map_err(refused)?;
        let (db, affinity, request) = (
            Arc::clone(&self.db),
            self.affinity(group),
            self.request_id(group, n),
        );
        let answer = put_answer(
            blocking(move || db.write_as_in(affinity, request, &object, change, expect)).await,
        );
        self.sealed_change(group, answer)
    }

    /// One transaction of `puts` in workspace `group`; as [`Db::txn`].
    ///
    /// # Errors
    ///
    /// As [`Db::txn`], and `TOO_LARGE` for an id over [`MAX_SCOPED_ID`].
    pub async fn txn(
        &self,
        group: u64,
        puts: Vec<ScopedWrite>,
        deadline: Duration,
    ) -> Result<ScopedOk, ScopedPutError> {
        let objects = self.objects(group, &puts).map_err(refused)?;
        let (db, affinity) = (Arc::clone(&self.db), self.affinity(group));
        let answer = put_answer(
            blocking(move || db.txn(&txn_puts(affinity, &objects, &puts), deadline)).await,
        );
        self.sealed(group, answer)
    }

    /// [`Self::txn`] under the app's request number `n`, as [`Db::txn_as`].
    ///
    /// # Errors
    ///
    /// As [`Db::txn_as`].
    pub async fn txn_as(
        &self,
        group: u64,
        n: u64,
        puts: Vec<ScopedWrite>,
        deadline: Duration,
    ) -> Result<ScopedOk, ScopedPutError> {
        let objects = self.objects(group, &puts).map_err(refused)?;
        let (db, affinity, request) = (
            Arc::clone(&self.db),
            self.affinity(group),
            self.request_id(group, n),
        );
        let answer = put_answer(
            blocking(move || db.txn_as(request, &txn_puts(affinity, &objects, &puts), deadline))
                .await,
        );
        self.sealed(group, answer)
    }

    fn objects(&self, group: u64, puts: &[ScopedWrite]) -> Result<Vec<Bytes>, ApiError> {
        puts.iter()
            .map(|put| scoped_id(&self.scope, group, &put.id))
            .collect()
    }

    /// What became of the app's request `n` in workspace `group`; as [`Db::status_in`], asked
    /// with `generation`.
    ///
    /// # Errors
    ///
    /// As [`Db::status_in`].
    pub async fn status(
        &self,
        group: u64,
        n: u64,
        generation: Option<Generation>,
    ) -> Result<TxnStatus, ApiError> {
        let (db, affinity, request) = (
            Arc::clone(&self.db),
            self.affinity(group),
            self.request_id(group, n),
        );
        read_answer(blocking(move || db.status_in(affinity, request, generation)).await)
    }

    /// What became of a request this scope sent, asked with the generation it was fenced with.
    ///
    /// # Errors
    ///
    /// As [`Db::status_in`]. `INVALID_ARGUMENT`, with nothing asked, for another scope's token.
    pub async fn status_of(&self, sent: &ScopedSent) -> Result<TxnStatus, ApiError> {
        self.own(sent)?;
        let (db, affinity) = (Arc::clone(&self.db), self.affinity(sent.group));
        let (request, generation) = (
            sent.request.identity.request,
            sent.request.expected_generation,
        );
        read_answer(blocking(move || db.status_in(affinity, request, generation)).await)
    }

    fn own(&self, sent: &ScopedSent) -> Result<(), ApiError> {
        if sent.scope == self.scope {
            Ok(())
        } else {
            Err(ApiError::invalid(
                "the request was sent by another scope; a scope resends and asks only its own: \
                 nothing sent",
            ))
        }
    }

    /// Send a request this scope sent again, unchanged, with kernel deadline `deadline`; as
    /// [`Db::resend_within`].
    ///
    /// # Errors
    ///
    /// As [`Db::resend_within`]. `INVALID_ARGUMENT`, with nothing sent, for another scope's
    /// token.
    pub async fn resend_within(
        &self,
        sent: ScopedSent,
        deadline: Duration,
    ) -> Result<ScopedOk, ScopedPutError> {
        self.own(&sent).map_err(refused)?;
        let (db, group) = (Arc::clone(&self.db), sent.group);
        let request = *sent.request;
        let answer = put_answer(blocking(move || db.resend_within(request, deadline)).await);
        self.sealed(group, answer)
    }

    /// Up to `limit` objects of workspace `group` whose id starts with `prefix`, after `after`
    /// when given; as [`Db::list_in`], with ids inside the scope. An empty prefix lists the
    /// whole workspace. A workspace is the widest list: rDB keeps no list of a scope's
    /// workspaces.
    ///
    /// # Errors
    ///
    /// As [`Db::list_in`], and `TOO_LARGE` for a `prefix` or `after` over [`MAX_SCOPED_ID`].
    pub async fn list(
        &self,
        group: u64,
        prefix: Bytes,
        after: Option<Bytes>,
        limit: usize,
    ) -> Result<ObjectPage, ApiError> {
        let head = self.scope.prefix(group);
        let prefix = scoped_id(&self.scope, group, &prefix)?;
        let after = after
            .map(|after| scoped_id(&self.scope, group, &after))
            .transpose()?;
        let (db, affinity) = (Arc::clone(&self.db), self.affinity(group));
        let page = read_answer(
            blocking(move || db.list_in(affinity, &prefix, after.as_deref(), limit)).await,
        )?;
        let strip = |id: Bytes| {
            if id.starts_with(&head) {
                Ok(id.slice(head.len()..))
            } else {
                Err(ApiError::host(&format!(
                    "a scoped list answered id {id:02x?}, outside its scope"
                )))
            }
        };
        Ok(ObjectPage {
            entries: page
                .entries
                .into_iter()
                .map(|entry| {
                    Ok(ObjectEntry {
                        id: strip(entry.id)?,
                        ..entry
                    })
                })
                .collect::<Result<_, ApiError>>()?,
            next: page.next.map(strip).transpose()?,
            generation: page.generation,
            at: page.at,
        })
    }

    /// Delete every object of workspace `group` whose id starts with `prefix`; as
    /// [`Db::delete_prefix_in`]. An empty prefix empties the workspace. Not atomic: run it again
    /// after an error.
    ///
    /// # Errors
    ///
    /// As [`Db::delete_prefix_in`], and `TOO_LARGE` for a `prefix` over [`MAX_SCOPED_ID`].
    pub async fn delete_prefix(
        &self,
        group: u64,
        prefix: Bytes,
        deadline: Duration,
    ) -> Result<u64, PrefixDeleteError> {
        let prefix = scoped_id(&self.scope, group, &prefix).map_err(|error| PrefixDeleteError {
            deleted: 0,
            error: PutError {
                error,
                request: None,
            },
        })?;
        let (db, affinity) = (Arc::clone(&self.db), self.affinity(group));
        blocking(move || db.delete_prefix_in(affinity, &prefix, deadline))
            .await
            .unwrap_or_else(|detail| {
                Err(PrefixDeleteError {
                    deleted: 0,
                    error: PutError {
                        error: ApiError::host(&detail),
                        request: None,
                    },
                })
            })
    }
}

/// `puts` as the [`TxnPut`]s [`Db::txn`] takes, under their scoped ids.
fn txn_puts<'a>(
    affinity: AffinityId,
    objects: &'a [Bytes],
    puts: &'a [ScopedWrite],
) -> Vec<TxnPut<'a>> {
    objects
        .iter()
        .zip(puts)
        .map(|(object, put)| TxnPut {
            group: affinity,
            object,
            value: &put.value,
            if_version: put.if_version,
        })
        .collect()
}
