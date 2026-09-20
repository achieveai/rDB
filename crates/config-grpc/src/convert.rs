//! Mechanical Protobuf ↔ `config-core` conversions (spec §6.2).
//!
//! The wire schema mirrors the core types one-for-one, so this module has nowhere to invent
//! semantics — which is the point: a mapping that had to decide anything would be a second
//! place where the API's meaning lives.

use std::time::Duration;

use bytes::Bytes;
use config_core::{
    ConfigError, DedupKey, DeleteRequest, GetRequest, GetResponse, ListPage, ListRequest,
    ListResponse, MutationEvent, MutationEventKind, MutationOutcome, MutationResponse, PageRequest,
    PutRequest, Record, WatchItem, WatchRequest,
};

use crate::pb;

impl From<Record> for pb::Record {
    fn from(r: Record) -> Self {
        Self {
            key: r.key,
            value: r.value,
            create_revision: r.create_revision,
            mod_revision: r.mod_revision,
        }
    }
}

impl From<pb::Record> for Record {
    fn from(r: pb::Record) -> Self {
        Self {
            key: r.key,
            value: r.value,
            create_revision: r.create_revision,
            mod_revision: r.mod_revision,
        }
    }
}

impl From<pb::GetRequest> for GetRequest {
    fn from(r: pb::GetRequest) -> Self {
        Self { key: r.key }
    }
}

impl From<GetRequest> for pb::GetRequest {
    fn from(r: GetRequest) -> Self {
        Self { key: r.key }
    }
}

impl From<GetResponse> for pb::GetResponse {
    fn from(r: GetResponse) -> Self {
        Self {
            record: r.record.map(Into::into),
            read_revision: r.read_revision,
        }
    }
}

impl From<pb::GetResponse> for GetResponse {
    fn from(r: pb::GetResponse) -> Self {
        Self {
            record: r.record.map(Into::into),
            read_revision: r.read_revision,
        }
    }
}

impl From<pb::ListRequest> for ListRequest {
    /// The M3 arguments only. `page_token` is deliberately dropped here: this conversion feeds
    /// [`config_core::ConfigStore::list`], which has no cursor, and silently carrying one into
    /// it would be a pinned walk nobody asked for.
    fn from(r: pb::ListRequest) -> Self {
        Self {
            prefix: r.prefix,
            max_items: r.max_items,
            max_bytes: r.max_bytes,
        }
    }
}

impl From<ListRequest> for pb::ListRequest {
    fn from(r: ListRequest) -> Self {
        Self {
            prefix: r.prefix,
            max_items: r.max_items,
            max_bytes: r.max_bytes,
            page_token: None,
        }
    }
}

/// A wire `List` with explicit presence on `page_token` becomes the paginated request (M6).
///
/// `Some(empty)` is the opt-in for a first page and `None` never reaches here — the client
/// plane routes an absent `page_token` to the M3 path instead, which is what keeps
/// "a `List` without a token is unchanged" structural rather than conventional (M6-84).
impl From<pb::ListRequest> for PageRequest {
    fn from(r: pb::ListRequest) -> Self {
        let page_token = r.page_token.clone().filter(|token| !token.is_empty());
        Self {
            list: ListRequest::from(r),
            page_token,
        }
    }
}

impl From<PageRequest> for pb::ListRequest {
    fn from(r: PageRequest) -> Self {
        Self {
            prefix: r.list.prefix,
            max_items: r.list.max_items,
            max_bytes: r.list.max_bytes,
            // An absent token would mean "the M3 call"; a walk that has not started yet is an
            // explicitly present empty one.
            page_token: Some(r.page_token.unwrap_or_default()),
        }
    }
}

impl From<ListPage> for pb::ListResponse {
    fn from(r: ListPage) -> Self {
        Self {
            records: r.items.into_iter().map(Into::into).collect(),
            read_revision: r.revision,
            truncated: r.truncated,
            next_page_token: r.next_page_token,
        }
    }
}

impl From<pb::ListResponse> for ListPage {
    fn from(r: pb::ListResponse) -> Self {
        Self {
            items: r.records.into_iter().map(Into::into).collect(),
            revision: r.read_revision,
            truncated: r.truncated,
            // An empty token on the wire is no token: a server that has nothing more to give
            // must not look like one that handed back an unusable cursor.
            next_page_token: r.next_page_token.filter(|token| !token.is_empty()),
        }
    }
}

impl From<ListResponse> for pb::ListResponse {
    /// The M3 response carries no cursor, and never gains one on the way out: a caller that did
    /// not opt in must not receive a token it would then feel obliged to follow.
    fn from(r: ListResponse) -> Self {
        Self {
            records: r.records.into_iter().map(Into::into).collect(),
            read_revision: r.read_revision,
            truncated: r.truncated,
            next_page_token: None,
        }
    }
}

impl From<pb::ListResponse> for ListResponse {
    fn from(r: pb::ListResponse) -> Self {
        Self {
            records: r.records.into_iter().map(Into::into).collect(),
            read_revision: r.read_revision,
            truncated: r.truncated,
        }
    }
}

/// A wire deduplication key becomes a core one only when its `client_id` is exactly 16 bytes.
///
/// Refused rather than padded or truncated: padding would silently merge two clients'
/// namespaces, and truncating would let a caller address a namespace it did not name
/// (ADR-0025).
impl TryFrom<pb::DedupKey> for DedupKey {
    type Error = ConfigError;

    fn try_from(k: pb::DedupKey) -> Result<Self, Self::Error> {
        let client_id: [u8; 16] = k.client_id.as_ref().try_into().map_err(|_| {
            ConfigError::invalid_argument(format!(
                "dedup client_id must be exactly 16 bytes, got {}",
                k.client_id.len()
            ))
        })?;
        Ok(DedupKey::new(client_id, k.request_id))
    }
}

impl From<DedupKey> for pb::DedupKey {
    fn from(k: DedupKey) -> Self {
        Self {
            client_id: Bytes::copy_from_slice(&k.client_id),
            request_id: k.request_id,
        }
    }
}

/// Fallible from M5 on, because a malformed dedup key is an `InvalidArgument` rather than a
/// key the server is free to ignore: a caller that believes it sent a dedup key and got an
/// ordinary application back would resubmit into a second application.
impl TryFrom<pb::PutRequest> for PutRequest {
    type Error = ConfigError;

    fn try_from(r: pb::PutRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            key: r.key,
            value: r.value,
            expected_mod_revision: r.expected_mod_revision,
            dedup: r.dedup.map(DedupKey::try_from).transpose()?,
        })
    }
}

impl From<PutRequest> for pb::PutRequest {
    fn from(r: PutRequest) -> Self {
        Self {
            key: r.key,
            value: r.value,
            expected_mod_revision: r.expected_mod_revision,
            dedup: r.dedup.map(Into::into),
        }
    }
}

/// Fallible for the same reason as [`PutRequest`]'s conversion.
impl TryFrom<pb::DeleteRequest> for DeleteRequest {
    type Error = ConfigError;

    fn try_from(r: pb::DeleteRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            key: r.key,
            expected_mod_revision: r.expected_mod_revision,
            dedup: r.dedup.map(DedupKey::try_from).transpose()?,
        })
    }
}

impl From<DeleteRequest> for pb::DeleteRequest {
    fn from(r: DeleteRequest) -> Self {
        Self {
            key: r.key,
            expected_mod_revision: r.expected_mod_revision,
            dedup: r.dedup.map(Into::into),
        }
    }
}

impl From<MutationOutcome> for pb::MutationOutcome {
    fn from(o: MutationOutcome) -> Self {
        match o {
            MutationOutcome::Applied => Self::Applied,
            MutationOutcome::Conflict => Self::Conflict,
            MutationOutcome::NotFound => Self::NotFound,
        }
    }
}

impl From<MutationResponse> for pb::MutationResponse {
    fn from(r: MutationResponse) -> Self {
        Self {
            outcome: pb::MutationOutcome::from(r.outcome) as i32,
            revision: r.revision,
            exists: r.exists,
            current_mod_revision: r.current_mod_revision,
            dedup_hit: r.dedup_hit,
            dedup_recorded: r.dedup_recorded,
        }
    }
}

impl TryFrom<pb::MutationResponse> for MutationResponse {
    type Error = ConfigError;

    /// `MUTATION_OUTCOME_UNSPECIFIED` (and any unknown tag) is a protocol violation, not a
    /// fourth outcome: guessing here would turn a version skew into silent data loss.
    fn try_from(r: pb::MutationResponse) -> Result<Self, Self::Error> {
        let outcome = match pb::MutationOutcome::try_from(r.outcome) {
            Ok(pb::MutationOutcome::Applied) => MutationOutcome::Applied,
            Ok(pb::MutationOutcome::Conflict) => MutationOutcome::Conflict,
            Ok(pb::MutationOutcome::NotFound) => MutationOutcome::NotFound,
            _ => {
                return Err(ConfigError::invalid_argument(format!(
                    "server returned unknown mutation outcome tag {}",
                    r.outcome
                )))
            }
        };
        Ok(Self {
            outcome,
            revision: r.revision,
            exists: r.exists,
            current_mod_revision: r.current_mod_revision,
            dedup_hit: r.dedup_hit,
            dedup_recorded: r.dedup_recorded,
        })
    }
}

// ---- M4: Watch (ADR-0020) -------------------------------------------------------------

/// A `WatchRequest` off the wire.
///
/// Not a `From`, because `progress_interval_ms` can be *invalid* rather than merely absent:
/// an explicit `0` is a caller mistake the engine must see as `InvalidArgument`, and a `From`
/// would have to silently repair it.
pub fn watch_request_from_pb(req: pb::WatchRequest) -> Result<WatchRequest, ConfigError> {
    let progress_interval = match req.progress_interval_ms {
        None => None,
        Some(ms) => {
            if ms == 0 {
                return Err(ConfigError::InvalidArgument {
                    detail: "progress_interval_ms must be greater than zero; omit the field to \
                             take this node's default"
                        .to_string(),
                });
            }
            Some(Duration::from_millis(u64::from(ms)))
        }
    };
    Ok(WatchRequest {
        prefix: req.prefix,
        start_after_revision: req.start_after_revision,
        progress_interval,
    })
}

impl From<&WatchRequest> for pb::WatchRequest {
    fn from(req: &WatchRequest) -> Self {
        Self {
            prefix: req.prefix.clone(),
            start_after_revision: req.start_after_revision,
            // Saturating rather than wrapping: the engine's own upper bound is an hour, which
            // fits a `u32` of milliseconds, so the only way to reach the clamp is a request
            // the engine would refuse anyway — and it must refuse it as "too large", which a
            // wrapped value would hide.
            progress_interval_ms: req
                .progress_interval
                .map(|d| u32::try_from(d.as_millis()).unwrap_or(u32::MAX)),
        }
    }
}

impl From<MutationEvent> for pb::Event {
    fn from(event: MutationEvent) -> Self {
        let change = match event.kind {
            MutationEventKind::Put {
                value,
                create_revision,
            } => pb::event::Change::Put(pb::Record {
                key: event.key.clone(),
                value,
                create_revision,
                mod_revision: event.revision,
            }),
            MutationEventKind::Delete => pb::event::Change::Delete(pb::Deleted {}),
        };
        Self {
            revision: event.revision,
            key: event.key,
            change: Some(change),
        }
    }
}

impl From<WatchItem> for pb::WatchResponse {
    fn from(item: WatchItem) -> Self {
        let body = match item {
            WatchItem::Event(event) => pb::watch_response::Body::Event(event.into()),
            WatchItem::Progress { revision } => {
                pb::watch_response::Body::Progress(pb::Progress { revision })
            }
        };
        Self { body: Some(body) }
    }
}

/// A `WatchResponse` off the wire.
///
/// A frame with no body, or a `put` event with no record, is a protocol violation rather than
/// something to guess at: silently dropping it would put a gap in a stream whose entire
/// purpose is to have none.
pub fn watch_item_from_pb(response: pb::WatchResponse) -> Result<WatchItem, ConfigError> {
    let invalid = |detail: &str| ConfigError::InvalidArgument {
        detail: detail.to_string(),
    };
    match response.body {
        Some(pb::watch_response::Body::Progress(p)) => Ok(WatchItem::Progress {
            revision: p.revision,
        }),
        Some(pb::watch_response::Body::Event(event)) => {
            let kind = match event.change {
                Some(pb::event::Change::Put(record)) => MutationEventKind::Put {
                    value: record.value,
                    create_revision: record.create_revision,
                },
                Some(pb::event::Change::Delete(_)) => MutationEventKind::Delete,
                None => return Err(invalid("watch event carried neither a put nor a delete")),
            };
            Ok(WatchItem::Event(MutationEvent {
                revision: event.revision,
                key: event.key,
                kind,
            }))
        }
        None => Err(invalid("watch response carried no body")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn record(n: u8) -> Record {
        Record {
            key: Bytes::from(vec![b'k', n]),
            value: Bytes::from(vec![b'v', n]),
            create_revision: 10 + u64::from(n),
            mod_revision: 20 + u64::from(n),
        }
    }

    /// Every field of every message survives `core -> pb -> core`.
    ///
    /// Mechanical, and that is exactly why it is worth asserting: a transposed
    /// `create_revision`/`mod_revision` compiles, passes every higher-level test that only
    /// checks an outcome, and silently corrupts CAS.
    #[test]
    fn core_to_pb_and_back_preserves_every_field() {
        let r = record(1);
        let back: Record = pb::Record::from(r.clone()).into();
        assert_eq!(back, r);

        let get = GetRequest {
            key: Bytes::from_static(b"/app/a"),
        };
        let back: GetRequest = pb::GetRequest::from(get.clone()).into();
        assert_eq!(back.key, get.key);

        let get_response = GetResponse {
            record: Some(record(2)),
            read_revision: 77,
        };
        let back: GetResponse = pb::GetResponse::from(get_response.clone()).into();
        assert_eq!(back, get_response);
        // The empty case is a different branch of `Option::map`.
        let empty = GetResponse {
            record: None,
            read_revision: 3,
        };
        let back: GetResponse = pb::GetResponse::from(empty.clone()).into();
        assert_eq!(back, empty);

        let list = ListRequest {
            prefix: Bytes::from_static(b"/app/"),
            max_items: 50,
            max_bytes: 4096,
        };
        let back: ListRequest = pb::ListRequest::from(list.clone()).into();
        assert_eq!(back, list);

        let list_response = ListResponse {
            records: vec![record(3), record(4)],
            read_revision: 91,
            truncated: true,
        };
        let back: ListResponse = pb::ListResponse::from(list_response.clone()).into();
        assert_eq!(back, list_response);

        let put = PutRequest {
            key: Bytes::from_static(b"/app/a"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: Some(12),
            dedup: None,
        };
        let back = PutRequest::try_from(pb::PutRequest::from(put.clone())).expect("round trip");
        assert_eq!(
            (back.key, back.value, back.expected_mod_revision),
            (put.key, put.value, put.expected_mod_revision)
        );

        let delete = DeleteRequest {
            key: Bytes::from_static(b"/app/a"),
            expected_mod_revision: Some(0),
            dedup: None,
        };
        let back =
            DeleteRequest::try_from(pb::DeleteRequest::from(delete.clone())).expect("round trip");
        assert_eq!(
            (back.key, back.expected_mod_revision),
            (delete.key, delete.expected_mod_revision)
        );
    }

    /// `None` and `Some(0)` are different CAS preconditions ("no expectation" versus "must not
    /// exist"), so the optional revision fields may never collapse into a default.
    #[test]
    fn absent_and_zero_expectations_stay_distinct() {
        for expected in [None, Some(0), Some(1)] {
            let put = PutRequest {
                key: Bytes::from_static(b"/app/a"),
                value: Bytes::from_static(b"v"),
                expected_mod_revision: expected,
                dedup: None,
            };
            let back = PutRequest::try_from(pb::PutRequest::from(put)).expect("round trip");
            assert_eq!(back.expected_mod_revision, expected);

            // `ListRequest` says the same thing with `0` rather than an option, so the
            // sentinel is what must survive here.
            let list = ListRequest {
                prefix: Bytes::from_static(b"/"),
                max_items: expected.unwrap_or(0) as u32,
                max_bytes: expected.unwrap_or(0),
            };
            let back: ListRequest = pb::ListRequest::from(list.clone()).into();
            assert_eq!(back, list);
        }
    }

    #[test]
    fn every_mutation_outcome_round_trips() {
        for outcome in [
            MutationOutcome::Applied,
            MutationOutcome::Conflict,
            MutationOutcome::NotFound,
        ] {
            let response = MutationResponse {
                outcome,
                revision: 5,
                exists: true,
                current_mod_revision: 4,
                dedup_hit: false,
                dedup_recorded: false,
            };
            let wire = pb::MutationResponse::from(response.clone());
            let back = MutationResponse::try_from(wire).expect("known outcome decodes");
            assert_eq!(back, response);
        }
    }

    /// M5-130 (finding C5B-05): both dedup flags survive the wire, in all four combinations.
    ///
    /// They are independent, which is the whole point of carrying the second one. `dedup_hit`
    /// is history -- was this submission a duplicate? `dedup_recorded` is the forecast the
    /// caller actually acts on -- will a resubmission be recognized? The combination that
    /// matters is `(false, false)` on an *applied* mutation: the record was refused by the
    /// global cap, so the write succeeded and a retry would apply it a second time. A wire
    /// format that dropped `dedup_recorded` would make that indistinguishable from a normally
    /// recorded write, which is precisely the case ADR-0015's retry exception must not cover.
    #[test]
    fn both_dedup_flags_survive_the_wire_independently() {
        for (dedup_hit, dedup_recorded) in
            [(false, false), (false, true), (true, true), (true, false)]
        {
            let response = MutationResponse {
                outcome: MutationOutcome::Applied,
                revision: 7,
                exists: true,
                current_mod_revision: 7,
                dedup_hit,
                dedup_recorded,
            };
            let wire = pb::MutationResponse::from(response.clone());
            assert_eq!(wire.dedup_hit, dedup_hit);
            assert_eq!(
                wire.dedup_recorded, dedup_recorded,
                "the wire must carry dedup_recorded, not infer it from dedup_hit"
            );
            let back = MutationResponse::try_from(wire).expect("decodes");
            assert_eq!(back, response);
        }
    }

    /// A pre-C5B-05 server leaves field 6 unset, and proto3 decodes an absent bool as `false`.
    /// That default is the safe one by construction: "no record retains this outcome" is what
    /// a server that does not know about the field is in fact telling us, and a client that
    /// reads it will decline to auto-retry rather than double-apply.
    #[test]
    fn an_older_server_reads_as_not_recorded() {
        let wire = pb::MutationResponse {
            outcome: pb::MutationOutcome::Applied as i32,
            revision: 3,
            exists: true,
            current_mod_revision: 3,
            dedup_hit: false,
            ..Default::default()
        };
        let back = MutationResponse::try_from(wire).expect("decodes");
        assert!(
            !back.dedup_recorded,
            "an absent dedup_recorded must mean 'not recorded', never 'assume recorded'"
        );
    }

    /// An unknown tag is a version skew, and guessing a fourth outcome would mean reporting a
    /// write as applied on the strength of a number we do not recognize.
    #[test]
    fn an_unknown_outcome_tag_is_rejected_not_guessed() {
        for tag in [0, 99, -1] {
            let wire = pb::MutationResponse {
                outcome: tag,
                revision: 1,
                exists: false,
                current_mod_revision: 0,
                dedup_hit: false,
                dedup_recorded: false,
            };
            let error = MutationResponse::try_from(wire).expect_err("unknown tag is refused");
            assert!(
                matches!(error, ConfigError::InvalidArgument { .. }),
                "expected InvalidArgument for tag {tag}, got {error:?}"
            );
        }
    }
}
