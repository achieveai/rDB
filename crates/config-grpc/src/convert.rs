//! Mechanical Protobuf ↔ `config-core` conversions (spec §6.2).
//!
//! The wire schema mirrors the core types one-for-one, so this module has nowhere to invent
//! semantics — which is the point: a mapping that had to decide anything would be a second
//! place where the API's meaning lives.

use config_core::{
    ConfigError, DeleteRequest, GetRequest, GetResponse, ListRequest, ListResponse,
    MutationOutcome, MutationResponse, PutRequest, Record,
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
        }
    }
}

impl From<ListResponse> for pb::ListResponse {
    fn from(r: ListResponse) -> Self {
        Self {
            records: r.records.into_iter().map(Into::into).collect(),
            read_revision: r.read_revision,
            truncated: r.truncated,
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

impl From<pb::PutRequest> for PutRequest {
    fn from(r: pb::PutRequest) -> Self {
        Self {
            key: r.key,
            value: r.value,
            expected_mod_revision: r.expected_mod_revision,
        }
    }
}

impl From<PutRequest> for pb::PutRequest {
    fn from(r: PutRequest) -> Self {
        Self {
            key: r.key,
            value: r.value,
            expected_mod_revision: r.expected_mod_revision,
        }
    }
}

impl From<pb::DeleteRequest> for DeleteRequest {
    fn from(r: pb::DeleteRequest) -> Self {
        Self {
            key: r.key,
            expected_mod_revision: r.expected_mod_revision,
        }
    }
}

impl From<DeleteRequest> for pb::DeleteRequest {
    fn from(r: DeleteRequest) -> Self {
        Self {
            key: r.key,
            expected_mod_revision: r.expected_mod_revision,
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
        })
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
        };
        let back: PutRequest = pb::PutRequest::from(put.clone()).into();
        assert_eq!(
            (back.key, back.value, back.expected_mod_revision),
            (put.key, put.value, put.expected_mod_revision)
        );

        let delete = DeleteRequest {
            key: Bytes::from_static(b"/app/a"),
            expected_mod_revision: Some(0),
        };
        let back: DeleteRequest = pb::DeleteRequest::from(delete.clone()).into();
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
            };
            let back: PutRequest = pb::PutRequest::from(put).into();
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
            };
            let wire = pb::MutationResponse::from(response.clone());
            let back = MutationResponse::try_from(wire).expect("known outcome decodes");
            assert_eq!(back, response);
        }
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
            };
            let error = MutationResponse::try_from(wire).expect_err("unknown tag is refused");
            assert!(
                matches!(error, ConfigError::InvalidArgument { .. }),
                "expected InvalidArgument for tag {tag}, got {error:?}"
            );
        }
    }
}
