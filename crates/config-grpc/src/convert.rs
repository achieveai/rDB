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
