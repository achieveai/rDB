//! The contracts every rDB crate and every kernel module speaks.
//!
//! One module per seam, so a reviewer can read a seam without reading the rest, and so the
//! ownership table in spike §3 maps to files rather than to regions of one file.
//!
//! | Module | Seam (spike §4) | Specification |
//! |---|---|---|
//! | [`ids`] | identities shared by every seam | §2 |
//! | [`digest`] | the value lineage, envelopes and traces are compared by | §6.1, §8.1 |
//! | [`version`] | refuse an unknown mandatory version before decoding | §5.4 |
//! | [`errors`] | the public error set and its retry rules | §5.4 |
//! | [`txn`] | the client request/result contract | §5.1, §5.3 |
//! | [`membership`] | the pinned copy set; authenticated peer to copy | §6.2 |
//! | [`envelope`] | the replication envelope and its acknowledgement | §6.1 |
//! | [`event`] | event/effect: `step(state, event) -> effects` | §5.2 |
//! | [`time`] | logical ticks, timers, the bounded-clock estimate | §7.2 |
//! | [`storage`] | atomic batches, the three typed watermarks, the read view | §5.2, §6.1 |
//! | [`control`] | single-record CAS and a watch that may gap | §7.1 |
//! | [`transport`] | untrusted, unordered delivery | §6.1 |
//! | [`trace`] | what the harness declares and the oracle folds | spike §4, §6 |
//! | [`authority`] | the authority decision and view; the shared partition mode | §7.2, §7.3 |

pub mod authority;
pub mod control;
pub mod digest;
pub mod envelope;
pub mod errors;
pub mod event;
pub mod ids;
pub mod membership;
pub mod storage;
pub mod time;
pub mod trace;
pub mod transport;
pub mod txn;
pub mod version;
