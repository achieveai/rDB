//! `rdb_dev`: three rDB nodes and three rEtcd voters in one process, driven line by line.
//!
//! ```text
//! cargo run -p rdb-api --example rdb_dev -- [--dir D] [--log-dir L] [--script F] [--keep]
//!                                           [--hold 1-2,1-3] [--put-timeout-ms N]
//!                                           [--dedup-cap N] [--dedup-retention-ms N]
//!                                           [--control host:port[,host:port...]] [--requests F]
//!                                           [--abort-after-commit] [--pause-before-sync N=MS]
//! cargo run -p rdb-api --example rdb_dev -- --help
//! ```
//!
//! `--help` (or `-h`) prints this text to stdout and exits 0; arguments after it are not read.
//!
//! `--control E` uses an external rEtcd (such as `scripts/local-cluster.sh up`) at the client
//! endpoints E instead of three in-process voters. `--dir` is then never removed, so a second
//! run with the same `--dir` and E reopens it. The `control` verbs reach rdb_dev's own voters
//! and are refused under `--control`. Without `--control`, a `--dir` this run did not create is
//! never removed either; `Db::open` decides whether it can be opened.
//!
//! `--requests F` appends every write request this run records for `retry` to F, one JSON
//! line each, synced, after its answer, so a later run with the same `--requests F` resends an
//! earlier run's request unchanged: `retry <request>` after a reopen (M10 S1-4). The request
//! names the generation it was sent in, and a reopen starts a new one, so the resend answers
//! `GENERATION_CHANGED` and applies nothing (spec §5.3); `status <request>` then says what
//! became of it. A fresh compile under the same id is no resend: it reads the object's moved
//! version, so its digest differs and it answers `REQUEST_ID_REUSE`.
//! F holds keys and values, so it is not a log; use one F per `--dir`. A write still in
//! flight when the process dies is not in F. Past the dedup retention a request is no longer
//! recognised (`--dedup-retention-ms`).
//! A line of F that does not read, such as a last line a kill cut short, is skipped with a
//! `warn` naming its line; the other requests still retry. An append to F that fails is said
//! with `err`, and the run exits 1.
//!
//! `--abort-after-commit` is a dev and test hook (A-R184): the process aborts as soon as a
//! recovery commits, before the new generation is linked or marked activated. With `--control`
//! and the same `--dir`, the next run must roll that commit forward. Nothing is printed when it
//! fires; the abort may lose the last log lines.
//!
//! `--pause-before-sync N=MS` is a dev and test hook (A-R198): node N's next barrier sync (the
//! fsync a recovery waits on before it commits) sleeps MS ms first, once, as a slow disk would.
//! It is said at start and logged as `pause_before_sync_armed` (warn); the sleep itself is
//! logged as `test_sync_paused` with `node` and `millis`. N is 1, 2 or 3; repeat the flag for
//! more nodes. A bootstrap and a reopen both sync before they commit.
//!
//! `--dedup-cap N` lowers T1's dedup cap, the retained answers allowed before a new write is
//! `OVERLOADED`, so a full index can be reached by hand. Above 65536 the open is refused.
//! `--dedup-retention-ms N` keeps every retained answer N ms instead of 24 h. A published
//! write's dedup entry and status go with the owner's trim duty (`host_duty kind=trim`, once a
//! second). A `CONDITION_FAILED` answer goes N ms after it was given, even when nothing
//! publishes after it, so a cap filled by condition failures alone also frees up by hand. A
//! `retry` of a trimmed request is no longer recognised and is sent afresh; every rdb_dev write
//! carries a version condition, so it answers `CONDITION_FAILED` rather than applying twice.
//!
//! Limits, checked before anything is compiled, sent or read: an object id is at most 1,024
//! bytes, for every write and read; a `doc`, `map`, `set` or `list` write carries at most 255
//! ops, and a `txn` at most 255 puts. A map or set write also writes its root, so with distinct
//! keys the compile's 255-write cap refuses it first, at 255 keys. `members` and `items` take
//! `--limit` 1,000 at most, a count of items, not bytes. A text or byte map key or set member,
//! in a write or in `members --after`, is at most 1,024 bytes (`é` counts 2). A request over a
//! limit answers `INVALID_ARGUMENT`, and its detail ends `request R not sent` (a write) or
//! `request R not read` (a read): R is the id it used up. A write also prints `request=-`; a
//! read prints no `request=` field, so its R is in the detail only.
//!
//! Commands are read from `--script` or stdin, one per line. A `#` at the start of a
//! word starts a comment; one inside a word, as in `{"tag":"#1"}`, or inside a `"..."`
//! string, as in `{"m":"hello #world"}`, is kept, so a comment after JSON needs a space
//! before its `#`: `{"a":1} # note`, not `{"a":1}# note`. Words split on
//! spaces; `"..."` makes one word, so `""` is an empty object or value. A value verb (`doc`,
//! `map`, `set`, `members`, `list`, `items`) reads its JSON values from the line as written,
//! quotes and all:
//!
//! ```text
//! put <object> <value> [--if-version N]   write a byte string
//! put --as <n> <object> <value> [--if-version N]
//!                                         the same put under request id app_request_id(n),
//!                                         your own number n below 2^63, used once; recorded
//!                                         for `retry` like any put
//! put& <object> <value> [--if-version N]  the same put in the background: prints `bg
//!                                         <object>: queued` and returns at once, and `bg
//!                                         <object>: <answer>` when it lands; not remembered
//!                                         for `retry`. Queued is not sent: `sleep` before a
//!                                         next command that needs the put in flight
//! txn [--deadline-ms N] <op>...           write every op as one transaction, or none. An op
//!                                         is [<group>:]<object>=<value>[@<version>]: the
//!                                         object ends at the first `=`, a group is only a
//!                                         leading `digits:` (default 1), and a version only a
//!                                         trailing `@digits`. The deadline defaults to the put
//!                                         timeout; past 30000 it is refused
//! txn& [--deadline-ms N] <op>...          the same txn in the background, as `put&`: `bg txn:
//!                                         queued`, then `bg txn: <answer>`; not remembered
//! retry [<request>]                       send a write's request again, unchanged: the
//!                                         latest one's, or the one this session sent as
//!                                         <request>, or with `--requests F` the one an
//!                                         earlier run sent as <request>. With nothing to
//!                                         send it says `nothing to retry` and fails nothing
//! retry --id <request>                    the same as `retry <request>`
//! retry [<request>] --deadline-ms N       the same request with a fresh deadline of N ms
//! retry [<request>] --payload <value>     the same put and request id with another value,
//!                                         compiled afresh (REQUEST_ID_REUSE); not remembered.
//!                                         A `put --as` put's only: under an id this Db
//!                                         numbered it is refused and nothing is sent
//!                                         (A-R195), and never a txn's
//! doc <object> [--absent | --expect V] <op>...
//!                                         change a JSON document, ops in order, as one write.
//!                                         An op is `--json J` (the whole document), `set
//!                                         <path> J`, `remove <path>` or `incr <path> N`; a path
//!                                         is `/a/0`. `--absent`: only if missing; `--expect
//!                                         V`: only at version V; neither: at the version the
//!                                         owner sees. `retry` resends it unchanged; `retry
//!                                         --payload` refuses it
//! map|set <object> [--absent | --expect V] <op>...
//!                                         change a map or a set, ops in order, as one write.
//!                                         An op is `put K J`, `add K`, `del K` or `need K
//!                                         present|absent`; K is a JSON scalar, or
//!                                         `{"$bytes":"<hex>"}` for a byte string. A map takes
//!                                         `put`, a set `add`: the other is refused
//!                                         (`KindMismatch`). No ops with `--absent` creates an
//!                                         empty one. Guards and `retry` as for `doc`
//! members <object> [--after K] [--limit N]
//!                                         a page of a map or set in key order (type, then
//!                                         value: `3` before `1.5`), after K or
//!                                         from the first, N at most (default 100): `map
//!                                         version=V count=C listed=L ...`, then `  K = J` per
//!                                         entry or `  K` per member
//! list <object> [--absent | --expect V] <op>...
//!                                         change a list, ops in order, as one write. An op is
//!                                         `push J`, `insert P J`, `remove P`, `replace P J` or
//!                                         `move P Q`. A negative P counts from the end (`-1`
//!                                         is the last item), read against the list as the
//!                                         earlier ops left it. No ops with `--absent` creates
//!                                         an empty list. Guards and `retry` as for `doc`
//! items <object> [--from P | --token G:V:P] [--limit N]
//!                                         a page of a list from position P (default 0; `-1`
//!                                         the last) or from a page's token, N at most
//!                                         (default 100): `list version=V count=C listed=L
//!                                         ...`, then `  P J` per item (`inline=false` when it
//!                                         has its own record), then `token=G:V:P` when items
//!                                         remain. A token the list moved past answers
//!                                         `STALE_CONTINUATION`: read again from the start
//! blob upload <object> [--absent | --expect V] --upload U --chunk-size N <text>
//!       [--stop-after K]                  store <text> as a blob: chunk i is its bytes from
//!                                         i*N, each one write (`chunk i ok ...`), then the
//!                                         publish (`publish ok ...`). U, the upload id, is 32
//!                                         hex digits the caller picks. The guard is the
//!                                         publish's; chunks take none. It stops at a refused
//!                                         chunk. `--stop-after K` sends K chunks, then prints
//!                                         the `blob publish` line to finish with. `""` is a
//!                                         blob of no chunks
//! blob chunk <object> --upload U --index I <text>
//!                                         store one chunk. The same bytes again answer
//!                                         `unchanged (already stored)`, and send nothing
//! blob publish <object> [--absent | --expect V] --upload U --size N --chunk-size N
//!       --sha256 H                        publish upload U as the blob; H is 64 hex digits.
//!                                         That blob already there and published answers
//!                                         `unchanged (already published)`; already there
//!                                         but not yet published, `UNKNOWN_OUTCOME` with
//!                                         `request=-`: ask `status` of the publish that
//!                                         wrote it. While reads are refused, as after a
//!                                         fence, it answers as `read` does. Each sends
//!                                         nothing
//! blob gc <object> --floor F             delete one batch of the object's chunks its blob
//!                                         does not name, at versions up to F. Nothing left to
//!                                         delete answers `unchanged (nothing to collect)`
//! read <object> [--offset O --len L]      read a blob at the publication barrier: `blob
//!                                         version=V size=S chunks=N upload=U ...`, with
//!                                         `bytes="..."` for a range of at least one byte.
//!                                         Without `--offset`/`--len` it reads the manifest
//!                                         only, so a damaged chunk shows on a range read only
//! get <object>                         read at the publication barrier: `value=` for a byte
//!                                         string, `doc=<json>` for any other document; another
//!                                         kind is refused, naming `members`, `items` or `read`
//! get <object> --previous                 read the previously published view at once, never
//!                                         waiting for a write in flight (ReadPrevious)
//! status <request> [<generation>]         what became of a request this session sent
//! nodes                                   each node's view of partition 1, with the boot its
//!                                         running process has
//! node restart <n>                        stop node n and start it again on its data at its
//!                                         boot plus one; prints `node n boot b`. The new
//!                                         process holds no role and stays out until the folder is
//!                                         opened again (#42). So
//!                                         any restart stops writes, safely, for the life of
//!                                         this run: after a secondary's, L1 pauses within
//!                                         about 2 s of the next write (`PROTECTION_PAUSED`)
//!                                         because that copy is never durable again; after
//!                                         the owner's, no node
//!                                         holds a grant. Reads: after a secondary's they keep
//!                                         answering, unless a write was in flight that no
//!                                         other copy can take yet: then fresh reads answer
//!                                         `UNKNOWN_OUTCOME` and `get --previous` still
//!                                         answers. That lasts while no copy that did not
//!                                         restart can take the write: if the other secondary
//!                                         was only unreachable, healing its link resolves
//!                                         it; only with both secondaries restarted does it
//!                                         wait for M10. After the owner's every read is
//!                                         `UNAVAILABLE`. Every `PROTECTION_PAUSED`,
//!                                         `UNAVAILABLE`, `UNKNOWN_OUTCOME` or `NOT_PRIMARY`
//!                                         after it names the restart, and so does a `status`
//!                                         of `Unknown` for a request this session sent: that
//!                                         is not evidence of a lost write. After one
//!                                         secondary's restart it resolves once the other can
//!                                         take the write; after the owner's, or both
//!                                         secondaries', it waits for the node to rejoin.
//!                                         Restarting is not a recovery: catch-up and re-pin
//!                                         are M10 (achieveai/rDB#42). To write again,
//!                                         quit and start rdb_dev again. Refused until the
//!                                         first `ready`
//! admin clear-grant <n>                   remove grants/<n> if node n's old process left it
//!                                         and it may go: `absent`, `current` (the running
//!                                         process's own), `cleared rev=R`, `conflict`, or
//!                                         `refused ...`. Only `not-proven-expired` clears by
//!                                         waiting, and says how long; `frozen` never clears
//!                                         in M9 (no takeover runs), `partition-in-transfer
//!                                         pP` waits for pP to be `serving` again. Clearing
//!                                         never lets a restarted owner write again in M9
//! admin clear <o>                        clear a damaged object: prove it damaged, then
//!                                         delete all of it, healthy entries too. Its root
//!                                         or any one entry being damaged is enough. To keep
//!                                         the rest, write over a bad map entry or set member
//!                                         first (`map put`/`del` or `set del` of its key);
//!                                         to keep the rest under a damaged root, copy the
//!                                         healthy entries out first with `members --after`,
//!                                         carrying on past a bad key with `--after` that
//!                                         key. A byte 0/3 or newer
//!                                         build's record anywhere in the object refuses the
//!                                         clear: open it with the newer build. It deletes
//!                                         one batch of its records per call, each at the version
//!                                         it was read at (`clear ok ...`). Repeat until `clear
//!                                         unchanged (nothing to clear; nothing sent)`. A
//!                                         healthy object answers `CONDITION_FAILED`, a newer
//!                                         build's record `INCOMPATIBLE_VERSION`, both with
//!                                         `request=-`. Nothing left but not yet published
//!                                         answers `UNKNOWN_OUTCOME` with `request=-`: if an
//!                                         earlier clear sent deletes, ask its `status`. While
//!                                         reads are refused, as after a fence, it answers as
//!                                         `read` does. `retry` resends it unchanged
//! admin damage <o> [--element K | --item P] [--as digest|kind0|newer] [--api-version N]
//!                                         dev only: overwrite the object's stored root, or
//!                                         with --element its stored map entry or set member
//!                                         K (JSON, as `map` takes it), or with --item list
//!                                         item P's own record, with a sealed document
//!                                         whose digest fails (`digest`, the default), whose
//!                                         kind is 0x00 (`kind0`), or whose codec is 0x02,
//!                                         digest correct (`newer`: a newer build's record). A
//!                                         set member holding any bytes is damage too. A set
//!                                         member has no envelope, so on a set --element takes
//!                                         `digest` only; `kind0` and `newer` are refused, and
//!                                         nothing is written. For a newer build's set, damage
//!                                         the root with --as newer. With
//!                                         --element the root's own bytes are written again
//!                                         in the same write, so a read meets that fault and
//!                                         a `map put`/`del` of K repairs a `digest` entry. A
//!                                         missing record is refused, and so is an item kept
//!                                         inside its block (`no stored record`). --item does
//!                                         not write the list's root, so whatever `--as` says,
//!                                         a read answers `element version V is above its
//!                                         root's version`; the item digest path is rdb-value's
//!                                         row d2_a_write_refuses_a_damaged_item_record_as_
//!                                         the_read_does
//!                                         --api-version N sends the write as API version N
//!                                         (default 1): any other is another build's request
//!                                         (newer or older), refused before apply
//! admin damage-chunk <o> --upload U --index I
//!                                         dev only: overwrite that stored chunk with other
//!                                         bytes, so a range read over it (`--offset`/`--len`)
//!                                         is `CORRUPT_BLOB`. A plain `read` reads the
//!                                         manifest only and does not see it
//! control                                 the partitions/ and grants/ records in rEtcd, read
//!                                         from the rEtcd leader now, never through the Db's
//!                                         store
//! control voters                          `voters leader=N running=[..] bound=N`: the rEtcd
//!                                         leader, the running voters, and the voter the Db's
//!                                         control store calls now
//! control stop <voter>                    stop one rEtcd voter (1, 2 or 3). The Db's store
//!                                         moves off it first; refused if no other runs
//! control start <voter>                   start a stopped voter again on its own data
//! control gap                             compact rEtcd through its current revision, then
//!                                         drop the Db's watches: a re-watch from an older
//!                                         cursor is refused `RevisionCompacted` and reloads
//! control drop-watches                    end the Db's watch streams; they re-watch from
//!                                         their cursors with no reload
//! control fail list <unavailable|deadline|notleader|invalid|off>
//!                                         every list the Db's store sends answers that error,
//!                                         until `off`. `control` reads are not affected
//! control fail put <prefix> <lose-reply|drop|off> [n]
//!                                         the next n puts (default 1) the Db's store sends
//!                                         under <prefix>, such as `grants/` or
//!                                         `partitions/`, answer `DeadlineExceededUnknownOutcome`:
//!                                         `lose-reply` after rEtcd applied the put, `drop`
//!                                         without sending it. Then the fault is spent; `off`
//!                                         ends it sooner
//! control lifecycle <p> <serving|fencing|drained>
//!                                         rewrite partitions/<p>'s lifecycle on the rEtcd
//!                                         leader, at its revision. Put `serving` back after
//! control freeze-grant <n>                set grants/<n>'s frozen bit on the rEtcd leader, at
//!                                         its revision. Nothing in M9 undoes it: run it last
//! link hold|heal <a>-<b> | link heal all | links
//! link drop <a>-<b>                       lose every peer message node a sends to node b,
//!                                         one direction only, until `link heal` of the pair
//!                                         or `link heal all`; the sender is never told. A
//!                                         hold delays, a drop loses. `links` prints both
//! wait recovered|ready [<seconds>]        block until the partition gets there
//!                                         (default 30). `wait ready` asks Db::wait_ready: after
//!                                         node 1 restarted, or a failed heal, it stops at once,
//!                                         says the next step, and the run exits 1. Every 10 s
//!                                         it waits it says `still waiting for ready: recovery
//!                                         phase P`, node 1's phase then
//! sleep <millis>
//! quit
//! ```
//!
//! While it runs, a poller prints the partition's progress as it changes: `recovered gen=N`,
//! `waiting to admit writes (l1=.. authority=..)`, and `ready` once node 1 admits writes: its L1
//! admits and its A1 holds a grant. `not ready: writes not admitted` when either half stops, the
//! waiting line then saying which. When node 1's grant is fenced, that line says why. While A1
//! still polls its own record it adds that writes resume by themselves, so the node is not
//! restarted (#42); once the poll has stopped, or for a frozen, foreign-boot or other-authority
//! record, it says nothing in M9 replaces the grant and writes stay stopped for the run. After a secondary restarts,
//! L1 still admits writes for a while, so the line reads `ready for now; writes pause soon, at
//! the latest about 2 s after the next write, because ..` until they do: about 2 s after the
//! restart, or, after a fence, about 2 s after the next write (tester q8). A put before `ready`
//! is refused, never left hanging. A recovery committed below `Active` that does not activate
//! prints `stalled node=N recovery_rebuild_stalled ...`, and again whenever the line changes:
//!
//! - when F1 pinned its rebuild, at F1's rebuild deadline (the discovery window, ~2 s) with
//!   `unproven=[..]`, the copies that have not proved the pinned point;
//! - when F1 never pinned it, after `REBUILD_PIN_WAIT_MILLIS` (5 s), with `waited_ms=` and
//!   "F1 never pinned its rebuild".
//!
//! The line clears when the partition activates, or when a newer recovery starts watching its
//! own rebuild, and the poller then prints `stall cleared node=N recovery=.. recovered=..
//! admits=..`.
//!
//! The Db's control store follows the rEtcd leader (`support/hostile_store.rs`): a call a
//! voter answers `NotLeader`, or refuses because it stopped, sends the next call to the leader
//! then, logged as `control_store_rebind`.
//!
//! The JSONL log goes to `<log-dir>/rdb_dev.jsonl`; its path is printed to stderr as `log=`.

#[path = "support/hostile_store.rs"]
mod hostile_store;
#[path = "../../rdb-value/examples/doc_scenario/json.rs"]
mod json;

use std::io::{BufRead, Write as _};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use config_core::{
    ConfigStore, GetRequest, ListRequest, MutationOutcome, NodeId as VoterId, PutRequest,
};
use config_testkit::cluster::{Cluster, StorageKind};
use config_testkit::TestTimers;
use rdb_api::admin::{self, Clearance};
use rdb_api::host::NodeStatus;
use rdb_api::{
    ApiError, Change, CollectionKind, Db, DbConfig, Delta, ElemOp, Expected, Int, ListEdit, Look,
    Op, OpenError, Path, PutError, ReadOk, Timeouts, Token, TxnPut, Value,
};
use rdb_core::authority::grant::{ClearRefusal, GrantRecord};
use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
use rdb_core::contracts::authority::DenyReason;
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::errors::RetryRule;
use rdb_core::contracts::event::Budgets;
use rdb_core::contracts::ids::{AffinityId, BootId, Generation, NodeId, PartitionId, RequestId};
use rdb_core::contracts::txn::{TxnRequest, TxnStatus};
use sha2::{Digest as _, Sha256};

use hostile_store::{HostileStore, ListFault, PutFault};

/// The control voters' Raft timers. Faster than the harness default, whose failover can outlast
/// A1's write horizon (walk 1L): `tests::control_failover_millis` is 1000 ms here, 3000 ms there.
const CONTROL_TIMERS: TestTimers = TestTimers {
    heartbeat: Duration::from_millis(100),
    election_timeout_min: Duration::from_millis(300),
    election_timeout_max: Duration::from_millis(500),
};

#[derive(Debug)]
struct Args {
    dir: PathBuf,
    log_dir: PathBuf,
    script: Option<PathBuf>,
    keep: bool,
    hold: Vec<(NodeId, NodeId)>,
    /// How long a put waits for its answer. Short values reach the Db's own timeout,
    /// before the kernel answers (walk w24).
    put_timeout: Option<std::time::Duration>,
    /// T1's dedup cap, lowered as a dev budget.
    dedup_cap: Option<usize>,
    /// How long a published write's answer is retained, shortened as a dev budget.
    dedup_retention: Option<Duration>,
    /// `--control E`: an external rEtcd's client endpoints. The run then starts no voters and
    /// never removes `--dir`, so a later run can reopen it.
    control: Option<Vec<String>>,
    /// `--requests F`: where each recorded write request is appended, for a later run's `retry`.
    requests: Option<PathBuf>,
    /// `--abort-after-commit`: abort the whole process once a recovery commits, before it is
    /// linked or marked activated (A-R184). The next open must roll it forward.
    abort_after_commit: bool,
    /// `--pause-before-sync <node>=<ms>`, repeatable: that node's next barrier sync sleeps
    /// `ms` before it fsyncs, once (A-R198).
    pause_before_sync: Vec<(NodeId, Duration)>,
    /// `--help` or `-h`: print [`usage`] and exit 0, running nothing.
    help: bool,
}

/// The usage: this file's own `//!` header, so the help and the source comment cannot drift.
/// The header's code fences are left out; everything else is printed as written.
fn usage() -> String {
    include_str!("rdb_dev.rs")
        .lines()
        .take_while(|line| line.starts_with("//!"))
        .map(|line| {
            line.strip_prefix("//! ")
                .or_else(|| line.strip_prefix("//!"))
                .unwrap_or(line)
        })
        .filter(|line| !line.starts_with("```"))
        .fold(String::new(), |mut text, line| {
            text.push_str(line);
            text.push('\n');
            text
        })
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let pid = std::process::id();
    let mut parsed = Args {
        dir: PathBuf::from(format!("C:/rdb_test_data/m9/dev-{pid}")),
        log_dir: PathBuf::from(format!("C:/rdb_test_data/m9/logs-{pid}")),
        script: None,
        keep: false,
        hold: Vec::new(),
        put_timeout: None,
        dedup_cap: None,
        dedup_retention: None,
        control: None,
        requests: None,
        abort_after_commit: false,
        pause_before_sync: Vec::new(),
        help: false,
    };
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().ok_or_else(|| format!("{name} needs a value"));
        match arg.as_str() {
            "--dir" => parsed.dir = PathBuf::from(value("--dir")?),
            "--log-dir" => parsed.log_dir = PathBuf::from(value("--log-dir")?),
            "--script" => parsed.script = Some(PathBuf::from(value("--script")?)),
            "--keep" => parsed.keep = true,
            "--help" | "-h" => {
                parsed.help = true;
                return Ok(parsed);
            }
            "--abort-after-commit" => parsed.abort_after_commit = true,
            "--pause-before-sync" => parsed
                .pause_before_sync
                .push(parse_pause(&value("--pause-before-sync")?)?),
            "--requests" => parsed.requests = Some(PathBuf::from(value("--requests")?)),
            "--control" => {
                let endpoints: Vec<String> = value("--control")?
                    .split(',')
                    .map(str::trim)
                    .filter(|e| !e.is_empty())
                    .map(str::to_owned)
                    .collect();
                if endpoints.is_empty() {
                    return Err("--control takes host:port[,host:port...]".to_owned());
                }
                parsed.control = Some(endpoints);
            }
            "--put-timeout-ms" => {
                let text = value("--put-timeout-ms")?;
                let millis = text
                    .parse::<u64>()
                    .map_err(|_| format!("--put-timeout-ms takes a number, not {text:?}"))?;
                parsed.put_timeout = Some(std::time::Duration::from_millis(millis));
            }
            "--dedup-cap" => {
                let text = value("--dedup-cap")?;
                parsed.dedup_cap = Some(
                    text.parse::<usize>()
                        .map_err(|_| format!("--dedup-cap takes a number, not {text:?}"))?,
                );
            }
            "--dedup-retention-ms" => {
                let text = value("--dedup-retention-ms")?;
                let millis = text
                    .parse::<u64>()
                    .map_err(|_| format!("--dedup-retention-ms takes a number, not {text:?}"))?;
                parsed.dedup_retention = Some(Duration::from_millis(millis));
            }
            "--hold" => {
                for pair in value("--hold")?.split(',') {
                    parsed.hold.push(parse_pair(pair)?);
                }
            }
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    Ok(parsed)
}

fn parse_node(text: &str) -> Result<NodeId, String> {
    match text.trim().parse::<u32>() {
        Ok(n @ 1..=3) => Ok(NodeId(n)),
        _ => Err(format!("{text:?}: a node is 1, 2 or 3")),
    }
}

/// `--pause-before-sync`'s `<node>=<ms>`; every refusal names the flag.
fn parse_pause(text: &str) -> Result<(NodeId, Duration), String> {
    let (node, millis) = text
        .split_once('=')
        .ok_or_else(|| format!("--pause-before-sync takes <node>=<ms>, not {text:?}"))?;
    let node = parse_node(node).map_err(|e| format!("--pause-before-sync: {e}"))?;
    let millis = millis.trim().parse::<u64>().map_err(|_| {
        format!("--pause-before-sync: {millis:?} is not a number of ms, in {text:?}")
    })?;
    Ok((node, Duration::from_millis(millis)))
}

fn parse_pair(text: &str) -> Result<(NodeId, NodeId), String> {
    let (a, b) = text
        .split_once('-')
        .ok_or_else(|| format!("{text:?}: a link is <a>-<b>"))?;
    let (a, b) = (parse_node(a)?, parse_node(b)?);
    if a == b {
        return Err(format!("{text:?}: a link joins two different nodes"));
    }
    Ok((a, b))
}

fn main() -> ExitCode {
    let args = match parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("rdb_dev: {message}");
            return ExitCode::from(2);
        }
    };
    if args.help {
        print!("{}", usage());
        return ExitCode::SUCCESS;
    }
    // An existing --dir goes to `Db::open`, which decides whether it can be reopened (M10
    // S1-1); rdb_dev refuses nothing about it itself.
    let mut config = config_log::LogConfig::for_application("rdb_dev");
    config.dir.clone_from(&args.log_dir);
    config.file_name = "rdb_dev.jsonl".to_string();
    let guard = match config_log::init(config) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("rdb_dev: log init: {e}");
            return ExitCode::from(2);
        }
    };
    eprintln!("log={}", args.log_dir.join("rdb_dev.jsonl").display());
    // The gate's port range keeps the voters' port-0 binds out of the host's busy dynamic pool.
    // `run` names the voters' stores under --dir itself; this keeps the harness's own scratch
    // folder there too, never %TEMP%. Only `main` sets it: it is this process's, not a row's.
    if std::env::var_os("RETCD_TEST_PORT_RANGE").is_none() {
        std::env::set_var("RETCD_TEST_PORT_RANGE", "20000-26999");
    }
    if std::env::var_os("RETCD_TEST_DATA_DIR").is_none() {
        std::env::set_var("RETCD_TEST_DATA_DIR", args.dir.join("control"));
    }
    let code = run(&args, Db::open);
    drop(guard);
    code
}

/// The run, with `open` in place of [`Db::open`] so a row can fail it.
fn run<Open, Opening>(args: &Args, open: Open) -> ExitCode
where
    Open: FnOnce(DbConfig, Arc<dyn ConfigStore>, tokio::runtime::Handle) -> Opening,
    Opening: std::future::Future<Output = Result<Db, OpenError>>,
{
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("rdb_dev: runtime: {e}");
            return ExitCode::from(1);
        }
    };
    let config = DbConfig {
        dir: args.dir.clone(),
        hold: args.hold.clone(),
        timeouts: Timeouts {
            put: args.put_timeout.unwrap_or(Timeouts::default().put),
            ..Timeouts::default()
        },
        dedup_cap: args.dedup_cap,
        dedup_retention: args.dedup_retention,
    };
    // Before the voters start (PC-2): a refused option starts nothing.
    if let Err(e) = config.check() {
        eprintln!("rdb_dev: open: {e}");
        return ExitCode::from(1);
    }
    // Whether --dir held anything before this run: such a directory is never removed (M10
    // S1-1), whatever `open` decides about it.
    let existed = std::fs::read_dir(&args.dir).is_ok_and(|mut d| d.next().is_some());
    // Before the open, with `--dir` spelled as `DbConfig::dir` is: the hook matches on it.
    if args.abort_after_commit {
        rdb_api::host::abort_after_commit(&args.dir);
        tracing::warn!(dir = %args.dir.display(), "abort_after_commit_armed");
        say("abort-after-commit: this process aborts once a recovery commits");
    }
    for &(node, pause) in &args.pause_before_sync {
        let node_dir = args.dir.join("nodes").join(node.0.to_string());
        rdb_api::host::pause_before_sync(&node_dir, pause);
        let ms = u64::try_from(pause.as_millis()).unwrap_or(u64::MAX);
        tracing::warn!(node = node.0, ms, "pause_before_sync_armed");
        say(&format!(
            "pause-before-sync: node {}'s next barrier sync sleeps {ms} ms before it fsyncs",
            node.0
        ));
    }
    let (store, local) = match &args.control {
        Some(endpoints) => {
            match config_client::GrpcClient::connect(
                endpoints.clone(),
                config_client::GrpcClientOptions::default(),
            ) {
                Ok(client) => {
                    say(&format!(
                        "control: external rEtcd at {}",
                        endpoints.join(",")
                    ));
                    (Arc::new(client) as Arc<dyn ConfigStore>, None)
                }
                Err(e) => {
                    eprintln!("rdb_dev: --control: {e}");
                    return ExitCode::from(1);
                }
            }
        }
        None => {
            // The voters' data goes under --dir by name, not through RETCD_TEST_DATA_DIR: the
            // environment is the process's, and the rows that call `run` share one process.
            let voters = voters_dir(&args.dir);
            let mut builder = Cluster::builder()
                .nodes(3)
                .storage(StorageKind::ROCKS)
                .timers(CONTROL_TIMERS);
            for id in 1..=3 {
                let node = voters.join(format!("node-{id}"));
                if let Err(e) = std::fs::create_dir_all(&node) {
                    eprintln!("rdb_dev: create {}: {e}", node.display());
                    return ExitCode::from(1);
                }
                builder = builder.data_dir(VoterId(id), node);
            }
            let cluster = Arc::new(rt.block_on(builder.start()));
            let leader = rt.block_on(cluster.leader());
            let hostile = Arc::new(HostileStore::new(&cluster, leader));
            say(&format!("control up: 3 voters, leader {leader:?}"));
            (
                Arc::clone(&hostile) as Arc<dyn ConfigStore>,
                Some((cluster, hostile, voters)),
            )
        }
    };
    tracing::info!(
        dir = %args.dir.display(),
        existed,
        external_control = args.control.is_some(),
        holds = ?args.hold,
        "rdb_dev_start"
    );
    let mut db = match rt.block_on(open(config, Arc::clone(&store), rt.handle().clone())) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("rdb_dev: open: {e}");
            close(store, local, rt, args, existed);
            return ExitCode::from(1);
        }
    };
    // A fresh folder bootstraps; a folder with data takes over from its last run (M10 S1-1).
    if db.heal_state() == rdb_api::db::HealState::NotNeeded {
        say("bootstrap: partition 1 recovery started");
    } else {
        say("reopen: partition 1 takes over from the last run (heal started)");
    }
    let progress = Progress::default();
    let stop = AtomicBool::new(false);
    let control = local.as_ref().map(|(cluster, hostile, _)| Control {
        cluster,
        store: hostile,
    });
    let code = std::thread::scope(|scope| {
        scope.spawn(|| poll(&db, &progress, &stop));
        let code = repl(scope, args, &db, &rt, &*store, control.as_ref(), &progress);
        stop.store(true, Ordering::Relaxed);
        code
    });
    db.shutdown();
    drop(db);
    close(store, local, rt, args, existed);
    code
}

/// End the run once the Db is gone, whether it opened or not (F-006). The direct client holds
/// the voter's store open: drop every handle on it before the cluster closes its stores, or
/// their RocksDB files outlive the run. Then `--dir` goes, unless `--keep`, `--control`, or
/// it held anything before the run (`existed`): a directory this run did not create is the
/// user's, and under `--control` it is meant to be reopened.
///
/// The store holds the cluster weakly, so the cluster comes back here unless a call is
/// resolving the leader at this instant; that is waited out for up to 2 s, and past it the
/// voters are not shut down and the run says so.
fn close(
    store: Arc<dyn ConfigStore>,
    local: Option<(Arc<Cluster>, Arc<HostileStore>, PathBuf)>,
    rt: tokio::runtime::Runtime,
    args: &Args,
    existed: bool,
) {
    drop(store);
    if let Some((cluster, hostile, voters)) = local {
        drop(hostile);
        let started = Instant::now();
        let mut shared = cluster;
        let cluster = loop {
            match Arc::try_unwrap(shared) {
                Ok(cluster) => break Some(cluster),
                Err(still) if started.elapsed() >= Duration::from_secs(2) => {
                    eprintln!(
                        "rdb_dev: the cluster is still shared ({} handles); voters not shut down",
                        Arc::strong_count(&still)
                    );
                    break None;
                }
                Err(still) => {
                    shared = still;
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        };
        if let Some(cluster) = cluster {
            rt.block_on(cluster.shutdown());
            // The cluster leaves a directory it was handed to its caller; with the stores
            // closed, it goes, `--keep` or not, as the cluster's own data root did.
            remove(&voters);
        }
    }
    drop(rt);
    if args.keep || args.control.is_some() {
        return;
    }
    if !existed {
        remove(&args.dir);
    }
}

/// What the REPL's `control` verbs reach: the voters, and the Db's store over them.
struct Control<'a> {
    cluster: &'a Arc<Cluster>,
    store: &'a HostileStore,
}

/// A folder for this run's voters under `<dir>/control` that no earlier run left behind.
fn voters_dir(dir: &std::path::Path) -> PathBuf {
    let control = dir.join("control");
    (0_u32..)
        .map(|n| control.join(format!("voters-{}-{n}", std::process::id())))
        .find(|path| !path.exists())
        .expect("a free voters folder")
}

/// Remove `dir`, retrying for up to 2 s while Windows still holds a file the voters closed.
fn remove(dir: &std::path::Path) {
    let started = Instant::now();
    loop {
        match std::fs::remove_dir_all(dir) {
            Ok(()) => return,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) if started.elapsed() >= Duration::from_secs(2) => {
                eprintln!("rdb_dev: remove {}: {e}", dir.display());
                return;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
}

fn say(line: &str) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
}

/// What the poller has seen.
#[derive(Debug, Default)]
struct Progress {
    inner: Mutex<Seen>,
}

#[derive(Debug, Default, Clone)]
struct Seen {
    recovered: Option<Generation>,
    ready: bool,
    waiting: Option<String>,
    /// The reason last printed on a `ready` or `not ready` line.
    reason: Option<String>,
    faults: Vec<String>,
    /// Each node's stall line as last printed, until it clears.
    stalled: std::collections::BTreeMap<u32, String>,
    /// While `wait ready` runs: its own `reached ready` line says ready, so the poller does not
    /// (k237, lead ruling on the G4 follow-up: the poller's `ready` printed before it).
    awaiting_ready: bool,
}

impl Progress {
    fn seen(&self) -> Seen {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// Print the partition's progress lines as it changes, every 100 ms.
fn poll(db: &Db, progress: &Progress, stop: &AtomicBool) {
    while !stop.load(Ordering::Relaxed) {
        poll_once(db, progress);
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Print each progress line that changed since it was last printed. The poller calls it every
/// 100 ms, and the REPL before each command, so a change the last command made is said before
/// the next command's output, not inside it. The status is read under the lock, so a line from
/// an older reading never prints after a newer one (tester G4: a `not ready` line read while
/// `admin damage` was in flight printed inside the next `members` listing, after it published).
fn poll_once(db: &Db, progress: &Progress) {
    let mut seen = progress
        .inner
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let nodes = db.node_status();
    for node in &nodes {
        if let Some(fault) = &node.fault {
            let line = format!("fault node={} {fault}", node.node.0);
            if !seen.faults.contains(&line) {
                say(&line);
                seen.faults.push(line);
            }
        }
        if let Some(line) = stall_change(seen.stalled.get(&node.node.0), node) {
            say(&line);
            match &node.stalled {
                Some(stalled) => seen.stalled.insert(node.node.0, stalled.clone()),
                None => seen.stalled.remove(&node.node.0),
            };
        }
    }
    if let Some(owner) = nodes.iter().find(|n| n.node == NodeId(1)) {
        if owner.recovered.is_some() && owner.recovered != seen.recovered {
            seen.recovered = owner.recovered;
            say(&format!(
                "recovered gen={}",
                owner.recovered.map_or(0, |g| g.0)
            ));
        }
        let admits = owner.admits == Some(true);
        if let Some(line) = ready_step(
            &mut seen,
            owner,
            db.restart_note().as_deref(),
            &db.links().held(),
            &db.links().dropped(),
        ) {
            say(&line);
        }
        if seen.recovered.is_some() && !admits {
            let why = waiting_line(owner);
            if seen.waiting.as_ref() != Some(&why) {
                say(&why);
                seen.waiting = Some(why);
            }
        }
    } else if db.restart_failed(NodeId(1)) {
        if let Some(line) = stopped_owner_step(&mut seen, db.restart_note().as_deref()) {
            say(&line);
        }
    }
}

/// L1's pause age, in whole seconds rounded up, under the budgets [`Db::open`] runs every node
/// with: a write made while a copy is out stays unsafe and pauses writes this long after it.
const PAUSE_AGE_SECS: u64 = Budgets::SPEC_DEFAULTS.pause_age_millis.div_ceil(1000);

/// What a line says when nothing in M9 brings writes back.
const STUCK: &str =
    "writes stay stopped for this run: to write again, quit and start rdb_dev again";

/// The reason a `ready` or `not ready` line carries, when a user could act on it wrongly. Each
/// part is derived from the state that decides it (tester walk 7, W7-D1, W7-D2).
///
/// After a restart (`restart_note`), while node 1 still admits writes, L1 pauses them soon: on
/// its own within about 2 s of the restart (walk 6c, tester r1, with no write), and at the
/// latest about [`PAUSE_AGE_SECS`] after the next write, which stays unsafe with a copy out
/// (tester r3). So the line promises "soon" and that bound, never a pause tied only to the
/// next write (W7-D1). Once node 1 does not admit, the line says writes stay stopped only when
/// that holds: L1 paused or reprotecting, which with a copy out nothing in M9 resumes, or the
/// owner itself restarted. Not admitting because of a fence the poll can lift (FR, tester r3)
/// promises the resume and then the pause (W7-D2).
///
/// A fenced grant. While A1's self-replace poll is live it looks stuck but is not: A1 replaces
/// the grant itself once the old one's expiry is proven, and a restart there would end recovery
/// for the run (Message 6). The line promises that only while the poll is live and the reason
/// is not one [`never_replaced`] names (F-004): once the poll stopped (a frozen, foreign or
/// deleted record, F-016), nothing in M9 lifts the fence.
///
/// With no restart, L1 paused or reprotecting while `held` holds a link of node 1's keeps writes
/// out until those links heal and the copies catch up (W8-D2): the line names the links, fenced
/// or not, as a condition. Without a held link (startup) the copies catch up by themselves.
///
/// Not fenced, with A1's self-replace stopped (W8-D1): stuck, as [`gave_up_line`] says.
///
/// F1 blocked comes first (W10-D1): stuck, as [`blocked_line`] says, over any other reason.
/// F1 resolving its activation resumes by itself ([`resolving_line`]), over the fence's resume
/// line and the held-link line, but never over a stuck state: a fence nothing lifts, a stopped
/// self-replace, or a restart with L1 refusing. After a restart, the pause follows the resume.
fn ready_reason(
    owner: &NodeStatus,
    admits: bool,
    restart_note: Option<&str>,
    held: &[(NodeId, NodeId)],
    dropped: &[(NodeId, NodeId)],
) -> Option<String> {
    let fence = owner.fence.filter(|_| !admits);
    if let Some(line) = blocked_line(owner).filter(|_| !admits) {
        return Some(match restart_note {
            Some(note) => format!("{line}; {note}"),
            None => line,
        });
    }
    let gave_up =
        (!admits && fence.is_none() && owner.replace_stopped).then(|| gave_up_line(owner));
    let resolving = resolving_line(owner, held, dropped)
        .filter(|_| !admits && fence.is_none_or(|reason| fence_lifts(owner, reason)));
    let Some(note) = restart_note else {
        if let Some(line) = gave_up
            .or(resolving)
            .or_else(|| waits_on_line(owner, held, dropped))
        {
            return Some(line);
        }
        return match (fence, links_wait(owner, held, dropped)) {
            (Some(reason), _) => Some(fence_line(owner, reason, held, dropped)),
            (None, Some((mode, links))) if !admits => Some(format!(
                "l1={mode} and {links}: writes wait for node {}'s copies to catch \
                 up, which needs those links: heal them (link heal all)",
                owner.node.0
            )),
            (None, _) => None,
        };
    };
    let pause = format!(
        "pause soon, at the latest about {PAUSE_AGE_SECS} s after the next write, because {note}"
    );
    if admits {
        return Some(format!("writes {pause}"));
    }
    if owner.boot != rdb_api::host::BOOT || l1_rejecting(owner).is_some() {
        return Some(format!("{note}; {STUCK}"));
    }
    if let Some(line) = gave_up {
        return Some(format!("{line}; {note}"));
    }
    if let Some(line) = resolving {
        return Some(format!("{line}; once they resume, they {pause}"));
    }
    Some(match fence {
        Some(reason) if fence_lifts(owner, reason) => {
            format!(
                "{}; once they resume, they {pause}",
                fence_line(owner, reason, held, dropped)
            )
        }
        Some(reason) => format!("{}; {note}", fence_line(owner, reason, held, dropped)),
        None => format!("writes are not admitted now; if they come back, they {pause}"),
    })
}

/// L1's mode while it refuses writes, `paused` or `reprotecting`: with a copy out it waits on
/// that copy and its links, and after a restart nothing in M9 brings the copy back.
fn l1_rejecting(owner: &NodeStatus) -> Option<&str> {
    owner
        .protection
        .as_deref()
        .filter(|mode| matches!(*mode, "paused" | "reprotecting"))
}

/// L1's mode while it refuses writes, and which of `owner`'s links `held` holds or `dropped`
/// drops, when there are both: writes then wait on those links (W8-D2). A dropped direction
/// keeps the copies from catching up as a held link does (M9 S3 resume-line table). The
/// second part reads `links 1-2 are held`, `links 2->1 are dropped`, or both joined by `and`.
fn links_wait<'a>(
    owner: &'a NodeStatus,
    held: &[(NodeId, NodeId)],
    dropped: &[(NodeId, NodeId)],
) -> Option<(&'a str, String)> {
    let mode = l1_rejecting(owner)?;
    Some((mode, node_links(owner, held, dropped)?))
}

/// Which of `owner`'s links `held` holds or `dropped` drops: `links 1-2 are held`,
/// `links 2->1 are dropped`, or both joined by `and`. `None` when there are none.
fn node_links(
    owner: &NodeStatus,
    held: &[(NodeId, NodeId)],
    dropped: &[(NodeId, NodeId)],
) -> Option<String> {
    let names = |links: &[(NodeId, NodeId)], between: &str| -> Vec<String> {
        links
            .iter()
            .filter(|(a, b)| *a == owner.node || *b == owner.node)
            .map(|(a, b)| format!("{}{between}{}", a.0, b.0))
            .collect()
    };
    let parts: Vec<String> = [
        (names(held, "-"), "held"),
        (names(dropped, "->"), "dropped"),
    ]
    .into_iter()
    .filter(|(links, _)| !links.is_empty())
    .map(|(links, how)| format!("links {} are {how}", links.join(", ")))
    .collect();
    (!parts.is_empty()).then(|| parts.join(" and "))
}

/// The line for node 1's queue frozen on a write it applied and has not published
/// (`waits_on`), with its grant not fenced: every write is refused until that write publishes,
/// whatever A1 and L1 say (U1, ruling A-R109). It names the request, as `in_flight` holds it,
/// and its seq. With a link of node 1's held or dropped the copies cannot take it, so the line
/// names those links and says healing them resolves it; without one, it publishes once the
/// copies take it.
fn waits_on_line(
    owner: &NodeStatus,
    held: &[(NodeId, NodeId)],
    dropped: &[(NodeId, NodeId)],
) -> Option<String> {
    let seq = owner.waits_on.filter(|_| owner.fence.is_none())?;
    let node = owner.node.0;
    let what = match owner.in_flight {
        // The kernel's own start record carries a reserved id, not a client's (PB-3).
        Some(request)
            if request == rdb_core::contracts::ids::RequestIdentity::START_RECORD.request =>
        {
            format!("the start record's outcome (seq {})", seq.0)
        }
        // Status does not know an admin damage write: it is another client's (S6 P-3).
        Some(request) if is_damage_request(request) => {
            format!("an admin damage write's outcome (seq {})", seq.0)
        }
        Some(request) => format!("request {}'s outcome (seq {})", request.0, seq.0),
        None => format!("the outcome of seq {}", seq.0),
    };
    Some(match node_links(owner, held, dropped) {
        Some(links) => format!(
            "partition 1 waits on {what}, which node {node} applied and has not published, and \
             {links}; writes resume once it publishes, which needs those links: heal them (link \
             heal all)"
        ),
        None => format!(
            "partition 1 waits on {what}, which node {node} applied and has not published; \
             writes resume once node {node}'s copies take it and it publishes"
        ),
    })
}

/// The line for F1 `Blocked(<reason>)` on node 1: recovery cannot finish, so writes stay out
/// whatever the grant, the links or L1 do (W10-D1). Since A-R102 an unanswered activation
/// resolves instead ([`resolving_line`]), so this is only the recovery CAS or a real dead end.
/// It names the reason and promises no way out.
fn blocked_line(owner: &NodeStatus) -> Option<String> {
    let recovery = owner.recovery.as_deref()?;
    recovery
        .starts_with("Blocked(")
        .then(|| format!("node {}'s recovery is {recovery}, so {STUCK}", owner.node.0))
}

/// The line for F1 `ActivationUnresolved` on node 1: the activation CAS went unanswered (tester
/// r24n: the store down), and F1 reads and proposes again by itself once the store answers
/// (A-R102), so writes come back with no restart. With L1 paused or reprotecting and a link of
/// node 1's held, writes also wait on those links, so the line names both (W11-D1, tester w6:
/// links held again mid-outage, `PROTECTION_PAUSED` after the store answered).
fn resolving_line(
    owner: &NodeStatus,
    held: &[(NodeId, NodeId)],
    dropped: &[(NodeId, NodeId)],
) -> Option<String> {
    if owner.recovery.as_deref() != Some("ActivationUnresolved") {
        return None;
    }
    let node = owner.node.0;
    Some(match links_wait(owner, held, dropped) {
        Some((mode, links)) => format!(
            "node {node}'s recovery is waiting for the control store to confirm activation, \
             l1={mode} and {links}; writes resume only once the store answers \
             and node {node}'s copies catch up, which needs those links: heal them (link heal \
             all)"
        ),
        None => format!(
            "node {node}'s recovery is waiting for the control store to confirm activation; \
             writes resume by themselves once it answers"
        ),
    })
}

/// The line for A1 `unheld` with its self-replace stopped (`replace_stopped`): a freeze, or a
/// record not this boot's, landed between grants, and A1 will not try again (W8-D1, tester
/// r6_3200). Nothing in M9 replaces the grant.
fn gave_up_line(owner: &NodeStatus) -> String {
    let node = owner.node.0;
    format!(
        "node {node}'s grant is {} and node {node} stopped trying for one: its grant record is \
         frozen or not its own; nothing in M9 replaces it, so {STUCK}",
        owner.authority
    )
}

/// Whether A1's self-replace can still lift a fence for `reason`: its poll is live, and the
/// record is not one it answers `Never` ([`never_replaced`]).
const fn fence_lifts(owner: &NodeStatus, reason: DenyReason) -> bool {
    owner.fence_polling && !never_replaced(reason)
}

/// The fenced grant's line: it resumes by itself while [`fence_lifts`], else it is stuck. With
/// L1 paused or reprotecting and a link of node 1's held, the grant comes back by itself and
/// writes still wait on those links (W8-D2, tester r9: `held(grant 3)`, `l1=paused`, writes back
/// only after `link heal all`).
fn fence_line(
    owner: &NodeStatus,
    reason: DenyReason,
    held: &[(NodeId, NodeId)],
    dropped: &[(NodeId, NodeId)],
) -> String {
    let node = owner.node.0;
    if let (true, Some((mode, links))) =
        (fence_lifts(owner, reason), links_wait(owner, held, dropped))
    {
        format!(
            "node {node}'s grant is {}, l1={mode} and {links}; writes resume only \
             once the old grant's expiry is proven and node {node}'s copies catch up, which needs \
             those links: heal them (link heal all); do not restart node {node}: a restarted \
             node does not rejoin in M9 (achieveai/rDB#42)",
            owner.authority
        )
    } else if fence_lifts(owner, reason) {
        format!(
            "node {node}'s grant is {}; writes resume by themselves once the old grant's expiry \
             is proven, so do not restart node {node}: a restarted node does not rejoin in M9 \
             (achieveai/rDB#42)",
            owner.authority
        )
    } else {
        format!(
            "node {node}'s grant is {}; nothing in M9 replaces it, so {STUCK}",
            owner.authority
        )
    }
}

/// A fence reason whose grant record A1's self-replace answers `Never` (`grant::self_replace`):
/// frozen, another boot's, or under another authority generation. The first poll stops on it,
/// so the line says so from the fence on rather than for one renewal interval later.
const fn never_replaced(reason: DenyReason) -> bool {
    matches!(
        reason,
        DenyReason::Frozen | DenyReason::BootMismatch | DenyReason::AuthorityGenerationChanged
    )
}

/// The `ready` or `not ready` line to print for `owner`, given what was printed last, and what
/// `seen` now holds as printed: on each change of `admits`, and again when a new reason appears
/// (a restart while ready, a fence after the line, a restart after a fence). A reason that goes
/// away prints the bare line, so a promise it made never outlives its state (W8-D1, tester
/// r6_3200: the fence's resume line stayed the last word after A1 stopped, `unheld`).
fn ready_step(
    seen: &mut Seen,
    owner: &NodeStatus,
    restart_note: Option<&str>,
    held: &[(NodeId, NodeId)],
    dropped: &[(NodeId, NodeId)],
) -> Option<String> {
    // A queue frozen on a write admits nothing, whatever A1 and L1 say (U1).
    let admits = owner.admits == Some(true) && owner.waits_on.is_none();
    step_line(
        seen,
        admits,
        ready_reason(owner, admits, restart_note, held, dropped),
    )
}

/// The poller's line once node 1's restart failed: no process runs it, so `node_status` leaves
/// it out, and writes stay stopped for the run (W7-D3). `restart_note` names the failed
/// restart; it is never `None` after one.
fn stopped_owner_step(seen: &mut Seen, restart_note: Option<&str>) -> Option<String> {
    let note = restart_note?;
    step_line(seen, false, Some(format!("{note}; {STUCK}")))
}

/// The `ready` or `not ready` line for `admits` and `reason`, given what `seen` holds as
/// printed, as [`ready_step`] describes. A bare `ready` waits while `wait ready` runs: that
/// wait ends with its own line and records it as printed ([`wait_ready_line`]).
fn step_line(seen: &mut Seen, admits: bool, reason: Option<String>) -> Option<String> {
    if admits == seen.ready && reason == seen.reason {
        return None;
    }
    if seen.awaiting_ready && admits && reason.is_none() {
        return None;
    }
    let line = match (admits, &reason) {
        (true, None) => "ready".to_owned(),
        (true, Some(why)) => format!("ready for now; {why}"),
        (false, None) => "not ready: writes not admitted".to_owned(),
        (false, Some(why)) => format!("not ready: writes not admitted: {why}"),
    };
    seen.ready = admits;
    seen.reason = reason;
    Some(line)
}

/// The line to print when a node's stall line changes, given the one printed last. Not a node
/// fault (the node keeps serving), so it is not kept with them: a stall that clears says so,
/// or the last stall line would still read as writes paused after the partition healed.
fn stall_change(printed: Option<&String>, node: &NodeStatus) -> Option<String> {
    match (printed, &node.stalled) {
        (Some(was), Some(now)) if was == now => None,
        (_, Some(now)) => Some(format!("stalled node={} {now}", node.node.0)),
        (Some(_), None) => Some(format!(
            "stall cleared node={} recovery={} recovered={} admits={}",
            node.node.0,
            node.recovery.as_deref().unwrap_or("-"),
            node.recovered
                .map_or_else(|| "-".to_owned(), |g| g.0.to_string()),
            node.admits
                .map_or_else(|| "-".to_owned(), |a| a.to_string()),
        )),
        (None, None) => None,
    }
}

fn waiting_line(owner: &NodeStatus) -> String {
    format!(
        "waiting to admit writes (l1={} authority={} {})",
        owner.protection.as_deref().unwrap_or("inert"),
        owner.authority,
        owner.holds.map_or_else(
            || "holds=nothing".to_owned(),
            |h| format!("applied={} durable={}", h.applied, h.durable)
        ),
    )
}

/// `gen= epoch= applied= durable=` of what the node holds, or `holds=nothing` before it holds
/// a lineage: a secondary adopts no generation, so zeros there would read as real (OB2).
fn holds_part(node: &NodeStatus) -> String {
    node.holds.map_or_else(
        || "holds=nothing".to_owned(),
        |h| {
            format!(
                "gen={} epoch={} applied={} durable={}",
                h.generation.0, h.owner_epoch.0, h.applied, h.durable
            )
        },
    )
}

/// One command per line; the exit code is 1 when any command failed to parse, and 2 at once
/// when a line cannot be read. A background put runs on `scope`, so the run waits for it before
/// shutting down.
fn repl<'scope, 'env>(
    scope: &'scope std::thread::Scope<'scope, 'env>,
    args: &Args,
    db: &'env Db,
    rt: &tokio::runtime::Runtime,
    store: &dyn ConfigStore,
    control: Option<&Control<'_>>,
    progress: &Progress,
) -> ExitCode {
    let input: Box<dyn BufRead> = match &args.script {
        Some(path) => match std::fs::File::open(path) {
            Ok(file) => Box::new(std::io::BufReader::new(file)),
            Err(e) => {
                eprintln!("rdb_dev: --script {}: {e}", path.display());
                return ExitCode::from(2);
            }
        },
        None => Box::new(std::io::BufReader::new(std::io::stdin())),
    };
    let mut sent = Sent {
        file: args.requests.clone(),
        ..Sent::default()
    };
    let mut bad = false;
    let deadline = args.put_timeout.unwrap_or(Timeouts::default().put);
    for line in input.lines() {
        let line = match command(line) {
            Ok(Some(line)) => line,
            Ok(None) => continue,
            Err(e) => {
                tracing::error!(error = %e, "repl_input_failed");
                eprintln!("rdb_dev: {e}");
                return ExitCode::from(2);
            }
        };
        // What the last command changed is said before this one's output (tester G4).
        poll_once(db, progress);
        if args.script.is_some() {
            say(&format!("> {line}"));
        }
        log_command(&line);
        // A value verb reads its JSON from the raw line (risk R1), so it skips `split_words`.
        if let Some(verb) = line
            .split_whitespace()
            .next()
            .filter(|verb| VALUE_VERBS.contains(verb))
        {
            let read = match verb {
                "members" => Some(parse_members(&line)),
                "items" => Some(parse_items(&line)),
                _ => None,
            };
            let done = if let Some(read) = read {
                read.map(|(object, look)| read_line(db.read(object.as_bytes(), look)))
            } else {
                parse_change(&line).map(|(object, expect, change)| {
                    write_line(db, db.write(object.as_bytes(), change, expect), &mut sent);
                })
            };
            if let Err(e) = done {
                say(&format!("usage: {e}; {}", usage_of(verb)));
                bad = true;
            }
            continue;
        }
        let words = match split_words(&line) {
            Ok(words) => words,
            Err(e) => {
                say(&format!("usage: {e}"));
                bad = true;
                continue;
            }
        };
        let words: Vec<&str> = words.iter().map(String::as_str).collect();
        // Nothing to retry is an answer, not a mistyped line: no `usage:`, and no exit 1.
        if let ["retry", rest @ ..] = words.as_slice() {
            match sent.adopt(rest) {
                Ok(warnings) => warnings.iter().for_each(|w| say(&format!("warn {w}"))),
                Err(e) => {
                    say(&format!("err {e}"));
                    bad = true;
                    continue;
                }
            }
            if let Some(nothing) = sent.nothing_to_retry(rest) {
                say(&nothing);
                continue;
            }
        }
        let outcome = match words.as_slice() {
            ["quit" | "exit"] => break,
            ["put", "--as", n, object, value, rest @ ..] => parse_app_id(n).and_then(|id| {
                let if_version = parse_if_version(rest)?;
                let answer = db.put_as(id, object.as_bytes(), value.as_bytes(), if_version);
                let kind = Kind::Put {
                    object: object.as_bytes().to_vec(),
                    if_version,
                };
                put_line(db, answer, Some((&mut sent, kind)));
                Ok(())
            }),
            ["put", "--as", ..] => Err("put --as <n> <object> <value> [--if-version N]".to_owned()),
            ["put", object, value, rest @ ..] => match parse_if_version(rest) {
                Ok(if_version) => {
                    let answer = db.put(object.as_bytes(), value.as_bytes(), if_version);
                    let kind = Kind::Put {
                        object: object.as_bytes().to_vec(),
                        if_version,
                    };
                    put_line(db, answer, Some((&mut sent, kind)));
                    Ok(())
                }
                Err(e) => Err(e),
            },
            ["put&", object, value, rest @ ..] => parse_if_version(rest).map(|if_version| {
                let (object, value) = ((*object).to_owned(), (*value).to_owned());
                say(&format!("bg {object}: queued"));
                scope.spawn(move || {
                    let answer = db.put(object.as_bytes(), value.as_bytes(), if_version);
                    say(&format!("bg {object}: {}", put_text(&answer)));
                    bg_note(db, &format!("bg {object}"), &answer);
                });
            }),
            ["txn", rest @ ..] => parse_txn(rest, deadline).map(|(deadline, ops)| {
                let answer = db.txn(&txn_puts(&ops), deadline);
                put_line(db, answer, Some((&mut sent, Kind::Txn)));
            }),
            ["txn&", rest @ ..] => parse_txn(rest, deadline).map(|(deadline, ops)| {
                say("bg txn: queued");
                scope.spawn(move || {
                    let answer = db.txn(&txn_puts(&ops), deadline);
                    say(&format!("bg txn: {}", put_text(&answer)));
                    bg_note(db, "bg txn", &answer);
                });
            }),
            ["retry", rest @ .., "--payload", value] => {
                sent.pick(rest).and_then(|(request, kind)| {
                    let id = request.identity.request;
                    let (object, if_version) = payload_put(kind, id)?;
                    put_line(
                        db,
                        db.put_as(id, &object, value.as_bytes(), if_version),
                        None,
                    );
                    Ok(())
                })
            }
            ["retry", rest @ .., "--deadline-ms", millis] => match millis.parse::<u64>() {
                Ok(ms) => sent.pick(rest).map(|(request, _)| {
                    let deadline = Duration::from_millis(ms);
                    let answer = db.resend_within(request, deadline);
                    sent.resent_within(deadline, &answer);
                    put_line(db, answer, None);
                }),
                Err(_) => Err("retry [<request>] --deadline-ms N: N is milliseconds".to_owned()),
            },
            ["retry", rest @ ..] => sent.pick(rest).map(|(request, kind)| {
                let answer = match kind {
                    Kind::Put { .. } | Kind::Value | Kind::Clear => db.resend(request),
                    // A txn waits as it was sent: its own deadline, not the put timeout.
                    Kind::Txn => {
                        let deadline = Duration::from_millis(request.remaining_millis);
                        db.resend_within(request, deadline)
                    }
                };
                put_line(db, answer, None);
            }),
            ["get", object] => {
                read_line(db.read(object.as_bytes(), Look::Doc).map_err(get_hint));
                Ok(())
            }
            ["blob", "upload", object, rest @ ..] => blob_upload(db, object, rest, &mut sent),
            ["blob", "chunk", object, rest @ ..] => blob_chunk(db, object, rest, &mut sent),
            ["blob", "publish", object, rest @ ..] => blob_publish(db, object, rest, &mut sent),
            ["blob", "gc", object, rest @ ..] => blob_gc(db, object, rest, &mut sent),
            ["blob", ..] => Err(BLOB_USAGE.to_owned()),
            ["read", object, rest @ ..] => parse_range(rest).map(|look| {
                read_line(db.read(object.as_bytes(), look));
            }),
            ["read", ..] => Err(READ_USAGE.to_owned()),
            ["get", object, "--previous"] => {
                get_line(db.get_previous(object.as_bytes()));
                Ok(())
            }
            ["status", request, rest @ ..] => status_line(db, &sent, request, rest),
            ["nodes"] => {
                let answered = db.node_status();
                for node in [1, 2, 3].map(NodeId) {
                    say(&answered
                        .iter()
                        .find(|status| status.node == node)
                        .map_or_else(
                            || missing_node_line(node, db.boot_of(node), db.restart_failed(node)),
                            node_line,
                        ));
                }
                Ok(())
            }
            ["node", "restart", node] => parse_node(node).map(|node| {
                say(&match db.restart_node(node) {
                    Ok(boot) => format!("node {} boot {}", node.0, boot.0),
                    Err(e) => format!("err node restart {}: {e}", node.0),
                });
            }),
            ["node", ..] => Err("node restart <n>: n is 1, 2 or 3".to_owned()),
            ["admin", "clear-grant", node] => parse_node(node).map(|node| {
                say(&match rt.block_on(admin::clear_grant(db, store, node)) {
                    Ok(clearance) => clearance_line(clearance, node, db),
                    Err(e) => admin_error_line(node, &e),
                });
            }),
            ["admin", "damage-chunk", object, rest @ ..] => damage_chunk(db, object, rest),
            ["admin", "damage", _, ..] => damage_record(db, &line),
            ["admin", "clear", object] => {
                let answer = db.write(object.as_bytes(), Change::Clear, None);
                kind_write_line(
                    db,
                    "clear ",
                    "nothing to clear",
                    answer,
                    &mut sent,
                    Kind::Clear,
                );
                Ok(())
            }
            ["admin", ..] => Err(format!(
                "admin clear-grant <n>: n is 1, 2 or 3 | admin clear <object> | {DAMAGE_USAGE} | \
                 {DAMAGE_RECORD_USAGE}"
            )),
            ["control", ..] => match control {
                Some(control) => control_verb(&words, &line, rt, control),
                None => {
                    say(&format!("err {line}: {CONTROL_EXTERNAL}"));
                    Ok(())
                }
            },
            ["links"] => {
                let names = |pairs: Vec<(NodeId, NodeId)>| {
                    pairs
                        .iter()
                        .map(|(a, b)| format!("{}-{}", a.0, b.0))
                        .collect::<Vec<_>>()
                };
                say(&format!(
                    "held={:?} dropped={:?}",
                    names(db.links().held()),
                    names(db.links().dropped())
                ));
                Ok(())
            }
            ["link", "heal", "all"] => {
                say(&format!("healed delivered={}", db.links().heal_all()));
                Ok(())
            }
            ["link", verb @ ("hold" | "heal" | "drop"), pair] => parse_pair(pair).map(|(a, b)| {
                if *verb == "hold" {
                    db.links().hold(a, b);
                    say(&format!("held {}-{}", a.0, b.0));
                } else if *verb == "drop" {
                    db.links().drop_direction(a, b);
                    say(&format!("dropping {}->{}", a.0, b.0));
                } else {
                    say(&format!(
                        "healed {}-{} delivered={}",
                        a.0,
                        b.0,
                        db.links().heal(a, b)
                    ));
                }
            }),
            ["wait", what @ ("recovered" | "ready"), rest @ ..] => {
                let secs = rest.first().map_or(Ok(30), |s| s.parse::<u64>());
                match secs {
                    Ok(secs) => {
                        let within = Duration::from_secs(secs);
                        let waited = if *what == "ready" {
                            wait_ready_line(
                                progress,
                                within,
                                WAIT_PROGRESS_EVERY,
                                |slice| db.wait_ready(slice),
                                say,
                            )
                        } else {
                            wait_recovered(progress, within, || db.heal_state())
                        };
                        match waited {
                            Ok(line) => say(&line),
                            // A script must see that this open will never be ready (A-R191).
                            Err(line) => {
                                say(&line);
                                bad = true;
                            }
                        }
                        Ok(())
                    }
                    Err(_) => Err("wait takes whole seconds".to_owned()),
                }
            }
            ["sleep", millis] => match millis.parse::<u64>() {
                Ok(ms) => {
                    std::thread::sleep(Duration::from_millis(ms));
                    Ok(())
                }
                Err(_) => Err("sleep takes milliseconds".to_owned()),
            },
            _ => Err(format!("unknown command {line:?}")),
        };
        if let Err(e) = outcome {
            say(&format!("usage: {e}"));
            bad = true;
        }
    }
    exit_code(bad, &sent)
}

/// How a REPL run ends: 1 when a line was bad or an append to `--requests F` failed (k214).
fn exit_code(bad: bool, sent: &Sent) -> ExitCode {
    if bad || sent.save_failed.get() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

/// Why a `control` verb is refused under `--control`: they reach rdb_dev's own voters, and an
/// external rEtcd is not rdb_dev's to freeze, stop or fault (M10 R3c).
const CONTROL_EXTERNAL: &str =
    "refused under --control: the store is an external rEtcd, not rdb_dev's own voters";

/// A `control` verb against rdb_dev's own voters.
fn control_verb(
    words: &[&str],
    line: &str,
    rt: &tokio::runtime::Runtime,
    control: &Control<'_>,
) -> Result<(), String> {
    match words {
        ["control", "lifecycle", partition, word] => {
            parse_lifecycle(partition, word).map(|(partition, lifecycle)| {
                say(&set_lifecycle(rt, control.cluster, partition, lifecycle));
            })
        }
        ["control", "lifecycle", ..] => Err(format!(
            "control lifecycle <p> <{}>",
            LIFECYCLE_WORDS.join("|")
        )),
        ["control", "freeze-grant", node] => parse_node(node).map(|node| {
            say(&freeze_grant(rt, control.cluster, node));
        }),
        ["control", "freeze-grant", ..] => Err("control freeze-grant <n>".to_owned()),
        ["control"] => {
            control_lines(rt, control.cluster);
            Ok(())
        }
        ["control", "voters"] => {
            say(&voters_line(control));
            Ok(())
        }
        ["control", "gap"] => {
            say(&gap(rt, control));
            Ok(())
        }
        ["control", "drop-watches"] => {
            say(&format!("dropped {} watches", control.store.drop_watches()));
            Ok(())
        }
        ["control", verb @ ("gap" | "drop-watches"), ..] => {
            Err(format!("control {verb}: takes no arguments"))
        }
        ["control", "fail", "list", word] => ListFault::parse(word).map(|fault| {
            control.store.fail_lists(fault);
            say(&format!("fail list {word}"));
        }),
        ["control", "fail", "put", prefix, word, rest @ ..] => {
            parse_put_fault(word, rest).map(|(fault, count)| {
                control.store.fail_puts(prefix, fault, count);
                say(&match fault {
                    Some(_) => format!("fail put {prefix} {word} {count}"),
                    None => format!("fail put {prefix} off"),
                });
            })
        }
        ["control", "fail", ..] => Err(format!(
            "control fail list <{}> | control fail put <prefix> <{}> [n]",
            ListFault::WORDS,
            PutFault::WORDS
        )),
        ["control", verb @ ("stop" | "start"), voter] => {
            parse_voter(control.cluster, voter).map(|voter| {
                say(&if *verb == "stop" {
                    stop_voter(rt, control, voter)
                } else {
                    start_voter(rt, control.cluster, voter)
                });
            })
        }
        ["control", "stop" | "start", ..] => Err(format!(
            "control stop|start <voter>: exactly one voter, one of {:?}",
            control
                .cluster
                .ids()
                .iter()
                .map(|id| id.0)
                .collect::<Vec<_>>()
        )),
        _ => Err(format!("unknown command {line:?}")),
    }
}

/// The `repl_command` log line for one typed command: its verb, and the object it names as
/// `key_hex` (at most 32 bytes), never the line. A line carries keys, values, map keys and
/// paths, and a log never holds their text (ADR-0013, A-R154, D5). A verb the grammar does not
/// know logs as `unknown`, since a mistyped line can be anything.
fn log_command(line: &str) {
    let (verb, object) = command_fields(line);
    match object {
        Some(object) => {
            let key_hex = hex::encode(&object.as_bytes()[..object.len().min(32)]);
            tracing::info!(verb, key_hex = %key_hex, "repl_command");
        }
        None => tracing::info!(verb, "repl_command"),
    }
}

/// A command's verb as the grammar names it (`put`, `blob upload`, `wait ready`), and the
/// object it writes or reads, when it names one. A line `split_words` refuses names no object.
fn command_fields(line: &str) -> (&'static str, Option<String>) {
    const VERBS: [&str; 25] = [
        "put", "put&", "txn", "txn&", "retry", "doc", "map", "set", "members", "list", "items",
        "get", "read", "status", "nodes", "node", "admin", "control", "links", "link", "wait",
        "sleep", "quit", "exit", "blob",
    ];
    const SUBVERBS: [&str; 19] = [
        "put --as",
        "blob upload",
        "blob chunk",
        "blob publish",
        "blob gc",
        "admin clear-grant",
        "admin damage-chunk",
        "admin damage",
        "admin clear",
        "link hold",
        "link heal",
        "link drop",
        "node restart",
        "wait recovered",
        "wait ready",
        "control lifecycle",
        "control freeze-grant",
        "control fail",
        "control stop",
    ];
    let mut first = line.split_whitespace();
    let Some(verb) = first
        .next()
        .and_then(|verb| VERBS.iter().copied().find(|known| *known == verb))
    else {
        return ("unknown", None);
    };
    let pair = first.next().map(|sub| format!("{verb} {sub}"));
    let verb = pair
        .and_then(|pair| SUBVERBS.iter().copied().find(|known| *known == pair))
        .unwrap_or(verb);
    let at = match verb {
        "put" | "put&" | "get" | "read" | "doc" | "map" | "set" | "members" | "list" | "items" => 1,
        "blob upload" | "blob chunk" | "blob publish" | "blob gc" | "admin damage-chunk"
        | "admin damage" | "admin clear" => 2,
        "put --as" => 3,
        _ => return (verb, None),
    };
    let object = split_words(line)
        .ok()
        .and_then(|mut words| (words.len() > at).then(|| words.swap_remove(at)));
    (verb, object)
}

/// One input line as a command: `None` for a blank or comment-only line. An unreadable line
/// is an error, never the end of input (F-007): a script that is not UTF-8, such as one
/// PowerShell 5.1 wrote as UTF-16LE, otherwise ran nothing and exited 0.
fn command(line: std::io::Result<String>) -> Result<Option<String>, String> {
    let line = line.map_err(|e| format!("input: {e}; scripts must be UTF-8"))?;
    let line = uncommented(&line).trim().to_owned();
    Ok((!line.is_empty()).then_some(line))
}

/// `line` up to its comment: a `#` at the start of a word and outside a `"..."` string, so JSON
/// such as `{"tag":"#1"}` or `{"m":"hello #world"}` is kept whole. A `\"` does not end a
/// string (S4 A-2, review F3).
fn uncommented(line: &str) -> &str {
    let (mut word_start, mut quoted, mut escaped) = (true, false, false);
    for (at, c) in line.char_indices() {
        if quoted {
            match c {
                _ if escaped => escaped = false,
                '\\' => escaped = true,
                '"' => quoted = false,
                _ => {}
            }
        } else if c == '#' && word_start {
            return &line[..at];
        } else if c == '"' {
            quoted = true;
        }
        word_start = c.is_whitespace();
    }
    line
}

/// The size limit `json.rs` reads an argument under. `rdb_dev` reads its values from the
/// line instead, so this only satisfies the shared file's import.
const MAX_JSON_TEXT: usize = 1 << 20;
/// What a JSON integer refusal advises instead. `rdb_dev` has no CBOR input, so not `--cbor-hex`
/// (tester P1-2).
const JSON_INTEGER_ADVICE: &str = "write 0 or write a float";

/// A JSON input refusal, as `json.rs` builds it.
#[derive(Debug)]
struct Failure {
    error: String,
    detail: String,
}

impl Failure {
    const fn refused(error: String, detail: String) -> Self {
        Self { error, detail }
    }
}

/// An argument as text, refused over `limit` bytes; `json.rs`'s import.
fn arg_text(arg: &str, limit: usize) -> Result<String, Failure> {
    if arg.len() > limit {
        return Err(Failure::refused(
            "InputTooLarge".into(),
            format!("over the {limit}-byte limit"),
        ));
    }
    Ok(arg.to_owned())
}

/// A value verb's line, read word by word. A JSON value is read from the raw text with
/// serde's stream reader (risk R1), so its quotes and spaces are kept: `split_words` would turn
/// `"b"` into `b`.
struct Words<'a> {
    rest: &'a str,
}

impl<'a> Words<'a> {
    const fn new(line: &'a str) -> Self {
        Self { rest: line }
    }

    /// The next word, up to whitespace.
    fn next(&mut self) -> Option<&'a str> {
        let text = self.rest.trim_start();
        if text.is_empty() {
            self.rest = text;
            return None;
        }
        let end = text.find(char::is_whitespace).unwrap_or(text.len());
        self.rest = &text[end..];
        Some(&text[..end])
    }

    /// The next word, which `what` needs.
    fn take(&mut self, what: &str) -> Result<&'a str, String> {
        self.next().ok_or_else(|| format!("missing {what}"))
    }

    /// The next JSON value, for `what`. A value that does not end at whitespace or the end of
    /// the line is refused: `1x` is not `1` then `x`.
    fn json(&mut self, what: &str) -> Result<Value, String> {
        let text = self.rest.trim_start();
        if text.is_empty() {
            return Err(format!("missing {what} (a JSON value)"));
        }
        let (value, used) =
            json::json_value(text).map_err(|f| format!("{what}: {}: {}", f.error, f.detail))?;
        let after = &text[used..];
        if !after.is_empty() && !after.starts_with(char::is_whitespace) {
            return Err(format!(
                "{what}: the JSON value ends at {:?}, not at a space",
                &text[..used]
            ));
        }
        self.rest = after;
        Ok(value)
    }

    /// The next map key or set member, for `what`: a JSON value, or `{"$bytes":"<hex>"}` for a
    /// byte string, the form a read prints (A-R121). Only at key position: a map is never a key,
    /// so the form cannot mean a map here, and no `$$` escape applies. A document value takes no
    /// `$bytes` input.
    fn key(&mut self, what: &str) -> Result<Value, String> {
        let value = self.json(what)?;
        let digits = match &value {
            Value::Map(map) => {
                let mut entries = map.iter();
                match (entries.next(), entries.next()) {
                    (Some((name, Value::Text(text))), None) if name.as_str() == "$bytes" => {
                        Some(text.clone())
                    }
                    (Some((name, _)), None) if name.as_str() == "$bytes" => {
                        return Err(format!(
                            r#"{what}: $bytes needs a hex string, as {{"$bytes":"00ff"}}"#
                        ));
                    }
                    _ => None,
                }
            }
            _ => None,
        };
        let Some(digits) = digits else {
            return Ok(value);
        };
        hex::decode(&digits)
            .map(Value::Bytes)
            .map_err(|e| format!("{what}: $bytes {digits:?} is not hex: {e}"))
    }
}

/// `--absent` or `--expect V`, once, before the ops; neither is `None`.
fn parse_expect(words: &mut Words<'_>) -> Result<Option<Expected>, String> {
    let before = words.rest;
    match words.next() {
        Some("--absent") => Ok(Some(Expected::Absent)),
        Some("--expect") => {
            let v = words.take("a version after --expect")?;
            v.parse()
                .map(|v| Some(Expected::Version(v)))
                .map_err(|_| format!("--expect {v:?} is not a version"))
        }
        _ => {
            words.rest = before;
            Ok(None)
        }
    }
}

/// The value verbs, each read from the raw line (risk R1).
const VALUE_VERBS: [&str; 6] = ["doc", "map", "set", "members", "list", "items"];

const DOC_USAGE: &str = "doc <object> [--absent | --expect V] <op>...; an op is --json J, \
                         set <path> J, remove <path> or incr <path> N";
const COLLECTION_USAGE: &str = "map|set <object> [--absent | --expect V] <op>...; an op is put K \
                                J, add K, del K or need K present|absent";
const MEMBERS_USAGE: &str = "members <object> [--after K] [--limit N]";
const LIST_USAGE: &str =
    "list <object> [--absent | --expect V] <op>...; an op is push J, insert P J, \
                          remove P, replace P J or move P Q; a negative P counts from the end";
const ITEMS_USAGE: &str = "items <object> [--from P | --token G:V:P] [--limit N]";

/// The usage line for `verb`'s refusals.
fn usage_of(verb: &str) -> &'static str {
    match verb {
        "doc" => DOC_USAGE,
        "members" => MEMBERS_USAGE,
        "list" => LIST_USAGE,
        "items" => ITEMS_USAGE,
        _ => COLLECTION_USAGE,
    }
}

/// `doc|map|set|list <object> [--absent | --expect V] <op>...`: the object, the guard and the change.
fn parse_change(line: &str) -> Result<(String, Option<Expected>, Change), String> {
    let mut words = Words::new(line);
    let verb = words.take("a verb")?;
    let object = words.take("<object>")?.to_owned();
    let expect = parse_expect(&mut words)?;
    let change = match verb {
        "doc" => Change::Doc(Delta(doc_ops(&mut words)?)),
        "map" => Change::Map(elem_ops(&mut words)?),
        "set" => Change::Set(elem_ops(&mut words)?),
        "list" => Change::List(list_edits(&mut words)?),
        other => return Err(format!("{other:?} is not a write")),
    };
    Ok((object, expect, change))
}

/// A `doc` line's ops; at least one.
fn doc_ops(words: &mut Words<'_>) -> Result<Vec<Op>, String> {
    let mut ops = Vec::new();
    while let Some(word) = words.next() {
        let path = |words: &mut Words<'_>| {
            let text = words.take("a path")?;
            Path::parse(text).map_err(|e| format!("{e} in path {text:?}"))
        };
        ops.push(match word {
            "--json" => Op::Replace(words.json("--json")?),
            "set" => Op::Set(path(words)?, words.json("set")?),
            "remove" => Op::Remove(path(words)?),
            "incr" => {
                let at = path(words)?;
                let n = words.take("an integer")?;
                let by = n
                    .parse::<i128>()
                    .ok()
                    .and_then(Int::new)
                    .ok_or_else(|| format!("incr {n:?} is not an integer in -2^64..2^64-1"))?;
                Op::Increment(at, by)
            }
            "--absent" | "--expect" => return Err(format!("{word} goes before the ops")),
            other => return Err(format!("unexpected {other:?}")),
        });
    }
    if ops.is_empty() {
        return Err("no op".to_owned());
    }
    Ok(ops)
}

/// A `map` or `set` line's ops, possibly none (a create of an empty collection). Either verb
/// takes `put` and `add`, as `doc_scenario` does: the library refuses the other kind's op
/// (`KindMismatch`), so the refusal is the library's, not the tool's.
fn elem_ops(words: &mut Words<'_>) -> Result<Vec<ElemOp>, String> {
    let mut ops = Vec::new();
    while let Some(word) = words.next() {
        ops.push(match word {
            "put" => ElemOp::Put(words.key("put K")?, words.json("put's value J")?),
            "add" => ElemOp::Add(words.key("add K")?),
            "del" => ElemOp::Remove(words.key("del K")?),
            "need" => {
                let key = words.key("need K")?;
                match words.take("present or absent")? {
                    "present" => ElemOp::NeedPresent(key),
                    "absent" => ElemOp::NeedAbsent(key),
                    other => return Err(format!("need takes present or absent, not {other:?}")),
                }
            }
            "--absent" | "--expect" => return Err(format!("{word} goes before the ops")),
            other => return Err(format!("unexpected {other:?}")),
        });
    }
    Ok(ops)
}

/// `rdb_dev` lists this many elements when `--limit` is not given (arch §2: `limit` is uncapped
/// until S5).
const DEFAULT_LIMIT: usize = 100;

/// `members <object> [--after K] [--limit N]`: the object and the look.
fn parse_members(line: &str) -> Result<(String, Look), String> {
    let mut words = Words::new(line);
    words.next(); // `members`
    let object = words.take("<object>")?.to_owned();
    let (mut after, mut limit) = (None, None);
    while let Some(word) = words.next() {
        match word {
            "--after" if after.is_none() => after = Some(words.key("--after K")?),
            "--limit" if limit.is_none() => {
                let n = words.take("a count after --limit")?;
                limit = Some(
                    n.parse::<usize>()
                        .map_err(|_| format!("--limit {n:?} is not a count"))?,
                );
            }
            other => return Err(format!("unexpected {other:?}")),
        }
    }
    let limit = limit.unwrap_or(DEFAULT_LIMIT);
    Ok((object, Look::Members { after, limit }))
}

/// A `list` line's edits, possibly none (a create of an empty list). A position is an integer;
/// negative counts from the end, resolved by the owner (A-R116).
fn list_edits(words: &mut Words<'_>) -> Result<Vec<ListEdit>, String> {
    let position = |words: &mut Words<'_>, what: &str| {
        let text = words.take(what)?;
        text.parse::<i64>()
            .map_err(|_| format!("{what} {text:?} is not a position"))
    };
    let mut edits = Vec::new();
    while let Some(word) = words.next() {
        edits.push(match word {
            "push" => ListEdit::Push(words.json("push J")?),
            "insert" => ListEdit::Insert {
                at: position(words, "insert P")?,
                value: words.json("insert's value J")?,
            },
            "remove" => ListEdit::Remove {
                at: position(words, "remove P")?,
            },
            "replace" => ListEdit::Replace {
                at: position(words, "replace P")?,
                value: words.json("replace's value J")?,
            },
            "move" => ListEdit::Move {
                from: position(words, "move P")?,
                to: position(words, "move's Q")?,
            },
            "--absent" | "--expect" => return Err(format!("{word} goes before the ops")),
            other => return Err(format!("unexpected {other:?}")),
        });
    }
    Ok(edits)
}

/// `items <object> [--from P | --token G:V:P] [--limit N]`: the object and the look. Without
/// either start it reads from position 0.
fn parse_items(line: &str) -> Result<(String, Look), String> {
    let mut words = Words::new(line);
    words.next(); // `items`
    let object = words.take("<object>")?.to_owned();
    let (mut from, mut token, mut limit) = (None, None, None);
    while let Some(word) = words.next() {
        match word {
            "--from" if from.is_none() && token.is_none() => {
                let p = words.take("a position after --from")?;
                from = Some(
                    p.parse::<i64>()
                        .map_err(|_| format!("--from {p:?} is not a position"))?,
                );
            }
            "--token" if from.is_none() && token.is_none() => {
                token = Some(parse_token(words.take("G:V:P after --token")?)?);
            }
            "--limit" if limit.is_none() => {
                let n = words.take("a count after --limit")?;
                limit = Some(
                    n.parse::<usize>()
                        .map_err(|_| format!("--limit {n:?} is not a count"))?,
                );
            }
            other => return Err(format!("unexpected {other:?}")),
        }
    }
    let limit = limit.unwrap_or(DEFAULT_LIMIT);
    let look = match token {
        Some(token) => Look::ItemsAfter { token, limit },
        None => Look::Items {
            at: from.unwrap_or(0),
            limit,
        },
    };
    Ok((object, look))
}

/// `G:V:P`, as `items` prints a page's `token=`.
fn parse_token(text: &str) -> Result<Token, String> {
    let parts: Vec<&str> = text.split(':').collect();
    let [g, v, p] = parts.as_slice() else {
        return Err(format!("--token {text:?} is not G:V:P"));
    };
    let number = |part: &str| {
        part.parse::<u64>()
            .map_err(|_| format!("--token {text:?} is not G:V:P"))
    };
    Ok(Token {
        generation: Generation(number(g)?),
        version: number(v)?,
        position: number(p)?,
    })
}

/// A value write's answer, remembered for `retry` as [`Kind::Value`].
fn write_line(db: &Db, answer: Result<Option<rdb_api::PutOk>, PutError>, sent: &mut Sent) {
    labelled_write_line(db, "", "nothing to write", answer, sent);
}

/// [`write_line`] with `label` before the answer, and `unchanged` saying why a write that
/// compiled to nothing was not sent. Such a write is the latest and sent nothing, so a bare
/// `retry` after it has nothing to send. Whether the write was sent or answered unchanged.
fn labelled_write_line(
    db: &Db,
    label: &str,
    unchanged: &str,
    answer: Result<Option<rdb_api::PutOk>, PutError>,
    sent: &mut Sent,
) -> bool {
    kind_write_line(db, label, unchanged, answer, sent, Kind::Value)
}

/// [`labelled_write_line`] for a write `retry` names as `kind`.
fn kind_write_line(
    db: &Db,
    label: &str,
    unchanged: &str,
    answer: Result<Option<rdb_api::PutOk>, PutError>,
    sent: &mut Sent,
    kind: Kind,
) -> bool {
    match answer {
        Ok(None) => {
            say(&format!("{label}unchanged ({unchanged}; nothing sent)"));
            sent.record(None, kind);
            true
        }
        Ok(Some(ok)) => {
            labelled_put_line(db, label, Ok(ok), Some((sent, kind)));
            true
        }
        Err(e) => {
            labelled_put_line(db, label, Err(e), Some((sent, kind)));
            false
        }
    }
}

const BLOB_USAGE: &str = "blob upload <object> [--absent | --expect V] --upload U --chunk-size N \
                          <text> [--stop-after K] | blob chunk <object> --upload U --index I \
                          <text> | blob publish <object> [--absent | --expect V] --upload U \
                          --size N --chunk-size N --sha256 H | blob gc <object> --floor F; U is \
                          32 hex digits, H 64";
const READ_USAGE: &str = "read <object> [--offset O --len L]";

/// A `blob` line's words after the object: `--absent` or `--expect V`, the `--name value`
/// flags in `takes`, each once, and at most one other word, the text.
#[derive(Debug, Default)]
struct BlobArgs<'a> {
    expect: Option<Expected>,
    flags: std::collections::BTreeMap<&'a str, &'a str>,
    text: Option<&'a str>,
}

impl<'a> BlobArgs<'a> {
    fn parse(words: &[&'a str], takes: &[&str]) -> Result<Self, String> {
        let mut args = Self::default();
        let mut words = words.iter();
        while let Some(&word) = words.next() {
            match word {
                "--absent" if args.expect.is_none() => args.expect = Some(Expected::Absent),
                "--expect" if args.expect.is_none() => {
                    let v = words.next().ok_or("missing a version after --expect")?;
                    let v = v
                        .parse()
                        .map_err(|_| format!("--expect {v:?} is not a version"))?;
                    args.expect = Some(Expected::Version(v));
                }
                flag if takes.contains(&flag) => {
                    let value = words
                        .next()
                        .ok_or_else(|| format!("missing a value after {flag}"))?;
                    if args.flags.insert(flag, value).is_some() {
                        return Err(format!("{flag} given twice"));
                    }
                }
                text if args.text.is_none() && !text.starts_with("--") => args.text = Some(text),
                other => return Err(format!("unexpected {other:?}")),
            }
        }
        Ok(args)
    }

    /// The value of `flag`, which must be given.
    fn flag(&self, flag: &str) -> Result<&'a str, String> {
        self.flags
            .get(flag)
            .copied()
            .ok_or_else(|| format!("missing {flag}"))
    }

    /// The number after `flag`, which must be given.
    fn number<T: std::str::FromStr>(&self, flag: &str) -> Result<T, String> {
        let text = self.flag(flag)?;
        text.parse()
            .map_err(|_| format!("{flag} {text:?} is not a number"))
    }

    fn text(&self) -> Result<&'a str, String> {
        self.text.ok_or_else(|| "missing <text>".to_owned())
    }

    /// `--upload U`: 32 hex digits, the 16-byte upload id (S4 A-7).
    fn upload(&self) -> Result<rdb_api::Upload, String> {
        let text = self.flag("--upload")?;
        hex_bytes(text).map_err(|e| format!("--upload {text:?}: {e}; it is 32 hex digits"))
    }
}

/// `text` as exactly `N` bytes of hex.
fn hex_bytes<const N: usize>(text: &str) -> Result<[u8; N], String> {
    let bytes = hex::decode(text).map_err(|e| e.to_string())?;
    <[u8; N]>::try_from(bytes).map_err(|bytes| format!("{} bytes, not {N}", bytes.len()))
}

/// `blob upload`: each chunk, then the publish, unless `--stop-after` stops it first.
fn blob_upload(db: &Db, object: &str, rest: &[&str], sent: &mut Sent) -> Result<(), String> {
    let args = BlobArgs::parse(rest, &["--upload", "--chunk-size", "--stop-after"])?;
    let upload = args.upload()?;
    let text = args.text()?.as_bytes();
    let chunk_size: u64 = args.number("--chunk-size")?;
    let piece = usize::try_from(chunk_size)
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| format!("--chunk-size {chunk_size} is not a chunk length"))?;
    let stop_after = match args.flags.get("--stop-after") {
        Some(_) => Some(args.number::<usize>("--stop-after")?),
        None => None,
    };
    let size = u64::try_from(text.len()).expect("a line fits u64");
    let sha256: [u8; 32] = Sha256::digest(text).into();
    let chunks = text.chunks(piece).take(stop_after.unwrap_or(usize::MAX));
    for (index, bytes) in chunks.enumerate() {
        let change = Change::Chunk {
            upload,
            index: u32::try_from(index).map_err(|_| format!("chunk {index} is past u32"))?,
            bytes: Bytes::copy_from_slice(bytes),
        };
        let answer = db.write(object.as_bytes(), change, None);
        if !labelled_write_line(
            db,
            &format!("chunk {index} "),
            "already stored",
            answer,
            sent,
        ) {
            return Ok(());
        }
    }
    if let Some(k) = stop_after {
        let k = k.min(text.len().div_ceil(piece));
        let guard = match args.expect {
            Some(Expected::Absent) => " --absent".to_owned(),
            Some(Expected::Version(v)) => format!(" --expect {v}"),
            None => String::new(),
        };
        say(&format!(
            "stopped after {k} chunks; finish with: blob publish {object}{guard} --upload {} \
             --size {size} --chunk-size {chunk_size} --sha256 {}",
            hex::encode(upload),
            hex::encode(sha256)
        ));
        return Ok(());
    }
    let change = Change::Publish {
        upload,
        size,
        chunk_size,
        sha256,
    };
    let answer = db.write(object.as_bytes(), change, args.expect);
    labelled_write_line(db, "publish ", "already published", answer, sent);
    Ok(())
}

/// `blob chunk`: one chunk. A guard is passed on for the owner to refuse.
fn blob_chunk(db: &Db, object: &str, rest: &[&str], sent: &mut Sent) -> Result<(), String> {
    let args = BlobArgs::parse(rest, &["--upload", "--index"])?;
    let index: u32 = args.number("--index")?;
    let change = Change::Chunk {
        upload: args.upload()?,
        index,
        bytes: Bytes::copy_from_slice(args.text()?.as_bytes()),
    };
    let answer = db.write(object.as_bytes(), change, args.expect);
    labelled_write_line(
        db,
        &format!("chunk {index} "),
        "already stored",
        answer,
        sent,
    );
    Ok(())
}

/// `blob publish`: publish an upload whose chunks are stored.
fn blob_publish(db: &Db, object: &str, rest: &[&str], sent: &mut Sent) -> Result<(), String> {
    let args = BlobArgs::parse(rest, &["--upload", "--size", "--chunk-size", "--sha256"])?;
    if args.text.is_some() {
        return Err("blob publish takes no <text>".to_owned());
    }
    let sha = args.flag("--sha256")?;
    let change = Change::Publish {
        upload: args.upload()?,
        size: args.number("--size")?,
        chunk_size: args.number("--chunk-size")?,
        sha256: hex_bytes(sha)
            .map_err(|e| format!("--sha256 {sha:?}: {e}; it is 64 hex digits"))?,
    };
    let answer = db.write(object.as_bytes(), change, args.expect);
    labelled_write_line(db, "publish ", "already published", answer, sent);
    Ok(())
}

/// `blob gc`: one batch of the object's unreachable chunks at or below the floor. A guard is
/// passed on for the owner to refuse.
fn blob_gc(db: &Db, object: &str, rest: &[&str], sent: &mut Sent) -> Result<(), String> {
    let args = BlobArgs::parse(rest, &["--floor"])?;
    if args.text.is_some() {
        return Err("blob gc takes no <text>".to_owned());
    }
    let change = Change::Gc {
        floor: args.number("--floor")?,
    };
    let answer = db.write(object.as_bytes(), change, args.expect);
    labelled_write_line(db, "gc ", "nothing to collect", answer, sent);
    Ok(())
}

/// The lowest request id of `admin damage` and `damage-chunk`. Node status names an in-flight
/// request by id alone, with no client, so a note tells them apart by the ids this process
/// issued ([`is_damage_request`], S6 P-3, D8).
const DAMAGE_REQUEST_BASE: u64 = 1 << 32;

/// Damage ids one run may issue before it could reach those of a run started a millisecond
/// later.
const DAMAGE_IDS_PER_MILLI: u64 = 1 << 12;

/// The first damage id of a run started at `unix_millis`. Every run starts its own range, so a
/// later run never reuses an earlier run's id: the dedup index still holds that id under the
/// damage client, and a reuse was refused `GENERATION_CHANGED` with the earlier run's
/// generation (A-R191 2, tester d8b/d8c).
fn damage_start(unix_millis: u64) -> u64 {
    DAMAGE_REQUEST_BASE + unix_millis * DAMAGE_IDS_PER_MILLI
}

/// This run's first damage id, fixed at its first use.
fn this_runs_damage_start() -> u64 {
    static START: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *START.get_or_init(|| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock reads after 1970");
        damage_start(u64::try_from(now.as_millis()).expect("millis since 1970 fit a u64"))
    })
}

/// How many damage ids this run has issued.
static DAMAGE_ISSUED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A fresh request id for `admin damage` or `damage-chunk`, under their own client.
fn damage_request() -> RequestId {
    RequestId(this_runs_damage_start() + DAMAGE_ISSUED.fetch_add(1, Ordering::Relaxed))
}

/// Whether this process issued `request` for `admin damage` or `damage-chunk`. By origin, not
/// by size: from a folder's second open on, the `Db`'s own ids start at `(n-1)<<40`, and an id
/// alone was then read as a damage write (tester D8).
fn is_damage_request(request: RequestId) -> bool {
    let start = this_runs_damage_start();
    (start..start + DAMAGE_ISSUED.load(Ordering::Relaxed)).contains(&request.0)
}

/// `admin damage-chunk`: overwrite one stored chunk with other bytes, through a replicated
/// write, so a later read of the blob finds a chunk that fails its hash: `CORRUPT_BLOB`. A dev
/// tool only, to reach that answer by hand; the product never writes a chunk this way.
fn damage_chunk(db: &Db, object: &str, rest: &[&str]) -> Result<(), String> {
    use rdb_core::contracts::errors::ErrorKind;
    use rdb_core::contracts::ids::{ClientId, RequestIdentity, TenantId};
    use rdb_core::contracts::txn::{Condition, Mutation};
    let args = BlobArgs::parse(rest, &["--upload", "--index"])?;
    if args.expect.is_some() || args.text.is_some() {
        return Err(DAMAGE_USAGE.to_owned());
    }
    let index: u32 = args.number("--index")?;
    let upload = args.flag("--upload")?;
    let root = rdb_value::keys::root_key(TenantId(1), AffinityId(1), object.as_bytes());
    let key = rdb_value::keys::chunk_key(&root, &args.upload()?, index);
    let value = rdb_value::envelope::seal(rdb_value::envelope::Kind::Chunk, b"damaged")
        .expect("seven bytes fit an envelope");
    let request = TxnRequest {
        api_version: rdb_core::contracts::version::API_VERSION,
        identity: RequestIdentity {
            tenant: TenantId(1),
            client: ClientId(2),
            request: damage_request(),
        },
        affinity: AffinityId(1),
        expected_generation: None,
        remaining_millis: 5_000,
        // PB-6: only a stored chunk is damaged; a missing one is refused, never written.
        conditions: vec![Condition::Present { key: key.clone() }],
        mutations: vec![Mutation::Put {
            key,
            value,
            expected_version: None,
        }],
    };
    // PB-5: no request id is printed. This write is under damage-chunk's own client, so
    // `status` and `retry` would look up another write under the same number.
    say(&match db.resend(request) {
        Ok(ok) => format!(
            "damage-chunk ok p=1 gen={} seq={} {:?} request=- (damage-chunk's own client; \
             status does not know it)",
            ok.generation.0, ok.seq.0, ok.durability
        ),
        Err(PutError { error, .. }) if error.kind == ErrorKind::ConditionFailed => format!(
            "err admin damage-chunk: no stored chunk {index} for upload {upload} under {object}"
        ),
        Err(PutError { error, .. }) => format!(
            "err admin damage-chunk: {} retry={:?} request=- ({})",
            error.name(),
            error.retry,
            error.detail
        ),
    });
    Ok(())
}

const DAMAGE_USAGE: &str = "admin damage-chunk <object> --upload U --index I";

/// `admin damage`: overwrite an object's stored root record, through a replicated write under
/// damage-chunk's own client, so a later read of it fails (M9 S6). A dev tool only; the product
/// never writes a record this way. `--as`:
/// - `digest` (the default): a sealed document with one payload byte flipped, so the digest
///   check fails: `CORRUPT_HISTORY`;
/// - `kind0`: `kind` `0x00`, digest recomputed: damage, since no build writes kind 0;
/// - `newer`: `codec_version` `0x02`, digest recomputed: a record a newer build could have
///   written, valid but unreadable here.
///
/// Only a stored record is overwritten: a missing one is refused, never written.
fn damage_record(db: &Db, line: &str) -> Result<(), String> {
    use rdb_core::contracts::errors::ErrorKind;
    use rdb_core::contracts::ids::{ClientId, RequestIdentity, TenantId};
    use rdb_core::contracts::txn::{Condition, Mutation};
    use rdb_value::envelope::{digest, seal, Kind};
    // Read from the raw line, as a value verb is, so a quoted K keeps its quotes: `"b"` is the
    // text b, `1` the integer.
    let mut words = Words::new(line);
    let usage = || DAMAGE_RECORD_USAGE.to_owned();
    let (Some("admin"), Some("damage"), Some(object)) = (words.next(), words.next(), words.next())
    else {
        return Err(usage());
    };
    let (mut how, mut element, mut item, mut api_version) = (None, None, None, None);
    while let Some(flag) = words.next() {
        match (flag, words.rest.trim_start()) {
            ("--as", _) if how.is_none() => match words.next() {
                Some(word @ ("digest" | "kind0" | "newer")) => how = Some(word),
                _ => return Err(usage()),
            },
            ("--element", text) if element.is_none() && item.is_none() => {
                let value = words.key("--element K")?;
                let typed = text[..text.len() - words.rest.len()].trim_end();
                element = Some((value, typed));
            }
            ("--item", _) if item.is_none() && element.is_none() => {
                let p = words.next().ok_or_else(usage)?;
                item = Some(p.parse::<i64>().map_err(|_| usage())?);
            }
            ("--api-version", _) if api_version.is_none() => {
                let n = words.next().ok_or_else(usage)?;
                api_version = Some(n.parse::<u16>().map_err(|_| usage())?);
            }
            _ => return Err(usage()),
        }
    }
    let how = how.unwrap_or("digest");
    let payload = rdb_value::cbor::encode(&rdb_value::value::Value::Map(Default::default()))
        .expect("an empty map encodes");
    let mut bytes = seal(Kind::Document, &payload)
        .expect("an empty map fits an envelope")
        .to_vec();
    match how {
        "digest" => bytes[rdb_value::envelope::HEADER_LEN] ^= 0xFF,
        reseal => {
            if reseal == "kind0" {
                bytes[1] = 0x00;
            } else {
                bytes[2] = 0x02;
            }
            let head: [u8; 8] = bytes[..8].try_into().expect("a header is 40 bytes");
            let sum = digest(&head, &bytes[rdb_value::envelope::HEADER_LEN..]);
            bytes[8..rdb_value::envelope::HEADER_LEN].copy_from_slice(&sum);
        }
    }
    let root = rdb_value::keys::root_key(TenantId(1), AffinityId(1), object.as_bytes());
    // An element gets the same bytes: a map entry that does not open, or a set member that holds
    // bytes, either of which a read refuses. So no read is needed to learn the kind.
    let (key, what) = match (&element, item) {
        (Some((k, typed)), _) => (
            rdb_value::keys::element_key(&root, k)
                .map_err(|e| format!("admin damage: --element {e}"))?,
            format!("{object} element {typed}"),
        ),
        // M9 S7: an item's own record, found by one items read. An inline item has none, so its
        // key is not stored and the `Present` condition refuses it.
        (None, Some(at)) => {
            let what = format!("{object} item {at}");
            let page = match db.read(object.as_bytes(), Look::Items { at, limit: 1 }) {
                Ok(ReadOk {
                    seen: rdb_api::Seen::Items(page),
                    ..
                }) => page,
                Ok(ReadOk {
                    seen: rdb_api::Seen::Absent,
                    ..
                }) => {
                    say(&format!("err admin damage: no stored record at {what}"));
                    return Ok(());
                }
                Ok(_) => return Err(format!("admin damage: --item: {object} is not a list")),
                Err(error) => {
                    // The lookup's own error, without a retry rule: nothing was written.
                    say(&format!(
                        "err admin damage: --item read {} ({})",
                        error.name(),
                        error.detail
                    ));
                    return Ok(());
                }
            };
            let Some(found) = page.items.first() else {
                say(&format!("err admin damage: no stored record at {what}"));
                return Ok(());
            };
            (rdb_value::keys::item_key(&root, found.id), what)
        }
        (None, None) => (root.to_bytes(), object.to_owned()),
    };
    let mut mutations = vec![Mutation::Put {
        key: key.clone(),
        value: Bytes::from(bytes),
        expected_version: None,
    }];
    // An element written alone lands at a version above its root's, and a read names that
    // first (`element version V is above its root's`), never the fault `--as` asked for; a map
    // write over it is refused too. So the root's own bytes go in the same write, at the
    // version read: both carry the write's version (lead, on the 8e083a9 walk).
    if element.is_some() {
        match root_rewrite(db, object, &root, &what) {
            // A-R211: a set member has no envelope, so a sealed one there is only bytes, which
            // reads as damage whatever `--as` says. Refused before anything is written.
            Ok((_, CollectionKind::Set)) if matches!(how, "newer" | "kind0") => {
                return Err(format!(
                    "admin damage: {SET_MEMBER_AS}; not a member with --as {how}"
                ));
            }
            Ok((put, _)) => mutations.push(put),
            Err(said) => {
                say(&format!("err admin damage: {said}"));
                return Ok(());
            }
        }
    }
    let request = TxnRequest {
        // M9 S7: `--api-version N` sends the write as a newer (or older) build's request.
        api_version: api_version.unwrap_or(rdb_core::contracts::version::API_VERSION),
        identity: RequestIdentity {
            tenant: TenantId(1),
            client: ClientId(2),
            request: damage_request(),
        },
        affinity: AffinityId(1),
        expected_generation: None,
        remaining_millis: 5_000,
        // Only a stored record is damaged; a missing one is refused, never written.
        conditions: vec![Condition::Present { key }],
        mutations,
    };
    // No request id is printed, as for damage-chunk (PB-5): another client's number.
    say(&match db.resend(request) {
        Ok(ok) => format!(
            "damage ok p=1 gen={} seq={} {:?} as={how} request=- (damage's own client; status \
             does not know it)",
            ok.generation.0, ok.seq.0, ok.durability
        ),
        Err(PutError { error, .. }) if error.kind == ErrorKind::ConditionFailed => {
            format!("err admin damage: no stored record at {what}")
        }
        Err(PutError { error, .. }) => format!(
            "err admin damage: {} retry={:?} request=- ({})",
            error.name(),
            error.retry,
            error.detail
        ),
    });
    Ok(())
}

/// The map's or set's root record as it is stored, `{"keys": 1, "count": n}` sealed under its
/// kind (ADR-rdb-0013), written back at the version a root-only read saw. A read with no
/// element listed checks no element, so a root over an already damaged entry still reads.
fn root_rewrite(
    db: &Db,
    object: &str,
    root: &rdb_value::keys::RootKey,
    what: &str,
) -> Result<(rdb_core::contracts::txn::Mutation, CollectionKind), String> {
    use rdb_core::contracts::errors::ErrorKind;
    use rdb_value::collection::KEY_PROFILE_V1;
    use rdb_value::value::{Map, MapKey};
    let not_collection = format!("--element: {object} is not a map or a set");
    let look = Look::Members {
        after: None,
        limit: 0,
    };
    let found = match db.read(object.as_bytes(), look) {
        Ok(ReadOk {
            seen: rdb_api::Seen::Members(page),
            ..
        }) => page.collection,
        Ok(ReadOk {
            seen: rdb_api::Seen::Absent,
            ..
        }) => return Err(format!("no stored record at {what}")),
        Ok(_) => return Err(not_collection),
        Err(error) if error.kind == ErrorKind::InvalidArgument => return Err(not_collection),
        Err(error) => {
            return Err(format!(
                "--element root read {} ({})",
                error.name(),
                error.detail
            ))
        }
    };
    let mut fields = Map::new();
    fields.insert(
        MapKey::new("keys"),
        Value::Integer(Int::new(KEY_PROFILE_V1).expect("1 is an integer")),
    );
    fields.insert(MapKey::new("count"), Value::Integer(Int::from(found.count)));
    let payload = rdb_value::cbor::encode(&Value::Map(fields)).expect("a two-field map encodes");
    let sealed = rdb_value::envelope::seal(found.kind.kind(), &payload)
        .expect("a root payload is a few bytes");
    Ok((
        rdb_core::contracts::txn::Mutation::Put {
            key: root.to_bytes(),
            value: sealed,
            expected_version: Some(found.version),
        },
        found.kind,
    ))
}

/// Why `admin damage <set> --element K` takes only `--as digest` (A-R211).
const SET_MEMBER_AS: &str = "a set member holds no envelope, so any bytes in it are damage; \
     damage a set member with --as digest, or a newer build's set with the root and --as newer";

const DAMAGE_RECORD_USAGE: &str = "admin damage <object> [--element K | --item P] \
     [--as digest|kind0|newer] [--api-version N]; K is a map key or set member, P a list \
     position, N 0..=65535";

/// `read`'s words after the object: nothing, the manifest alone, or `--offset O --len L`.
fn parse_range(rest: &[&str]) -> Result<Look, String> {
    let number = |flag: &str, text: &str| {
        text.parse::<u64>()
            .map_err(|_| format!("{flag} {text:?} is not a number"))
    };
    match rest {
        [] => Ok(Look::Range { offset: 0, len: 0 }),
        ["--offset", offset, "--len", len] | ["--len", len, "--offset", offset] => {
            Ok(Look::Range {
                offset: number("--offset", offset)?,
                len: number("--len", len)?,
            })
        }
        _ => Err(READ_USAGE.to_owned()),
    }
}

/// A `Db::read` answer as lines. A byte-string document prints as `get` always has; any other
/// document as JSON.
/// `get` reads a document. On another kind the read says only `a Map does not take this op`
/// (tester, embed.md walk), so the verb that reads that kind is named after it.
fn get_hint(mut error: ApiError) -> ApiError {
    let verb = [
        ("a Map does not take this op", "members <object>"),
        ("a Set does not take this op", "members <object>"),
        ("a List does not take this op", "items <object>"),
        ("a Blob does not take this op", "read <object>"),
    ]
    .into_iter()
    .find_map(|(said, verb)| (error.detail == said).then_some(verb));
    if let Some(verb) = verb {
        error.detail = format!(
            "{}; `get` reads a document, read this one with `{verb}`",
            error.detail
        );
    }
    error
}

fn read_line(answer: Result<ReadOk, ApiError>) {
    let ok = match answer {
        Ok(ok) => ok,
        Err(error) => {
            // `retry=` too: a stale token's `Reconcile` is the line's point (C-2).
            say(&format!(
                "err {} retry={:?} ({})",
                error.name(),
                error.retry,
                error.detail
            ));
            return;
        }
    };
    let view = format!(
        "gen={} at={}{}",
        ok.generation.0,
        ok.at.0,
        if ok.waited { " waited" } else { "" }
    );
    match ok.seen {
        rdb_api::Seen::Absent => say(&format!("absent {view}")),
        rdb_api::Seen::Doc(doc) => match doc.value {
            Value::Bytes(bytes) => say(&format!(
                "value={} version={} {view}",
                String::from_utf8_lossy(&bytes),
                doc.version
            )),
            value => say(&format!(
                "doc={} version={} {view}",
                json::render(&value),
                doc.version
            )),
        },
        // `map version=V count=N`, then one line per element: `K = J` for a map entry, `K` for
        // a set member.
        rdb_api::Seen::Members(page) => {
            let root = page.collection;
            let kind = match root.kind {
                CollectionKind::Map => "map",
                CollectionKind::Set => "set",
            };
            say(&format!(
                "{kind} version={} count={} listed={} {view}",
                root.version,
                root.count,
                page.members.len()
            ));
            for member in page.members {
                match member.value {
                    Some(value) => say(&format!(
                        "  {} = {}",
                        json::render(&member.key),
                        json::render(&value)
                    )),
                    None => say(&format!("  {}", json::render(&member.key))),
                }
            }
        }
        // `list version=V count=C listed=L`, one `  P J` line per item (`inline=false` when the
        // item has its own record), then `token=G:V:P` when items remain.
        rdb_api::Seen::Items(page) => {
            let root = page.list;
            say(&format!(
                "list version={} count={} listed={} {view}",
                root.version,
                root.count,
                page.items.len()
            ));
            for item in page.items {
                say(&format!(
                    "  {} {}{}",
                    item.position,
                    json::render(&item.value),
                    if item.inline { "" } else { " inline=false" }
                ));
            }
            if let Some(next) = page.next {
                say(&format!(
                    "token={}:{}:{}",
                    next.generation.0, next.version, next.position
                ));
            }
        }
        // `blob version=V size=S chunks=N upload=U`, then `bytes="..."` for a non-empty range.
        rdb_api::Seen::Range { blob, bytes } => {
            let m = &blob.manifest;
            let range = if bytes.is_empty() {
                String::new()
            } else {
                format!(" bytes={:?}", String::from_utf8_lossy(&bytes))
            };
            say(&format!(
                "blob version={} size={} chunks={} upload={}{range} {view}",
                blob.version,
                m.size,
                m.chunks(),
                hex::encode(m.upload)
            ));
        }
    }
}

/// Split a command line on spaces; `"..."` is one word, and `""` an empty one.
fn split_words(line: &str) -> Result<Vec<String>, String> {
    let mut words = Vec::new();
    let mut chars = line.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
            continue;
        }
        let mut word = String::new();
        if c == '"' {
            chars.next();
            loop {
                match chars.next() {
                    Some('"') => break,
                    Some(c) => word.push(c),
                    None => return Err(format!("unclosed quote in {line:?}")),
                }
            }
        } else {
            while let Some(&c) = chars.peek() {
                if c.is_whitespace() {
                    break;
                }
                word.push(c);
                chars.next();
            }
        }
        words.push(word);
    }
    Ok(words)
}

/// One txn op as typed.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TxnOp {
    group: u64,
    object: String,
    value: String,
    if_version: Option<u64>,
}

fn txn_puts(ops: &[TxnOp]) -> Vec<TxnPut<'_>> {
    ops.iter()
        .map(|op| TxnPut {
            group: AffinityId(op.group),
            object: op.object.as_bytes(),
            value: op.value.as_bytes(),
            if_version: op.if_version,
        })
        .collect()
}

/// `[--deadline-ms N] <op>...`, with `default` as the deadline when none is given. No ops is
/// not a usage error: the kernel refuses an empty transaction, and that is worth seeing.
fn parse_txn(rest: &[&str], default: Duration) -> Result<(Duration, Vec<TxnOp>), String> {
    const USAGE: &str = "txn [--deadline-ms N] [<group>:]<object>=<value>[@<version>]...";
    let (deadline, ops) = match rest {
        ["--deadline-ms", millis, ops @ ..] => (
            Duration::from_millis(
                millis
                    .parse()
                    .map_err(|_| format!("{USAGE}: --deadline-ms takes milliseconds"))?,
            ),
            ops,
        ),
        ops => (default, ops),
    };
    let ops = ops
        .iter()
        .map(|op| parse_op(op).map_err(|e| format!("{USAGE}: {e}")))
        .collect::<Result<_, _>>()?;
    Ok((deadline, ops))
}

/// `[<group>:]<object>=<value>[@<version>]` (S1 ruling A5): the object ends at the first `=`,
/// a group is only a leading `digits:`, and a version only a trailing `@digits`. Anything else
/// stays in the object or the value.
fn parse_op(op: &str) -> Result<TxnOp, String> {
    let (left, right) = op
        .split_once('=')
        .ok_or_else(|| format!("{op:?} has no `=`"))?;
    let (group, object) = match left.split_once(':') {
        Some((digits, object)) if is_digits(digits) => (
            digits
                .parse()
                .map_err(|_| format!("{op:?}: group {digits} is too large"))?,
            object,
        ),
        _ => (1, left),
    };
    let (value, if_version) = match right.rsplit_once('@') {
        Some((value, digits)) if is_digits(digits) => (
            value,
            Some(
                digits
                    .parse()
                    .map_err(|_| format!("{op:?}: version {digits} is too large"))?,
            ),
        ),
        _ => (right, None),
    };
    Ok(TxnOp {
        group,
        object: object.to_owned(),
        value: value.to_owned(),
        if_version,
    })
}

/// `control fail put <prefix> <word> [n]`'s fault and count: at least one put, 1 by default.
fn parse_put_fault(word: &str, rest: &[&str]) -> Result<(Option<PutFault>, u64), String> {
    let fault = PutFault::parse(word)?;
    let count = match rest {
        [] => 1,
        [n] => match n.parse::<u64>() {
            Ok(n) if n > 0 => n,
            _ => {
                return Err(format!(
                    "control fail put: n is a count of 1 or more, not {n:?}"
                ))
            }
        },
        _ => {
            return Err(format!(
                "control fail put <prefix> <{}> [n]",
                PutFault::WORDS
            ))
        }
    };
    Ok((fault, count))
}

fn is_digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit())
}

/// Whether `request` is an app's id, one `app_request_id` built, which `put_as` takes.
fn is_app_id(request: RequestId) -> bool {
    let bit = rdb_api::app_request_id(0).expect("0 is below 2^63").0;
    request.0 & bit != 0
}

/// The request id `put --as <n>` sends under: `app_request_id(n)` (A-R195a).
fn parse_app_id(n: &str) -> Result<RequestId, String> {
    n.parse::<u64>()
        .ok()
        .and_then(rdb_api::app_request_id)
        .ok_or_else(|| format!("put --as N: N is a number below 2^63, not {n}"))
}

fn parse_if_version(rest: &[&str]) -> Result<Option<u64>, String> {
    match rest {
        [] => Ok(None),
        ["--if-version", n] => n
            .parse()
            .map(Some)
            .map_err(|_| "--if-version takes a number".to_owned()),
        _ => Err("put <object> <value> [--if-version N]".to_owned()),
    }
}

/// What `retry <id> --payload` re-puts: the object and condition of the put `request` was
/// sent as. A txn has no single object, so it is refused (S1 guard G10).
/// Only under an app id: `put_as` refuses one this Db numbered (A-R195a).
fn payload_put(kind: Kind, request: RequestId) -> Result<(Vec<u8>, Option<u64>), String> {
    match kind {
        Kind::Put { object, if_version } if is_app_id(request) => Ok((object, if_version)),
        Kind::Put { .. } => Err(format!(
            "retry --payload sends a new value under request {}, an id this Db numbered itself, \
             and the Db refuses that: nothing was sent. Send the write under an id of your own \
             instead: put --as <n> <object> <value>, then retry it with --payload",
            request.0
        )),
        Kind::Txn => Err(format!(
            "retry --payload changes a put's value; request {} was a txn",
            request.0
        )),
        Kind::Value => Err(format!(
            "retry --payload changes a put's value; request {} was a value write: retry it \
             unchanged, or send a new write",
            request.0
        )),
        Kind::Clear => Err(format!(
            "retry --payload changes a put's value; request {} was a clear: retry it unchanged",
            request.0
        )),
    }
}

/// What a recorded request was sent as. Saved beside the request in `--requests F`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum Kind {
    /// A put, with the object and condition it was put with, for `retry --payload`.
    Put {
        object: Vec<u8>,
        if_version: Option<u64>,
    },
    /// A txn.
    Txn,
    /// A `Db::write`: `doc`, `map`, `set`, `list` or a `blob` line. Resent unchanged, never with
    /// another payload (S4 A-5).
    Value,
    /// An `admin clear`, resent unchanged as a value write is.
    Clear,
}

/// Every write request this session sent, and which came last. `retry` replays the
/// latest, whatever it answered, or nothing when the latest sent nothing (PC11).
#[derive(Debug, Default)]
struct Sent {
    /// The request, and what it was sent as.
    by_id: std::collections::BTreeMap<u64, (TxnRequest, Kind)>,
    latest: Option<Option<u64>>,
    /// `--requests F`: each recorded request is appended here, and an id this run did not send
    /// is looked up here.
    file: Option<PathBuf>,
    /// An append to `--requests F` failed: the run exits 1 (k214).
    save_failed: std::cell::Cell<bool>,
    /// The lines of F already warned about: each is said once per run, not on every retry.
    warned: std::collections::BTreeSet<usize>,
}

/// `true` when `file` is not empty and its last byte is not a newline.
fn ends_mid_line(file: &mut std::fs::File) -> std::io::Result<bool> {
    use std::io::{Read as _, Seek as _, SeekFrom};
    if file.metadata()?.len() == 0 {
        return Ok(false);
    }
    file.seek(SeekFrom::End(-1))?;
    let mut last = [0u8; 1];
    file.read_exact(&mut last)?;
    Ok(last[0] != b'\n')
}

/// What `retry <rest>` names, its `--payload V` or `--deadline-ms N` aside.
#[derive(Debug, PartialEq, Eq)]
enum RetryTarget {
    /// The latest write.
    Latest,
    /// Request `id`, as `retry <id>` or `retry --id <id>`.
    Id(u64),
    /// Anything else: [`Sent::pick`] answers it with a usage line.
    Other,
}

fn retry_target(rest: &[&str]) -> RetryTarget {
    let target = match rest {
        [target @ .., "--payload" | "--deadline-ms", _] => target,
        target => target,
    };
    match target {
        [] => RetryTarget::Latest,
        [id] | ["--id", id] => id.parse().map_or(RetryTarget::Other, RetryTarget::Id),
        _ => RetryTarget::Other,
    }
}

impl Sent {
    fn record(&mut self, request: Option<TxnRequest>, kind: Kind) {
        let id = request.map(|request| {
            let id = request.identity.request.0;
            self.save(&request, &kind);
            self.by_id.insert(id, (request, kind));
            id
        });
        self.latest = Some(id);
    }

    /// A retry's answer: the request it carries is what was sent last under its id, so the
    /// next `retry` of that id sends it (D1). An answer with no request sent nothing, and
    /// changes nothing.
    fn resent(&mut self, answer: &Result<rdb_api::PutOk, PutError>) {
        let request = match answer {
            Ok(ok) => Some(&*ok.sent),
            Err(PutError { request, .. }) => request.as_deref(),
        };
        if let Some(request) = request {
            let id = request.identity.request.0;
            if let Some(entry) = self.by_id.get_mut(&id) {
                entry.0 = request.clone();
                self.latest = Some(Some(id));
            }
            if let Some((request, kind)) = self.by_id.get(&id) {
                self.save(request, kind);
            }
        }
    }

    /// A `retry --deadline-ms` answer. A deadline past `MAX_TXN_DEADLINE` is refused before
    /// anything is sent, and the request handed back is the one picked, unchanged, so nothing
    /// changes: the next bare `retry` still sends the latest (PR #36 F-002).
    fn resent_within(&mut self, deadline: Duration, answer: &Result<rdb_api::PutOk, PutError>) {
        if deadline <= rdb_api::MAX_TXN_DEADLINE {
            self.resent(answer);
        }
    }

    /// The plain answer when `retry <rest>` has nothing to send: no write yet, a latest write
    /// that sent no request, or a request id this session never sent. For that last one, the
    /// way to resend an earlier run's request is `retry --id`. `None` when there is something
    /// to send, or when `rest` is not a retry of a recorded request at all.
    fn nothing_to_retry(&self, rest: &[&str]) -> Option<String> {
        match retry_target(rest) {
            RetryTarget::Latest => match self.latest {
                None => Some("nothing to retry: this run has sent no write yet".to_owned()),
                Some(None) => Some(
                    "nothing to retry: the latest write was refused before it was sent".to_owned(),
                ),
                Some(Some(_)) => None,
            },
            RetryTarget::Id(id) if !self.by_id.contains_key(&id) => Some(match &self.file {
                Some(path) => format!(
                    "nothing to retry as request {id}: this run did not send it, and {} \
                         holds no request {id}",
                    path.display()
                ),
                None => format!(
                    "nothing to retry as request {id}: this run did not send it. To resend \
                         an earlier run's request, start that run and this one with --requests \
                         F, then retry {id}"
                ),
            }),
            RetryTarget::Id(_) | RetryTarget::Other => None,
        }
    }

    /// Take request `id` from `--requests F` when this run did not send it, so `retry` resends
    /// an earlier run's request unchanged. F's last line for `id` wins: a retry appends what it
    /// sent. A missing F holds nothing; an F that cannot be read is an error. A line that does
    /// not parse is skipped, and the answer names it among the warnings, by line only.
    fn adopt(&mut self, rest: &[&str]) -> Result<Vec<String>, String> {
        let (Some(path), RetryTarget::Id(id)) = (&self.file, retry_target(rest)) else {
            return Ok(Vec::new());
        };
        if self.by_id.contains_key(&id) {
            return Ok(Vec::new());
        }
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(format!("--requests {}: {e}", path.display())),
        };
        let mut found = None;
        let mut warnings = Vec::new();
        let last = text.lines().count();
        for (at, line) in text.lines().enumerate() {
            let entry: (TxnRequest, Kind) = match serde_json::from_str(line) {
                Ok(entry) => entry,
                // Skipped, so one bad line never blocks the other ids (k214). Where and what
                // class of error, never serde's message: that quotes the text it met, and a
                // line holds keys and values.
                Err(e) => {
                    let line = at + 1;
                    if !self.warned.insert(line) {
                        continue;
                    }
                    let torn = line == last && !text.ends_with('\n') && e.is_eof();
                    tracing::warn!(line, torn, class = ?e.classify(), "requests_file_line_skipped");
                    warnings.push(if torn {
                        format!(
                            "--requests {} line {line}: cut short, as a kill during its write \
                             leaves it; skipped",
                            path.display()
                        )
                    } else {
                        format!(
                            "--requests {} line {line}: not a saved request ({:?} error at \
                             column {}); skipped",
                            path.display(),
                            e.classify(),
                            e.column()
                        )
                    });
                    continue;
                }
            };
            if entry.0.identity.request.0 == id {
                found = Some(entry);
            }
        }
        if let Some(entry) = found {
            self.by_id.insert(id, entry);
        }
        Ok(warnings)
    }

    /// Append `request` to `--requests F`, synced, when there is an F. A failed write is said
    /// on the run's output, logged, and makes the run exit 1: a later run could not retry it.
    fn save(&self, request: &TxnRequest, kind: &Kind) {
        let Some(path) = &self.file else {
            return;
        };
        let saved = serde_json::to_string(&(request, kind))
            .map_err(|e| e.to_string())
            .and_then(|line| {
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .read(true)
                    .append(true)
                    .open(path)
                    .map_err(|e| e.to_string())?;
                // A kill during an append leaves F ending mid-line. Start a line of our own, or
                // this request would be glued onto that tail and lost to a later run.
                let torn = ends_mid_line(&mut file).map_err(|e| e.to_string())?;
                writeln!(file, "{}{line}", if torn { "\n" } else { "" })
                    .and_then(|()| file.sync_data())
                    .map_err(|e| e.to_string())
            });
        if let Err(e) = saved {
            let request = request.identity.request.0;
            tracing::error!(error = %e, request, "requests_file_write_failed");
            self.save_failed.set(true);
            say(&format!(
                "err --requests {}: {e}; a later run cannot retry request {request}",
                path.display()
            ));
        }
    }

    fn pick(&self, rest: &[&str]) -> Result<(TxnRequest, Kind), String> {
        let id = match rest {
            [] => match self.latest {
                None => return Err("no write to retry yet".to_owned()),
                Some(None) => {
                    return Err("the latest write sent no request; nothing to retry".to_owned())
                }
                Some(Some(id)) => id,
            },
            [id] | ["--id", id] => id
                .parse()
                .map_err(|_| "retry [<request>]: a request id is a number".to_owned())?,
            _ => return Err("retry [<request>] [--payload <value> | --deadline-ms N]".to_owned()),
        };
        self.by_id
            .get(&id)
            .cloned()
            .ok_or_else(|| format!("this session sent no write as request {id}"))
    }
}

/// Print a put's or txn's answer, and remember what it sent when `record` names where. A
/// refusal is followed by a `note:` line when node 1's queue waits on a write it applied and
/// has not published ([`waits_note`]).
fn put_line(db: &Db, answer: Result<rdb_api::PutOk, PutError>, record: Option<(&mut Sent, Kind)>) {
    labelled_put_line(db, "", answer, record);
}

/// [`put_line`] with `label` before the answer, as `chunk 0 ok ...`.
fn labelled_put_line(
    db: &Db,
    label: &str,
    answer: Result<rdb_api::PutOk, PutError>,
    record: Option<(&mut Sent, Kind)>,
) {
    let remember = |request: Option<TxnRequest>| {
        if let Some((sent, kind)) = record {
            sent.record(request, kind);
        }
    };
    say(&format!("{label}{}", put_text(&answer)));
    if answer.is_err() {
        if let Some(note) = waits_note(db) {
            say(&note);
        }
    }
    match answer {
        Ok(ok) => remember(Some(*ok.sent)),
        Err(PutError { request, .. }) => remember(request.map(|request| *request)),
    }
}

/// After a refusal, the reason when node 1's queue waits on a write it applied and has not
/// published (U1, ruling A-R109): the refusal names only its kind, `PROTECTION_PAUSED`, and not
/// the write the partition waits on, nor that healing the links resolves it.
fn waits_note(db: &Db) -> Option<String> {
    let nodes = db.node_status();
    let owner = nodes.iter().find(|node| node.node == NodeId(1))?;
    let links = db.links();
    waits_on_line(owner, &links.held(), &links.dropped()).map(|line| format!("note: {line}"))
}

/// [`waits_note`] for a background `put&` or `txn&`, under the same `label` as its answer
/// line, so the note cannot be read as another command's.
fn bg_note(db: &Db, label: &str, answer: &Result<rdb_api::PutOk, PutError>) {
    if answer.is_err() {
        if let Some(note) = waits_note(db) {
            say(&format!("{label}: {note}"));
        }
    }
}

/// A put's answer as one line.
fn put_text(answer: &Result<rdb_api::PutOk, PutError>) -> String {
    match answer {
        Ok(ok) => format!(
            "ok p=1 gen={} seq={} {:?} request={}",
            ok.generation.0, ok.seq.0, ok.durability, ok.request.0
        ),
        Err(PutError { error, request }) => {
            let id = request
                .as_ref()
                .map_or_else(|| "-".to_owned(), |r| r.identity.request.0.to_string());
            format!(
                "err {} retry={:?} no_mutation={} request={id} ({})",
                error.name(),
                error.retry,
                error.no_mutation,
                error.detail
            )
        }
    }
}

fn get_line(answer: Result<rdb_api::GetOk, rdb_api::ApiError>) {
    match answer {
        Ok(ok) => match ok.value {
            Some((version, bytes)) => say(&format!(
                "value={} version={version} gen={} at={}{}",
                String::from_utf8_lossy(&bytes),
                ok.generation.0,
                ok.at.0,
                if ok.waited { " waited" } else { "" }
            )),
            None => say(&format!("absent gen={} at={}", ok.generation.0, ok.at.0)),
        },
        Err(error) => say(&format!("err {} ({})", error.name(), error.detail)),
    }
}

fn status_line(db: &Db, sent: &Sent, request: &str, rest: &[&str]) -> Result<(), String> {
    let request = request
        .parse::<u64>()
        .map_err(|_| "status <request> [<generation>]".to_owned())?;
    let generation = match rest {
        [] => None,
        [g] => Some(Generation(
            g.parse()
                .map_err(|_| "a generation is a number".to_owned())?,
        )),
        _ => return Err("status <request> [<generation>]".to_owned()),
    };
    // The nodes first: a request node 1 holds in flight that publishes between the two looks is
    // then answered by the status, never called not in flight (Msg 8, W6-D1).
    let nodes = db.node_status();
    match db.status(RequestId(request), generation) {
        // After a restart, `Unknown` can be a write this session saw `ok` (tester W3-D2).
        Ok(TxnStatus::Unknown) => {
            // `None` when node 1 did not answer in time (F-025).
            let in_flight = nodes
                .iter()
                .find(|node| node.node == NodeId(1))
                .map(|owner| owner.in_flight == Some(RequestId(request)));
            let restarted = restarted_nodes(|node| db.boot_of(node));
            let note = db.restart_note();
            let ours = sent.by_id.contains_key(&request);
            say(&unknown_line(ours, in_flight, &restarted, note.as_deref()));
        }
        Ok(status) => say(&known_status_line(request, &status)),
        Err(error) => say(&format!("err {} ({})", error.name(), error.detail)),
    }
    Ok(())
}

/// A `status` answer other than `Unknown`, in plain numbers as a put's `ok` line (PB-2).
fn known_status_line(request: u64, status: &TxnStatus) -> String {
    use rdb_core::contracts::txn::Outcome;
    match status {
        TxnStatus::Resolved(result) => format!(
            "status resolved: {} p={} gen={} seq={} epoch={} {:?} request={request}",
            match result.outcome {
                Outcome::Published => "published",
                Outcome::RecoveredApplied => "recovered-applied",
            },
            result.partition.0,
            result.generation.0,
            result.seq.0,
            result.owner_epoch.0,
            result.durability,
        ),
        TxnStatus::Unresolved { seq } => {
            format!("status unresolved: seq={} request={request}", seq.0)
        }
        TxnStatus::Unknown => "status Unknown".to_owned(),
        TxnStatus::Expired => format!("status expired request={request}"),
    }
}

/// The nodes that restarted, from each node's boot (`boot_of`), never from the nodes that
/// answered `node_status`: that leaves out a node whose restart failed, and `status` of a write
/// in flight then said it waits for M10 while the other secondary could still take it.
fn restarted_nodes(boot_of: impl Fn(NodeId) -> Option<BootId>) -> Vec<NodeId> {
    [1, 2, 3]
        .map(NodeId)
        .into_iter()
        .filter(|&node| boot_of(node).is_some_and(|boot| boot != rdb_api::host::BOOT))
        .collect()
}

/// `status Unknown`, with the restart note when there is one, unless this session never sent
/// the request (`ours` false): that outcome is known (tester walk 5). After the owner's
/// restart its outcome waits for M10. Otherwise it follows what node 1 holds (Msg 8, W6-D1): a
/// request not `in_flight` there was refused before it was queued, or never arrived, or was
/// answered and its answer trimmed past retention, so it may have applied (M9 close-out D1:
/// `Sent` keeps no answer, so the line cannot tell these apart), and nothing will resolve it.
/// One in flight waits for M10 after both secondaries' restarts, and
/// after one secondary's resolves once the other can take it, as when a held link heals
/// (W5-D1). `in_flight` is `None` when node 1 did not answer in time: whether it holds the
/// request is then not known, so the line promises nothing and says to ask again (F-025).
fn unknown_line(
    ours: bool,
    in_flight: Option<bool>,
    restarted: &[NodeId],
    restart_note: Option<&str>,
) -> String {
    let Some(note) = restart_note.filter(|_| ours) else {
        return "status Unknown".to_owned();
    };
    let others: Vec<u32> = [2, 3]
        .into_iter()
        .filter(|node| !restarted.contains(&NodeId(*node)))
        .collect();
    let owner_restarted = restarted.contains(&NodeId(1));
    match others.as_slice() {
        _ if !owner_restarted && in_flight.is_none() => format!(
            "status Unknown (node 1 did not answer in time, so whether it holds it in flight is \
             not known; try again; {note})"
        ),
        _ if !owner_restarted && in_flight == Some(false) => format!(
            "status Unknown (node 1 does not hold it in flight, so waiting will not change \
             this: it was refused before it was queued, or never arrived, or its answer was \
             trimmed after retention (it may have applied); {note})"
        ),
        [other] if !owner_restarted => format!(
            "status Unknown (not evidence of a lost write: it resolves once node {other}, which \
             did not restart, can take it; {note})"
        ),
        _ => format!(
            "status Unknown (not evidence of a lost write: its outcome cannot be known until a \
             restarted node rejoins, which is M10; {note})"
        ),
    }
}

/// The `nodes` line for a node `node_status` left out, which it does both for a node whose
/// restart failed, so no process runs it (`failed`, F-013), and for one that did not answer in
/// time. A node left out is never dropped from the list silently (W7-D3).
fn missing_node_line(node: NodeId, boot: Option<BootId>, failed: bool) -> String {
    let boot = boot.map_or_else(|| "?".to_owned(), |boot| boot.0.to_string());
    if failed {
        format!(
            "node={} boot={boot} stopped: its restart as boot {boot} failed, so no process runs \
             it",
            node.0
        )
    } else {
        format!(
            "node={} boot={boot} did not answer in time; try again",
            node.0
        )
    }
}

/// One node's line. `published=` is left off a secondary: P1 publishes nothing there, so its
/// `0@<gen>` would read as a real position (PC12).
fn node_line(node: &NodeStatus) -> String {
    let published = match node.published {
        _ if node.role == "secondary" => String::new(),
        Some((g, s)) => format!(" published={}@{}", s.0, g.0),
        None => " published=-".to_owned(),
    };
    format!(
        "node={} boot={} {} authority={} recovery={} recovered={} role={} l1={} admits={}{published}{}{}",
        node.node.0,
        node.boot.0,
        holds_part(node),
        node.authority,
        node.recovery.as_deref().unwrap_or("-"),
        node.recovered
            .map_or_else(|| "-".to_owned(), |g| g.0.to_string()),
        node.role,
        node.protection.as_deref().unwrap_or("inert"),
        node.admits
            .map_or_else(|| "-".to_owned(), |a| a.to_string()),
        node.reload_pending
            .map_or_else(String::new, |n| format!(" reload pending attempt={n}")),
        node.fault
            .as_ref()
            .map_or_else(String::new, |f| format!(" FAULT={f}")),
    )
}

/// `control`: the records, read from the voter that is leader now through a client of its own,
/// never through the Db's store, so what that store is doing never hides them (critic A3).
fn control_lines(rt: &tokio::runtime::Runtime, cluster: &Cluster) {
    let Some(leader) = cluster.leader_now() else {
        say("err control: no rEtcd leader now; try again");
        return;
    };
    // Only this thread stops a voter, so the leader is still running here.
    let store = cluster.client(leader);
    for prefix in ["partitions/", "grants/"] {
        let listed = rt.block_on(store.list(ListRequest {
            prefix: Bytes::from_static(prefix.as_bytes()),
            max_items: 0,
            max_bytes: 0,
        }));
        match listed {
            Ok(response) => {
                for record in response.records {
                    let key = String::from_utf8_lossy(&record.key).into_owned();
                    let body = if prefix == "partitions/" {
                        PartitionRecord::decode(&record.value).map(|r| format!("{r:?}"))
                    } else {
                        GrantRecord::decode(&record.value).map(|r| format!("{r:?}"))
                    };
                    say(&format!(
                        "{key} rev={} {}",
                        record.mod_revision,
                        body.unwrap_or_else(|| "<undecodable>".to_owned())
                    ));
                }
            }
            Err(e) => say(&format!("err control list {prefix}: {e}")),
        }
    }
}

/// `admin clear-grant`'s error as one line, with its retry rule and mutation proof as a `put`
/// error prints them, and in plain words whether running it again can help (F-012): a permanent
/// store error and a transient one otherwise read alike. The words follow the retry rule and
/// the mutation proof together (F-027).
fn admin_error_line(node: NodeId, error: &ApiError) -> String {
    // From `retry` and `no_mutation` together (F-027): a delete that may have applied is worth
    // running again even when the call itself fails the same way, because the rerun reads the
    // record before it deletes anything.
    let transient = match error.retry {
        RetryRule::RefreshRoute | RetryRule::RetryAfterRecovery | RetryRule::BoundedJitter => {
            Some(true)
        }
        RetryRule::QueryStatus => None,
        RetryRule::Definitive
        | RetryRule::Reconcile
        | RetryRule::Quarantine
        | RetryRule::NotWired => Some(false),
    };
    let words = match (transient, error.no_mutation) {
        (None, _) => "run it again: it reads where the record stands before it deletes anything",
        (Some(true), true) => "running it again can help",
        (Some(true), false) => {
            "running it again can help, and shows whether the delete applied: it reads the \
             record first"
        }
        (Some(false), true) => "running it again will not help: it fails the same way each time",
        (Some(false), false) => {
            "run it again to see whether the delete applied: it reads the record first, and \
             only a record still there fails the same way again"
        }
    };
    format!(
        "err admin clear-grant {}: {} retry={:?} no_mutation={} ({}); {words}",
        node.0,
        error.name(),
        error.retry,
        error.no_mutation,
        error.detail
    )
}

/// `admin clear-grant`'s answer as one line. Only `not-proven-expired` clears with time, so
/// only it says when to try again (W2-D1); the raw `until` stays in the `grant_clear` log line.
fn clearance_line(clearance: Clearance, node: NodeId, db: &Db) -> String {
    match clearance {
        Clearance::Absent => "absent".to_owned(),
        Clearance::Current => format!(
            "current: the record is node {}'s running boot {}; in M9 it clears only after that \
             node restarts, and a restart stops writes for the run (achieveai/rDB#42)",
            node.0,
            db.boot_of(node)
                .map_or_else(|| "?".to_owned(), |boot| boot.0.to_string())
        ),
        Clearance::Cleared(revision) => format!("cleared rev={}", revision.0),
        Clearance::Conflict => "conflict: the record moved; run it again".to_owned(),
        Clearance::Refused {
            refusal: ClearRefusal::NotProvenExpired,
            until_utc_ms,
        } => {
            let wait =
                u64::try_from(until_utc_ms).map_or(0, |until| until.saturating_sub(db.now().0));
            format!(
                "refused not-proven-expired: the grant may still be live; try again in {wait} ms"
            )
        }
        Clearance::Refused {
            refusal: ClearRefusal::Frozen,
            ..
        } => "refused frozen: this grant is frozen; M9 runs no takeover and nothing in M9 \
              unfreezes it, so it never clears in this run"
            .to_owned(),
        Clearance::Refused {
            refusal: ClearRefusal::PartitionInTransfer(partition),
            ..
        } => format!(
            "refused partition-in-transfer p{0}: p{0} is not serving; it clears only once p{0} \
             is back to serving (control lifecycle {0} serving), never by waiting",
            partition.0
        ),
    }
}

/// The words `control lifecycle` takes, in `PartitionLifecycle` order.
const LIFECYCLE_WORDS: [&str; 3] = ["serving", "fencing", "drained"];

fn parse_lifecycle(
    partition: &str,
    word: &str,
) -> Result<(PartitionId, PartitionLifecycle), String> {
    let usage = || format!("control lifecycle <p> <{}>", LIFECYCLE_WORDS.join("|"));
    let partition = partition
        .parse::<u32>()
        .map(PartitionId)
        .map_err(|_| format!("{}: {partition:?} is not a partition id", usage()))?;
    let lifecycle = match word {
        "serving" => PartitionLifecycle::Serving,
        "fencing" => PartitionLifecycle::Fencing,
        "drained" => PartitionLifecycle::FencingDrained,
        other => return Err(format!("{}: not {other:?}", usage())),
    };
    Ok((partition, lifecycle))
}

/// `control lifecycle`: read `partitions/<p>` on the rEtcd leader and write it back with
/// `lifecycle`, at the revision read. Example-only, like every raw `control` verb: no product
/// path writes a lifecycle other than `Serving` in M9. Returns the line to print.
fn set_lifecycle(
    rt: &tokio::runtime::Runtime,
    cluster: &Cluster,
    partition: PartitionId,
    lifecycle: PartitionLifecycle,
) -> String {
    let key = ControlKey::Partition(partition).encode();
    rewrite(rt, cluster, &key, |body| {
        let mut record = PartitionRecord::decode(body)?;
        record.lifecycle = lifecycle;
        Some((record.encode(), format!("{:?}", record.lifecycle)))
    })
}

/// `control freeze-grant`: read `grants/<n>` on the rEtcd leader and write it back frozen, at
/// the revision read. Returns the line to print.
fn freeze_grant(rt: &tokio::runtime::Runtime, cluster: &Cluster, node: NodeId) -> String {
    let key = ControlKey::Grant(node).encode();
    rewrite(rt, cluster, &key, |body| {
        let mut record = GrantRecord::decode(body)?;
        record.frozen = true;
        Some((record.encode(), format!("frozen boot={}", record.boot.0)))
    })
}

/// Read `key` on the rEtcd leader, let `change` rewrite its body, and put the result at the
/// revision read. `change` answers `None` for a body it cannot decode.
fn rewrite(
    rt: &tokio::runtime::Runtime,
    cluster: &Cluster,
    key: &str,
    change: impl FnOnce(&[u8]) -> Option<(Bytes, String)>,
) -> String {
    let Some(leader) = cluster.leader_now() else {
        return format!("err control {key}: no rEtcd leader now; try again");
    };
    let store = cluster.client(leader);
    let read = match rt.block_on(store.get(GetRequest {
        key: Bytes::from(key.to_owned()),
    })) {
        Ok(read) => read,
        Err(e) => return format!("err control get {key}: {e}"),
    };
    let Some(found) = read.record else {
        return format!("err control {key}: absent");
    };
    let Some((value, what)) = change(&found.value) else {
        return format!(
            "err control {key}: rev={} does not decode",
            found.mod_revision
        );
    };
    let put = rt.block_on(store.put(PutRequest {
        key: Bytes::from(key.to_owned()),
        value,
        expected_mod_revision: Some(found.mod_revision),
        dedup: None,
    }));
    match put {
        Ok(response) if response.outcome == MutationOutcome::Applied => {
            tracing::info!(key, from = found.mod_revision, revision = response.revision, what = %what, "control_rewrite");
            format!("{key} rev={} {what}", response.revision)
        }
        Ok(response) => format!(
            "err control {key}: {:?} at rev={}",
            response.outcome, found.mod_revision
        ),
        Err(e) => format!("err control put {key}: {e}"),
    }
}

/// `control gap`: compact through the revision the leader reads now, then drop the Db's
/// watches. Returns the line to print; a failure, a panic in the harness included, is printed.
fn gap(rt: &tokio::runtime::Runtime, control: &Control<'_>) -> String {
    let cluster = Arc::clone(control.cluster);
    let compacted = rt.block_on(rt.spawn(async move {
        let leader = cluster
            .leader_now()
            .ok_or_else(|| "no rEtcd leader now; try again".to_owned())?;
        let read = cluster
            .client(leader)
            .get(GetRequest {
                key: Bytes::from_static(b"partitions/1"),
            })
            .await
            .map_err(|e| e.to_string())?;
        cluster
            .compact_now(read.read_revision)
            .await
            .map_err(|e| e.to_string())
    }));
    let compacted = match compacted {
        Ok(Ok(revision)) => revision,
        Ok(Err(e)) => return format!("err control gap: {e}"),
        Err(e) => return format!("err control gap: {e}"),
    };
    tracing::info!(revision = compacted, "control_compacted");
    let dropped = control.store.drop_watches();
    format!("compacted through revision {compacted}; dropped {dropped} watches")
}

/// `voters leader=N running=[..] bound=N`; `leader=-` while none is elected.
fn voters_line(control: &Control<'_>) -> String {
    format!(
        "voters leader={} running={:?} bound={}",
        control
            .cluster
            .leader_now()
            .map_or_else(|| "-".to_owned(), |l| l.0.to_string()),
        control
            .cluster
            .running_ids()
            .iter()
            .map(|id| id.0)
            .collect::<Vec<_>>(),
        control.store.bound().0
    )
}

/// A voter the cluster was started with.
fn parse_voter(cluster: &Cluster, text: &str) -> Result<VoterId, String> {
    let ids = cluster.ids();
    text.parse::<u64>()
        .ok()
        .map(VoterId)
        .filter(|id| ids.contains(id))
        .ok_or_else(|| {
            format!(
                "control stop|start <voter>: a voter is one of {:?}, not {text:?}",
                ids.iter().map(|id| id.0).collect::<Vec<_>>()
            )
        })
}

/// `control stop <voter>`: move the Db's store off it, then stop it. Returns the line to print.
/// A failure is printed, never a panic: the stop runs as a task, so even a panic inside the
/// harness comes back as an error.
fn stop_voter(rt: &tokio::runtime::Runtime, control: &Control<'_>, voter: VoterId) -> String {
    if !control.cluster.running_ids().contains(&voter) {
        return format!("err control stop {}: not running", voter.0);
    }
    let moved = match control.store.stopping(voter) {
        Ok(moved) => moved,
        Err(e) => {
            tracing::warn!(voter = voter.0, error = %e, "control_stop_refused");
            return format!("err control stop {}: refused: {e}", voter.0);
        }
    };
    let cluster = Arc::clone(control.cluster);
    let stopped = rt.block_on(rt.spawn(async move { cluster.stop_node(voter).await }));
    control.store.stopped(voter);
    let moved = moved.map_or_else(String::new, |to| {
        format!(" (control store moved to voter {} first)", to.0)
    });
    match stopped {
        Ok(()) => {
            tracing::info!(voter = voter.0, "control_voter_stopped");
            format!("stopped voter {}{moved}", voter.0)
        }
        Err(e) => {
            tracing::error!(voter = voter.0, error = %e, "control_voter_stop_failed");
            format!("err control stop {}: {e}{moved}", voter.0)
        }
    }
}

/// `control start <voter>`, on its own data directory. Returns the line to print; a failure,
/// a panic inside the harness included, is printed, never raised.
fn start_voter(rt: &tokio::runtime::Runtime, cluster: &Arc<Cluster>, voter: VoterId) -> String {
    if cluster.running_ids().contains(&voter) {
        return format!("voter {} is already running", voter.0);
    }
    let shared = Arc::clone(cluster);
    let started = rt.block_on(rt.spawn(async move { shared.try_start_node(voter).await }));
    let failed = match started {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(e.to_string()),
        Err(e) => Some(e.to_string()),
    };
    match failed {
        None => {
            tracing::info!(voter = voter.0, "control_voter_started");
            format!("started voter {}", voter.0)
        }
        Some(e) => {
            tracing::error!(voter = voter.0, error = %e, "control_voter_start_failed");
            format!("err control start {}: {e}", voter.0)
        }
    }
}

/// `wait recovered`: the line it ends with. `Err` when the wait stopped because the open can never
/// get there (a failed heal): the run then exits 1.
fn wait_recovered(
    progress: &Progress,
    limit: Duration,
    heal: impl Fn() -> rdb_api::db::HealState,
) -> Result<String, String> {
    let started = Instant::now();
    loop {
        if progress.seen().recovered.is_some() {
            return Ok(format!(
                "reached recovered in {}ms",
                started.elapsed().as_millis()
            ));
        }
        // A failed heal never recovers: say why now, not after the whole wait (k151). Node 1
        // recovers only from the plan the heal hands it.
        if let rdb_api::db::HealState::Failed(error) = heal() {
            return Err(format!(
                "stopped waiting for recovered: {}; this open will not become ready",
                rdb_api::db::ReadyError::Heal(error)
            ));
        }
        if started.elapsed() >= limit {
            return Ok(format!(
                "timeout waiting for recovered after {}s",
                limit.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// How often `wait ready` says it is still waiting.
const WAIT_PROGRESS_EVERY: Duration = Duration::from_secs(10);

/// `wait ready`: the line it ends with, from `Db::wait_ready`, which answers the present rather
/// than rdb_dev's own latch (w3a03: after `node restart 1` the latch still said ready). `Err` when
/// this open can never be ready again: a failed heal, or node 1 restarted (`Stopped`, whose text
/// gives the next step). The run then exits 1. A timeout is said, and is not an error. A wait
/// that reached ready records that as printed, so the poller does not say it a second time, and
/// while it waits the poller holds back a bare `ready`, so it never comes before this line.
///
/// It asks `wait` for at most `every` at a time, and after each slice that ran out it says
/// `still waiting for ready: recovery phase P` with node 1's phase then: the tester saw ~40 s
/// with no line while rEtcd had no quorum. A timeout names the whole bound, not the last slice.
fn wait_ready_line(
    progress: &Progress,
    within: Duration,
    every: Duration,
    mut wait: impl FnMut(Duration) -> Result<(), rdb_api::db::ReadyError>,
    mut said: impl FnMut(&str),
) -> Result<String, String> {
    use rdb_api::db::ReadyError;
    progress
        .inner
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .awaiting_ready = true;
    let started = Instant::now();
    let mut left = within;
    let answer = loop {
        let slice = left.min(every);
        match wait(slice) {
            Err(ReadyError::TimedOut { phase, .. }) if slice < left => {
                left -= slice;
                tracing::info!(
                    phase = phase.as_str(),
                    waited_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "repl_wait_progress"
                );
                said(&format!("still waiting for ready: recovery phase {phase}"));
            }
            Err(ReadyError::TimedOut { phase, .. }) => {
                break Err(ReadyError::TimedOut {
                    within_ms: within.as_millis(),
                    phase,
                })
            }
            answered => break answered,
        }
    };
    let millis = started.elapsed().as_millis();
    {
        // Said by the caller, so the poller says it neither before nor after (k237): `ready`
        // with no reason is now what was printed. A later change, or a reason, is still the
        // poller's to say.
        let mut seen = progress
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        seen.awaiting_ready = false;
        if answer.is_ok() {
            seen.ready = true;
            seen.reason = None;
        }
    }
    let (outcome, line) = match &answer {
        Ok(()) => ("reached", Ok(format!("reached ready in {millis}ms"))),
        Err(e @ ReadyError::TimedOut { .. }) => (
            "timed_out",
            Ok(format!(
                "timeout waiting for ready after {}s: {e}",
                within.as_secs()
            )),
        ),
        Err(e @ ReadyError::Heal(_)) => (
            "stopped",
            Err(format!(
                "stopped waiting for ready: {e}; this open will not become ready"
            )),
        ),
        Err(e @ ReadyError::Stopped { .. }) => {
            ("stopped", Err(format!("stopped waiting for ready: {e}")))
        }
        // A-R196: its text ends "shut this Db down and open the folder again".
        Err(e @ ReadyError::Blocked { .. }) => {
            ("blocked", Err(format!("stopped waiting for ready: {e}")))
        }
        // A-R196(2a): its text says the data is kept and a reopen will not help.
        Err(e @ ReadyError::Quarantined) => (
            "quarantined",
            Err(format!("stopped waiting for ready: {e}")),
        ),
    };
    let error = answer.err().map(|e| e.to_string());
    tracing::info!(
        what = "ready",
        outcome,
        millis = u64::try_from(millis).unwrap_or(u64::MAX),
        error = error.as_deref(),
        "repl_wait"
    );
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The longest a stopped control leader can leave the voters without one, in ms. A follower
    /// that saw a committed leader waits out the leader lease (`election_timeout_max` in
    /// openraft 0.9) and then its own election timeout before it stands
    /// (`RaftCore::handle_tick_election`).
    const fn control_failover_millis(timers: TestTimers) -> u128 {
        timers.election_timeout_max.as_millis() * 2
    }

    /// The shortest time A1 still admits writes after the control leader stops, in ms: the grant,
    /// less the dispatch margin, the clock error and the one tick A1 keeps back, counted from a
    /// renewal that can be a whole renewal interval old when the leader stops.
    const fn write_horizon_floor_millis(budgets: &Budgets) -> u128 {
        (budgets.grant_millis
            - budgets.dispatch_margin_millis
            - budgets.clock_error_millis
            - 1
            - budgets.renew_millis) as u128
    }

    /// The rows that start voters hold this, so their runs do not overlap. No row sets
    /// `RETCD_TEST_DATA_DIR`: `run` names the voters' folder under `--dir` itself.
    static ENV: Mutex<()> = Mutex::new(());

    /// Whether `dir`'s `control` folder holds a voter store: a folder with a `node-1` in it.
    fn holds_voters(dir: &std::path::Path) -> bool {
        std::fs::read_dir(dir.join("control")).is_ok_and(|entries| {
            entries
                .flatten()
                .any(|entry| entry.path().join("node-1").is_dir())
        })
    }

    fn owner(stalled: Option<&str>, admits: bool) -> NodeStatus {
        NodeStatus {
            node: NodeId(1),
            boot: rdb_api::host::BOOT,
            fault: None,
            holds: None,
            authority: "valid".to_owned(),
            fence: None,
            fence_polling: false,
            replace_stopped: false,
            recovery: Some("Committed".to_owned()),
            recovered: Some(Generation(1)),
            role: "primary",
            protection: None,
            admits: Some(admits),
            published: None,
            in_flight: None,
            waits_on: None,
            stalled: stalled.map(str::to_owned),
            reload_pending: None,
        }
    }

    /// Walk 1L (M9 S2a): `control stop` of the control leader fenced the owner `Expired` and
    /// writes never came back. The voters elected 2338 ms after the stop, 3 ms past A1's
    /// horizon, because the default timers make a failover take up to 3000 ms against a
    /// horizon that can be as short as 2299 ms. A new leader has to be in place with a whole
    /// renewal interval to spare, so the renewal sent to it commits before the horizon.
    ///
    /// The slack this relies on (review F-004): with `CONTROL_TIMERS` and the spec budgets it is
    /// 1500 ms (1000 ms failover + 500 ms renewal) against the 2299 ms horizon, so 799 ms. The
    /// floor already takes off one renewal for the age of the last one, and the added renewal
    /// covers the 500 ms renewal tick. Nothing here counts the renewal's RPC and commit time
    /// on the new leader: those must fit in the 799 ms, and no term is added for them.
    #[test]
    fn control_failover_leaves_a_renewal_inside_the_write_horizon() {
        let budgets = Budgets::SPEC_DEFAULTS;
        let failover = control_failover_millis(CONTROL_TIMERS);
        let renewed_by = failover + u128::from(budgets.renew_millis);
        let horizon = write_horizon_floor_millis(&budgets);
        assert!(
            renewed_by < horizon,
            "a control failover of up to {failover} ms plus one renewal ({renewed_by} ms) \
             outlasts the {horizon} ms A1 still admits after the leader stops"
        );
    }

    /// Paper cut after the tester's walk at 1ea889e: a stall that clears said nothing, so the
    /// last line on screen still read as writes paused after a heal. Each change prints once,
    /// and a stall that returns after clearing prints again.
    #[test]
    fn a_stall_that_clears_prints_a_cleared_line_and_a_new_stall_prints_again() {
        let stall =
            "recovery_rebuild_stalled ... its writes stay paused until the database is opened again";
        let mut printed: Option<String> = None;
        let mut lines = Vec::new();
        for node in [
            owner(None, false),
            owner(Some(stall), false),
            owner(Some(stall), false),
            owner(None, true),
            owner(None, true),
            owner(Some(stall), false),
        ] {
            if let Some(line) = stall_change(printed.as_ref(), &node) {
                lines.push(line);
            }
            printed = node.stalled.clone();
        }
        assert_eq!(
            lines,
            [
                format!("stalled node=1 {stall}"),
                "stall cleared node=1 recovery=Committed recovered=1 admits=true".to_owned(),
                format!("stalled node=1 {stall}"),
            ]
        );
    }

    /// Message 6 paper cut: a fenced owner printed a bare `not ready`, and a user may then restart
    /// the node, which ends recovery in M9. The fence prints its reason when it comes after the
    /// line, nothing more while it lasts, the bare line as it clears with writes still out
    /// (W8-D1), and a restart after it prints again.
    /// W7-D2 (tester r3): that restart left the poll live, and writes came back, so the line
    /// promises the resume and the pause after it, not that writes stay stopped.
    #[test]
    fn a_fenced_owner_says_writes_resume_by_themselves_and_not_to_restart() {
        let mut fenced = owner(None, false);
        fenced.authority = "fenced(Expired)".to_owned();
        fenced.fence = Some(DenyReason::Expired);
        fenced.fence_polling = true;
        let note = "node 2 restarted (now boot 2); a restarted node stays out until the folder \
                    is opened again (#42)";
        let mut seen = Seen {
            ready: true,
            ..Seen::default()
        };
        let mut lines = Vec::new();
        for (node, restart) in [
            (owner(None, false), None),
            (fenced.clone(), None),
            (fenced.clone(), None),
            (owner(None, false), None),
            (fenced.clone(), Some(note)),
            (owner(None, true), None),
        ] {
            lines.extend(ready_step(&mut seen, &node, restart, &[], &[]));
        }
        assert_eq!(
            lines,
            [
                "not ready: writes not admitted".to_owned(),
                "not ready: writes not admitted: node 1's grant is fenced(Expired); writes \
                 resume by themselves once the old grant's expiry is proven, so do not restart \
                 node 1: a restarted node does not rejoin in M9 (achieveai/rDB#42)"
                    .to_owned(),
                "not ready: writes not admitted".to_owned(),
                format!(
                    "not ready: writes not admitted: node 1's grant is fenced(Expired); writes \
                     resume by themselves once the old grant's expiry is proven, so do not \
                     restart node 1: a restarted node does not rejoin in M9 (achieveai/rDB#42); \
                     once they resume, they pause soon, at the latest about 2 s after the next \
                     write, because {note}"
                ),
                "ready".to_owned(),
            ]
        );
    }

    /// Msg 8, F-004 and F-016: "resume by themselves" only while A1's after-fence poll is live
    /// and the reason is one the self-replace can lift. `fenced(Frozen)` never resumes, even
    /// before its first poll stops; `fenced(Expired)` whose poll stopped (a later freeze or
    /// delete) prints a new line saying so. The host row
    /// `status_says_the_after_fence_poll_stopped_when_the_record_is_frozen_later` proves the
    /// poll field on a real host.
    #[test]
    fn a_fence_promises_resume_only_while_the_poll_is_live_and_can_lift_it() {
        let fenced = |reason: DenyReason, polling: bool| {
            let mut node = owner(None, false);
            node.authority = format!("fenced({reason:?})");
            node.fence = Some(reason);
            node.fence_polling = polling;
            node
        };
        let resume = |reason: &str| {
            format!(
                "not ready: writes not admitted: node 1's grant is fenced({reason}); writes \
                 resume by themselves once the old grant's expiry is proven, so do not restart \
                 node 1: a restarted node does not rejoin in M9 (achieveai/rDB#42)"
            )
        };
        let stuck = |reason: &str| {
            format!(
                "not ready: writes not admitted: node 1's grant is fenced({reason}); nothing in \
                 M9 replaces it, so writes stay stopped for this run: to write again, quit and \
                 start rdb_dev again"
            )
        };
        for (reason, name) in [
            (DenyReason::Frozen, "Frozen"),
            (DenyReason::BootMismatch, "BootMismatch"),
            (
                DenyReason::AuthorityGenerationChanged,
                "AuthorityGenerationChanged",
            ),
        ] {
            for polling in [true, false] {
                let mut seen = Seen::default();
                assert_eq!(
                    ready_step(&mut seen, &fenced(reason, polling), None, &[], &[]),
                    Some(stuck(name)),
                    "{name}, polling={polling}"
                );
            }
        }
        let mut seen = Seen {
            ready: true,
            ..Seen::default()
        };
        let lines: Vec<String> = [
            fenced(DenyReason::Expired, true),
            fenced(DenyReason::Expired, true),
            fenced(DenyReason::Expired, false),
            fenced(DenyReason::Expired, false),
        ]
        .iter()
        .filter_map(|node| ready_step(&mut seen, node, None, &[], &[]))
        .collect();
        assert_eq!(lines, [resume("Expired"), stuck("Expired")]);
    }

    /// Walk 10, W10-D1 (tester r24n, r24n2, r26): a read-only start with links held, the store
    /// down, then `link heal all`. F1's activation CAS went unanswered and F1 blocked
    /// (`recovery=Blocked(ControlUnknown)`), while the poller kept promising writes resume by
    /// themselves; once a grant was back it printed the bare line, and writes never came back.
    /// A blocked recovery says so, names its reason and says writes stay stopped, over the
    /// fence's resume line and the held-link line. It promises no way out. Driven directly from
    /// r24n's states. Unit.
    #[test]
    fn a_blocked_recovery_says_writes_stay_stopped_over_any_resume_promise() {
        let blocked = "not ready: writes not admitted: node 1's recovery is \
                       Blocked(ControlUnknown), so writes stay stopped for this run: to write \
                       again, quit and start rdb_dev again";
        let with = |authority: &str, recovery: &str, fence: Option<DenyReason>| {
            let mut node = owner(None, false);
            node.authority = authority.to_owned();
            node.recovery = Some(recovery.to_owned());
            node.protection = Some("paused".to_owned());
            node.fence = fence;
            node.fence_polling = fence.is_some();
            node
        };
        let fenced = |recovery: &str| with("fenced(Expired)", recovery, Some(DenyReason::Expired));
        let held_links = [(NodeId(1), NodeId(2)), (NodeId(1), NodeId(3))];
        let mut seen = Seen::default();
        let lines: Vec<String> = [
            (fenced("Rebuilding"), &held_links[..]),
            (fenced("ActivationProposed"), &[][..]),
            (fenced("Blocked(ControlUnknown)"), &[][..]),
            (with("unheld", "Blocked(ControlUnknown)", None), &[][..]),
            (
                with("held(grant 2)", "Blocked(ControlUnknown)", None),
                &[][..],
            ),
            (
                with("held(grant 2)", "Blocked(ControlUnknown)", None),
                &held_links[..],
            ),
        ]
        .iter()
        .filter_map(|(node, links)| ready_step(&mut seen, node, None, links, &[]))
        .skip(2)
        .collect();
        assert_eq!(lines, [blocked]);
        let note = "node 2 restarted (now boot 2); a restarted node stays out until the folder \
                    is opened again (#42)";
        let stuck = with("held(grant 2)", "Blocked(ControlUnknown)", None);
        let mut healthy = stuck.clone();
        healthy.protection = Some("healthy".to_owned());
        for node in [stuck, healthy] {
            assert_eq!(
                ready_step(&mut Seen::default(), &node, Some(note), &[], &[]),
                Some(format!("{blocked}; {note}")),
                "{node:?}"
            );
        }
    }

    /// Walk 10, W10-D1 after the kernel fix (A-R102 and its addendum): r24n's activation CAS,
    /// unanswered with the store down, now leaves F1 in `ActivationUnresolved`, and F1 resolves
    /// by itself once the store answers. So the line says writes wait for the store and resume
    /// by themselves, over the fence's resume line, and `ready` follows once they do. A stuck
    /// state still wins: a fence nothing lifts, a self-replace that stopped, or a restart with
    /// L1 refusing keeps the stay-stopped line. After a restart with L1 healthy, the pause
    /// promise follows the resume. Driven directly from r24n's states. Unit.
    #[test]
    fn a_resolving_activation_says_writes_resume_once_the_store_answers() {
        let resolving = "node 1's recovery is waiting for the control store to confirm \
                         activation; writes resume by themselves once it answers";
        let with = |authority: &str, recovery: &str, fence: Option<DenyReason>| {
            let mut node = owner(None, false);
            node.authority = authority.to_owned();
            node.recovery = Some(recovery.to_owned());
            node.protection = Some("paused".to_owned());
            node.fence = fence;
            node.fence_polling = fence.is_some();
            node
        };
        let fenced = |recovery: &str| with("fenced(Expired)", recovery, Some(DenyReason::Expired));
        let held_links = [(NodeId(1), NodeId(2)), (NodeId(1), NodeId(3))];
        let mut serving = owner(None, true);
        serving.authority = "held(grant 2)".to_owned();
        serving.protection = Some("healthy".to_owned());
        let mut seen = Seen::default();
        let lines: Vec<String> = [
            (fenced("Rebuilding"), &held_links[..]),
            (fenced("ActivationProposed"), &[][..]),
            (fenced("ActivationUnresolved"), &[][..]),
            (with("held(grant 2)", "ActivationUnresolved", None), &[][..]),
            (serving, &[][..]),
        ]
        .iter()
        .filter_map(|(node, links)| ready_step(&mut seen, node, None, links, &[]))
        .skip(2)
        .collect();
        assert_eq!(
            lines,
            [
                format!("not ready: writes not admitted: {resolving}"),
                "ready".to_owned()
            ]
        );

        let reason = |node: &NodeStatus, note| ready_reason(node, false, note, &[], &[]);
        let note = "node 2 restarted (now boot 2); a restarted node stays out until the folder \
                    is opened again (#42)";
        let mut healthy = with("held(grant 2)", "ActivationUnresolved", None);
        healthy.protection = Some("healthy".to_owned());
        assert_eq!(
            reason(&healthy, Some(note)),
            Some(format!(
                "{resolving}; once they resume, they pause soon, at the latest about \
                 {PAUSE_AGE_SECS} s after the next write, because {note}"
            ))
        );
        let paused = with("held(grant 2)", "ActivationUnresolved", None);
        assert_eq!(
            reason(&paused, Some(note)),
            Some(format!("{note}; {STUCK}"))
        );
        let frozen = with(
            "fenced(Frozen)",
            "ActivationUnresolved",
            Some(DenyReason::Frozen),
        );
        assert_eq!(
            reason(&frozen, None),
            Some(format!(
                "node 1's grant is fenced(Frozen); nothing in M9 replaces it, so {STUCK}"
            ))
        );
        let mut stopped = with("unheld", "ActivationUnresolved", None);
        stopped.replace_stopped = true;
        assert_eq!(reason(&stopped, None), Some(gave_up_line(&stopped)));
    }

    /// Walk 11, W11-D1 (tester w6, w6r): while F1 resolved its activation with the store down,
    /// the tester held links 1-2 and 1-3 again. The resolving line, "writes resume by themselves
    /// once it answers", stayed the last word; when the store answered, F1 committed and writes
    /// stayed paused (`PROTECTION_PAUSED`) until `link heal all`. With L1 paused and a link of
    /// node 1's held, the resolving line names both conditions, as the fence line does (W8-D2).
    /// Driven directly from w6's states. Unit.
    #[test]
    fn a_resolving_activation_with_links_held_says_writes_also_wait_on_the_links() {
        let resolving = "node 1's recovery is waiting for the control store to confirm \
                         activation; writes resume by themselves once it answers";
        let both = "node 1's recovery is waiting for the control store to confirm activation, \
                    l1=paused and links 1-2, 1-3 are held; writes resume only once the store \
                    answers and node 1's copies catch up, which needs those links: heal them \
                    (link heal all)";
        let links = "l1=paused and links 1-2, 1-3 are held: writes wait for node 1's copies to \
                     catch up, which needs those links: heal them (link heal all)";
        let with = |authority: &str, recovery: &str, fence: Option<DenyReason>| {
            let mut node = owner(None, false);
            node.authority = authority.to_owned();
            node.recovery = Some(recovery.to_owned());
            node.protection = Some("paused".to_owned());
            node.fence = fence;
            node.fence_polling = fence.is_some();
            node
        };
        let resolving_fenced = with(
            "fenced(Expired)",
            "ActivationUnresolved",
            Some(DenyReason::Expired),
        );
        let committed = with("held(grant 3)", "Committed", None);
        let mut serving = owner(None, true);
        serving.authority = "held(grant 3)".to_owned();
        serving.protection = Some("healthy".to_owned());
        let held_links = [(NodeId(1), NodeId(2)), (NodeId(1), NodeId(3))];
        let mut seen = Seen::default();
        let lines: Vec<String> = [
            (&resolving_fenced, &[][..]),
            (&resolving_fenced, &held_links[..]),
            (&committed, &held_links[..]),
            (&serving, &[][..]),
        ]
        .iter()
        .filter_map(|(node, links)| ready_step(&mut seen, node, None, links, &[]))
        .collect();
        let not_ready = |why: &str| format!("not ready: writes not admitted: {why}");
        assert_eq!(
            lines,
            [
                not_ready(resolving),
                not_ready(both),
                not_ready(links),
                "ready".to_owned()
            ]
        );
    }

    /// Walk 8, W8-D1 (tester r6_3200): a freeze landed while A1 was between grants. A1 lost
    /// its self-replace CAS, answered the frozen record `NotOurs` and stopped, `unheld` for the
    /// rest of the run, and the fence's "resume by themselves" line stayed the last word because
    /// a reason that went away printed nothing. A reason that goes away while writes stay out
    /// is withdrawn with the bare line, so no promise outlives the state that made it.
    #[test]
    fn a_resume_promise_is_withdrawn_when_its_fence_goes_and_writes_stay_out() {
        // The tester's r6_3200 state, driven directly (the race took 0 of 16 live runs): A1
        // fenced with its poll live, then `unheld` with its self-replace stopped
        // (`replace_stopped`, kernel `self_replace_stopped`), then a later poll that changes
        // nothing. With and without a restart, the stuck line, once.
        let note = "node 2 restarted (now boot 2); a restarted node stays out until the folder \
                    is opened again (#42)";
        let gave_up = "node 1's grant is unheld and node 1 stopped trying for one: its grant \
                       record is frozen or not its own; nothing in M9 replaces it, so writes \
                       stay stopped for this run: to write again, quit and start rdb_dev again";
        let mut fenced = owner(None, false);
        fenced.authority = "fenced(Expired)".to_owned();
        fenced.fence = Some(DenyReason::Expired);
        fenced.fence_polling = true;
        let mut stopped = owner(None, false);
        stopped.authority = "unheld".to_owned();
        stopped.replace_stopped = true;
        for restart in [None, Some(note)] {
            let mut seen = Seen {
                ready: true,
                ..Seen::default()
            };
            let lines: Vec<String> = [&fenced, &stopped, &stopped]
                .iter()
                .filter_map(|node| ready_step(&mut seen, node, restart, &[], &[]))
                .skip(1)
                .collect();
            let want = restart.map_or_else(
                || format!("not ready: writes not admitted: {gave_up}"),
                |note| format!("not ready: writes not admitted: {gave_up}; {note}"),
            );
            assert_eq!(lines, [want], "restart={restart:?}");
        }
        // Fenced with the poll stopped keeps the fence's own stuck line.
        let mut fenced_stopped = fenced.clone();
        fenced_stopped.fence_polling = false;
        fenced_stopped.replace_stopped = true;
        assert_eq!(
            ready_step(&mut Seen::default(), &fenced_stopped, None, &[], &[]),
            Some(
                "not ready: writes not admitted: node 1's grant is fenced(Expired); nothing in M9 \
                 replaces it, so writes stay stopped for this run: to write again, quit and \
                 start rdb_dev again"
                    .to_owned()
            )
        );

        let mut fenced = owner(None, false);
        fenced.authority = "fenced(Expired)".to_owned();
        fenced.fence = Some(DenyReason::Expired);
        fenced.fence_polling = true;
        let mut unheld = owner(None, false);
        unheld.authority = "unheld".to_owned();
        let mut seen = Seen {
            ready: true,
            ..Seen::default()
        };
        let lines: Vec<String> = [fenced, unheld.clone(), unheld]
            .iter()
            .filter_map(|node| ready_step(&mut seen, node, None, &[], &[]))
            .collect();
        assert_eq!(
            lines,
            [
                "not ready: writes not admitted: node 1's grant is fenced(Expired); writes \
                 resume by themselves once the old grant's expiry is proven, so do not restart \
                 node 1: a restarted node does not rejoin in M9 (achieveai/rDB#42)",
                "not ready: writes not admitted",
            ]
        );
    }

    /// Node 1's links held, as `link hold 1-2; link hold 1-3` leaves them.
    const HELD: [(NodeId, NodeId); 2] = [(NodeId(1), NodeId(2)), (NodeId(1), NodeId(3))];

    /// M9 S3 resume-line table, cell "drop" (dev walk `tab-drop-fence`): ACKs 2->1 and 3->1
    /// dropped, a write in flight, L1 paused, then the fence. The fence line promised writes
    /// resume by themselves; the grant came back (`held(grant 1706)`) and writes did not, until
    /// `link heal all`. A dropped direction keeps the copies from catching up as a held link
    /// does, so every line that names held links names dropped ones too: the fence line, the
    /// bare L1 line once the fence is gone, the resolving line, and both kinds at once.
    #[test]
    fn a_resume_line_with_l1_paused_and_links_dropped_says_writes_also_wait_on_the_links() {
        const DROPPED: [(NodeId, NodeId); 2] = [(NodeId(2), NodeId(1)), (NodeId(3), NodeId(1))];
        let paused = |authority: &str, recovery: &str, fence: Option<DenyReason>| {
            let mut node = owner(None, false);
            node.authority = authority.to_owned();
            node.recovery = Some(recovery.to_owned());
            node.protection = Some("paused".to_owned());
            node.fence = fence;
            node.fence_polling = fence.is_some();
            node
        };
        let fenced = paused("fenced(Expired)", "Committed", Some(DenyReason::Expired));
        let regranted = paused("held(grant 1706)", "Committed", None);
        let mut serving = owner(None, true);
        serving.protection = Some("healthy".to_owned());
        let mut seen = Seen::default();
        let lines: Vec<String> = [
            (&fenced, &DROPPED[..]),
            (&regranted, &DROPPED[..]),
            (&regranted, &DROPPED[..]),
            (&serving, &[][..]),
        ]
        .iter()
        .filter_map(|(node, dropped)| ready_step(&mut seen, node, None, &[], dropped))
        .collect();
        assert_eq!(
            lines,
            [
                "not ready: writes not admitted: node 1's grant is fenced(Expired), l1=paused and \
                 links 2->1, 3->1 are dropped; writes resume only once the old grant's expiry is \
                 proven and node 1's copies catch up, which needs those links: heal them (link \
                 heal all); do not restart node 1: a restarted node does not rejoin in M9 \
                 (achieveai/rDB#42)",
                "not ready: writes not admitted: l1=paused and links 2->1, 3->1 are dropped: \
                 writes wait for node 1's copies to catch up, which needs those links: heal them \
                 (link heal all)",
                "ready",
            ]
        );

        let resolving = paused("held(grant 2)", "ActivationUnresolved", None);
        assert_eq!(
            ready_reason(&resolving, false, None, &[], &DROPPED),
            Some(
                "node 1's recovery is waiting for the control store to confirm activation, \
                 l1=paused and links 2->1, 3->1 are dropped; writes resume only once the store \
                 answers and node 1's copies catch up, which needs those links: heal them (link \
                 heal all)"
                    .to_owned()
            )
        );
        assert_eq!(
            ready_reason(
                &regranted,
                false,
                None,
                &[(NodeId(1), NodeId(2))],
                &[(NodeId(3), NodeId(1)), (NodeId(2), NodeId(3))]
            ),
            Some(
                "l1=paused and links 1-2 are held and links 3->1 are dropped: writes wait for \
                 node 1's copies to catch up, which needs those links: heal them (link heal all)"
                    .to_owned()
            ),
            "both kinds; a direction between two secondaries is not node 1's"
        );
    }

    /// M9 S3 resume-line table, cell "unresolved" (U1, ruling A-R109, kernel 935e65d): `put a 2`
    /// was in flight when node 1's ACKs were dropped, the fence froze the queue on its seq 3,
    /// and A1 replaced the grant. Every write is then refused until seq 3 publishes, whatever
    /// A1 and L1 say, so the line names the request and its seq, and says healing the links
    /// resolves it, and never `ready` while L1 admits. Once it publishes the queue thaws.
    #[test]
    fn a_resume_line_with_the_queue_frozen_on_a_write_names_it_and_the_links() {
        const DROPPED: [(NodeId, NodeId); 2] = [(NodeId(2), NodeId(1)), (NodeId(3), NodeId(1))];
        let waiting = |protection: &str, admits: bool| {
            let mut node = owner(None, admits);
            node.authority = "held(grant 6)".to_owned();
            node.protection = Some(protection.to_owned());
            node.in_flight = Some(RequestId(2));
            node.waits_on = Some(rdb_core::contracts::ids::Seq(3));
            node
        };
        let paused = waiting("paused", false);
        let admitting = waiting("healthy", true);
        let mut thawed = owner(None, true);
        thawed.protection = Some("healthy".to_owned());
        let mut seen = Seen::default();
        let lines: Vec<String> = [&paused, &admitting, &thawed]
            .iter()
            .filter_map(|node| ready_step(&mut seen, node, None, &[], &DROPPED))
            .collect();
        let waits = "partition 1 waits on request 2's outcome (seq 3), which node 1 applied and \
                     has not published, and links 2->1, 3->1 are dropped; writes resume once it \
                     publishes, which needs those links: heal them (link heal all)";
        assert_eq!(
            lines,
            [
                format!("not ready: writes not admitted: {waits}"),
                "ready".to_owned()
            ],
            "L1 admitting does not make it ready"
        );
        assert_eq!(
            ready_reason(&paused, false, None, &HELD, &[]),
            Some(
                "partition 1 waits on request 2's outcome (seq 3), which node 1 applied and has \
                 not published, and links 1-2, 1-3 are held; writes resume once it publishes, \
                 which needs those links: heal them (link heal all)"
                    .to_owned()
            )
        );
        let mut fenced = paused.clone();
        fenced.authority = "fenced(Expired)".to_owned();
        fenced.fence = Some(DenyReason::Expired);
        assert_eq!(
            waits_on_line(&fenced, &[], &DROPPED),
            None,
            "a fenced grant is the fence line's"
        );
    }

    /// Walk 8, W8-D2 (tester r9, walk 7's 4b): links held, a write in flight, then the fence.
    /// L1 was paused, and the fence line promised writes resume by themselves. The grant came
    /// back (`held(grant 3)`) and writes did not, until `link heal all`. With L1 paused or
    /// reprotecting and a link of node 1's held, under a live poll, the line names the links
    /// writes also wait on, and still says not to restart node 1. With no link of node 1's held,
    /// the copies catch up by themselves and the resume line stands.
    #[test]
    fn a_fence_with_l1_paused_and_links_held_says_writes_also_wait_on_the_links() {
        for mode in ["paused", "reprotecting"] {
            let mut node = owner(None, false);
            node.authority = "fenced(Expired)".to_owned();
            node.fence = Some(DenyReason::Expired);
            node.fence_polling = true;
            node.protection = Some(mode.to_owned());
            assert_eq!(
                ready_step(&mut Seen::default(), &node, None, &HELD, &[]),
                Some(format!(
                    "not ready: writes not admitted: node 1's grant is fenced(Expired), \
                     l1={mode} and links 1-2, 1-3 are held; writes resume only once the old \
                     grant's expiry is proven and node 1's copies catch up, which needs those \
                     links: heal them (link heal all); do not restart node 1: a restarted node \
                     does not rejoin in M9 (achieveai/rDB#42)"
                )),
                "l1={mode}"
            );
            for held in [&[][..], &[(NodeId(2), NodeId(3))][..]] {
                assert_eq!(
                    ready_step(&mut Seen::default(), &node, None, held, &[]),
                    Some(
                        "not ready: writes not admitted: node 1's grant is fenced(Expired); \
                         writes resume by themselves once the old grant's expiry is proven, so \
                         do not restart node 1: a restarted node does not rejoin in M9 \
                         (achieveai/rDB#42)"
                            .to_owned()
                    ),
                    "l1={mode} held={held:?}"
                );
            }
        }
    }

    /// W8-D2 live re-walk (`r9/walks/w8/w8d2.out`): once A1 held grant 2 again, the fence's
    /// reason went, and the withdrawal (W8-D1) printed the bare line while L1 stayed paused for
    /// 20 s, until `link heal all`. Not admitting with L1 paused or reprotecting, a link of node
    /// 1's held and no restart, names the links writes wait on, as a condition and not a
    /// promise: a stopped A1 may also hold them out. At startup (`w8d2b.out`) L1 is paused, then
    /// reprotecting, with no link held, and catches up by itself: no reason.
    #[test]
    fn l1_paused_with_links_held_says_writes_wait_on_those_links() {
        let fenced = |mode: &str| {
            let mut node = owner(None, false);
            node.authority = "fenced(Expired)".to_owned();
            node.fence = Some(DenyReason::Expired);
            node.fence_polling = true;
            node.protection = Some(mode.to_owned());
            node
        };
        let held = |mode: &str, admits: bool| {
            let mut node = owner(None, admits);
            node.authority = "held(grant 2)".to_owned();
            node.protection = Some(mode.to_owned());
            node
        };
        let mut startup = Seen::default();
        assert_eq!(
            ready_step(&mut startup, &held("paused", false), None, &[], &[]),
            None
        );
        assert_eq!(
            ready_step(&mut startup, &held("reprotecting", false), None, &[], &[]),
            None
        );
        let wait = "not ready: writes not admitted: l1=paused and links 1-2, 1-3 are held: \
                    writes wait for node 1's copies to catch up, which needs those links: heal \
                    them (link heal all)";
        let mut seen = Seen {
            ready: true,
            ..Seen::default()
        };
        let lines: Vec<String> = [
            (held("paused", false), &HELD[..]),
            (fenced("paused"), &HELD[..]),
            (held("paused", false), &HELD[..]),
            (held("paused", false), &HELD[..]),
            (held("reprotecting", false), &[][..]),
            (held("healthy", true), &[][..]),
        ]
        .iter()
        .filter_map(|(node, links)| ready_step(&mut seen, node, None, links, &[]))
        .collect();
        assert_eq!(
            lines,
            [
                wait.to_owned(),
                "not ready: writes not admitted: node 1's grant is fenced(Expired), l1=paused \
                 and links 1-2, 1-3 are held; writes resume only once the old grant's expiry is \
                 proven and node 1's copies catch up, which needs those links: heal them (link \
                 heal all); do not restart node 1: a restarted node does not rejoin in M9 \
                 (achieveai/rDB#42)"
                    .to_owned(),
                wait.to_owned(),
                "not ready: writes not admitted".to_owned(),
                "ready".to_owned(),
            ]
        );
    }

    /// Message 7: after a secondary restarts, L1 keeps admitting writes for a while (walk 6c),
    /// and a bare `ready` then read as if writes would last. While admitted the line says they
    /// are about to pause and why; once L1 pauses, the not-ready line says so for the run.
    /// W7-D1 (tester r1): the pause came 1.89 s after the restart with no write at all, so the
    /// line promises only "soon", and an upper bound counted from the next write: a write with
    /// a copy out stays unsafe and pauses at L1's pause age (r3: 2.02 s).
    #[test]
    fn ready_after_a_restart_says_writes_pause_soon_not_only_after_the_next_write() {
        let note = "node 2 restarted (now boot 2); a restarted node stays out until the folder \
                    is opened again (#42)";
        let mut paused = owner(None, false);
        paused.protection = Some("paused".to_owned());
        let mut seen = Seen::default();
        let mut lines = Vec::new();
        for (node, restart) in [
            (owner(None, true), None),
            (owner(None, true), Some(note)),
            (owner(None, true), Some(note)),
            (paused, Some(note)),
        ] {
            lines.extend(ready_step(&mut seen, &node, restart, &[], &[]));
        }
        assert_eq!(
            lines,
            [
                "ready".to_owned(),
                format!(
                    "ready for now; writes pause soon, at the latest about 2 s after the next \
                     write, because {note}"
                ),
                format!(
                    "not ready: writes not admitted: {note}; writes stay stopped for this run: \
                     to write again, quit and start rdb_dev again"
                ),
            ]
        );
    }

    /// W7-D1, W7-D2: every `ready`/`not ready` reason is derived from the state that decides it,
    /// checked against each state of the tester's walk-7 text matrix (s2b-tester.md, "Text
    /// matrix"). The stuck line ("writes stay stopped for this run") prints only where nothing
    /// in M9 brings writes back: L1 paused or reprotecting with a copy out, the owner itself
    /// restarted, or a fence no self-replace lifts. A fence whose poll is live, with a
    /// secondary restarted (FR, r3), promises the resume and the pause after it. Unit.
    #[test]
    fn every_ready_line_matches_the_state_that_decides_it() {
        let note = "node 2 restarted (now boot 2); a restarted node stays out until the folder \
                    is opened again (#42)";
        let failed = "node 2 stopped: its restart as boot 2 failed; a restarted node stays out \
                      until the folder is opened again (#42)";
        let owner_note = "the owner, node 1, restarted (now boot 2); a restarted node stays out \
                          until the folder is opened again (#42)";
        let stuck = "writes stay stopped for this run: to write again, quit and start rdb_dev \
                     again";
        let pause = |note: &str| {
            format!("pause soon, at the latest about 2 s after the next write, because {note}")
        };
        let resume = |reason: &str| {
            format!(
                "node 1's grant is fenced({reason}); writes resume by themselves once the old \
                 grant's expiry is proven, so do not restart node 1: a restarted node does not \
                 rejoin in M9 (achieveai/rDB#42)"
            )
        };
        let fenced_stuck = |reason: &str| {
            format!("node 1's grant is fenced({reason}); nothing in M9 replaces it, so {stuck}")
        };
        let with = |admits: bool, protection: Option<&str>, fence: Option<(DenyReason, bool)>| {
            let mut node = owner(None, admits);
            node.protection = protection.map(str::to_owned);
            if let Some((reason, polling)) = fence {
                node.authority = format!("fenced({reason:?})");
                node.fence = Some(reason);
                node.fence_polling = polling;
            }
            node
        };
        let mut restarted_owner = owner(None, false);
        restarted_owner.boot = BootId(2);
        restarted_owner.admits = None;
        restarted_owner.protection = None;
        let mut unheld = owner(None, false);
        unheld.authority = "unheld".to_owned();
        let expiring = Some((DenyReason::Expired, true));
        let rows: Vec<(&str, NodeStatus, Option<&str>, Option<String>)> = vec![
            (
                "H",
                with(true, Some("healthy"), None),
                None,
                Some("ready".to_owned()),
            ),
            (
                "HR",
                with(true, Some("healthy"), None),
                Some(note),
                Some(format!("ready for now; writes {}", pause(note))),
            ),
            (
                "SF admitting",
                with(true, Some("healthy"), None),
                Some(failed),
                Some(format!("ready for now; writes {}", pause(failed))),
            ),
            (
                "HP",
                with(false, Some("paused"), None),
                Some(note),
                Some(format!("not ready: writes not admitted: {note}; {stuck}")),
            ),
            (
                "HP reprotecting",
                with(false, Some("reprotecting"), None),
                Some(note),
                Some(format!("not ready: writes not admitted: {note}; {stuck}")),
            ),
            (
                "SF paused",
                with(false, Some("paused"), None),
                Some(failed),
                Some(format!("not ready: writes not admitted: {failed}; {stuck}")),
            ),
            (
                "FP",
                with(false, Some("healthy"), expiring),
                None,
                Some(format!(
                    "not ready: writes not admitted: {}",
                    resume("Expired")
                )),
            ),
            (
                "FS",
                with(false, Some("healthy"), Some((DenyReason::Expired, false))),
                None,
                Some(format!(
                    "not ready: writes not admitted: {}",
                    fenced_stuck("Expired")
                )),
            ),
            (
                "FZ",
                with(false, Some("healthy"), Some((DenyReason::Frozen, true))),
                None,
                Some(format!(
                    "not ready: writes not admitted: {}",
                    fenced_stuck("Frozen")
                )),
            ),
            (
                "U",
                unheld.clone(),
                None,
                Some("not ready: writes not admitted".to_owned()),
            ),
            (
                "U with a restart",
                unheld,
                Some(note),
                Some(format!(
                    "not ready: writes not admitted: writes are not admitted now; if they come \
                     back, they {}",
                    pause(note)
                )),
            ),
            (
                "FR",
                with(false, Some("healthy"), expiring),
                Some(note),
                Some(format!(
                    "not ready: writes not admitted: {}; once they resume, they {}",
                    resume("Expired"),
                    pause(note)
                )),
            ),
            (
                "FR, L1 paused",
                with(false, Some("paused"), expiring),
                Some(note),
                Some(format!("not ready: writes not admitted: {note}; {stuck}")),
            ),
            (
                "FR, poll stopped",
                with(false, Some("healthy"), Some((DenyReason::Expired, false))),
                Some(note),
                Some(format!(
                    "not ready: writes not admitted: {}; {note}",
                    fenced_stuck("Expired")
                )),
            ),
            (
                "RO",
                restarted_owner,
                Some(owner_note),
                Some(format!(
                    "not ready: writes not admitted: {owner_note}; {stuck}"
                )),
            ),
        ];
        for (state, node, restart, line) in rows {
            let admits = node.admits == Some(true);
            let mut seen = Seen {
                ready: !admits,
                ..Seen::default()
            };
            assert_eq!(
                ready_step(&mut seen, &node, restart, &[], &[]),
                line,
                "state {state}"
            );
        }
    }

    /// Tester W3-D2 and walk 5: after a restart, `Unknown` for a request this session sent
    /// carries the note, and promises M10 only after the owner's restart or both secondaries'.
    /// After one secondary's it names the copy that can still resolve it (W5-D1). A request
    /// never sent, or no restart, stays bare. Msg 8, W6-D1: one node 1 does not hold in flight
    /// promises nothing, unless the owner restarted; the Db row
    /// `a_retry_of_a_put_refused_paused_after_a_restart_does_not_promise_a_status` proves
    /// `in_flight` on a real Db.
    #[test]
    fn status_unknown_carries_the_restart_note_and_waits_for_m10_only_when_no_copy_can_resolve() {
        let note = Some("NOTE");
        assert_eq!(unknown_line(true, Some(true), &[], None), "status Unknown");
        assert_eq!(
            unknown_line(false, Some(false), &[NodeId(2)], note),
            "status Unknown"
        );
        assert_eq!(
            unknown_line(true, Some(true), &[NodeId(2)], note),
            "status Unknown (not evidence of a lost write: it resolves once node 3, which did \
             not restart, can take it; NOTE)"
        );
        assert_eq!(
            unknown_line(true, Some(true), &[NodeId(3)], note),
            "status Unknown (not evidence of a lost write: it resolves once node 2, which did \
             not restart, can take it; NOTE)"
        );
        let m10 = "status Unknown (not evidence of a lost write: its outcome cannot be known \
                   until a restarted node rejoins, which is M10; NOTE)";
        assert_eq!(
            unknown_line(true, Some(true), &[NodeId(2), NodeId(3)], note),
            m10
        );
        assert_eq!(unknown_line(true, Some(true), &[NodeId(1)], note), m10);
        assert_eq!(
            unknown_line(true, Some(true), &[NodeId(1), NodeId(2)], note),
            m10
        );
        assert_eq!(unknown_line(true, Some(false), &[NodeId(1)], note), m10);
        let refused = "status Unknown (node 1 does not hold it in flight, so waiting will not \
                       change this: it was refused before it was queued, or never arrived, or \
                       its answer was trimmed after retention (it may have applied); NOTE)";
        assert_eq!(unknown_line(true, Some(false), &[NodeId(3)], note), refused);
        assert_eq!(
            unknown_line(true, Some(false), &[NodeId(2), NodeId(3)], note),
            refused
        );
    }

    /// F-025: `node_status` leaves out a node 1 that does not answer in time, and the line then
    /// read as "not in flight; waiting will not change this", which nothing showed. Without an
    /// answer it says so and to ask again; after the owner's own restart the M10 line stands.
    /// Unit.
    #[test]
    fn status_unknown_says_try_again_when_node_1_did_not_answer() {
        let note = Some("NOTE");
        let silent = "status Unknown (node 1 did not answer in time, so whether it holds it in \
                      flight is not known; try again; NOTE)";
        assert_eq!(unknown_line(true, None, &[NodeId(3)], note), silent);
        assert_eq!(
            unknown_line(true, None, &[NodeId(2), NodeId(3)], note),
            silent
        );
        assert_eq!(
            unknown_line(false, None, &[NodeId(3)], note),
            "status Unknown"
        );
        assert_eq!(
            unknown_line(true, None, &[NodeId(1)], note),
            "status Unknown (not evidence of a lost write: its outcome cannot be known until a \
             restarted node rejoins, which is M10; NOTE)"
        );
    }

    /// M9 close-out D1 (tester-m9c, `m9c-walks/d1-repro.txt`): after node 2's restart, `status`
    /// of a write this session saw `ok` (seq 2), once its answer was trimmed past
    /// `--dedup-retention-ms`, said only "refused before it was queued, or never arrived". The
    /// write had applied. Node 1 not holding a request this session sent must leave room for an
    /// applied write whose answer was trimmed. Unit.
    #[test]
    fn status_unknown_of_a_request_not_in_flight_allows_a_trimmed_answer() {
        for restarted in [&[NodeId(2)][..], &[NodeId(3)], &[NodeId(2), NodeId(3)]] {
            let line = unknown_line(true, Some(false), restarted, Some("NOTE"));
            assert!(
                line.contains("its answer was trimmed after retention (it may have applied)"),
                "{restarted:?}: {line}"
            );
            assert!(
                !line.contains("or never arrived;"),
                "refused or lost is not the only cause: {restarted:?}: {line}"
            );
        }
    }

    /// Msg 9 adjacent defect: `node_status` leaves out a node whose restart failed (SF), so a
    /// restart set built from it missed node 2, and `status` of a write in flight said it waits
    /// for M10 although node 3, which did not restart, can still take it. The set comes from
    /// each node's boot, answered or not. Unit; walk w13 (r9/walks) shows it live.
    #[test]
    fn a_failed_secondary_restart_counts_as_restarted_for_status() {
        // Node 2's restart failed: it answers no `node_status`, but its boot moved on.
        let boots = |node: NodeId| {
            Some(if node == NodeId(2) {
                BootId(2)
            } else {
                BootId(1)
            })
        };
        let restarted = restarted_nodes(boots);
        assert_eq!(
            restarted,
            [NodeId(2)],
            "node 2 did not answer: its restart failed"
        );
        assert_eq!(
            unknown_line(true, Some(true), &restarted, Some("NOTE")),
            "status Unknown (not evidence of a lost write: it resolves once node 3, which did \
             not restart, can take it; NOTE)"
        );
    }

    /// W7-D3 (tester r12): after node 1's restart failed, the poller kept its last `ready`,
    /// because `node_status` leaves node 1 out and the poll printed nothing then. It now prints
    /// the stuck line once, naming the failed restart. `nodes` names a node left out, as
    /// stopped or as not answering, instead of dropping it (r11, r12). Unit; the live walk
    /// w12 (r9/walks) shows both on a real run.
    #[test]
    fn a_failed_owner_restart_says_not_ready_and_nodes_names_the_stopped_node() {
        let note = "the owner, node 1, stopped: its restart as boot 2 failed; a restarted node \
                    stays out until the folder is opened again (#42)";
        let mut seen = Seen::default();
        lines_ready(&mut seen);
        assert_eq!(
            stopped_owner_step(&mut seen, Some(note)),
            Some(format!(
                "not ready: writes not admitted: {note}; writes stay stopped for this run: to \
                 write again, quit and start rdb_dev again"
            ))
        );
        assert_eq!(
            stopped_owner_step(&mut seen, Some(note)),
            None,
            "printed once"
        );
        assert_eq!(
            missing_node_line(NodeId(1), Some(BootId(2)), true),
            "node=1 boot=2 stopped: its restart as boot 2 failed, so no process runs it"
        );
        assert_eq!(
            missing_node_line(NodeId(2), Some(BootId(1)), false),
            "node=2 boot=1 did not answer in time; try again"
        );
    }

    /// `seen` after a `ready` line, as the poller holds it before a failure.
    fn lines_ready(seen: &mut Seen) {
        assert_eq!(
            ready_step(seen, &owner(None, true), None, &[], &[]),
            Some("ready".to_owned())
        );
    }

    /// F-012 (tester walk 7): rdb_dev printed an admin error as `KIND (detail)` only, so a
    /// permanent store error and a transient one read alike. The line carries `retry=` and
    /// `no_mutation=` as a `put` error does, and says in words whether running it again helps.
    /// Unit.
    #[test]
    fn an_admin_error_says_whether_running_it_again_can_help() {
        let error = |retry, no_mutation| ApiError {
            kind: rdb_core::contracts::errors::ErrorKind::Unavailable,
            retry,
            no_mutation,
            detail: "control store: list partitions/: x; nothing deleted".to_owned(),
            retry_after: None,
        };
        assert_eq!(
            admin_error_line(NodeId(1), &error(RetryRule::BoundedJitter, true)),
            "err admin clear-grant 1: UNAVAILABLE retry=BoundedJitter no_mutation=true (control \
             store: list partitions/: x; nothing deleted); running it again can help"
        );
        assert_eq!(
            admin_error_line(NodeId(1), &error(RetryRule::NotWired, true)),
            "err admin clear-grant 1: UNAVAILABLE retry=NotWired no_mutation=true (control \
             store: list partitions/: x; nothing deleted); running it again will not help: it \
             fails the same way each time"
        );
        assert!(
            admin_error_line(NodeId(1), &error(RetryRule::QueryStatus, false)).ends_with(
                "no_mutation=false (control store: list partitions/: x; nothing deleted); \
                        run it again: it reads where the record stands before it deletes \
                        anything"
            )
        );
    }

    /// Re-review F-027: the words follow `retry` and `no_mutation` together, never `retry`
    /// alone. A permanent delete error is `NotWired` with `no_mutation=false`, and its detail
    /// says the delete may have applied and to run it again to read where it stands; "running
    /// it again will not help" then contradicted it. A rerun reads the record first, so it
    /// shows whether the delete applied; only a record still there fails the same way. Unit.
    #[test]
    fn an_admin_error_that_may_have_deleted_says_a_rerun_shows_whether_it_did() {
        let error = |retry| ApiError {
            kind: rdb_core::contracts::errors::ErrorKind::Unavailable,
            retry,
            no_mutation: false,
            detail: "control store: delete grants/1 at revision 5: permission denied; the \
                     delete may have applied; run it again to read where it stands"
                .to_owned(),
            retry_after: None,
        };
        let permanent = admin_error_line(NodeId(1), &error(RetryRule::NotWired));
        assert_eq!(
            permanent,
            "err admin clear-grant 1: UNAVAILABLE retry=NotWired no_mutation=false (control \
             store: delete grants/1 at revision 5: permission denied; the delete may have \
             applied; run it again to read where it stands); run it again to see whether the \
             delete applied: it reads the record first, and only a record still there fails \
             the same way again"
        );
        let transient = admin_error_line(NodeId(1), &error(RetryRule::BoundedJitter));
        assert!(
            transient.ends_with(
                "; running it again can help, and shows whether the delete applied: it reads \
                 the record first"
            ),
            "{transient}"
        );
    }

    /// F-005, F-006: an open that fails tears the run down as a clean quit does, and without
    /// `--keep` leaves no directory. The voters' data goes under `--dir`, as `run` names it, so
    /// the row also proves the order: a store handle still open when the voters close keeps
    /// their RocksDB files, and on Windows `--dir` then cannot be removed.
    /// Integration (~2 s): three real rEtcd voters; the Db open is the injected failure.
    #[test]
    fn an_open_that_fails_exits_1_and_leaves_no_directory() {
        let _env = ENV
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = config_testkit::fs::temp_dir();
        let dir = root.path().join("run");
        let env_before = std::env::var_os("RETCD_TEST_DATA_DIR");
        let args = Args {
            dir: dir.clone(),
            log_dir: root.path().join("logs"),
            script: None,
            keep: false,
            hold: Vec::new(),
            put_timeout: None,
            dedup_cap: None,
            dedup_retention: None,
            control: None,
            requests: None,
            abort_after_commit: false,
            pause_before_sync: Vec::new(),
            help: false,
        };
        let voters_under_dir = AtomicBool::new(false);
        let code = run(&args, |config, _, _| {
            voters_under_dir.store(holds_voters(&config.dir), Ordering::Relaxed);
            async { Err(OpenError::Node("node 2: injected".to_owned())) }
        });
        assert_eq!(code, ExitCode::from(1), "a failed open exits 1");
        assert!(
            voters_under_dir.load(Ordering::Relaxed),
            "the voters' data was not under {}",
            dir.display()
        );
        // Found by W1: a row that leaves this changed nests every later row's
        // folders one level deeper, and the fifth level passed MAX_PATH inside RocksDB.
        assert_eq!(
            std::env::var_os("RETCD_TEST_DATA_DIR"),
            env_before,
            "the row left RETCD_TEST_DATA_DIR changed"
        );
        assert!(
            !dir.exists(),
            "the failed open left {} behind",
            dir.display()
        );
    }

    /// D1 (S1 dev walk, fbf2b95): `txn --deadline-ms 0 a=5` was refused, `retry 4
    /// --deadline-ms 2000` published it at seq 3, and a plain `retry 4` then resent the
    /// deadline-0 request it was first sent with. Check 2 refused that before the dedup replay,
    /// so the retry answered DEADLINE_BEFORE_ADMISSION, not seq 3. The next retry sends what
    /// was sent last, whether that send was answered (D1's own path) or refused. Unit.
    #[test]
    fn the_next_retry_resends_the_deadline_the_last_retry_was_sent_with() {
        use rdb_core::contracts::ids::{ClientId, RequestIdentity, TenantId};
        let request = |remaining_millis| TxnRequest {
            api_version: rdb_core::contracts::version::API_VERSION,
            identity: RequestIdentity {
                tenant: TenantId(1),
                client: ClientId(1),
                request: RequestId(4),
            },
            affinity: AffinityId(1),
            expected_generation: Some(Generation(1)),
            remaining_millis,
            conditions: Vec::new(),
            mutations: Vec::new(),
        };
        let mut sent = Sent::default();
        sent.record(Some(request(0)), Kind::Txn);
        let deadline = |sent: &Sent| sent.pick(&["4"]).expect("request 4").0.remaining_millis;
        // D1: `retry 4 --deadline-ms 2000` published at seq 3.
        sent.resent(&Ok(rdb_api::PutOk {
            request: RequestId(4),
            generation: Generation(1),
            owner_epoch: rdb_core::contracts::ids::OwnerEpoch(1),
            seq: rdb_core::contracts::ids::Seq(3),
            outcome: rdb_core::contracts::txn::Outcome::Published,
            durability: rdb_core::contracts::txn::Durability::BufferedOnTwo,
            sent: Box::new(request(2_000)),
        }));
        assert_eq!(
            deadline(&sent),
            2_000,
            "the answered retry's deadline is kept"
        );
        assert_eq!(sent.pick(&[]).expect("latest").0.remaining_millis, 2_000);
        // A refusal hands the request it sent back; that is the next retry's too.
        sent.resent(&Err(PutError {
            error: rdb_api::ApiError::invalid("refused after it was sent"),
            request: Some(Box::new(request(1_500))),
        }));
        assert_eq!(
            deadline(&sent),
            1_500,
            "the refused retry's deadline is kept"
        );
    }

    /// PR #36 F-002: `txn --deadline-ms 0 a=1` (request 1), `txn b=1` (request 2), then `retry 1
    /// --deadline-ms 30001`. The deadline is past `MAX_TXN_DEADLINE`, so nothing was sent, but
    /// the request it handed back made request 1 the latest, and a bare `retry` resent it, not
    /// request 2. A retry that sends still makes its request the latest. Unit.
    #[test]
    fn a_retry_refused_before_sending_leaves_the_latest_request_alone() {
        use rdb_core::contracts::ids::{ClientId, RequestIdentity, TenantId};
        let request = |id, remaining_millis| TxnRequest {
            api_version: rdb_core::contracts::version::API_VERSION,
            identity: RequestIdentity {
                tenant: TenantId(1),
                client: ClientId(1),
                request: RequestId(id),
            },
            affinity: AffinityId(1),
            expected_generation: Some(Generation(1)),
            remaining_millis,
            conditions: Vec::new(),
            mutations: Vec::new(),
        };
        let mut sent = Sent::default();
        sent.record(Some(request(1, 0)), Kind::Txn);
        sent.record(Some(request(2, 2_000)), Kind::Txn);
        let latest = |sent: &Sent| sent.pick(&[]).expect("latest").0.identity.request;
        let over = rdb_api::MAX_TXN_DEADLINE + Duration::from_millis(1);
        sent.resent_within(
            over,
            &Err(PutError {
                error: rdb_api::ApiError::invalid("past the maximum; nothing was sent"),
                request: Some(Box::new(request(1, 0))),
            }),
        );
        assert_eq!(latest(&sent), RequestId(2), "nothing was sent");
        // At the maximum it is sent: the refusal's request is the next retry's.
        sent.resent_within(
            rdb_api::MAX_TXN_DEADLINE,
            &Err(PutError {
                error: rdb_api::ApiError::invalid("refused after it was sent"),
                request: Some(Box::new(request(1, 30_000))),
            }),
        );
        assert_eq!(
            latest(&sent),
            RequestId(1),
            "a retry that sends is the latest"
        );
        assert_eq!(
            sent.pick(&["1"]).expect("request 1").0.remaining_millis,
            30_000
        );
    }

    /// The tester's PC-2: `--dedup-cap 65537` started three rEtcd voters, then the open refused
    /// the cap. The cap is checked first now, so a refused cap starts nothing and exits 1. With
    /// `--keep`, voters that had started would leave their data under `--dir`. Unit when green
    /// (~10 ms); its red run started the voters (~2 s).
    #[test]
    fn a_refused_dedup_cap_exits_1_before_any_voter_starts() {
        let _env = ENV
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = config_testkit::fs::temp_dir();
        let dir = root.path().join("run");
        let args = Args {
            dir: dir.clone(),
            log_dir: root.path().join("logs"),
            script: None,
            keep: true,
            hold: Vec::new(),
            put_timeout: None,
            dedup_cap: Some(65_537),
            dedup_retention: None,
            control: None,
            requests: None,
            abort_after_commit: false,
            pause_before_sync: Vec::new(),
            help: false,
        };
        let opened = AtomicBool::new(false);
        let code = run(&args, |_, _, _| {
            opened.store(true, Ordering::Relaxed);
            async { Err(OpenError::Node("never reached".to_owned())) }
        });
        assert_eq!(code, ExitCode::from(1), "a refused cap exits 1");
        assert!(!opened.load(Ordering::Relaxed), "the open was never tried");
        assert!(
            !dir.join("control").exists(),
            "no voter started: {} exists",
            dir.join("control").display()
        );
    }

    /// S1 ruling A5 (guard G16): an op is `[<group>:]<object>=<value>[@<version>]`. The object
    /// ends at the first `=`, a group is only a leading `digits:`, and a version only a
    /// trailing `@digits`; anything else stays in the object or the value. Unit (~1 ms).
    #[test]
    fn a_txn_op_reads_a_group_and_a_version_only_where_the_grammar_puts_them() {
        let op = |group, object: &str, value: &str, if_version| TxnOp {
            group,
            object: object.to_owned(),
            value: value.to_owned(),
            if_version,
        };
        let default = Duration::from_millis(7);
        let (deadline, ops) = parse_txn(
            &["2:a=1@3", "a=x=y", "k:v=1", "a=b@c", "1x:a=1", "a=@1"],
            default,
        )
        .expect("all parse");
        assert_eq!(deadline, default);
        assert_eq!(
            ops,
            [
                op(2, "a", "1", Some(3)),
                op(1, "a", "x=y", None),
                op(1, "k:v", "1", None),
                op(1, "a", "b@c", None),
                op(1, "1x:a", "1", None),
                op(1, "a", "", Some(1)),
            ]
        );
        let (deadline, _) = parse_txn(&["--deadline-ms", "0", "a=1"], default).expect("deadline");
        assert_eq!(deadline, Duration::ZERO);
        assert!(parse_txn(&["a"], default).is_err(), "no `=`");
        assert!(
            parse_txn(&["99999999999999999999:a=1"], default).is_err(),
            "group too large"
        );
        assert!(
            parse_txn(&["--deadline-ms", "x", "a=1"], default).is_err(),
            "bad deadline"
        );
    }

    /// S1 ruling A7, guard G10: `retry <id> --payload` re-puts the object and condition of a
    /// put. A txn has no single object, so it is refused with a usage line. Unit (~1 ms).
    /// A-R195a: the put is one sent under an app id (`put --as`); a Db-numbered one is
    /// `retry_payload_under_a_db_numbered_id_says_to_put_as`'s.
    #[test]
    fn retry_payload_re_puts_a_put_and_refuses_a_txn() {
        let put = Kind::Put {
            object: b"a".to_vec(),
            if_version: Some(3),
        };
        let app = rdb_api::app_request_id(4).expect("an app id");
        assert_eq!(payload_put(put, app), Ok((b"a".to_vec(), Some(3))));
        let refused = payload_put(Kind::Txn, RequestId(5)).expect_err("a txn has no one object");
        assert!(refused.ends_with("request 5 was a txn"), "{refused}");
    }

    /// A-R195a: `retry <id> --payload` sends the put again with `put_as`, and `put_as` refuses
    /// an id this Db numbered itself (A-R195). So rdb_dev refuses it first, plainly, naming the
    /// next step, instead of passing on the API's `app_request_id` advice. Unit.
    #[test]
    fn retry_payload_under_a_db_numbered_id_says_to_put_as() {
        let put = Kind::Put {
            object: b"a".to_vec(),
            if_version: None,
        };
        let refused = payload_put(put, RequestId(4)).expect_err("a Db-numbered put");
        assert!(
            refused.contains("request 4")
                && refused.contains("put --as <n> <object> <value>")
                && refused.contains("nothing was sent"),
            "{refused}"
        );
        // The walk's first build printed this over three lines (an escaped `\n` in the source).
        assert!(
            !refused.contains('\n'),
            "one line, as the REPL prints it: {refused:?}"
        );
    }

    /// A-R195a: `put --as <n>` sends under `app_request_id(n)`; a number that is not one, or is
    /// 2^63 or more, is refused before anything is sent. Unit.
    #[test]
    fn put_as_takes_the_app_s_number_and_refuses_one_out_of_range() {
        assert_eq!(
            parse_app_id("7"),
            Ok(rdb_api::app_request_id(7).expect("7"))
        );
        for bad in ["x", "-1", "9223372036854775808"] {
            let refused = parse_app_id(bad).expect_err(bad);
            assert!(refused.starts_with("put --as N:"), "{bad}: {refused}");
        }
    }

    /// F-007: a script PowerShell 5.1 wrote as UTF-16LE is an input error, not the end of the
    /// input, so the run says so and exits 2 instead of running nothing and exiting 0. Unit.
    #[test]
    fn an_unreadable_line_is_an_input_error_not_the_end_of_input() {
        let utf16le: Vec<u8> = [0xFF, 0xFE]
            .into_iter()
            .chain(
                "put a 1\r\nquit\r\n"
                    .encode_utf16()
                    .flat_map(u16::to_le_bytes),
            )
            .collect();
        let first = std::io::BufRead::lines(std::io::Cursor::new(utf16le))
            .next()
            .expect("one line");
        let error = command(first).expect_err("a UTF-16LE line is not a command");
        assert!(error.starts_with("input: "), "{error}");
        assert_eq!(command(Ok("  # a comment".to_owned())), Ok(None));
        assert_eq!(
            command(Ok("put a 1 # set a".to_owned())),
            Ok(Some("put a 1".to_owned()))
        );
    }

    /// Review F3 (A-R133): a `#` inside a `"..."` string is kept, even at the start of a word;
    /// a `\"` does not end the string; and a `#` word after the string is still a comment. At
    /// 2cc5b24 the first two lines were cut at their `#`. Unit.
    #[test]
    fn a_hash_inside_a_json_string_is_kept_and_one_after_it_is_a_comment() {
        let command = |line: &str| command(Ok(line.to_owned()));
        let kept = |line: &str| Ok(Some(line.to_owned()));
        let spaced = r##"doc d --absent --json {"m":"hello #world"}"##;
        assert_eq!(command(spaced), kept(spaced), "a spaced # in a string");
        let escaped = r##"doc d --absent --json {"m":"say \" #hi"}"##;
        assert_eq!(
            command(escaped),
            kept(escaped),
            "a # after an escaped quote"
        );
        assert_eq!(
            command(r##"doc d --absent --json {"m":"a #b"}  # a real comment"##),
            kept(r##"doc d --absent --json {"m":"a #b"}"##),
            "a trailing comment"
        );
    }

    /// S4-1 1j, guard 6 (A-2): a `#` starts a comment only at the start of a word, so one
    /// inside a JSON value is kept, and the value is read from the raw line with its quotes
    /// (R1). Mutant m6 (cut at any `#`) fails the first assertion. Unit.
    #[test]
    fn a_hash_inside_a_word_is_kept_and_json_is_read_from_the_raw_line() {
        assert_eq!(
            command(Ok(
                r##"doc t --absent --json {"tag":"#1"}   # a comment"##.to_owned()
            )),
            Ok(Some(r##"doc t --absent --json {"tag":"#1"}"##.to_owned()))
        );
        assert_eq!(command(Ok("#only a comment".to_owned())), Ok(None));
        let (object, expect, change) =
            parse_change(r#"doc d --expect 2 incr /n 5 set /tags/1 "b""#).expect("a doc line");
        let path = |text| Path::parse(text).expect("a path");
        assert_eq!(
            (object.as_str(), expect, change),
            (
                "d",
                Some(Expected::Version(2)),
                Change::Doc(Delta(vec![
                    Op::Increment(path("/n"), Int::from(5_u64)),
                    Op::Set(path("/tags/1"), Value::Text("b".to_owned())),
                ]))
            ),
            "the quotes around \"b\" survive: it is text, not a bare word"
        );
        let refused =
            parse_change(r#"doc d set /a "a"x"#).expect_err("\"a\"x is not one JSON value");
        assert!(refused.contains("not at a space"), "{refused}");
    }

    /// S4-1 1s (A-5): `retry <R> --payload` refuses a value write; it is resent unchanged or not
    /// at all. Unit.
    #[test]
    fn retry_payload_refuses_a_value_write() {
        let refused =
            payload_put(Kind::Value, RequestId(5)).expect_err("a value write takes no payload");
        assert!(refused.contains("request 5 was a value write"), "{refused}");
    }

    /// A-R121: at key position only, `{"$bytes":"<hex>"}` is a byte-string key, the form a
    /// read prints. Bad hex is refused; `$$bytes` is no escape there, so it stays a map, which
    /// the owner refuses as a key. It fails if `Words::key` is `Words::json`. Unit.
    #[test]
    fn a_bytes_key_is_entered_as_a_read_prints_it() {
        let (_, _, change) =
            parse_change(r#"map m --absent put {"$bytes":"00ff"} 1"#).expect("a map line");
        assert_eq!(
            change,
            Change::Map(vec![ElemOp::Put(
                Value::Bytes(vec![0x00, 0xff]),
                Value::Integer(Int::from(1_u64))
            )])
        );
        let (_, look) = parse_members(r#"members m --after {"$bytes":"00"}"#).expect("a page");
        assert_eq!(
            look,
            Look::Members {
                after: Some(Value::Bytes(vec![0x00])),
                limit: DEFAULT_LIMIT
            }
        );
        let refused = parse_change(r#"set s add {"$bytes":"zz"}"#).expect_err("zz is not hex");
        assert!(refused.contains("is not hex"), "{refused}");
        // P3-2 (A-R124c): a `$bytes` that is not a string names the form it expects.
        let refused = parse_change(r#"set s add {"$bytes":1}"#).expect_err("1 is not hex text");
        assert!(
            refused.contains(r#"$bytes needs a hex string, as {"$bytes":"00ff"}"#),
            "{refused}"
        );
        let (_, _, escaped) = parse_change(r#"set s add {"$$bytes":"00"}"#).expect("a set line");
        assert!(
            matches!(&escaped, Change::Set(ops) if matches!(ops[..], [ElemOp::Add(Value::Map(_))])),
            "{escaped:?}"
        );
    }

    /// D5 (tester S1-8 at 066ab6e, ADR-0013, A-R154): `repl_command` logged the whole typed
    /// line, so 27 lines of one walk carried key and value text. Every command of the grammar
    /// is fed with keys, values, map keys and paths that all start `zq`, a pair this row's name
    /// and module do not contain: no line of the test's log may hold `zq`. Each command still
    /// logs one `repl_command` line with its verb, and a command that names an object logs it
    /// as `key_hex`. Unit (~10 ms).
    #[config_log::retcd_test]
    fn d5_a_typed_line_logs_its_verb_and_key_hex_never_its_text() {
        const METHOD: &str = "d5_a_typed_line_logs_its_verb_and_key_hex_never_its_text";
        let typed = [
            ("put zqk1 zqv1", "put", Some("zqk1")),
            ("put --as 7 zqk18 zqv18", "put --as", Some("zqk18")),
            ("put& zqk2 zqv2 --if-version 3", "put&", Some("zqk2")),
            ("txn zqk3=zqv3 2:zqk4=zqv4@1", "txn", None),
            (
                r#"doc zqk5 --absent set /zqpath "zqv5""#,
                "doc",
                Some("zqk5"),
            ),
            (r#"map zqk6 put "zqmember" "zqv6""#, "map", Some("zqk6")),
            (r#"members zqk7 --after "zqafter""#, "members", Some("zqk7")),
            ("get zqk8", "get", Some("zqk8")),
            ("read zqk9 --offset 0 --len 1", "read", Some("zqk9")),
            (
                "blob upload zqk10 --upload 000102030405060708090a0b0c0d0e0f --chunk-size 4 zqv10",
                "blob upload",
                Some("zqk10"),
            ),
            (
                r#"admin damage zqk11 --element "zqelem""#,
                "admin damage",
                Some("zqk11"),
            ),
            ("retry --payload zqv12", "retry", None),
            ("zqunknown zqk14", "unknown", None),
            (r#"put "zqk15 spaced" zqv15"#, "put", Some("zqk15 spaced")),
            (r#"put "zqk16unterminated zqv16"#, "put", None),
            ("status 4", "status", None),
            ("wait ready 60", "wait ready", None),
            // ADR-0013: a key logs its first 32 bytes only.
            (
                "get zqk17-6789012345678901234567890123456789",
                "get",
                Some("zqk17-6789012345678901234567890123456789"),
            ),
        ];
        for (line, _, _) in typed {
            log_command(line);
        }
        let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
        let leaked: Vec<String> = lines
            .iter()
            .map(ToString::to_string)
            .filter(|text| text.contains("zq"))
            .collect();
        assert!(leaked.is_empty(), "key or value text logged: {leaked:#?}");
        let logged: Vec<_> = lines
            .iter()
            .filter(|line| line["@m"] == "repl_command")
            .collect();
        assert_eq!(
            logged.len(),
            typed.len(),
            "one repl_command line per command"
        );
        for (line, (typed, verb, key)) in logged.iter().zip(typed) {
            assert_eq!(line["verb"], verb, "{typed}");
            let key_hex = key.map(|key| hex::encode(&key.as_bytes()[..key.len().min(32)]));
            assert_eq!(line["key_hex"].as_str(), key_hex.as_deref(), "{typed}");
        }
    }

    /// Tester paper cut (S1-8 at 066ab6e): `retry` with nothing sent printed a `usage:` line
    /// and the run exited 1, as if the line were mistyped. Each way of having nothing to send
    /// is a plain `nothing to retry` answer, and a request id this run never sent points at
    /// `--requests F`. A recorded request is not nothing. Unit.
    #[test]
    fn retry_with_nothing_sent_says_so_plainly_and_names_requests_for_an_old_request() {
        use rdb_core::contracts::ids::{ClientId, RequestIdentity, TenantId};
        let mut sent = Sent::default();
        for rest in [&[][..], &["--deadline-ms", "100"], &["--payload", "v"]] {
            let said = sent.nothing_to_retry(rest).expect("nothing was sent");
            assert!(said.starts_with("nothing to retry: "), "{said}");
        }
        sent.record(None, Kind::Txn);
        let said = sent.nothing_to_retry(&[]).expect("the latest sent nothing");
        assert!(said.starts_with("nothing to retry: "), "{said}");
        for rest in [&["7"][..], &["--id", "7"], &["7", "--deadline-ms", "100"]] {
            let said = sent
                .nothing_to_retry(rest)
                .expect("request 7 was never sent");
            assert!(said.contains("--requests F, then retry 7"), "{said}");
        }
        let request = TxnRequest {
            api_version: rdb_core::contracts::version::API_VERSION,
            identity: RequestIdentity {
                tenant: TenantId(1),
                client: ClientId(1),
                request: RequestId(7),
            },
            affinity: AffinityId(1),
            expected_generation: Some(Generation(1)),
            remaining_millis: 0,
            conditions: Vec::new(),
            mutations: Vec::new(),
        };
        sent.record(Some(request), Kind::Txn);
        assert_eq!(sent.nothing_to_retry(&[]), None);
        assert_eq!(sent.nothing_to_retry(&["--id", "7"]), None);
        // Not a retry of a recorded request: `pick` answers these, with a usage line.
        assert_eq!(sent.nothing_to_retry(&["x"]), None);
    }

    /// Tester S1-4 (A-R171): a same-id resend after a reopen must send the request unchanged,
    /// and a fresh compile cannot: it reads the object's moved version, so the walk at 57da754
    /// answered `REQUEST_ID_REUSE` to the same object and value. `--requests F` carries each
    /// recorded request to a later run: that run adopts request 7 from F with the same digest,
    /// the last line for an id wins, and an id F lacks is a plain `nothing to retry`. A damaged
    /// line is the next row's. Unit (~5 ms, one temp file).
    #[test]
    fn a_later_run_adopts_an_earlier_runs_request_from_requests_file_unchanged() {
        use rdb_core::contracts::ids::{ClientId, RequestIdentity, TenantId};
        use rdb_core::contracts::txn::Mutation;
        let request = |id, value: &'static [u8]| TxnRequest {
            api_version: rdb_core::contracts::version::API_VERSION,
            identity: RequestIdentity {
                tenant: TenantId(1),
                client: ClientId(1),
                request: RequestId(id),
            },
            affinity: AffinityId(1),
            expected_generation: Some(Generation(1)),
            remaining_millis: 5_000,
            conditions: Vec::new(),
            mutations: vec![Mutation::Put {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(value),
                expected_version: None,
            }],
        };
        let root = {
            let _env = ENV
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            config_testkit::fs::temp_dir()
        };
        let path = root.path().join("requests.json");
        let mut first = Sent {
            file: Some(path.clone()),
            ..Sent::default()
        };
        first.record(Some(request(6, b"six")), Kind::Txn);
        first.record(Some(request(7, b"old")), Kind::Txn);
        first.record(Some(request(7, b"new")), Kind::Value);
        first.record(None, Kind::Txn);

        let mut later = Sent {
            file: Some(path.clone()),
            ..Sent::default()
        };
        assert_eq!(later.adopt(&["7"]), Ok(Vec::new()));
        assert_eq!(later.nothing_to_retry(&["--id", "7"]), None);
        let (adopted, kind) = later.pick(&["7"]).expect("request 7 adopted");
        assert_eq!(
            adopted.request_digest(),
            request(7, b"new").request_digest(),
            "the last line for 7 is resent unchanged"
        );
        assert!(matches!(kind, Kind::Value), "{kind:?}");
        assert_eq!(later.adopt(&["9"]), Ok(Vec::new()));
        let said = later.nothing_to_retry(&["9"]).expect("F holds no 9");
        assert!(said.ends_with("holds no request 9"), "{said}");
    }

    /// Tester k214 (A-R189). A damaged line in `--requests F` blocked every retry, and a failed
    /// append to F printed `err` yet exited 0. Now a line that does not read is skipped with a
    /// warning that names its line, never its text, and the other ids still retry; a last line
    /// a kill cut short is said as such, once per run. A failed append makes the run exit 1. Unit
    /// (~5 ms).
    #[test]
    fn a_damaged_or_torn_line_in_requests_file_is_skipped_and_a_failed_append_exits_1() {
        use rdb_core::contracts::ids::{ClientId, RequestIdentity, TenantId};
        use rdb_core::contracts::txn::Mutation;
        let request = |id, value: &'static [u8]| TxnRequest {
            api_version: rdb_core::contracts::version::API_VERSION,
            identity: RequestIdentity {
                tenant: TenantId(1),
                client: ClientId(1),
                request: RequestId(id),
            },
            affinity: AffinityId(1),
            expected_generation: Some(Generation(1)),
            remaining_millis: 5_000,
            conditions: Vec::new(),
            mutations: vec![Mutation::Put {
                key: Bytes::from_static(b"zqkey"),
                value: Bytes::from_static(value),
                expected_version: None,
            }],
        };
        let root = {
            let _env = ENV
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            config_testkit::fs::temp_dir()
        };
        let path = root.path().join("requests.json");
        let line = |id, value| {
            serde_json::to_string(&(request(id, value), Kind::Txn)).expect("a request line")
        };
        let torn = line(8, b"zqeight");
        let text = format!(
            "{}\n[\"zqsecret\"\n{}\n{}",
            line(6, b"zqsix"),
            line(7, b"zqseven"),
            &torn[..torn.len() / 2]
        );
        std::fs::write(&path, text).expect("write F");
        let mut later = Sent {
            file: Some(path.clone()),
            ..Sent::default()
        };
        let warnings = later
            .adopt(&["7"])
            .expect("a bad line does not block a retry");
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        assert!(
            warnings[0].contains(" line 2: not a saved request"),
            "{warnings:?}"
        );
        assert!(warnings[0].ends_with("; skipped"), "{warnings:?}");
        assert!(warnings[1].contains(" line 4: cut short"), "{warnings:?}");
        assert!(warnings.iter().all(|w| !w.contains("zq")), "{warnings:?}");
        let (adopted, _) = later.pick(&["7"]).expect("request 7 adopted past line 2");
        assert_eq!(
            adopted.request_digest(),
            request(7, b"zqseven").request_digest()
        );
        // Tester w360c: each retry re-read F and printed the same warnings again. Once per run.
        assert_eq!(
            later.adopt(&["6"]),
            Ok(Vec::new()),
            "6 too, warned once per run"
        );
        assert!(later.pick(&["6"]).is_ok());
        assert_eq!(exit_code(false, &later), ExitCode::SUCCESS);

        // F ends in a torn line: this run's next save starts a line of its own, so a later run
        // still retries it (an append glued onto the torn tail would lose it).
        later.record(Some(request(9, b"zqnine")), Kind::Txn);
        let mut after = Sent {
            file: Some(path.clone()),
            ..Sent::default()
        };
        after.adopt(&["9"]).expect("F reads");
        assert!(
            after.pick(&["9"]).is_ok(),
            "request 9 saved after a torn line"
        );

        // F is a folder: the append fails, and the run exits 1 though every line was fine.
        let mut unwritable = Sent {
            file: Some(root.path().to_path_buf()),
            ..Sent::default()
        };
        unwritable.record(Some(request(9, b"nine")), Kind::Txn);
        assert_eq!(exit_code(false, &unwritable), ExitCode::from(1));
        assert_eq!(exit_code(true, &later), ExitCode::from(1));
    }
    /// Tester S1-9 paper cut (k151): on a reopen whose heal failed, `wait ready 30` sat out
    /// all 30 s and printed a bare timeout, though the open could never become ready. `wait
    /// ready` and `wait recovered` end as soon as the heal has failed, and say so with the heal's
    /// error: node 1 recovers only from the plan a heal hands it. A heal still running is
    /// waited on. A stopped wait is an error, so the run exits 1 (A-R191). Unit (~150 ms).
    #[test]
    fn a_wait_ends_at_once_with_the_heal_error_when_the_heal_failed() {
        use rdb_api::db::{HealState, ReadyError};
        let progress = Progress::default();
        let started = Instant::now();
        let recovered = wait_recovered(&progress, Duration::from_secs(2), || {
            HealState::Failed(admin::HealError::NodeStopped)
        })
        .expect_err("a failed heal is a failed wait: exit 1 (A-R191)");
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "waited {:?}: {recovered}",
            started.elapsed()
        );
        // `wait ready` asks Db::wait_ready, which ends at once on a failed heal itself.
        let ready = wait_ready_line(
            &progress,
            Duration::from_secs(2),
            WAIT_PROGRESS_EVERY,
            |_| Err(ReadyError::Heal(admin::HealError::NodeStopped)),
            |_| (),
        )
        .expect_err("a failed heal is a failed wait: exit 1 (A-R191)");
        for (what, line) in [("recovered", recovered), ("ready", ready)] {
            assert_eq!(
                line,
                format!(
                    "stopped waiting for {what}: the reopen's heal failed: node 1 stopped \
                     before the heal reached it; this open will not become ready"
                )
            );
        }
        let line = wait_recovered(&progress, Duration::from_millis(100), || HealState::Running)
            .expect("a timeout is said, as before");
        assert_eq!(line, "timeout waiting for recovered after 0s");
    }

    /// G4 follow-up (stress run at edd664e, `g4/n2.out`): the poller printed `ready` before
    /// `wait ready`'s own `reached ready`, when it saw writes admitted before the wait returned.
    /// While the wait runs the poller holds back a bare `ready`; after it, the wait's line is
    /// what was printed. A wait that times out leaves `ready` to the poller. Unit.
    #[test]
    fn the_poller_never_says_ready_before_wait_ready_does() {
        use rdb_api::db::ReadyError;
        let progress = Progress::default();
        let mut polled = Vec::new();
        let line = wait_ready_line(
            &progress,
            Duration::from_secs(1),
            WAIT_PROGRESS_EVERY,
            |_| {
                // The poller, mid-wait, sees writes admitted.
                polled.push(step_line(&mut progress.inner.lock().unwrap(), true, None));
                Ok(())
            },
            |_| (),
        )
        .expect("ready");
        assert_eq!(polled, vec![None], "the poller held back its `ready`");
        assert!(line.starts_with("reached ready in "), "{line}");
        let mut seen = progress.inner.lock().unwrap();
        assert!(!seen.awaiting_ready);
        assert_eq!(
            step_line(&mut seen, true, None),
            None,
            "said once, by the wait"
        );
        assert_eq!(
            step_line(&mut seen, true, Some("why".to_owned())).as_deref(),
            Some("ready for now; why"),
            "a reason is still the poller's to say"
        );
        drop(seen);

        let progress = Progress::default();
        let line = wait_ready_line(
            &progress,
            Duration::from_millis(10),
            WAIT_PROGRESS_EVERY,
            |_| {
                Err(ReadyError::TimedOut {
                    within_ms: 10,
                    phase: "Discovering".to_owned(),
                })
            },
            |_| (),
        )
        .expect("a timeout is said");
        assert!(line.starts_with("timeout waiting for ready"), "{line}");
        assert_eq!(
            step_line(&mut progress.inner.lock().unwrap(), true, None).as_deref(),
            Some("ready"),
            "after a timeout, ready is the poller's to say"
        );
    }

    /// Tester D8 (aa3b82e): from a folder's second open on, the request floor `(n-1)<<40` puts
    /// every write the app sends above `DAMAGE_REQUEST_BASE`, and the waits-on line called the
    /// app's own put "an admin damage write" and hid its id. The label now goes by origin: only
    /// an id `admin damage` or `damage-chunk` issued is a damage write. Unit.
    #[test]
    fn a_waits_on_line_names_a_second_opens_put_by_its_id_and_only_a_damage_id_as_damage() {
        let waiting = |request: RequestId| {
            let mut node = owner(None, false);
            node.in_flight = Some(request);
            node.waits_on = Some(rdb_core::contracts::ids::Seq(6));
            node
        };
        let second_open = RequestId((1 << 40) + 1);
        let line = waits_on_line(&waiting(second_open), &HELD, &[]).expect("a waits-on line");
        assert!(
            line.starts_with("partition 1 waits on request 1099511627777's outcome (seq 6),"),
            "{line}"
        );
        let damage = damage_request();
        let line = waits_on_line(&waiting(damage), &HELD, &[]).expect("a waits-on line");
        assert!(
            line.starts_with("partition 1 waits on an admin damage write's outcome (seq 6),"),
            "{line}"
        );
    }

    /// Tester d8b/d8c (A-R191 2): every run issued damage ids from the same `1<<32`, so a
    /// later run's first `admin damage` reused an id the dedup index still held under the
    /// damage client, and was refused `GENERATION_CHANGED` with the earlier run's generation.
    /// Runs started a millisecond apart now issue from ranges that never meet. Unit.
    #[test]
    fn damage_ids_of_two_runs_a_millisecond_apart_never_meet() {
        let at = 1_761_000_000_000;
        let (first, next) = (damage_start(at), damage_start(at + 1));
        assert!(
            next >= first + DAMAGE_IDS_PER_MILLI,
            "run 2 starts at {next}, inside run 1's ids from {first}"
        );
        assert!(first > DAMAGE_REQUEST_BASE, "{first}");
        let issued = damage_request();
        assert!(is_damage_request(issued), "{issued:?}");
        assert!(!is_damage_request(RequestId(issued.0 + (1 << 20))));
    }

    /// Tester w3a03 (A-R191, k227): `wait ready` read rdb_dev's own latch, so after `node
    /// restart 1` it said "reached ready in 0ms" and exited 0, while node 1 will not admit a
    /// write again in this open. It now asks `Db::wait_ready`, which answers the present:
    /// `Stopped`, with the next step, and the run exits 1. Integration (~10 s): three real
    /// rEtcd voters, an open, and a real restart of node 1.
    #[config_log::retcd_test]
    fn wait_ready_after_node_1_restarts_says_the_next_step_and_exits_1() {
        const METHOD: &str = "wait_ready_after_node_1_restarts_says_the_next_step_and_exits_1";
        let _env = ENV
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = config_testkit::fs::temp_dir();
        let dir = root.path().join("run");
        let script = root.path().join("script.txt");
        std::fs::write(&script, "wait ready 60\nnode restart 1\nwait ready 10\n")
            .expect("write the script");
        let args = Args {
            dir,
            log_dir: root.path().join("logs"),
            script: Some(script),
            keep: false,
            hold: Vec::new(),
            put_timeout: None,
            dedup_cap: None,
            dedup_retention: None,
            control: None,
            requests: None,
            abort_after_commit: false,
            pause_before_sync: Vec::new(),
            help: false,
        };
        let code = run(&args, Db::open);
        let waits: Vec<_> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .into_iter()
            .filter(|line| line["@m"] == "repl_wait")
            .collect();
        assert_eq!(waits.len(), 2, "{waits:#?}");
        assert_eq!(waits[0]["outcome"], "reached", "{waits:#?}");
        assert_eq!(waits[1]["outcome"], "stopped", "{waits:#?}");
        let said = waits[1]["error"].as_str().unwrap_or_default();
        assert!(
            said.contains("node 1, restarted") && said.ends_with("open the folder again"),
            "{said}"
        );
        assert_eq!(code, ExitCode::from(1), "a stopped wait exits 1");
    }

    /// k325 P1: `--help` and `-h` ask for the usage, which is this file's `//!` header without
    /// its comment markers or code fences. Before, both were an unknown argument (exit 2) and the
    /// help lived only in the source. `main` prints it to stdout and exits 0. Unit.
    #[test]
    fn help_asks_for_the_header_s_usage() {
        let args = |words: &[&str]| parse(words.iter().map(|w| (*w).to_owned()));
        for flag in ["--help", "-h"] {
            assert!(args(&[flag]).expect(flag).help, "{flag}");
            assert!(
                args(&["--keep", flag, "--not-a-flag"]).expect(flag).help,
                "{flag}: what follows it is not read"
            );
        }
        assert!(!args(&[]).expect("defaults").help, "off unless asked");
        let text = usage();
        assert!(
            text.starts_with("`rdb_dev`: three rDB nodes and three rEtcd voters"),
            "{text}"
        );
        for says in [
            "[--abort-after-commit] [--pause-before-sync N=MS]",
            "put <object> <value> [--if-version N]",
            "The JSONL log goes to `<log-dir>/rdb_dev.jsonl`",
        ] {
            assert!(text.contains(says), "{says}");
        }
        assert!(!text.contains("//!") && !text.contains("```"), "{text}");
    }

    /// A-R198: `--pause-before-sync N=MS` parses, refuses bad input naming the flag, and arms
    /// the host hook, so node N's barrier sync sleeps. The flag proves itself here: a run with
    /// it says it is armed, and node 1's sync then logs the sleep. Why a slow sync must not fail
    /// the barrier is `m10_a_reopen_whose_owner_syncs_slowly_still_completes_its_barrier`'s
    /// proof, not this row's. Integration (~3 s): three real rEtcd voters and one open.
    #[config_log::retcd_test]
    fn pause_before_sync_parses_and_arms_the_node_s_barrier_sync() {
        const METHOD: &str = "pause_before_sync_parses_and_arms_the_node_s_barrier_sync";
        let args = |words: &[&str]| parse(words.iter().map(|w| (*w).to_owned()));
        let two = args(&["--pause-before-sync", "1=50", "--pause-before-sync", "3=7"])
            .expect("two good pauses");
        assert_eq!(
            two.pause_before_sync,
            [
                (NodeId(1), Duration::from_millis(50)),
                (NodeId(3), Duration::from_millis(7))
            ]
        );
        assert!(
            args(&[]).expect("defaults").pause_before_sync.is_empty(),
            "off unless given"
        );
        for (bad, says) in [
            ("4=10", "a node is 1, 2 or 3"),
            ("1=x", "is not a number of ms"),
            ("1=", "is not a number of ms"),
            ("1", "takes <node>=<ms>"),
        ] {
            let refused = args(&["--pause-before-sync", bad]).expect_err(bad);
            assert!(
                refused.starts_with("--pause-before-sync") && refused.contains(says),
                "{bad}: {refused}"
            );
        }
        let refused = args(&["--pause-before-sync"]).expect_err("no value");
        assert_eq!(refused, "--pause-before-sync needs a value");

        let _env = ENV
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // RETCD_TEST_DATA_DIR is left as it is: each row that points it into its own `--dir`
        // nests the next row's folders one level deeper, and a sixth level passed Windows'
        // MAX_PATH inside RocksDB.
        let root = config_testkit::fs::temp_dir();
        let dir = root.path().join("run");
        let script = root.path().join("script.txt");
        std::fs::write(&script, "wait ready 60\n").expect("write the script");
        let run_args = Args {
            dir,
            log_dir: root.path().join("logs"),
            script: Some(script),
            ..args(&["--pause-before-sync", "1=50"]).expect("one pause")
        };
        assert_eq!(run(&run_args, Db::open), ExitCode::SUCCESS, "the run");
        let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
        let armed: Vec<_> = lines
            .iter()
            .filter(|line| line["@m"] == "pause_before_sync_armed")
            .collect();
        assert_eq!(armed.len(), 1, "{armed:#?}");
        assert_eq!(
            (&armed[0]["@l"], &armed[0]["node"], &armed[0]["ms"]),
            (&"Warning".into(), &1.into(), &50.into()),
            "{armed:#?}"
        );
        // The node threads carry no test span here, so their lines go to the run's untagged
        // file. Only this row arms a pause in this binary, and the ENV lock keeps the other
        // opens out while it runs.
        config_log::layer::flush_test_logs();
        let untagged = config_log::testing::test_log_dir()
            .join(format!("_untagged-{}.jsonl", std::process::id()));
        let text = std::fs::read_to_string(&untagged)
            .unwrap_or_else(|e| panic!("read {}: {e}", untagged.display()));
        let paused: Vec<_> = text
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|line| line["@m"] == "test_sync_paused")
            .map(|line| (line["node"].clone(), line["millis"].clone()))
            .collect();
        assert_eq!(paused, [(1.into(), 50.into())], "node 1's sync slept once");
    }

    /// Tester (lead, 2026-10-23): `wait ready` printed nothing for ~40 s while rEtcd had no
    /// quorum; only the final timeout named the state. It now says node 1's phase after each
    /// `every` it waits, and its timeout names the whole bound, not the last slice. Unit.
    #[test]
    fn wait_ready_says_the_phase_every_slice_while_it_waits() {
        use rdb_api::db::ReadyError;
        let timed_out = |slice: Duration| ReadyError::TimedOut {
            within_ms: slice.as_millis(),
            phase: "ProposalUnresolved".to_owned(),
        };
        let progress = Progress::default();
        let (mut asked, mut said) = (Vec::new(), Vec::new());
        let line = wait_ready_line(
            &progress,
            Duration::from_secs(25),
            Duration::from_secs(10),
            |slice| {
                asked.push(slice.as_secs());
                Err(timed_out(slice))
            },
            |line| said.push(line.to_owned()),
        )
        .expect("a timeout is said, not an error");
        assert_eq!(asked, [10, 10, 5], "slices of at most `every`, 25 s in all");
        assert_eq!(
            said,
            ["still waiting for ready: recovery phase ProposalUnresolved"; 2]
        );
        assert_eq!(
            line,
            "timeout waiting for ready after 25s: node 1 did not admit writes within 25000 ms; \
             recovery phase ProposalUnresolved"
        );

        // Ready in the second slice: one line said, then the reached line.
        let (mut slices, mut said) = (0, Vec::new());
        let line = wait_ready_line(
            &progress,
            Duration::from_secs(25),
            Duration::from_secs(10),
            |slice| {
                slices += 1;
                if slices == 1 {
                    Err(timed_out(slice))
                } else {
                    Ok(())
                }
            },
            |line| said.push(line.to_owned()),
        )
        .expect("ready");
        assert!(line.starts_with("reached ready in "), "{line}");
        assert_eq!(said.len(), 1, "{said:?}");
    }

    /// Lead, on 5eeca8a: only `TimedOut` asks for another slice. An answer no wait changes
    /// (a failed heal, node 1 restarted, `Blocked`, `Quarantined`) ends the wait on the slice
    /// that gave it, with nothing said in between, and the run exits 1. Unit.
    #[test]
    fn wait_ready_ends_on_the_first_final_answer_without_another_slice() {
        use rdb_api::db::ReadyError;
        let finals = [
            ReadyError::Heal(admin::HealError::NodeStopped),
            ReadyError::Stopped {
                note: "node 1 restarted".to_owned(),
            },
            ReadyError::Blocked {
                reason: "BarrierIncomplete".to_owned(),
            },
            ReadyError::Quarantined,
        ];
        let progress = Progress::default();
        for answer in finals {
            let (mut slices, mut said) = (0, Vec::<String>::new());
            let line = wait_ready_line(
                &progress,
                Duration::from_secs(60),
                Duration::from_secs(10),
                |_| {
                    slices += 1;
                    Err(answer.clone())
                },
                |line| said.push(line.to_owned()),
            )
            .expect_err("a final answer exits 1");
            assert_eq!((slices, said.len()), (1, 0), "{answer:?}: {line}");
            assert!(line.contains(&answer.to_string()), "{answer:?}: {line}");
        }
    }

    /// Tester, embed.md walk at 6ed41b8: `get m1` on a map answered only `a Map does not take
    /// this op`. It now names the verb that reads that kind; any other error is left as it is.
    /// Unit.
    #[test]
    fn get_on_another_kind_names_the_verb_that_reads_it() {
        use rdb_core::contracts::errors::ErrorKind;
        let refused = |detail: &str| ApiError::new(ErrorKind::InvalidArgument, detail);
        for (kind, verb) in [
            ("Map", "members"),
            ("Set", "members"),
            ("List", "items"),
            ("Blob", "read"),
        ] {
            let said = get_hint(refused(&format!("a {kind} does not take this op"))).detail;
            assert_eq!(
                said,
                format!(
                    "a {kind} does not take this op; `get` reads a document, read this one with \
                     `{verb} <object>`"
                )
            );
        }
        let other = refused("object id 2000 bytes, over the 1024-byte limit");
        assert_eq!(get_hint(other.clone()), other, "left as it is");
    }

    /// Tester k237 (e3b1e30): `wait ready` printed "reached ready", and the poller printed a
    /// second bare `ready` line inside the next command's output. One transition is said once:
    /// a wait that reached ready records it as printed, so the poller says nothing more until
    /// the state changes again. Unit.
    #[test]
    fn a_reached_wait_ready_is_not_said_again_by_the_poller() {
        let progress = Progress::default();
        let line = wait_ready_line(
            &progress,
            Duration::from_secs(1),
            WAIT_PROGRESS_EVERY,
            |_| Ok(()),
            |_| (),
        )
        .expect("ready is not an error");
        assert!(line.starts_with("reached ready in "), "{line}");
        let mut seen = progress.seen();
        assert_eq!(step_line(&mut seen, true, None), None, "said once");
        assert_eq!(
            step_line(&mut seen, false, None).as_deref(),
            Some("not ready: writes not admitted"),
            "a later change is still said"
        );
    }

    /// The child of [`abort_after_commit_aborts_and_the_next_open_rolls_forward`]: one rdb_dev
    /// run with `--abort-after-commit`, against the parent's voters. Passes at once unless the
    /// parent started it.
    #[test]
    fn zz_abort_after_commit_child() {
        let (Ok(dir), Ok(control)) = (
            std::env::var(ABORT_CHILD_DIR),
            std::env::var(ABORT_CHILD_CONTROL),
        ) else {
            return;
        };
        let dir = PathBuf::from(dir);
        let script = dir.with_extension("child-script.txt");
        std::fs::write(&script, "wait ready 60\n").expect("write the child's script");
        let args = Args {
            log_dir: dir.with_extension("child-logs"),
            script: Some(script),
            control: Some(control.split(',').map(str::to_owned).collect()),
            abort_after_commit: true,
            ..parse(std::iter::empty()).expect("defaults")
        };
        let args = Args { dir, ..args };
        run(&args, Db::open);
    }

    const ABORT_CHILD_DIR: &str = "RDB_DEV_ABORT_CHILD_DIR";
    const ABORT_CHILD_CONTROL: &str = "RDB_DEV_ABORT_CHILD_CONTROL";

    /// A-R184: `--abort-after-commit` is how the tester reaches a commit that no copy links.
    /// Run 1 writes and quits; run 2 reopens with the flag and dies by abort once its recovery
    /// commits; open 3 rolls that commit forward, becomes ready, and reads run 1's write back.
    /// Integration (~10 s): three real rEtcd voters this test hosts, so all three opens meet
    /// the same control store, and a child process for the run that aborts.
    #[test]
    fn abort_after_commit_aborts_and_the_next_open_rolls_forward() {
        let _env = ENV
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = config_testkit::fs::temp_dir();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        let cluster = rt.block_on(
            Cluster::builder()
                .nodes(3)
                .storage(StorageKind::ROCKS)
                .timers(CONTROL_TIMERS)
                .start(),
        );
        rt.block_on(cluster.leader());
        let control = cluster.client_endpoints();
        let dir = root.path().join("run");

        let script = root.path().join("run1.txt");
        std::fs::write(&script, "wait ready 60\nput zqk zqv1\n").expect("write run 1's script");
        let run1 = Args {
            log_dir: root.path().join("run1-logs"),
            script: Some(script),
            control: Some(control.clone()),
            ..parse(std::iter::empty()).expect("defaults")
        };
        let run1 = Args {
            dir: dir.clone(),
            ..run1
        };
        assert_eq!(run(&run1, Db::open), ExitCode::SUCCESS, "run 1 writes");

        let child = std::process::Command::new(std::env::current_exe().expect("this binary"))
            .args([
                "tests::zz_abort_after_commit_child",
                "--exact",
                "--test-threads=1",
            ])
            .env(ABORT_CHILD_DIR, &dir)
            .env(ABORT_CHILD_CONTROL, control.join(","))
            .output()
            .expect("spawn run 2");
        assert!(
            !child.status.success()
                && !String::from_utf8_lossy(&child.stdout).contains("test result"),
            "run 2 must die by abort, not end or panic; got {:?}: {}",
            child.status,
            String::from_utf8_lossy(&child.stdout)
        );

        let store: Arc<dyn ConfigStore> = Arc::new(
            config_client::GrpcClient::connect(
                control,
                config_client::GrpcClientOptions::default(),
            )
            .expect("a control client"),
        );
        let config = DbConfig {
            dir,
            hold: Vec::new(),
            timeouts: Timeouts::default(),
            dedup_cap: None,
            dedup_retention: None,
        };
        let mut db = rt
            .block_on(Db::open(config, store, rt.handle().clone()))
            .expect("open 3 rolls the commit forward");
        db.wait_ready(Duration::from_secs(60))
            .expect("open 3 becomes ready");
        let got = db.get(b"zqk").expect("read back");
        assert_eq!(
            got.value.map(|(_, bytes)| bytes.to_vec()),
            Some(b"zqv1".to_vec()),
            "run 1's write"
        );
        db.shutdown();
        rt.block_on(cluster.shutdown());
    }

    /// Three real rEtcd voters, and one `Db` on them at `<dir>/run`, ready. The caller holds the
    /// `ENV` lock and shuts down the Db, then the cluster, on the runtime returned.
    fn ready_on_voters(dir: &std::path::Path) -> (tokio::runtime::Runtime, Cluster, Db) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        let cluster = rt.block_on(
            Cluster::builder()
                .nodes(3)
                .storage(StorageKind::ROCKS)
                .timers(CONTROL_TIMERS)
                .start(),
        );
        rt.block_on(cluster.leader());
        let store: Arc<dyn ConfigStore> = Arc::new(
            config_client::GrpcClient::connect(
                cluster.client_endpoints(),
                config_client::GrpcClientOptions::default(),
            )
            .expect("a control client"),
        );
        let config = DbConfig {
            dir: dir.join("run"),
            hold: Vec::new(),
            timeouts: Timeouts::default(),
            dedup_cap: None,
            dedup_retention: None,
        };
        let db = rt
            .block_on(Db::open(config, store, rt.handle().clone()))
            .expect("opens");
        db.wait_ready(Duration::from_secs(60)).expect("ready");
        (rt, cluster, db)
    }

    /// A-R211: `admin damage <set> --element K` with `--as newer` or `--as kind0` is refused
    /// before anything is written. A set member has no envelope, so the sealed record the flag
    /// names would only be bytes in it, which reads as damage (CORRUPT_HISTORY), never as a newer
    /// build's record: the echo `as=newer` over member-bytes damage misled the tester. The REPL
    /// prints the refusal as `usage: ...` and the run exits 1, as for any refused line. `--as
    /// digest` still damages the member. The help says both. Integration (~3 s): three real
    /// rEtcd voters and one open.
    #[test]
    fn damage_a_set_member_as_newer_or_kind0_is_refused_and_writes_nothing() {
        let _env = ENV
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let help = usage();
        for says in [
            "or any one entry being damaged is enough",
            "on a set --element takes",
            "`digest` only; `kind0` and `newer` are refused, and",
        ] {
            assert!(help.contains(says), "help lacks {says:?}");
        }
        assert!(
            !help.contains("clear is for a"),
            "the stricter wording is gone"
        );

        let root = config_testkit::fs::temp_dir();
        let (rt, cluster, mut db) = ready_on_voters(root.path());
        let x = || Value::Text("x".to_owned());
        db.write(
            b"s",
            Change::Set(vec![
                ElemOp::Add(x()),
                ElemOp::Add(Value::Text("y".to_owned())),
            ]),
            Some(Expected::Absent),
        )
        .expect("the set is written");
        let members = || Look::Members {
            after: None,
            limit: 10,
        };
        let version = |db: &Db| match db.read(b"s", members()) {
            Ok(ReadOk {
                seen: rdb_api::Seen::Members(page),
                ..
            }) => (page.collection.version, page.members.len()),
            other => panic!("the set reads: {other:?}"),
        };
        let before = version(&db);
        for how in ["newer", "kind0"] {
            let refused =
                damage_record(&db, &format!(r#"admin damage s --element "x" --as {how}"#))
                    .expect_err(how);
            assert_eq!(
                refused,
                format!(
                    "admin damage: a set member holds no envelope, so any bytes in it are \
                     damage; damage a set member with --as digest, or a newer build's set with \
                     the root and --as newer; not a member with --as {how}"
                )
            );
            assert_eq!(version(&db), before, "{how}: nothing was written");
        }
        damage_record(&db, r#"admin damage s --element "x""#).expect("digest is the default");
        let damaged = db.read(b"s", members()).expect_err("x holds bytes");
        assert!(
            damaged.detail.contains("set member record holds"),
            "{damaged:?}"
        );
        db.shutdown();
        rt.block_on(cluster.shutdown());
    }

    /// k325 P2 (k326): a put and an `admin clear` refused for a newer build's record say the
    /// next step, `open the folder with the newer build`, as embed.md does. Before, the put said
    /// only what it met and the clear only `not cleared`. Kind, retry and `no_mutation` are
    /// unchanged (A-R200). Integration (~3 s): three real rEtcd voters and one open.
    #[test]
    fn a_newer_build_s_refusal_names_the_next_step() {
        use rdb_core::contracts::errors::ErrorKind;
        const STEP: &str = "; open the folder with the newer build";
        let _env = ENV
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = config_testkit::fs::temp_dir();
        let (rt, cluster, mut db) = ready_on_voters(root.path());
        db.put(b"x", b"1", None).expect("x is written");
        damage_record(&db, "admin damage x --as newer").expect("the line parses");

        let put = db
            .put(b"x", b"2", None)
            .expect_err("a newer build's record");
        let clear = db
            .write(b"x", Change::Clear, None)
            .expect_err("a newer build's record");
        for (what, error, ends) in [
            ("put", &put.error, STEP.to_owned()),
            ("clear", &clear.error, format!("{STEP}; not cleared")),
        ] {
            assert_eq!(
                (error.kind, error.retry, error.no_mutation),
                (ErrorKind::IncompatibleVersion, RetryRule::Quarantine, true),
                "{what}: {error:?}"
            );
            assert!(
                error.detail.contains("written by a newer build: ")
                    && error.detail.ends_with(&ends),
                "{what}: {error:?}"
            );
        }
        db.shutdown();
        rt.block_on(cluster.shutdown());
    }

    /// Lead, on the 8e083a9 walk: `admin damage m --element K` (`--as digest`, the default)
    /// wrote the entry at a version above its root's, so a read answered `element version 3 is
    /// above its root's version 2`, not the digest fault the mode names, and a `map put` over
    /// the entry was refused. The tool now writes the root's own bytes in the same write, so
    /// both carry its version: a read meets the digest fault, and a put over the entry repairs
    /// it (ADR-rdb-0013 §10). Integration (~3 s): three real rEtcd voters and one open.
    #[test]
    fn damage_element_as_digest_reads_as_a_digest_fault_and_a_put_repairs_it() {
        let _env = ENV
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = config_testkit::fs::temp_dir();
        let (rt, cluster, mut db) = ready_on_voters(root.path());
        let int = |n: u64| Value::Integer(Int::from(n));
        let text = |t: &str| Value::Text(t.to_owned());
        let entries = (1..=3).map(|n| ElemOp::Put(int(n), text(&n.to_string())));
        db.write(b"m", Change::Map(entries.collect()), None)
            .expect("the map is written");

        damage_record(&db, "admin damage m --element 2").expect("the line parses");
        let members = Look::Members {
            after: None,
            limit: 10,
        };
        let fault = db
            .read(b"m", members.clone())
            .expect_err("entry 2 is damaged");
        assert!(
            fault.detail.contains("do not hash to the stored digest")
                && !fault.detail.contains("above its root"),
            "a digest fault: {fault:?}"
        );

        db.write(
            b"m",
            Change::Map(vec![ElemOp::Put(int(2), text("again"))]),
            None,
        )
        .expect("a put over a bad digest repairs the entry");
        let Ok(ReadOk {
            seen: rdb_api::Seen::Members(page),
            ..
        }) = db.read(b"m", members)
        else {
            panic!("the map reads again after the repair");
        };
        let read: Vec<_> = page
            .members
            .into_iter()
            .map(|member| (member.key, member.value))
            .collect();
        assert_eq!(
            read,
            [
                (int(1), Some(text("1"))),
                (int(2), Some(text("again"))),
                (int(3), Some(text("3")))
            ]
        );
        db.shutdown();
        rt.block_on(cluster.shutdown());
    }
}
