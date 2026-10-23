//! `pilot_kv`: a small app that keeps its data in rDB (M10 slice 1, arch-m10 R3c).
//!
//! ```text
//! pilot_kv --dir D --control E [--log-dir L] [--ready-secs S] [--write N] [--reopens K]
//!          [--verify] [--bench]
//! ```
//!
//! It opens the rDB folder `D` against the rEtcd at `E` (`host:port[,host:port...]`, plain
//! gRPC), new or already holding data, waits until it is ready (`Db::wait_ready`), then does
//! what its flags ask, in this order:
//!
//! * `--write N` puts N new keys, one at a time. After each acknowledged put it appends one line
//!   to the ack journal `D/pilot_kv.journal` (`key request seq generation`), written straight to
//!   the file before the next put, so a killed process loses no line it acknowledged. Keys are
//!   `pilot/00000000`, `pilot/00000001`, ...; each run continues after the highest journalled
//!   key. A key's value is derived from it, so the journal never stores values.
//! * `--reopens K` shuts the database down and opens it again, K times, printing each
//!   reopen's open-to-ready. `--verify` and `--bench` run after the last one.
//! * `--verify` reads every journalled key back and checks its exact value: journal ⊆ present.
//!   It prints `verified J/J ...` and exits 0, or names how many are missing or wrong and
//!   exits 4. The one key after the highest journalled is the put a kill may have caught in
//!   flight; finding it is allowed, and it is reported apart as `unjournalled_present`.
//! * `--bench` reads every journalled key once and prints the get latency, p50 and p99.
//!
//! Every open prints `ready ms=<open-to-ready> heal=<HealState>` (`reopen i ready ...` for a
//! reopen). Exits: 0 done; 1 a write or read failed; 2 bad arguments or setup; 3 the folder was
//! refused, by `Db::open` or by a reopen's heal (`open refused: <Variant>: ...`); 4 `--verify`
//! found a journalled key missing or wrong; 5 not ready in time.
//!
//! Logs go to `L/pilot_kv.jsonl` (default `D` with `.logs` appended) and never carry key or
//! value text (ADR-0013, A-R154): the pilot logs counts and indices only.

use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use config_core::ConfigStore;
use rdb_api::db::ReadyError;
use rdb_api::{Db, DbConfig, Timeouts};

/// The ack journal's file name, inside the rDB folder.
const JOURNAL: &str = "pilot_kv.journal";

#[derive(Debug, PartialEq, Eq)]
struct Args {
    dir: PathBuf,
    control: Vec<String>,
    log_dir: PathBuf,
    ready: Duration,
    write: Option<u64>,
    reopens: u64,
    verify: bool,
    bench: bool,
}

const USAGE: &str = "usage: pilot_kv --dir D --control host:port[,host:port...] [--log-dir L] \
                     [--ready-secs S] [--write N] [--reopens K] [--verify] [--bench]";

fn parse(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut dir = None;
    let mut control = Vec::new();
    let mut log_dir = None;
    let mut ready = Duration::from_secs(60);
    let mut write = None;
    let mut reopens = 0;
    let mut verify = false;
    let mut bench = false;
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().ok_or_else(|| format!("{name} needs a value"));
        match arg.as_str() {
            "--dir" => dir = Some(PathBuf::from(value("--dir")?)),
            "--control" => {
                control = value("--control")?
                    .split(',')
                    .map(str::trim)
                    .filter(|endpoint| !endpoint.is_empty())
                    .map(str::to_owned)
                    .collect();
            }
            "--log-dir" => log_dir = Some(PathBuf::from(value("--log-dir")?)),
            "--ready-secs" => {
                let text = value("--ready-secs")?;
                let secs = text
                    .parse::<u64>()
                    .map_err(|_| format!("--ready-secs takes whole seconds, got {text}"))?;
                ready = Duration::from_secs(secs);
            }
            "--write" => {
                let text = value("--write")?;
                let n = text
                    .parse::<u64>()
                    .map_err(|_| format!("--write takes a count, got {text}"))?;
                write = Some(n);
            }
            "--reopens" => {
                let text = value("--reopens")?;
                reopens = text
                    .parse::<u64>()
                    .map_err(|_| format!("--reopens takes a count, got {text}"))?;
            }
            "--verify" => verify = true,
            "--bench" => bench = true,
            other => return Err(format!("unknown argument {other}")),
        }
    }
    let dir = dir.ok_or("--dir is required")?;
    if control.is_empty() {
        return Err("--control is required: the rEtcd client endpoints".to_owned());
    }
    let log_dir = log_dir.unwrap_or_else(|| {
        let mut name = dir.as_os_str().to_owned();
        name.push(".logs");
        PathBuf::from(name)
    });
    Ok(Args {
        dir,
        control,
        log_dir,
        ready,
        write,
        reopens,
        verify,
        bench,
    })
}

/// The key of journal index `index`.
fn key(index: u64) -> String {
    format!("pilot/{index:08}")
}

/// The value written under index `index`: derived, so `--verify` knows it without a copy.
fn value(index: u64) -> Vec<u8> {
    format!("pilot-value-{index}").into_bytes()
}

/// One acknowledged put, as the journal records it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    index: u64,
    request: u64,
    seq: u64,
    generation: u64,
}

impl Entry {
    fn line(&self) -> String {
        format!(
            "{} {} {} {}\n",
            key(self.index),
            self.request,
            self.seq,
            self.generation
        )
    }
}

/// The journal as read: its whole lines, and whether a last line was cut short. A kill can land
/// inside the one write that appends a line; that line's put was acknowledged, but the line
/// never fully reached the file, so it is left out and counted, never guessed at.
#[derive(Debug, Default, PartialEq, Eq)]
struct Journal {
    entries: Vec<Entry>,
    torn: bool,
}

impl Journal {
    /// The index the next `--write` starts at: one past the highest journalled.
    fn next_index(&self) -> u64 {
        self.entries
            .iter()
            .map(|entry| entry.index + 1)
            .max()
            .unwrap_or(0)
    }
}

/// Parse a journal's text. A malformed whole line is an error, never skipped: a journal that
/// cannot be read cannot vouch for anything.
fn parse_journal(text: &str) -> Result<Journal, String> {
    let mut journal = Journal::default();
    for (number, line) in text.split_inclusive('\n').enumerate() {
        let Some(line) = line.strip_suffix('\n') else {
            // Only the last piece can lack its newline.
            journal.torn = true;
            break;
        };
        let line = line.strip_suffix('\r').unwrap_or(line);
        let fields: Vec<&str> = line.split(' ').collect();
        let bad = || format!("journal line {}: malformed", number + 1);
        let [key_text, request, seq, generation] = fields[..] else {
            return Err(bad());
        };
        let index = key_text
            .strip_prefix("pilot/")
            .and_then(|digits| digits.parse::<u64>().ok())
            .ok_or_else(bad)?;
        let number = |text: &str| text.parse::<u64>().map_err(|_| bad());
        journal.entries.push(Entry {
            index,
            request: number(request)?,
            seq: number(seq)?,
            generation: number(generation)?,
        });
    }
    Ok(journal)
}

fn read_journal(path: &Path) -> Result<Journal, String> {
    match std::fs::read_to_string(path) {
        Ok(text) => parse_journal(&text),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Journal::default()),
        Err(error) => Err(format!("{}: {error}", path.display())),
    }
}

/// What `--verify` found.
#[derive(Debug, Default, PartialEq, Eq)]
struct Report {
    journalled: u64,
    exact: u64,
    /// Indices whose key read back absent.
    missing: Vec<u64>,
    /// Indices whose key read back with another value.
    wrong: Vec<u64>,
    /// Whether the key after the highest journalled is present: a put acknowledged, or applied,
    /// just before a kill, whose line never reached the journal. Allowed.
    unjournalled_present: bool,
}

impl Report {
    fn holds(&self) -> bool {
        self.missing.is_empty() && self.wrong.is_empty()
    }
}

/// Check journal ⊆ present with exact values, reading each key through `get`, which answers a
/// key's value or `None` when absent. A read that fails is an error, never "absent".
fn verify(
    journal: &Journal,
    mut get: impl FnMut(&[u8]) -> Result<Option<Vec<u8>>, String>,
) -> Result<Report, String> {
    let mut report = Report::default();
    for entry in &journal.entries {
        report.journalled += 1;
        match get(key(entry.index).as_bytes())? {
            Some(found) if found == value(entry.index) => report.exact += 1,
            Some(_) => report.wrong.push(entry.index),
            None => report.missing.push(entry.index),
        }
    }
    let next = journal.next_index();
    report.unjournalled_present = get(key(next).as_bytes())?.is_some();
    Ok(report)
}

/// The value at `quantile` (0..=1) of sorted `samples`, nearest rank.
fn percentile(sorted: &[Duration], quantile: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    // Nearest rank: ceil(q * n), 1-based. The cast is exact for any journal that fits memory.
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let rank = (quantile * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn main() -> ExitCode {
    let args = match parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("pilot_kv: {message}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let mut log = config_log::LogConfig::for_application("pilot_kv");
    log.dir.clone_from(&args.log_dir);
    log.file_name = "pilot_kv.jsonl".to_owned();
    let guard = match config_log::init(log) {
        Ok(guard) => guard,
        Err(error) => {
            eprintln!("pilot_kv: log init: {error}");
            return ExitCode::from(2);
        }
    };
    eprintln!("log={}", args.log_dir.join("pilot_kv.jsonl").display());
    let code = run(&args);
    drop(guard);
    code
}

fn run(args: &Args) -> ExitCode {
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(error) => {
            eprintln!("pilot_kv: runtime: {error}");
            return ExitCode::from(2);
        }
    };
    let client = match config_client::GrpcClient::connect(
        args.control.clone(),
        config_client::GrpcClientOptions::default(),
    ) {
        Ok(client) => client,
        Err(error) => {
            eprintln!("pilot_kv: --control: {error}");
            return ExitCode::from(2);
        }
    };
    let store: Arc<dyn ConfigStore> = Arc::new(client);
    let config = DbConfig {
        dir: args.dir.clone(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
    };
    tracing::info!(
        write = args.write,
        reopens = args.reopens,
        verify = args.verify,
        "pilot_open"
    );
    let mut db = match open_ready(&rt, &store, &config, args.ready, "ready") {
        Ok(db) => db,
        Err(code) => return code,
    };
    let code = write_phase(&db, args);
    if code != ExitCode::SUCCESS {
        db.shutdown();
        return code;
    }
    for reopen in 1..=args.reopens {
        db.shutdown();
        let label = format!("reopen {reopen} ready");
        db = match open_ready(&rt, &store, &config, args.ready, &label) {
            Ok(db) => db,
            Err(code) => return code,
        };
    }
    let code = check_phase(&db, args);
    db.shutdown();
    code
}

/// Open the folder and wait until node 1 admits writes. Prints `<label> ms=<open-to-ready>
/// heal=<state>`. Exit 3 when the open is refused or a reopen's heal fails, 5 when it is not
/// ready within `within`.
fn open_ready(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
    config: &DbConfig,
    within: Duration,
    label: &str,
) -> Result<Db, ExitCode> {
    let started = Instant::now();
    let opened = rt.block_on(Db::open(
        config.clone(),
        Arc::clone(store),
        rt.handle().clone(),
    ));
    let mut db = match opened {
        Ok(db) => db,
        Err(error) => {
            let variant = variant(&error);
            tracing::error!(variant, %error, "pilot_open_refused");
            println!("open refused: {variant}: {error}");
            return Err(ExitCode::from(3));
        }
    };
    match db.wait_ready(within) {
        Ok(()) => {}
        Err(error @ ReadyError::Heal(_)) => {
            tracing::error!(%error, "pilot_heal_failed");
            println!("open refused: Heal: {error}");
            db.shutdown();
            return Err(ExitCode::from(3));
        }
        Err(
            error @ (ReadyError::TimedOut { .. }
            | ReadyError::Stopped { .. }
            | ReadyError::Blocked { .. }
            | ReadyError::Quarantined
            | ReadyError::Faulted { .. }),
        ) => {
            tracing::error!(%error, "pilot_not_ready");
            println!("not ready: {error}");
            db.shutdown();
            return Err(ExitCode::from(5));
        }
    }
    let ready = started.elapsed();
    let heal = variant(&db.heal_state());
    println!("{label} ms={} heal={heal}", millis(ready));
    tracing::info!(label, millis = millis(ready), heal, "pilot_ready");
    Ok(db)
}

/// The variant name of an enum value, for the line a script greps.
fn variant(value: &impl std::fmt::Debug) -> String {
    let debug = format!("{value:?}");
    debug
        .split(|c: char| !c.is_alphanumeric())
        .next()
        .unwrap_or_default()
        .to_owned()
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// `--write`: put new keys after the highest journalled one.
fn write_phase(db: &Db, args: &Args) -> ExitCode {
    let path = args.dir.join(JOURNAL);
    let journal = match read_journal(&path) {
        Ok(journal) => journal,
        Err(error) => {
            println!("journal unreadable: {error}");
            return ExitCode::from(1);
        }
    };
    if journal.torn {
        println!("journal: last line cut short by a kill; left out");
    }
    if let Some(n) = args.write {
        if let Err(code) = write(db, &path, journal.next_index(), n) {
            return code;
        }
    }
    ExitCode::SUCCESS
}

/// `--verify`, then `--bench`, over the journal as it stands.
fn check_phase(db: &Db, args: &Args) -> ExitCode {
    let path = args.dir.join(JOURNAL);
    let journal = match read_journal(&path) {
        Ok(journal) => journal,
        Err(error) => {
            println!("journal unreadable: {error}");
            return ExitCode::from(1);
        }
    };
    if args.verify {
        let get = |key: &[u8]| {
            db.get(key)
                .map(|ok| ok.value.map(|(_, bytes)| bytes.to_vec()))
                .map_err(|error| error.to_string())
        };
        let report = match verify(&journal, get) {
            Ok(report) => report,
            Err(error) => {
                println!("verify failed: a read failed: {error}");
                return ExitCode::from(1);
            }
        };
        tracing::info!(
            journalled = report.journalled,
            exact = report.exact,
            missing = report.missing.len(),
            wrong = report.wrong.len(),
            unjournalled_present = report.unjournalled_present,
            torn = journal.torn,
            "pilot_verify"
        );
        println!(
            "verified {}/{} journalled present with exact values; missing={} wrong={} \
             unjournalled_present={}",
            report.exact,
            report.journalled,
            report.missing.len(),
            report.wrong.len(),
            report.unjournalled_present
        );
        if !report.holds() {
            println!(
                "first missing index={:?} first wrong index={:?}",
                report.missing.first(),
                report.wrong.first()
            );
            return ExitCode::from(4);
        }
    }
    if args.bench {
        if let Err(code) = bench(db, &journal) {
            return code;
        }
    }
    ExitCode::SUCCESS
}

/// Put `n` keys from `first`, appending each acknowledged one to the journal at `path` before
/// the next put. Stops at the first refusal: a put that was not acknowledged is not journalled,
/// and the pilot does not guess at it.
fn write(db: &Db, path: &Path, first: u64, n: u64) -> Result<(), ExitCode> {
    // No `BufWriter`: each line goes to the OS in its own write, so a killed process has
    // handed over every line it wrote. Not fsynced; the proof is a process kill, not power loss.
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| {
            println!("journal: {}: {error}", path.display());
            ExitCode::from(1)
        })?;
    let started = Instant::now();
    for index in first..first.saturating_add(n) {
        let ok = match db.put(key(index).as_bytes(), &value(index), None) {
            Ok(ok) => ok,
            Err(error) => {
                tracing::error!(index, error = %error.error.name(), "pilot_put_refused");
                println!(
                    "put index={index} refused: {error}; wrote {} this run",
                    index - first
                );
                return Err(ExitCode::from(1));
            }
        };
        let entry = Entry {
            index,
            request: ok.request.0,
            seq: ok.seq.0,
            generation: ok.generation.0,
        };
        if let Err(error) = file.write_all(entry.line().as_bytes()) {
            println!("journal: {}: {error}", path.display());
            return Err(ExitCode::from(1));
        }
    }
    let elapsed = started.elapsed();
    tracing::info!(first, n, millis = millis(elapsed), "pilot_write");
    println!("wrote {n} from index {first} ms={}", millis(elapsed));
    Ok(())
}

/// Read each journalled key once; print get p50 and p99.
fn bench(db: &Db, journal: &Journal) -> Result<(), ExitCode> {
    let mut samples = Vec::with_capacity(journal.entries.len());
    for entry in &journal.entries {
        let started = Instant::now();
        if let Err(error) = db.get(key(entry.index).as_bytes()) {
            println!("bench: get index={} failed: {error}", entry.index);
            return Err(ExitCode::from(1));
        }
        samples.push(started.elapsed());
    }
    samples.sort();
    let p50 = percentile(&samples, 0.50);
    let p99 = percentile(&samples, 0.99);
    tracing::info!(
        n = samples.len(),
        p50_us = p50.as_micros(),
        p99_us = p99.as_micros(),
        "pilot_bench"
    );
    println!(
        "bench gets n={} p50_us={} p99_us={}",
        samples.len(),
        p50.as_micros(),
        p99.as_micros()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64) -> Entry {
        Entry {
            index,
            request: index + 1,
            seq: index + 1,
            generation: 1,
        }
    }

    /// C-8: the journal is what `--verify` trusts, so what `--write` appends must read back as
    /// the same entries, a run continues after the highest journalled key, and a malformed
    /// line is refused rather than skipped. A last line a kill cut short is left out and said.
    #[test]
    fn the_journal_reads_back_what_was_appended_and_refuses_what_it_cannot_read() {
        let text: String = [entry(0), entry(1), entry(7)]
            .iter()
            .map(Entry::line)
            .collect();
        let journal = parse_journal(&text).expect("whole lines");
        assert_eq!(journal.entries, [entry(0), entry(1), entry(7)]);
        assert!(!journal.torn);
        assert_eq!(journal.next_index(), 8);
        assert_eq!(parse_journal("").expect("empty").next_index(), 0);

        let cut = format!("{}pilot/00000008 9", entry(0).line());
        let journal = parse_journal(&cut).expect("a torn last line is not an error");
        assert_eq!((journal.entries, journal.torn), (vec![entry(0)], true));

        for bad in [
            "pilot/x 1 1 1\n",
            "pilot/00000001 1 1\n",
            "other/00000001 1 1 1\n",
        ] {
            assert!(parse_journal(bad).is_err(), "{bad:?}");
        }
    }

    /// C-8: journal ⊆ present, exact values. A journalled key that reads back absent or with
    /// another value is named; the key after the highest journalled may be present (a put the
    /// kill caught) and is reported apart; a failed read is an error, never "absent".
    #[test]
    fn verify_names_every_journalled_key_that_is_missing_or_wrong() {
        let journal = Journal {
            entries: vec![entry(0), entry(1), entry(2)],
            torn: false,
        };
        let present = |keys: &[(u64, Vec<u8>)]| {
            let keys: std::collections::BTreeMap<String, Vec<u8>> = keys
                .iter()
                .map(|(index, bytes)| (key(*index), bytes.clone()))
                .collect();
            move |asked: &[u8]| Ok(keys.get(std::str::from_utf8(asked).expect("utf8")).cloned())
        };

        let all = verify(
            &journal,
            present(&[(0, value(0)), (1, value(1)), (2, value(2))]),
        )
        .expect("reads");
        assert!(all.holds());
        assert_eq!((all.exact, all.unjournalled_present), (3, false));

        let caught = verify(
            &journal,
            present(&[(0, value(0)), (1, value(1)), (2, value(2)), (3, value(3))]),
        )
        .expect("reads");
        assert!(caught.holds());
        assert!(caught.unjournalled_present);

        let lost = verify(&journal, present(&[(0, value(0)), (2, value(1))])).expect("reads");
        assert!(!lost.holds());
        assert_eq!(
            (lost.missing, lost.wrong, lost.exact),
            (vec![1], vec![2], 1)
        );

        let failed = verify(&journal, |_: &[u8]| Err("UNAVAILABLE".to_owned()));
        assert!(failed.is_err());
        // One journalled read fails while every other read answers: still an error.
        let one_failed = verify(&journal, |asked: &[u8]| {
            if asked == key(1).as_bytes() {
                Err("UNAVAILABLE".to_owned())
            } else {
                Ok(None)
            }
        });
        assert!(one_failed.is_err(), "{one_failed:?}");
    }

    #[test]
    fn percentiles_are_nearest_rank() {
        let samples: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        assert_eq!(percentile(&samples, 0.50), Duration::from_millis(50));
        assert_eq!(percentile(&samples, 0.99), Duration::from_millis(99));
        assert_eq!(percentile(&[], 0.99), Duration::ZERO);
    }

    #[test]
    fn arguments_need_a_dir_and_a_control_store() {
        let parsed = |words: &[&str]| parse(words.iter().map(|word| (*word).to_owned()));
        assert!(parsed(&["--control", "127.0.0.1:1"]).is_err());
        assert!(parsed(&["--dir", "D"]).is_err());
        let args = parsed(&[
            "--dir",
            "D",
            "--control",
            "127.0.0.1:1, 127.0.0.1:2",
            "--write",
            "5",
            "--verify",
            "--reopens",
            "3",
        ])
        .expect("parses");
        assert_eq!(args.control, ["127.0.0.1:1", "127.0.0.1:2"]);
        assert_eq!(
            (args.write, args.reopens, args.verify, args.bench),
            (Some(5), 3, true, false)
        );
        assert!(parsed(&["--dir", "D", "--control", "c:1", "--reopens", "x"]).is_err());
        assert_eq!(args.log_dir, PathBuf::from("D.logs"));
    }

    /// The marked span of an example, as embed.md's code block for it must read.
    fn embed_block(example: &str) -> String {
        let example = example.replace("\r\n", "\n");
        let begin = example.find("// embed.md begin\n").expect("begin marker") + 18;
        let end = example.find("// embed.md end\n").expect("end marker");
        format!("```rust\n{}```\n", &example[begin..end])
    }

    const GUIDE: &str = include_str!("../../../docs/rdb/embed.md");

    /// Lead k212: embed.md's "did my write land?" code block must be code that compiles. It is
    /// the marked span of `examples/did_it_land.rs`, which clippy and the build compile; this
    /// row fails when the two differ, so the doc cannot drift from the compiled copy.
    #[test]
    fn the_did_it_land_snippet_in_embed_md_is_the_one_that_compiles() {
        let block = embed_block(include_str!("../examples/did_it_land.rs"));
        assert!(block.contains("fn did_it_land"), "{block}");
        let guide = GUIDE.replace("\r\n", "\n");
        assert!(guide.contains(&block), "embed.md lacks, verbatim:\n{block}");
    }

    /// Tester k156: section 4's sample was checked only by a hand build outside the tree. It is
    /// now the marked span of `examples/embed_sample.rs`, built with the workspace.
    #[test]
    fn the_open_sample_in_embed_md_is_the_one_that_compiles() {
        let block = embed_block(include_str!("../examples/embed_sample.rs"));
        assert!(block.contains("fn main"), "{block}");
        let guide = GUIDE.replace("\r\n", "\n");
        assert!(guide.contains(&block), "embed.md lacks, verbatim:\n{block}");
    }
}
