//! A pre-split (format 1-3) directory, built by folding a real format-4 store back into one
//! database.
//!
//! Every build before format 4 kept the Raft log in the root database. Folding a store this
//! build wrote, rather than assembling a directory by hand, keeps the fixture's bytes the bytes a
//! store really writes: the log entries, the vote and the pointers move into root `raft_log` and
//! `raft_meta` families, `raft-log/` is removed, and the marker is restamped.
//!
//! The log database's `identity` is not carried: a pre-split `raft_meta` never held one.
//! `found` picks what else the older version lacked: below 3 the `dedup` family and the
//! `retired_nodes` key, below 2 the `events` family and the v2 `state_meta` keys.
//!
//! Included with `#[path = "support/pre_split.rs"] mod pre_split;`, so it is not a test target.

#![allow(dead_code)]

use std::path::Path;

use config_storage::{
    log_dir, CF_DEDUP, CF_EVENTS, CF_RAFT_LOG, CF_RAFT_META, CF_STATE_META, COLUMN_FAMILIES,
    LOG_COLUMN_FAMILIES,
};

/// Fold the closed format-4 store at `dir` into a pre-split directory stamped `found`.
pub fn fold_to_pre_split(dir: &Path, found: u32) {
    fold_with(dir, found, |_| {});
}

/// [`fold_to_pre_split`], then `edit` the folded root database before it is closed, to seed
/// a log entry an older build would have written, say.
pub fn fold_with(dir: &Path, found: u32, edit: impl FnOnce(&rocksdb::DB)) {
    let log_path = log_dir(dir);
    let (entries, meta) = {
        let log = rocksdb::DB::open_cf_for_read_only(
            &rocksdb::Options::default(),
            &log_path,
            LOG_COLUMN_FAMILIES,
            false,
        )
        .expect("open the log database to fold it");
        let dump = |cf: &str| -> Vec<(Vec<u8>, Vec<u8>)> {
            let h = log.cf_handle(cf).expect("log family");
            log.iterator_cf(h, rocksdb::IteratorMode::Start)
                .map(|item| {
                    let (k, v) = item.expect("scan the log database");
                    (k.to_vec(), v.to_vec())
                })
                .collect()
        };
        let meta: Vec<_> = dump(CF_RAFT_META)
            .into_iter()
            .filter(|(k, _)| k.as_slice() != b"identity")
            .collect();
        (dump(CF_RAFT_LOG), meta)
    };
    std::fs::remove_dir_all(&log_path).expect("remove raft-log/");

    let mut db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir, COLUMN_FAMILIES)
        .expect("reopen the root database to fold into it");
    let opts = rocksdb::Options::default();
    db.create_cf(CF_RAFT_LOG, &opts)
        .expect("create root raft_log");
    db.create_cf(CF_RAFT_META, &opts)
        .expect("create root raft_meta");
    {
        let log_cf = db.cf_handle(CF_RAFT_LOG).expect("raft_log");
        for (k, v) in &entries {
            db.put_cf(log_cf, k, v).expect("fold a log entry");
        }
        let meta_cf = db.cf_handle(CF_RAFT_META).expect("raft_meta");
        for (k, v) in &meta {
            db.put_cf(meta_cf, k, v).expect("fold a raft_meta key");
        }
        let sm = db.cf_handle(CF_STATE_META).expect("state_meta");
        db.put_cf(sm, b"format_version", found.to_le_bytes())
            .expect("restamp the marker");
        if found < 3 {
            db.delete_cf(sm, b"retired_nodes").expect("drop the v3 key");
        }
        if found < 2 {
            db.delete_cf(sm, b"compact_revision")
                .expect("drop a v2 key");
            db.delete_cf(sm, b"journal_stats").expect("drop a v2 key");
        }
        edit(&db);
    }
    if found < 3 {
        db.drop_cf(CF_DEDUP).expect("drop the dedup family");
    }
    if found < 2 {
        db.drop_cf(CF_EVENTS).expect("drop the journal family");
    }
}

/// Every file under `dir` with its bytes, sorted by path: what "left exactly as it was found"
/// compares. RocksDB's info `LOG` files are left out; a read-only handle may rotate them
/// without touching the data.
pub fn snapshot_files(dir: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(root: &Path, at: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        for entry in std::fs::read_dir(at).expect("read dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                walk(root, &path, out);
                continue;
            }
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            if name == "LOG" || name.starts_with("LOG.old") {
                continue;
            }
            let rel = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .to_string();
            out.push((rel, std::fs::read(&path).expect("read file")));
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}
