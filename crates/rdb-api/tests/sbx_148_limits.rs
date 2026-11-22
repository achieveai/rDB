//! Issue #148: fixed size limits with a typed refusal. A value, an object id, a whole byte write
//! and a blob chunk, each at one under, at, and one over its limit, through the public `Db` on a
//! one-partition database. Over the limit is `TOO_LARGE`, `Definitive`, `no_mutation`, with
//! `ApiError::too_large` naming the limit, the size and the maximum, and nothing is written.

use std::sync::Arc;
use std::time::Duration;

use config_core::ConfigStore;
use rdb_api::db::DEFAULT_GROUP as GROUP;
use rdb_api::{
    ApiError, Change, Db, DbConfig, ErrorKind, Limit, RetryRule, Timeouts, TooLarge, TxnPut,
    MAX_OBJECT_ID, MAX_TXN_DEADLINE, MAX_VALUE, MAX_WRITE,
};

/// A fresh one-partition database on an in-memory rEtcd, ready for writes, with the data folder
/// and runtime it needs kept alive beside it.
fn open() -> (Db, impl Sized) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let config = DbConfig {
        failover: None,
        partitions: None,
        ack_floor: true,
        dir: dir.path().to_path_buf(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
        local: rdb_api::db::ALL_NODES.to_vec(),
        links: None,
        client: rdb_api::db::DEFAULT_CLIENT,
        boot: None,
        storage: rdb_api::StorageOptions::default(),
        addresses: None,
    };
    let db = rt
        .block_on(Db::open(config, store, rt.handle().clone()))
        .expect("open");
    db.wait_ready(Duration::from_secs(30)).expect("ready");
    (db, (dir, rt))
}

/// The refusal a call over `limit` gets: typed, definitive, and proof that nothing was written.
#[track_caller]
fn assert_too_large(error: &ApiError, limit: Limit, size: usize, max: usize) {
    assert_eq!(
        (
            error.kind,
            error.retry,
            error.no_mutation,
            error.too_large()
        ),
        (
            ErrorKind::TooLarge,
            RetryRule::Definitive,
            true,
            Some(TooLarge { limit, size, max })
        ),
        "{error}"
    );
    assert!(error.to_string().starts_with("TOO_LARGE ("), "{error}");
}

/// The bytes stored at `object`, or `None`.
fn stored(db: &Db, object: &[u8]) -> Option<usize> {
    let got = db.get_in(GROUP, object).expect("read");
    got.value.map(|(_, bytes)| bytes.len())
}

/// #148 row a. Cut: drop the Db's value check, and the 1,044,481-byte put, which fits today's
/// record, succeeds. Integration (~1-2 s): one database open.
#[config_log::retcd_test]
fn sbx_148_a_value_at_limit_minus_one_limit_and_plus_one() {
    let (mut db, _kept) = open();
    for len in [MAX_VALUE - 1, MAX_VALUE] {
        let value = vec![7_u8; len];
        db.put_in(GROUP, b"v", &value, None)
            .unwrap_or_else(|refused| panic!("a value of {len} bytes is written: {refused}"));
        assert_eq!(stored(&db, b"v"), Some(len), "read back");
    }
    let over = vec![7_u8; MAX_VALUE + 1];
    let refused = db
        .put_in(GROUP, b"w", &over, None)
        .expect_err("over the limit");
    assert_too_large(&refused.error, Limit::Value, MAX_VALUE + 1, MAX_VALUE);
    assert!(refused.request.is_none(), "nothing was sent");
    assert_eq!(stored(&db, b"w"), None, "nothing was written");
    db.shutdown();
}

/// #148 row b. Cut: `limit_refused` left untyped, as before #148, and the refusal carries no
/// `too_large`. Integration (~1 s): one database open.
#[config_log::retcd_test]
fn sbx_148_b_object_id_at_1023_1024_1025() {
    let (mut db, _kept) = open();
    for len in [MAX_OBJECT_ID - 1, MAX_OBJECT_ID] {
        let id = vec![b'k'; len];
        db.put_in(GROUP, &id, b"1", None)
            .unwrap_or_else(|refused| panic!("an id of {len} bytes is written: {refused}"));
        assert_eq!(stored(&db, &id), Some(1));
    }
    let id = vec![b'k'; MAX_OBJECT_ID + 1];
    let refused = db
        .put_in(GROUP, &id, b"1", None)
        .expect_err("over the limit");
    assert_too_large(
        &refused.error,
        Limit::ObjectId,
        MAX_OBJECT_ID + 1,
        MAX_OBJECT_ID,
    );
    let read = db
        .get_in(GROUP, &id)
        .expect_err("a read over the limit too");
    assert_too_large(&read, Limit::ObjectId, MAX_OBJECT_ID + 1, MAX_OBJECT_ID);
    db.shutdown();
}

/// #148 row c: a two-put transaction whose upper-bound sum is one under, at, and one over
/// `MAX_WRITE`. Ids `a` and `b` (no zero bytes), so the sum is `265 + 2 * (1 + 71) + v1 + v2`.
/// Cut: drop the Db's write check, and the exact record, a few bytes under the bound, is
/// admitted by the kernel. Integration (~1-2 s): one database open.
#[config_log::retcd_test]
fn sbx_148_c_txn_write_bound_at_limit_minus_one_limit_and_plus_one() {
    let (mut db, _kept) = open();
    let first = vec![1_u8; MAX_VALUE];
    let fixed = 265 + 2 * (1 + 71) + MAX_VALUE;
    let txn = |bound: usize| {
        let second = vec![2_u8; bound - fixed];
        db.txn(
            &[
                TxnPut {
                    group: GROUP,
                    object: b"a",
                    value: &first,
                    if_version: None,
                },
                TxnPut {
                    group: GROUP,
                    object: b"b",
                    value: &second,
                    if_version: None,
                },
            ],
            MAX_TXN_DEADLINE,
        )
    };
    for bound in [MAX_WRITE - 1, MAX_WRITE] {
        txn(bound).unwrap_or_else(|refused| panic!("a bound of {bound} is written: {refused}"));
        assert_eq!(stored(&db, b"b"), Some(bound - fixed));
    }
    let refused = txn(MAX_WRITE + 1).expect_err("over the limit");
    assert_too_large(&refused.error, Limit::Write, MAX_WRITE + 1, MAX_WRITE);
    assert_eq!(
        stored(&db, b"b"),
        Some(MAX_WRITE - fixed),
        "nothing was written"
    );
    db.shutdown();
}

/// #148 row d. Cut: drop the Db's chunk check, and the refusal comes back from the compile as
/// an untyped `INVALID_ARGUMENT`. Integration (~1-2 s): one database open.
#[config_log::retcd_test]
fn sbx_148_d_chunk_at_max_chunk_minus_one_limit_and_plus_one() {
    const MAX_CHUNK: usize = 1_044_480;
    let (mut db, _kept) = open();
    let chunk = |index: u32, len: usize| Change::Chunk {
        upload: [9; 16],
        index,
        bytes: vec![3_u8; len].into(),
    };
    for (index, len) in [(0, MAX_CHUNK - 1), (1, MAX_CHUNK)] {
        let written = db
            .write_in(GROUP, b"blob", chunk(index, len), None)
            .unwrap_or_else(|refused| panic!("a chunk of {len} bytes is stored: {refused}"));
        assert!(written.is_some(), "chunk {index} is new, so it is sent");
    }
    let refused = db
        .write_in(GROUP, b"blob", chunk(2, MAX_CHUNK + 1), None)
        .expect_err("over the limit");
    assert_too_large(&refused.error, Limit::Chunk, MAX_CHUNK + 1, MAX_CHUNK);
    db.shutdown();
}

/// PR #178 review r1 Q5 (lead ruling): a `write_in` document that is one byte string, the shape
/// a byte put writes, keeps the byte put's value limit: at `MAX_VALUE` it is written, one over
/// it is `TOO_LARGE` with `Limit::Value` and nothing is written. Cut: drop the document check in
/// `write_refusal`, and the 1,044,481-byte document is written and published, since it still
/// fits the 1 MiB record. Integration (~1-2 s): one database open.
#[config_log::retcd_test]
fn sbx_148_e_a_byte_document_write_keeps_the_value_limit() {
    use rdb_value::delta::{Delta, Op};
    use rdb_value::value::Value;
    let doc = |len: usize| Change::Doc(Delta(vec![Op::Replace(Value::Bytes(vec![5_u8; len]))]));
    let (mut db, _kept) = open();
    let written = db
        .write_in(GROUP, b"d", doc(MAX_VALUE), None)
        .unwrap_or_else(|refused| panic!("a document of {MAX_VALUE} bytes is written: {refused}"));
    assert!(written.is_some(), "a new value is sent");
    assert_eq!(stored(&db, b"d"), Some(MAX_VALUE), "read back");
    let refused = db
        .write_in(GROUP, b"e", doc(MAX_VALUE + 1), None)
        .expect_err("over the limit");
    assert_too_large(&refused.error, Limit::Value, MAX_VALUE + 1, MAX_VALUE);
    assert!(refused.request.is_none(), "nothing was sent");
    assert_eq!(stored(&db, b"e"), None, "nothing was written");
    db.shutdown();
}

/// PR #178 review r1 T11: the zero-byte surcharge at the `MAX_WRITE` boundary. Ids `\0a` and
/// `\0b` each count `2 + 1` (a 0x00 counts twice), so the sum is `265 + 2 * (2 + 1 + 71) + v1 +
/// v2`. At one under and at the limit the transaction is written. The over case is sized so the
/// sum without the surcharge is exactly `MAX_WRITE`; with it the sum is `MAX_WRITE + 2`, and it
/// is `TOO_LARGE` with that size. Cut: drop the zero bytes from the sum, and the over case is
/// written and published. Integration (~1-2 s): one database open.
#[config_log::retcd_test]
fn sbx_148_f_zero_bytes_in_ids_push_a_txn_over_max_write() {
    let (mut db, _kept) = open();
    let first = vec![1_u8; MAX_VALUE];
    let fixed = 265 + 2 * (2 + 1 + 71) + MAX_VALUE;
    let txn = |bound: usize| {
        let second = vec![2_u8; bound - fixed];
        db.txn(
            &[
                TxnPut {
                    group: GROUP,
                    object: b"\0a",
                    value: &first,
                    if_version: None,
                },
                TxnPut {
                    group: GROUP,
                    object: b"\0b",
                    value: &second,
                    if_version: None,
                },
            ],
            MAX_TXN_DEADLINE,
        )
    };
    for bound in [MAX_WRITE - 1, MAX_WRITE] {
        txn(bound).unwrap_or_else(|refused| panic!("a bound of {bound} is written: {refused}"));
        assert_eq!(stored(&db, b"\0b"), Some(bound - fixed));
    }
    // Without the surcharge this sum is MAX_WRITE, which the Db would admit.
    let refused = txn(MAX_WRITE + 2).expect_err("the surcharge puts it over");
    assert_too_large(&refused.error, Limit::Write, MAX_WRITE + 2, MAX_WRITE);
    assert_eq!(
        stored(&db, b"\0b"),
        Some(MAX_WRITE - fixed),
        "nothing was written"
    );
    db.shutdown();
}
