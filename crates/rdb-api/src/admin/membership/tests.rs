//! #80 S4 rows for [`super::add_node`] and the amended start check (design `arch-80-s34.md`
//! revision 3, rows M2, M3, M4 and the parts of M6 that need no layout 3). Unit: an in-memory
//! store, each well under a second.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_core::{ConfigStore, ListRequest};
use rdb_core::authority::node::NodeRecord;
use rdb_core::contracts::ids::NodeId;

use super::super::{
    declare, node_start_refusal, read_formation, read_nodes, Declaration, NodeDecl,
};
use super::{add_node, AddNodeError};
use crate::test_store::{Script, Scripted, TlsReported};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime")
}

/// Nodes 1 to `k`, node N at `127.0.0.N:7100` on machine `127.0.0.N`, and 16 partitions.
fn declaration(k: u32) -> Declaration {
    Declaration {
        nodes: (1..=k)
            .map(|n| NodeDecl {
                node: NodeId(n),
                addr: format!("127.0.0.{n}:7100"),
                machine: None,
            })
            .collect(),
        partitions: 16,
    }
}

/// Every key in `store`, with its value and revisions.
fn snapshot(
    rt: &tokio::runtime::Runtime,
    store: &dyn ConfigStore,
) -> Vec<(Bytes, Bytes, u64, u64)> {
    let listed = rt
        .block_on(store.list(ListRequest {
            prefix: Bytes::from_static(b""),
            max_items: 0,
            max_bytes: 0,
        }))
        .expect("the store answers");
    assert!(!listed.truncated, "a whole list");
    listed
        .records
        .into_iter()
        .map(|r| (r.key, r.value, r.create_revision, r.mod_revision))
        .collect()
}

/// A store holding a declaration of nodes 1 to `k`.
fn declared(rt: &tokio::runtime::Runtime, k: u32) -> Arc<dyn ConfigStore> {
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    rt.block_on(declare(&store, &declaration(k)))
        .expect("the rig's declaration");
    store
}

fn refusal(answer: Result<NodeRecord, AddNodeError>) -> String {
    match answer {
        Err(AddNodeError::Refused(why)) => why,
        other => panic!("refused, not {other:?}"),
    }
}

/// M2 (#80 S4, ruling C3: MAX_NODES is 8 and ids are never reused). On a database declared
/// with 5 nodes, three adds take ids 6, 7 and 8, each a layout 1 `nodes/{id}` record at its
/// address with its host as machine; the formation record, the reservation, `routes/0`, the
/// partition records and nodes 1-5's records keep their bytes and revisions (AC9). A 9th add
/// is refused, naming the rule, and the store is unchanged. The half of M2 that removes node 7
/// first waits on layout 3 (T8, T11b).
/// Cut: drop the `highest.0 >= max` check in `add_node`, and the 9th add writes `nodes/9`:
/// red, observed 2026-11-20.
#[test]
fn s80s4_add_node_takes_the_next_id_and_refuses_past_eight() {
    let rt = runtime();
    let store = declared(&rt, 5);
    let before = snapshot(&rt, &*store);
    for n in 6..=8 {
        let added = rt
            .block_on(add_node(&store, &format!("127.0.0.{n}:7100"), None))
            .unwrap_or_else(|e| panic!("add {n}: {e}"));
        assert_eq!(
            added,
            NodeRecord {
                node: NodeId(n),
                addr: format!("127.0.0.{n}:7100"),
                machine: format!("127.0.0.{n}"),
            }
        );
    }
    let records = rt.block_on(read_nodes(&*store)).expect("records");
    assert_eq!(
        records.keys().map(|n| n.0).collect::<Vec<_>>(),
        (1..=8).collect::<Vec<_>>()
    );
    let after = snapshot(&rt, &*store);
    for kept in &before {
        assert!(after.contains(kept), "{:?} kept its bytes", kept.0);
    }
    assert_eq!(
        after.len(),
        before.len() + 3,
        "three nodes/ records, nothing else"
    );
    let formation = rt
        .block_on(read_formation(&*store))
        .expect("read")
        .expect("declared");
    assert_eq!(formation.nodes, (1..=5).map(NodeId).collect::<Vec<_>>());

    let why = refusal(rt.block_on(add_node(&store, "127.0.0.9:7100", None)));
    assert!(why.contains("all 8 node ids are used"), "{why}");
    assert!(why.contains("ids are never reused (ruling C3)"), "{why}");
    assert!(why.ends_with("nothing was written"), "{why}");
    assert_eq!(snapshot(&rt, &*store), after, "the 9th add wrote nothing");
}

/// M3 (#80 S4): `add_node` refuses, writing nothing, a node on a recorded node's machine (the
/// same host on another port, or the same label), a bad address or label, a store with no
/// declaration, and a node on another machine over a plaintext control link (#80 S2 ruling 1).
/// The same remote node is added under mutual TLS. The half of M3 that reuses a removed node's
/// machine waits on layout 3 (T8, T11b).
/// Cut: skip `machine_clash` in `add_node`, and the shared-host add writes `nodes/6`: red,
/// observed 2026-11-20.
#[test]
fn s80s4_add_node_refuses_a_shared_host_label_or_plaintext_remote_and_writes_nothing() {
    let rt = runtime();
    let store = declared(&rt, 5);
    let before = snapshot(&rt, &*store);
    for (what, addr, machine, names) in [
        (
            "a recorded node's host on another port",
            "127.0.0.2:7200",
            None,
            vec!["nodes 2 (127.0.0.2:7100) and 6 (127.0.0.2:7200) are on one machine"],
        ),
        (
            "a recorded node's machine label",
            "127.0.0.6:7100",
            Some("127.0.0.3"),
            vec!["nodes 3 and 6 are both on machine \"127.0.0.3\""],
        ),
        (
            "an address with no port",
            "127.0.0.6",
            None,
            vec!["node 6: "],
        ),
        (
            "a label that is not one",
            "127.0.0.6:7100",
            Some("two words"),
            vec!["node 6: "],
        ),
        (
            "another machine over plaintext",
            "10.1.2.6:7100",
            None,
            vec![
                "node 6's address 10.1.2.6:7100 is on another machine",
                "control store link is not TLS",
            ],
        ),
    ] {
        let why = refusal(rt.block_on(add_node(&store, addr, machine)));
        for name in names {
            assert!(why.contains(name), "{what}: {name:?} in {why}");
        }
        assert!(why.ends_with("nothing was written"), "{what}: {why}");
        assert_eq!(
            snapshot(&rt, &*store),
            before,
            "{what}: nothing was written"
        );
    }

    let empty: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let why = refusal(rt.block_on(add_node(&empty, "127.0.0.6:7100", None)));
    assert!(why.contains("no database is declared"), "{why}");
    assert!(snapshot(&rt, &*empty).is_empty(), "nothing was written");

    let tls: Arc<dyn ConfigStore> = TlsReported::new(Arc::clone(&store));
    let added = rt
        .block_on(add_node(&tls, "10.1.2.6:7100", Some("Host-Six")))
        .expect("a remote node over mutual TLS");
    assert_eq!(
        (added.node, added.machine.as_str()),
        (NodeId(6), "host-six")
    );
}

/// M4 (#80 S4): two adds at once both read nodes 1-5 and both ask for id 6. The create is
/// create-only, so exactly one gets id 6 and the other is refused, "run add-node again", having
/// written nothing; run again, it gets id 7. A create whose reply is lost adopts its own bytes.
/// Cut: make the create unconditional (`expected_mod_revision: None`), and both adds answer
/// id 6, the second overwriting the first: red, observed 2026-11-20.
#[test]
fn s80s4_concurrent_add_nodes_take_distinct_ids() {
    let rt = runtime();
    let scripted = Arc::new(Scripted::gated_with(Script {
        pass_gate: u32::MAX,
        ..Script::default()
    }));
    let store: Arc<dyn ConfigStore> = scripted.clone();
    rt.block_on(declare(&store, &declaration(5)))
        .expect("the rig's declaration");
    // From here every put parks at the gate until released.
    scripted.rescript(Script::default());
    let arrived = |what: &str| {
        rt.block_on(async {
            tokio::time::timeout(Duration::from_secs(10), scripted.put_arrived.notified())
                .await
                .unwrap_or_else(|_| panic!("{what}'s create parks"));
        });
    };
    let a = {
        let store = Arc::clone(&store);
        rt.spawn(async move { add_node(&store, "127.0.0.6:7100", None).await })
    };
    arrived("a");
    let b = {
        let store = Arc::clone(&store);
        rt.spawn(async move { add_node(&store, "127.0.0.7:7100", None).await })
    };
    arrived("b");
    scripted.release();
    scripted.release();
    let a = rt.block_on(a).expect("a joins");
    let b = rt.block_on(b).expect("b joins");
    let (won, lost) = match (a, b) {
        (Ok(won), lost @ Err(_)) | (lost @ Err(_), Ok(won)) => (won, lost),
        (a, b) => panic!("exactly one add takes id 6: {a:?} {b:?}"),
    };
    assert_eq!(won.node, NodeId(6));
    let why = refusal(lost);
    assert!(why.contains("id 6 was just taken by another add"), "{why}");
    assert!(why.contains("run add-node again"), "{why}");
    let records = rt.block_on(read_nodes(&*store)).expect("records");
    assert_eq!(records.len(), 6, "the loser wrote nothing");
    assert_eq!(records[&NodeId(6)], won);

    let loser_addr = if won.addr == "127.0.0.6:7100" {
        "127.0.0.7:7100"
    } else {
        "127.0.0.6:7100"
    };
    scripted.rescript(Script {
        pass_gate: u32::MAX,
        lose_put_replies: 1,
        only_prefix: Some(b"nodes/".to_vec()),
        ..Script::default()
    });
    let again = rt
        .block_on(add_node(&store, loser_addr, None))
        .expect("run again after a lost reply, it adopts its own create");
    assert_eq!(again.node, NodeId(7));
    let records = rt.block_on(read_nodes(&*store)).expect("records");
    assert_eq!(records[&NodeId(7)], again);
}

/// M6, the parts that need no layout 3 (#80 S4, ADR-rdb-0025): the start check takes the
/// declared set from the records, so node 6, added at runtime and absent from the formation
/// record, starts; a node may leave out of `--peers` any node with a higher id, which dials it,
/// but never one with a lower id, which it dials. Every entry it gives is still checked: one
/// naming a node with no record is refused, and so is a node with no record of its own. The
/// removed-peer skip and the removed-own-id refusal wait on layout 3 (T8, T11b).
/// Cut: put back the old rule, every declared node but this one needs a peer address, and
/// node 3 with no entry for 4-6 is refused: red, observed 2026-11-20.
#[test]
fn s80s4_start_check_accepts_an_added_node_and_a_missing_higher_peer() {
    let rt = runtime();
    let store = declared(&rt, 5);
    rt.block_on(add_node(&store, "127.0.0.6:7100", None))
        .expect("node 6");
    let formation = rt
        .block_on(read_formation(&*store))
        .expect("read")
        .expect("declared");
    let records = rt.block_on(read_nodes(&*store)).expect("records");
    let peers = |ids: &[u32]| -> BTreeMap<NodeId, String> {
        ids.iter()
            .map(|n| (NodeId(*n), format!("127.0.0.{n}:7100")))
            .collect()
    };
    let start = |node: u32, given: &BTreeMap<NodeId, String>| {
        node_start_refusal(
            NodeId(node),
            &format!("127.0.0.{node}:7100"),
            given,
            Some(&formation),
            &records,
        )
    };
    assert_eq!(start(3, &peers(&[1, 2])), None, "4, 5 and 6 dial node 3");
    assert_eq!(
        start(3, &peers(&[1, 2, 3, 4, 5])),
        None,
        "6 added after --peers was written"
    );
    assert_eq!(
        start(6, &peers(&[1, 2, 3, 4, 5])),
        None,
        "node 6 is in the records only"
    );
    assert_eq!(start(1, &BTreeMap::new()), None, "node 1 dials nobody");
    for (what, node, given, names) in [
        (
            "node 6 without a lower node",
            6,
            peers(&[1, 2, 3, 4]),
            vec![
                "no peer address for node 5",
                "nodes/5",
                "node 6 dials every node with a lower id",
            ],
        ),
        (
            "a given peer with no record",
            3,
            peers(&[1, 2, 7]),
            vec![
                "peer address 7=127.0.0.7:7100",
                "nodes/7",
                "[1, 2, 3, 4, 5, 6]",
            ],
        ),
        (
            "a node with no record",
            7,
            peers(&[1, 2, 3, 4, 5, 6]),
            vec!["node 7 is not declared", "[1, 2, 3, 4, 5, 6]"],
        ),
    ] {
        let Some(text) = start(node, &given) else {
            panic!("{what}: refused");
        };
        for name in names {
            assert!(text.contains(name), "{what}: {name:?} in {text}");
        }
        assert!(text.ends_with("nothing was started"), "{what}: {text}");
    }
}

fn remove_refusal(answer: Result<NodeRecord, super::RemoveNodeError>) -> String {
    match answer {
        Err(super::RemoveNodeError::Refused(why)) => why,
        other => panic!("refused, not {other:?}"),
    }
}

/// M5 (#80 S4, N-2 and S-2). On a database declared with 5 nodes:
/// - node 2, which the generation-0 records pin, is refused "run admin drain 2";
/// - node 7, never added, is refused as not a live member;
/// - node 6, added and pinned by nothing, is removed: `nodes/6` becomes layout 3 at node 6's
///   address and machine, and every other key keeps its bytes and revisions;
/// - node 6 again is refused as already removed, and the next add takes id 7.
///
/// On a database declared with 3 nodes, node 3 is refused: 2 distinct live machines would be
/// left. Every refusal writes nothing. Unit.
///
/// Cut: drop the pin check, so node 2 is written removed ("refused, not Ok"), red, observed
/// 2026-11-21.
#[test]
fn s80s4_remove_node_writes_layout_3_once_and_refuses_a_pinned_node_and_too_few_machines() {
    let rt = runtime();
    let store = declared(&rt, 5);
    let six = rt
        .block_on(add_node(&store, "127.0.0.6:7100", None))
        .expect("node 6 is added");
    let before = snapshot(&rt, &*store);

    let why = remove_refusal(rt.block_on(super::remove_node(&store, NodeId(2))));
    assert!(why.contains("run admin drain 2"), "{why}");
    let why = remove_refusal(rt.block_on(super::remove_node(&store, NodeId(7))));
    assert!(why.contains("node 7 is not a live member"), "{why}");
    assert_eq!(snapshot(&rt, &*store), before, "a refusal writes nothing");

    let removed = rt
        .block_on(super::remove_node(&store, NodeId(6)))
        .expect("node 6 is removed");
    assert_eq!(removed, six);
    let after = snapshot(&rt, &*store);
    let key = Bytes::from(rdb_core::contracts::control::ControlKey::Node(NodeId(6)).encode());
    for (k, v, created, modified) in &before {
        let now = after
            .iter()
            .find(|(key, ..)| key == k)
            .expect("still there");
        if *k == key {
            assert_eq!(
                NodeRecord::decode_removed(&now.1),
                Some((six.clone(), None)),
                "nodes/6 is layout 3"
            );
            assert_eq!(now.2, *created);
        } else {
            assert_eq!(
                (&now.1, now.2, now.3),
                (v, *created, *modified),
                "{k:?} kept"
            );
        }
    }
    assert_eq!(after.len(), before.len());

    let why = remove_refusal(rt.block_on(super::remove_node(&store, NodeId(6))));
    assert!(why.contains("node 6 is already removed"), "{why}");
    let seven = rt
        .block_on(add_node(&store, "127.0.0.7:7100", None))
        .expect("an add after the remove");
    assert_eq!(seven.node, NodeId(7), "the removed id 6 is not reused");

    let three = declared(&rt, 3);
    let before = snapshot(&rt, &*three);
    let why = remove_refusal(rt.block_on(super::remove_node(&three, NodeId(3))));
    assert!(why.contains("leaves 2 distinct live machines"), "{why}");
    assert_eq!(snapshot(&rt, &*three), before, "a refusal writes nothing");
}
