//! Issue #79: the frozen key map and the `routes/0` codec. These values are a compatibility
//! contract with stored data in applications: a change here is a break, not a test update.

use config_log::retcd_test;
use rdb_core::contracts::digest::{Digest, Domain};
use rdb_core::contracts::ids::{AffinityId, PartitionId, TenantId};
use rdb_core::partition_map::{affinity_hash, partition_of, RoutesRoot, MAX_PARTITIONS};

#[retcd_test]
fn partition_map_frozen_values() {
    // (tenant, affinity, hash, partition at N=2, 64, 512), computed independently of this crate.
    let table: [(u32, u64, u64, u32, u32, u32); 10] = [
        (1, 1, 0x9dca_86b8_6999_d270, 2, 40, 316),
        (1, 2, 0x0037_cd63_a4aa_d9d1, 1, 1, 1),
        (1, 3, 0x08e4_4827_be11_2980, 1, 3, 18),
        (1, 7, 0xf5d6_b142_ccb8_7eda, 2, 62, 492),
        (0, 1, 0x7fe1_f950_41d2_1c2f, 1, 32, 256),
        (0, 2, 0x0dac_6bad_8018_b382, 1, 4, 28),
        (2, 1, 0x3e54_668b_9ff6_fdbf, 1, 16, 125),
        (2, 2, 0xb412_161e_7203_a7d4, 2, 46, 361),
        (u32::MAX, 1, 0xfe77_a6f8_a061_160b, 2, 64, 509),
        (u32::MAX, 2, 0xe6d9_3662_6856_d8ff, 2, 58, 462),
    ];
    for (tenant, affinity, hash, at2, at64, at512) in table {
        let row = format!("tenant {tenant}, group {affinity}");
        let got = affinity_hash(TenantId(tenant), AffinityId(affinity));
        assert_eq!(got, hash, "affinity_hash of {row}");
        assert_eq!(partition_of(got, 2), PartitionId(at2), "{row}");
        assert_eq!(partition_of(got, 64), PartitionId(at64), "{row}");
        assert_eq!(partition_of(got, 512), PartitionId(at512), "{row}");
    }
}

#[retcd_test]
fn a_tenant_is_hashed_as_exactly_eight_bytes() {
    let first_eight = |parts: &[&[u8]]| {
        let digest = Digest::of(Domain::Route, parts);
        u64::from_be_bytes(digest.0[..8].try_into().expect("eight bytes"))
    };
    let group = 1u64.to_le_bytes();
    for tenant in [0u32, 2, u32::MAX] {
        let got = affinity_hash(TenantId(tenant), AffinityId(1));
        let wide = u64::from(tenant).to_le_bytes();
        assert_eq!(got, first_eight(&[&wide, &group]), "tenant {tenant}");
        let narrow = tenant.to_le_bytes();
        assert_ne!(got, first_eight(&[&narrow, &group]), "tenant {tenant}");
    }
}

#[retcd_test]
fn the_ends_of_the_hash_land_in_the_first_and_last_partition() {
    for n in [1, 2, 3, 64, 511, MAX_PARTITIONS] {
        assert_eq!(partition_of(0, n), PartitionId(1), "hash 0 at N={n}");
        assert_eq!(
            partition_of(u64::MAX, n),
            PartitionId(n),
            "hash max at N={n}"
        );
    }
}

#[retcd_test]
fn routes_root_round_trips_and_refuses_what_it_does_not_know() {
    let root = RoutesRoot { partition_count: 2 };
    let body = root.encode();
    assert_eq!(RoutesRoot::decode(&body), Some(root));
    assert_eq!(
        RoutesRoot::decode(
            &RoutesRoot {
                partition_count: MAX_PARTITIONS
            }
            .encode()
        ),
        Some(RoutesRoot {
            partition_count: MAX_PARTITIONS
        })
    );
    let good: Vec<u8> = body.to_vec();
    let with = |at: usize, byte: u8| {
        let mut changed = good.clone();
        changed[at] = byte;
        changed
    };
    assert_eq!(RoutesRoot::decode(&with(0, 2)), None, "unknown layout");
    assert_eq!(
        RoutesRoot::decode(&with(1, 2)),
        None,
        "unknown hash version"
    );
    assert_eq!(RoutesRoot::decode(&good[..5]), None, "short");
    let mut long = good.clone();
    long.push(0);
    assert_eq!(RoutesRoot::decode(&long), None, "long");
    assert_eq!(
        RoutesRoot::decode(&[1, 1, 0, 0, 0, 0]),
        None,
        "a count of 0"
    );
    assert_eq!(
        RoutesRoot::decode(&[1, 1, 1, 2, 0, 0]),
        None,
        "a count of 513"
    );
}
