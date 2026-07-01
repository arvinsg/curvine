use super::super::*;
use super::fixtures::{request, test_managers};
use crate::pd::bgtable::BGTable;
use curvine_common::state::{make_table_id, CacheTierConfig, StorageType, WriteBufferConfig};

#[test]
fn create_namespace_materializes_hash_table_and_bgs() {
    let (ns_manager, bg_manager, bgtable_manager) = test_managers();
    let entry = ns_manager
        .test_build_create_entry(request("ns1"), 1, 1)
        .unwrap();
    assert_eq!(
        entry.namespace.cache_tier_tables,
        vec![make_table_id(1, 0).unwrap()]
    );
    assert_eq!(entry.tables.len(), 1);
    assert_eq!(entry.bgs.len(), 4);

    let outcome = ns_manager.apply_create_namespace(&entry, true).unwrap();
    assert_eq!(outcome, ApplyOutcome::Applied);

    let ns = ns_manager.get_namespace_by_name("ns1").unwrap();
    let table_id = ns.cache_tier_tables[0];
    assert_eq!(table_id, make_table_id(1, 0).unwrap());
    assert!(bgtable_manager.get_table(table_id).is_some());
    assert_eq!(bg_manager.list_all_bgs().len(), 4);
}

/// Expected `ApplyOutcome` variant for an apply case (only the variant matters).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpectOutcome {
    Applied,
    SkippedNoop,
    SkippedStale,
}

fn assert_outcome(outcome: &ApplyOutcome, expect: ExpectOutcome, name: &str) {
    let ok = matches!(
        (outcome, expect),
        (ApplyOutcome::Applied, ExpectOutcome::Applied)
            | (ApplyOutcome::SkippedNoop, ExpectOutcome::SkippedNoop)
            | (ApplyOutcome::SkippedStale { .. }, ExpectOutcome::SkippedStale)
    );
    assert!(ok, "case '{name}': expected {expect:?}, got {outcome:?}");
}

struct ApplyCase {
    name: &'static str,
    /// Optional namespace id whose slot-0 table is pre-seeded before apply.
    seed_table_ns: Option<NamespaceId>,
    /// namespace_id used to build the entry (2 makes the next-id CAS stale).
    namespace_id: NamespaceId,
    /// Apply the entry twice and assert `expect` on the second apply.
    apply_twice: bool,
    expect: ExpectOutcome,
}

#[test]
fn apply_create_namespace_outcome_cases() {
    let cases = [
        ApplyCase {
            name: "fresh create applies",
            seed_table_ns: None,
            namespace_id: 1,
            apply_twice: false,
            expect: ExpectOutcome::Applied,
        },
        ApplyCase {
            name: "same entry re-apply is idempotent noop",
            seed_table_ns: None,
            namespace_id: 1,
            apply_twice: true,
            expect: ExpectOutcome::SkippedNoop,
        },
        ApplyCase {
            name: "pre-existing table rejects create",
            seed_table_ns: Some(1),
            namespace_id: 1,
            apply_twice: false,
            expect: ExpectOutcome::SkippedStale,
        },
        ApplyCase {
            name: "stale next_namespace_id rejects create",
            seed_table_ns: None,
            namespace_id: 2,
            apply_twice: false,
            expect: ExpectOutcome::SkippedStale,
        },
    ];

    for case in cases {
        let (ns_manager, _bg_manager, bgtable_manager) = test_managers();
        if let Some(ns_id) = case.seed_table_ns {
            let table_id = make_table_id(ns_id, 0).unwrap();
            bgtable_manager.test_insert_table(BGTable::new_hash_table_with_config(
                table_id,
                ns_id,
                StorageType::Ssd,
                3,
                vec![],
                vec![],
                Default::default(),
            ));
        }
        let entry = ns_manager
            .test_build_create_entry(request("ns1"), case.namespace_id, 1)
            .unwrap();
        let outcome = if case.apply_twice {
            ns_manager.apply_create_namespace(&entry, true).unwrap();
            ns_manager.apply_create_namespace(&entry, true).unwrap()
        } else {
            ns_manager.apply_create_namespace(&entry, true).unwrap()
        };
        assert_outcome(&outcome, case.expect, case.name);
    }
}

#[test]
fn create_namespace_does_not_publish_namespace_when_bg_metadata_rejects() {
    let (ns_manager, bg_manager, bgtable_manager) = test_managers();
    let mut entry = ns_manager
        .test_build_create_entry(request("ns1"), 1, 1)
        .unwrap();
    entry.bgs[1].bg_id = entry.bgs[0].bg_id;

    let result = ns_manager.apply_create_namespace(&entry, true);

    assert!(result.is_err());
    assert!(ns_manager.get_namespace_by_name("ns1").is_none());
    assert!(bgtable_manager
        .get_table(entry.namespace.cache_tier_tables[0])
        .is_none());
    assert_eq!(bg_manager.list_all_bgs().len(), 0);
}

#[test]
fn build_create_entry_validation_cases() {
    let (ns_manager, _, _) = test_managers();
    let cases: Vec<(&str, CreateNamespaceRequest)> = vec![
        ("empty name", request("")),
        ("name with space", request("ns 1")),
        ("name with slash", request("ns/1")),
        ("name with invalid symbol", request("ns@1")),
        ("zero block_size", {
            let mut req = request("zero-block");
            req.block_size = 0;
            req
        }),
        ("duplicate pools", {
            let mut req = request("dup-pools");
            req.cache_tier_config = CacheTierConfig {
                pools: vec![StorageType::Ssd, StorageType::Ssd],
                replica_count: 3,
                bucket_count: 4,
                worker_labels: vec![],
            };
            req
        }),
        ("write buffer rejected in phase 1", {
            let mut req = request("with-write-buffer");
            req.write_buffer_config = Some(WriteBufferConfig {
                pool: StorageType::Ssd,
                replica_count: 3,
                capacity_bg_size: 16 << 30,
                min_active_bgs: 1,
                worker_labels: vec![],
            });
            req
        }),
    ];

    for (name, req) in cases {
        let err = ns_manager
            .test_build_create_entry(req, 1, 1)
            .expect_err(&format!("case '{name}' should fail validation"));
        assert!(
            matches!(err, FsError::InvalidArgument(_)),
            "case '{name}' should be InvalidArgument, got {err:?}"
        );
    }
}

#[test]
fn build_create_entry_accepts_valid_names() {
    let (ns_manager, _, _) = test_managers();
    for name in ["ns1", "ns.1", "ns_1", "ns-1", "NS-1.a_b"] {
        assert!(
            ns_manager
                .test_build_create_entry(request(name), 1, 1)
                .is_ok(),
            "name '{name}' should be accepted"
        );
    }
}
