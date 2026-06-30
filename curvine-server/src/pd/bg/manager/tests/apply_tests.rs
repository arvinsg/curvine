use super::fixtures::*;
use crate::pd::bg::BGListScope;
use crate::pd::journal::entry::{BGDeleteEntry, BGIdAllocatorEntry};
use crate::pd::journal::ApplyOutcome;
use curvine_common::state::{BGKind, BGState, BgId};

#[test]
fn create_bg_is_visible_immediately() {
    let mgr = test_manager();
    let outcome = create_bg(&mgr, make_bg(1, 10, vec![100, 101, 102]));

    assert_eq!(outcome, ApplyOutcome::Applied);
    assert!(mgr.get_bg(BGKind::Hash, 1).is_some());
}

#[test]
fn allocate_bg_id_is_cas_guarded() {
    let mgr = test_manager();

    let outcome = mgr
        .apply_allocate_bg_id(&BGIdAllocatorEntry {
            op_ms: 0,
            expected_next_bg_id: 1,
            next_bg_id: 5,
        })
        .unwrap();
    assert_eq!(outcome, ApplyOutcome::Applied);
    assert_eq!(mgr.get_next_bg_id().unwrap(), 5);

    let stale = mgr
        .apply_allocate_bg_id(&BGIdAllocatorEntry {
            op_ms: 1,
            expected_next_bg_id: 1,
            next_bg_id: 9,
        })
        .unwrap();
    assert!(matches!(stale, ApplyOutcome::SkippedStale { .. }));
}

enum UpdateExpectation {
    Applied {
        replica_set: Vec<u32>,
        isr: Vec<u32>,
    },
    Stale,
    Error,
}

struct UpdateCase {
    name: &'static str,
    expected_epoch: u64,
    replica_set: Option<Vec<u32>>,
    state: Option<BGState>,
    expect: UpdateExpectation,
}

#[test]
fn apply_update_bg_cases() {
    let cases = vec![
        UpdateCase {
            name: "replica_set shrink prunes isr and updates worker index",
            expected_epoch: 1,
            replica_set: Some(vec![100, 103]),
            state: None,
            expect: UpdateExpectation::Applied {
                replica_set: vec![100, 103],
                isr: vec![100],
            },
        },
        UpdateCase {
            name: "stale bg epoch is skipped without mutation",
            expected_epoch: 0,
            replica_set: Some(vec![200]),
            state: None,
            expect: UpdateExpectation::Stale,
        },
        UpdateCase {
            name: "hash bg cannot enter sealed state",
            expected_epoch: 1,
            replica_set: None,
            state: Some(BGState::Sealed),
            expect: UpdateExpectation::Error,
        },
    ];

    for case in cases {
        let mgr = test_manager();
        create_bg(&mgr, make_bg(1, 10, vec![100, 101, 102]));

        let mut entry = update_entry(1, case.expected_epoch);
        entry.replica_set = case.replica_set;
        entry.state = case.state;
        let result = mgr.apply_update_bg(&entry);

        match case.expect {
            UpdateExpectation::Applied { replica_set, isr } => {
                assert_eq!(result.unwrap(), ApplyOutcome::Applied, "{}", case.name);
                let bg = mgr.get_bg(BGKind::Hash, 1).unwrap();
                assert_eq!(bg.replica_set, replica_set, "{}", case.name);
                assert_eq!(bg.isr, isr, "{}", case.name);
                assert!(
                    mgr.bgs_on_worker(BGKind::Hash, 101, BGListScope::All)
                        .is_empty(),
                    "{}",
                    case.name
                );
                assert_eq!(
                    mgr.bgs_on_worker(BGKind::Hash, 103, BGListScope::All).len(),
                    1,
                    "{}",
                    case.name
                );
            }
            UpdateExpectation::Stale => {
                assert!(matches!(result.unwrap(), ApplyOutcome::SkippedStale { .. }));
                assert_eq!(
                    mgr.get_bg(BGKind::Hash, 1).unwrap().replica_set,
                    vec![100, 101, 102]
                );
            }
            UpdateExpectation::Error => {
                assert!(result.is_err(), "{}", case.name);
            }
        }
    }
}

#[test]
fn delete_bg_cleans_indexes_and_penalties() {
    let mgr = test_manager();
    create_bg(&mgr, make_bg(1, 10, vec![100, 101]));
    mgr.record_isr_penalty(BGKind::Hash, 1, 101);

    let outcome = mgr
        .apply_delete_bg(&BGDeleteEntry {
            op_ms: 1,
            kind: BGKind::Hash,
            bg_id: 1,
            expected_bg_epoch: 1,
        })
        .unwrap();

    assert_eq!(outcome, ApplyOutcome::Applied);
    assert!(mgr.get_bg(BGKind::Hash, 1).is_none());
    assert!(mgr
        .bgs_on_worker(BGKind::Hash, 100, BGListScope::All)
        .is_empty());
    assert!(!mgr.isr_penalty_active(BGKind::Hash, 1, 101));
}

struct HashStateCase {
    name: &'static str,
    initial_state: BGState,
    target_state: BGState,
    expect_ok: bool,
}

#[test]
fn apply_hash_state_transition_cases() {
    let cases = vec![
        HashStateCase {
            name: "active to degraded is allowed",
            initial_state: BGState::Active,
            target_state: BGState::Degraded,
            expect_ok: true,
        },
        HashStateCase {
            name: "degraded to active is allowed",
            initial_state: BGState::Degraded,
            target_state: BGState::Active,
            expect_ok: true,
        },
        HashStateCase {
            name: "degraded to deleting is allowed",
            initial_state: BGState::Degraded,
            target_state: BGState::Deleting,
            expect_ok: true,
        },
        HashStateCase {
            name: "hash bg cannot be sealed",
            initial_state: BGState::Active,
            target_state: BGState::Sealed,
            expect_ok: false,
        },
    ];

    for case in cases {
        let mgr = test_manager();
        let mut bg = make_bg(1, 10, vec![100]);
        bg.state = case.initial_state;
        create_bg(&mgr, bg);

        let mut entry = update_entry(1, 1);
        entry.state = Some(case.target_state);
        let result = mgr.apply_update_bg(&entry);

        if case.expect_ok {
            assert_eq!(result.unwrap(), ApplyOutcome::Applied, "{}", case.name);
            assert_eq!(
                mgr.get_bg(BGKind::Hash, 1).unwrap().state,
                case.target_state,
                "{}",
                case.name
            );
        } else {
            assert!(result.is_err(), "{}", case.name);
        }
    }
}

#[allow(dead_code)]
fn _assert_bg_id_type(_: BgId) {}

fn make_capacity_bg(
    bg_id: BgId,
    table_id: u16,
    state: BGState,
    replica_set: Vec<u32>,
) -> curvine_common::state::BlockGroupInfo {
    let mut bg = make_bg(bg_id, table_id, replica_set);
    bg.kind = BGKind::Capacity;
    bg.state = state;
    bg
}

#[test]
fn capacity_sealed_bg_keeps_metadata_but_not_runtime_replicas() {
    let mgr = test_manager();
    let bg = make_capacity_bg(10, 20, BGState::Active, vec![100, 101]);
    assert_eq!(create_bg(&mgr, bg), ApplyOutcome::Applied);
    mgr.set_replica_state(
        BGKind::Capacity,
        10,
        100,
        curvine_common::state::ReplicaState::Active,
    );

    let mut entry = update_entry(10, 1);
    entry.kind = BGKind::Capacity;
    entry.state = Some(BGState::Sealed);
    assert_eq!(mgr.apply_update_bg(&entry).unwrap(), ApplyOutcome::Applied);

    let sealed = mgr.get_bg(BGKind::Capacity, 10).unwrap();
    assert_eq!(sealed.state, BGState::Sealed);
    assert_eq!(sealed.replica_set, vec![100, 101]);
    assert!(sealed.replicas.is_empty());
    assert!(mgr
        .bgs_on_worker(BGKind::Capacity, 100, BGListScope::Active)
        .is_empty());
    assert_eq!(
        mgr.bgs_on_worker(BGKind::Capacity, 100, BGListScope::Sealed)
            .len(),
        1
    );
}
