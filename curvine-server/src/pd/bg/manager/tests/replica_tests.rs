use super::fixtures::*;
use curvine_common::state::{BGKind, BGState, ReplicaState, WorkerBGReport};

struct ReplicaReportCase {
    name: &'static str,
    bg: Option<curvine_common::state::BlockGroupInfo>,
    worker_id: u32,
    report: WorkerBGReport,
    initial_state: Option<ReplicaState>,
    expected_changed: usize,
    expected_state: ReplicaState,
}

#[test]
fn apply_replica_reports_observed_state_cases() {
    let cases = vec![
        ReplicaReportCase {
            name: "active report updates in-replica-set worker",
            bg: {
                let mut bg = make_bg(1, 10, vec![100]);
                bg.state = BGState::Active;
                Some(bg)
            },
            worker_id: 100,
            report: WorkerBGReport {
                bg_id: 1,
                state: ReplicaState::Active,
                ..Default::default()
            },
            initial_state: None,
            expected_changed: 1,
            expected_state: ReplicaState::Active,
        },
        ReplicaReportCase {
            name: "same state report is no-op",
            bg: {
                let mut bg = make_bg(1, 10, vec![100]);
                bg.state = BGState::Active;
                Some(bg)
            },
            worker_id: 100,
            report: WorkerBGReport {
                bg_id: 1,
                state: ReplicaState::Active,
                ..Default::default()
            },
            initial_state: Some(ReplicaState::Active),
            expected_changed: 0,
            expected_state: ReplicaState::Active,
        },
        ReplicaReportCase {
            name: "unknown bg report is ignored",
            bg: None,
            worker_id: 100,
            report: WorkerBGReport {
                bg_id: 999,
                state: ReplicaState::Active,
                ..Default::default()
            },
            initial_state: None,
            expected_changed: 0,
            expected_state: ReplicaState::Pending,
        },
        ReplicaReportCase {
            name: "worker outside replica_set is ignored",
            bg: {
                let mut bg = make_bg(1, 10, vec![101]);
                bg.state = BGState::Degraded;
                Some(bg)
            },
            worker_id: 100,
            report: WorkerBGReport {
                bg_id: 1,
                state: ReplicaState::Active,
                ..Default::default()
            },
            initial_state: None,
            expected_changed: 0,
            expected_state: ReplicaState::Pending,
        },
    ];

    for case in cases {
        let mgr = test_manager();
        if let Some(bg) = case.bg {
            create_bg(&mgr, bg);
        }
        if let Some(initial) = case.initial_state {
            mgr.set_replica_state(BGKind::Hash, case.report.bg_id, case.worker_id, initial);
        }

        let changed = mgr
            .apply_replica_reports(case.worker_id, &[case.report.clone()])
            .unwrap();

        assert_eq!(changed, case.expected_changed, "{}", case.name);
        assert_eq!(
            mgr.get_replica_state(BGKind::Hash, case.report.bg_id, case.worker_id),
            case.expected_state,
            "{}",
            case.name
        );
    }
}

struct HashSummaryCase {
    name: &'static str,
    replica_set: Vec<u32>,
    observed: Vec<(u32, ReplicaState)>,
    expected: BGState,
}

#[test]
fn summarize_hash_bg_state_cases() {
    let cases = vec![
        HashSummaryCase {
            name: "all replicas active -> active",
            replica_set: vec![100, 101],
            observed: vec![(100, ReplicaState::Active), (101, ReplicaState::Active)],
            expected: BGState::Active,
        },
        HashSummaryCase {
            name: "missing replica report -> degraded",
            replica_set: vec![100, 101],
            observed: vec![(100, ReplicaState::Active)],
            expected: BGState::Degraded,
        },
        HashSummaryCase {
            name: "syncing replica -> degraded",
            replica_set: vec![100, 101],
            observed: vec![(100, ReplicaState::Active), (101, ReplicaState::Syncing)],
            expected: BGState::Degraded,
        },
        HashSummaryCase {
            name: "empty replica set -> degraded",
            replica_set: vec![],
            observed: vec![],
            expected: BGState::Degraded,
        },
    ];

    for case in cases {
        let mgr = test_manager();
        let bg = make_bg(1, 10, case.replica_set);
        if !bg.replica_set.is_empty() {
            create_bg(&mgr, bg.clone());
        }
        for (worker_id, state) in case.observed {
            mgr.set_replica_state(BGKind::Hash, bg.bg_id, worker_id, state);
        }
        let bg = mgr
            .get_bg(BGKind::Hash, bg.bg_id)
            .map(|bg| (*bg).clone())
            .unwrap_or(bg);

        assert_eq!(summarize_hash_bg_state(&bg), case.expected, "{}", case.name);
    }
}

#[test]
fn summarize_non_hash_or_deleting_state_is_preserved() {
    let mut bg = make_bg(1, 10, vec![100]);
    bg.kind = BGKind::Capacity;
    bg.state = BGState::Sealed;
    assert_eq!(summarize_hash_bg_state(&bg), BGState::Sealed);

    bg.kind = BGKind::Hash;
    bg.state = BGState::Deleting;
    assert_eq!(summarize_hash_bg_state(&bg), BGState::Deleting);
}

struct PenaltyCleanupCase {
    name: &'static str,
    old_replica_set: Vec<u32>,
    old_isr: Vec<u32>,
    new_replica_set: Vec<u32>,
    new_isr: Vec<u32>,
    penalized_worker: u32,
    expect_cleared: bool,
}

#[test]
fn isr_penalty_cleanup_cases() {
    let cases = vec![
        PenaltyCleanupCase {
            name: "rejoining isr clears penalty",
            old_replica_set: vec![100, 101],
            old_isr: vec![100],
            new_replica_set: vec![100, 101],
            new_isr: vec![100, 101],
            penalized_worker: 101,
            expect_cleared: true,
        },
        PenaltyCleanupCase {
            name: "removed replica clears penalty",
            old_replica_set: vec![100, 101],
            old_isr: vec![100],
            new_replica_set: vec![100],
            new_isr: vec![100],
            penalized_worker: 101,
            expect_cleared: true,
        },
        PenaltyCleanupCase {
            name: "unrelated penalty is retained",
            old_replica_set: vec![100, 101, 102],
            old_isr: vec![100],
            new_replica_set: vec![100, 101, 102],
            new_isr: vec![100, 101],
            penalized_worker: 102,
            expect_cleared: false,
        },
    ];

    for case in cases {
        let mgr = test_manager();
        let mut old = make_bg(1, 10, case.old_replica_set);
        old.isr = case.old_isr;
        let mut new = old.clone();
        new.replica_set = case.new_replica_set;
        new.isr = case.new_isr;

        create_bg(&mgr, old.clone());
        mgr.record_isr_penalty(BGKind::Hash, old.bg_id, case.penalized_worker);
        mgr.cleanup_isr_penalties(&old, &new);

        assert_eq!(
            mgr.isr_penalty_active(BGKind::Hash, old.bg_id, case.penalized_worker),
            !case.expect_cleared,
            "{}",
            case.name
        );
    }
}
