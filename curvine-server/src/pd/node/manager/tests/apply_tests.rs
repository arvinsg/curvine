// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::fixtures::*;
use crate::pd::journal::entry::{
    MetaNodePayloadPatch, NodePayloadPatch, RemoveNodeEntry, UpdateNodePayloadEntry,
    UpdateNodeStatusEntry,
};
use curvine_common::state::{NodeInfo, NodePayload, NodeState, NodeType, PeerInfo, RwPolicy};

#[test]
fn apply_update_node_status_cases() {
    struct Case {
        name: &'static str,
        seed: Option<NodeInfo>,
        entry: UpdateNodeStatusEntry,
        expected_outcome: ExpectedOutcome,
        expected_state: Option<NodeState>,
        expected_last_heartbeat_ms: Option<u64>,
    }

    let mut live = make_node(1, NodeType::Worker, NodeState::Live);
    live.epoch = 1;
    live.last_heartbeat_ms = 1_000;

    let mut epoch2 = live.clone();
    epoch2.epoch = 2;

    let mut fresh_heartbeat = live.clone();
    fresh_heartbeat.last_heartbeat_ms = 30_000;

    let mut old_heartbeat = live.clone();
    old_heartbeat.last_heartbeat_ms = 10;

    let cases = vec![
        Case {
            name: "live to lost when heartbeat expired",
            seed: Some(live.clone()),
            entry: status_entry(80_000, 1, 1, NodeState::Live, Some(NodeState::Lost), None),
            expected_outcome: ExpectedOutcome::Applied,
            expected_state: Some(NodeState::Lost),
            expected_last_heartbeat_ms: Some(1_000),
        },
        Case {
            name: "skip live to lost when heartbeat is still fresh at apply",
            seed: Some(fresh_heartbeat),
            entry: status_entry(80_000, 1, 1, NodeState::Live, Some(NodeState::Lost), None),
            expected_outcome: ExpectedOutcome::Noop,
            expected_state: Some(NodeState::Live),
            expected_last_heartbeat_ms: Some(30_000),
        },
        Case {
            name: "reject stale epoch",
            seed: Some(epoch2),
            entry: status_entry(80_000, 1, 1, NodeState::Live, Some(NodeState::Lost), None),
            expected_outcome: ExpectedOutcome::Stale,
            expected_state: Some(NodeState::Live),
            expected_last_heartbeat_ms: Some(1_000),
        },
        Case {
            name: "reject stale state",
            seed: Some(live.clone()),
            entry: status_entry(
                20_000,
                1,
                1,
                NodeState::Lost,
                Some(NodeState::Offline),
                None,
            ),
            expected_outcome: ExpectedOutcome::Stale,
            expected_state: Some(NodeState::Live),
            expected_last_heartbeat_ms: Some(1_000),
        },
        Case {
            name: "idempotent already target state",
            seed: Some(live.clone()),
            entry: status_entry(20_000, 1, 1, NodeState::Lost, Some(NodeState::Live), None),
            expected_outcome: ExpectedOutcome::Noop,
            expected_state: Some(NodeState::Live),
            expected_last_heartbeat_ms: Some(1_000),
        },
        Case {
            name: "heartbeat only advances monotonically",
            seed: Some(old_heartbeat.clone()),
            entry: status_entry(20_000, 1, 1, NodeState::Live, None, Some(20)),
            expected_outcome: ExpectedOutcome::Applied,
            expected_state: Some(NodeState::Live),
            expected_last_heartbeat_ms: Some(20),
        },
        Case {
            name: "heartbeat only skips older timestamp",
            seed: Some(old_heartbeat),
            entry: status_entry(20_000, 1, 1, NodeState::Live, None, Some(5)),
            expected_outcome: ExpectedOutcome::Noop,
            expected_state: Some(NodeState::Live),
            expected_last_heartbeat_ms: Some(10),
        },
        Case {
            name: "missing node",
            seed: None,
            entry: status_entry(20_000, 1, 1, NodeState::Live, None, Some(20)),
            expected_outcome: ExpectedOutcome::NotFound,
            expected_state: None,
            expected_last_heartbeat_ms: None,
        },
    ];

    for case in cases {
        let mgr = test_manager();
        if let Some(seed) = &case.seed {
            insert_node(&mgr, seed);
        }
        let outcome = mgr.apply_update_node_status(&case.entry).unwrap();
        assert_outcome(outcome, case.expected_outcome);
        match case.expected_state {
            Some(expected_state) => {
                let updated = mgr.get_node(1).unwrap();
                assert_eq!(updated.state, expected_state, "{}", case.name);
                assert_eq!(
                    updated.last_heartbeat_ms,
                    case.expected_last_heartbeat_ms.unwrap(),
                    "{}",
                    case.name
                );
            }
            None => assert!(mgr.get_node(1).is_none(), "{}", case.name),
        }
    }
}

#[test]
fn apply_update_node_payload_meta_cases() {
    struct Case {
        name: &'static str,
        seed: NodeInfo,
        patch: MetaNodePayloadPatch,
        expected_outcome: ExpectedOutcome,
        expected_group_epoch: u64,
        expected_stats_inode_count: u64,
    }

    let peer = PeerInfo {
        node_id: 1,
        address: make_node(1, NodeType::Meta, NodeState::Live).base.address,
        is_leader: Some(true),
    };

    let mut base = make_node(1, NodeType::Meta, NodeState::Live);
    base.epoch = 1;
    if let NodePayload::Meta(ref mut payload) = base.payload {
        payload.group_id = 10;
        payload.group_epoch = 1;
        payload.peers = vec![peer.clone()];
        payload.rw_policy = RwPolicy::LeaderOnly;
        payload.stats.inode_count = 42;
    }

    let mut worker = make_node(1, NodeType::Worker, NodeState::Live);
    worker.epoch = 1;

    let cases = vec![
        Case {
            name: "apply newer meta payload",
            seed: base.clone(),
            patch: MetaNodePayloadPatch {
                group_id: 10,
                group_epoch: 2,
                peers: vec![peer.clone()],
                rw_policy: RwPolicy::LeaderWriteFollowerRead,
            },
            expected_outcome: ExpectedOutcome::Applied,
            expected_group_epoch: 2,
            expected_stats_inode_count: 42,
        },
        Case {
            name: "skip older meta payload",
            seed: base.clone(),
            patch: MetaNodePayloadPatch {
                group_id: 10,
                group_epoch: 0,
                peers: vec![peer.clone()],
                rw_policy: RwPolicy::LeaderOnly,
            },
            expected_outcome: ExpectedOutcome::Noop,
            expected_group_epoch: 1,
            expected_stats_inode_count: 42,
        },
        Case {
            name: "skip identical meta payload",
            seed: base.clone(),
            patch: MetaNodePayloadPatch {
                group_id: 10,
                group_epoch: 1,
                peers: vec![peer.clone()],
                rw_policy: RwPolicy::LeaderOnly,
            },
            expected_outcome: ExpectedOutcome::Noop,
            expected_group_epoch: 1,
            expected_stats_inode_count: 42,
        },
        Case {
            name: "reject same epoch with changed content",
            seed: base.clone(),
            patch: MetaNodePayloadPatch {
                group_id: 10,
                group_epoch: 1,
                peers: vec![peer.clone()],
                rw_policy: RwPolicy::LeaderWriteFollowerRead,
            },
            expected_outcome: ExpectedOutcome::Stale,
            expected_group_epoch: 1,
            expected_stats_inode_count: 42,
        },
        Case {
            name: "reject group mismatch",
            seed: base.clone(),
            patch: MetaNodePayloadPatch {
                group_id: 11,
                group_epoch: 2,
                peers: vec![peer.clone()],
                rw_policy: RwPolicy::LeaderOnly,
            },
            expected_outcome: ExpectedOutcome::Stale,
            expected_group_epoch: 1,
            expected_stats_inode_count: 42,
        },
        Case {
            name: "reject payload type mismatch",
            seed: worker,
            patch: MetaNodePayloadPatch {
                group_id: 10,
                group_epoch: 2,
                peers: vec![peer],
                rw_policy: RwPolicy::LeaderOnly,
            },
            expected_outcome: ExpectedOutcome::Stale,
            expected_group_epoch: 0,
            expected_stats_inode_count: 0,
        },
    ];

    for case in cases {
        let mgr = test_manager();
        insert_node(&mgr, &case.seed);
        let outcome = mgr
            .apply_update_node_payload(&UpdateNodePayloadEntry {
                op_ms: 20_000,
                node_id: 1,
                expected_epoch: 1,
                expected_state: NodeState::Live,
                patch: NodePayloadPatch::Meta(case.patch),
            })
            .unwrap();
        assert_outcome(outcome, case.expected_outcome);
        let updated = mgr.get_node(1).unwrap();
        if let NodePayload::Meta(payload) = updated.payload {
            assert_eq!(
                payload.group_epoch, case.expected_group_epoch,
                "{}",
                case.name
            );
            assert_eq!(
                payload.stats.inode_count, case.expected_stats_inode_count,
                "{}",
                case.name
            );
        }
    }
}

#[test]
fn apply_remove_node_cases() {
    struct Case {
        name: &'static str,
        seed: Option<NodeInfo>,
        entry: RemoveNodeEntry,
        expected_outcome: ExpectedOutcome,
        should_exist: bool,
    }

    let mut decommission = make_node(1, NodeType::Worker, NodeState::Decommission);
    decommission.epoch = 1;
    let mut epoch2 = decommission.clone();
    epoch2.epoch = 2;
    let live = make_node(1, NodeType::Worker, NodeState::Live);

    let cases = vec![
        Case {
            name: "remove decommission node",
            seed: Some(decommission),
            entry: RemoveNodeEntry {
                op_ms: 20_000,
                node_id: 1,
                expected_epoch: 1,
                expected_state: NodeState::Decommission,
            },
            expected_outcome: ExpectedOutcome::Applied,
            should_exist: false,
        },
        Case {
            name: "reject stale epoch",
            seed: Some(epoch2),
            entry: RemoveNodeEntry {
                op_ms: 20_000,
                node_id: 1,
                expected_epoch: 1,
                expected_state: NodeState::Decommission,
            },
            expected_outcome: ExpectedOutcome::Stale,
            should_exist: true,
        },
        Case {
            name: "reject stale state",
            seed: Some(live),
            entry: RemoveNodeEntry {
                op_ms: 20_000,
                node_id: 1,
                expected_epoch: 1,
                expected_state: NodeState::Decommission,
            },
            expected_outcome: ExpectedOutcome::Stale,
            should_exist: true,
        },
        Case {
            name: "missing node",
            seed: None,
            entry: RemoveNodeEntry {
                op_ms: 20_000,
                node_id: 1,
                expected_epoch: 1,
                expected_state: NodeState::Decommission,
            },
            expected_outcome: ExpectedOutcome::NotFound,
            should_exist: false,
        },
    ];

    for case in cases {
        let mgr = test_manager();
        if let Some(seed) = &case.seed {
            insert_node(&mgr, seed);
        }
        let outcome = mgr.apply_remove_node(&case.entry).unwrap();
        assert_outcome(outcome, case.expected_outcome);
        assert_eq!(
            mgr.get_node(1).is_some(),
            case.should_exist,
            "{}",
            case.name
        );
    }
}
