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
    DeleteNodeEntry, HeartbeatCheckpointEntry, NodePayloadUpdate, UpdateNodeStateEntry,
};
use curvine_common::state::{NodeInfo, NodePayload, NodeState, NodeType};

#[test]
fn apply_update_node_state_cases() {
    struct Case {
        name: &'static str,
        seed: NodeInfo,
        entry: UpdateNodeStateEntry,
        expected_outcome: ExpectedOutcome,
        expected_state: NodeState,
        expected_epoch: u64,
        expected_last_heartbeat_ms: u64,
    }

    let mut live = make_node(1, NodeType::Worker, NodeState::Live);
    live.epoch = 1;
    live.last_heartbeat_ms = 1_000;

    let mut epoch2 = live.clone();
    epoch2.epoch = 2;

    let mut heartbeat_advanced = live.clone();
    heartbeat_advanced.last_heartbeat_ms = 2_000;

    let cases = vec![
        Case {
            name: "live to lost",
            seed: live.clone(),
            entry: update_entry(1, 1, Some(NodeState::Live), None, NodeState::Lost),
            expected_outcome: ExpectedOutcome::Applied,
            expected_state: NodeState::Lost,
            expected_epoch: 1,
            expected_last_heartbeat_ms: 1_000,
        },
        Case {
            name: "reject stale epoch",
            seed: epoch2,
            entry: update_entry(1, 1, Some(NodeState::Live), None, NodeState::Lost),
            expected_outcome: ExpectedOutcome::Stale,
            expected_state: NodeState::Live,
            expected_epoch: 2,
            expected_last_heartbeat_ms: 1_000,
        },
        Case {
            name: "reject stale state",
            seed: live.clone(),
            entry: update_entry(1, 1, Some(NodeState::Lost), None, NodeState::Offline),
            expected_outcome: ExpectedOutcome::Stale,
            expected_state: NodeState::Live,
            expected_epoch: 1,
            expected_last_heartbeat_ms: 1_000,
        },
        Case {
            name: "idempotent already target state",
            seed: live.clone(),
            entry: update_entry(1, 1, Some(NodeState::Lost), None, NodeState::Live),
            expected_outcome: ExpectedOutcome::Noop,
            expected_state: NodeState::Live,
            expected_epoch: 1,
            expected_last_heartbeat_ms: 1_000,
        },
        Case {
            name: "heartbeat fence accepts matching timestamp",
            seed: live.clone(),
            entry: update_entry(1, 1, Some(NodeState::Live), Some(1_000), NodeState::Lost),
            expected_outcome: ExpectedOutcome::Applied,
            expected_state: NodeState::Lost,
            expected_epoch: 1,
            expected_last_heartbeat_ms: 1_000,
        },
        Case {
            name: "heartbeat fence skips advanced timestamp",
            seed: heartbeat_advanced,
            entry: update_entry(1, 1, Some(NodeState::Live), Some(1_000), NodeState::Lost),
            expected_outcome: ExpectedOutcome::Noop,
            expected_state: NodeState::Live,
            expected_epoch: 1,
            expected_last_heartbeat_ms: 2_000,
        },
    ];

    for case in cases {
        let mgr = test_manager();
        insert_node(&mgr, &case.seed);
        let outcome = mgr.apply_update_node_state(&case.entry).unwrap();
        assert_outcome(outcome, case.expected_outcome);
        let updated = mgr.get_node(1).unwrap();
        assert_eq!(updated.state, case.expected_state, "{}", case.name);
        assert_eq!(updated.epoch, case.expected_epoch, "{}", case.name);
        assert_eq!(
            updated.last_heartbeat_ms, case.expected_last_heartbeat_ms,
            "{}",
            case.name
        );
    }
}

#[test]
fn apply_heartbeat_checkpoint_cases() {
    struct Case {
        name: &'static str,
        seed: Option<NodeInfo>,
        entry: HeartbeatCheckpointEntry,
        expected_outcome: ExpectedOutcome,
        expected_last_heartbeat_ms: Option<u64>,
    }

    let mut live = make_node(1, NodeType::Worker, NodeState::Live);
    live.epoch = 1;
    live.last_heartbeat_ms = 10;

    let mut epoch2 = live.clone();
    epoch2.epoch = 2;

    let mut lost = live.clone();
    lost.state = NodeState::Lost;

    let cases = vec![
        Case {
            name: "checkpoint advances heartbeat",
            seed: Some(live.clone()),
            entry: HeartbeatCheckpointEntry {
                op_ms: 20_000,
                node_id: 1,
                expected_epoch: 1,
                expected_state: Some(NodeState::Live),
                last_heartbeat_ms: 20,
            },
            expected_outcome: ExpectedOutcome::Applied,
            expected_last_heartbeat_ms: Some(20),
        },
        Case {
            name: "reject stale epoch",
            seed: Some(epoch2),
            entry: HeartbeatCheckpointEntry {
                op_ms: 20_000,
                node_id: 1,
                expected_epoch: 1,
                expected_state: Some(NodeState::Live),
                last_heartbeat_ms: 999,
            },
            expected_outcome: ExpectedOutcome::Stale,
            expected_last_heartbeat_ms: Some(10),
        },
        Case {
            name: "reject stale state",
            seed: Some(lost),
            entry: HeartbeatCheckpointEntry {
                op_ms: 20_000,
                node_id: 1,
                expected_epoch: 1,
                expected_state: Some(NodeState::Live),
                last_heartbeat_ms: 999,
            },
            expected_outcome: ExpectedOutcome::Stale,
            expected_last_heartbeat_ms: Some(10),
        },
        Case {
            name: "skip older heartbeat",
            seed: Some(live.clone()),
            entry: HeartbeatCheckpointEntry {
                op_ms: 20_000,
                node_id: 1,
                expected_epoch: 1,
                expected_state: Some(NodeState::Live),
                last_heartbeat_ms: 5,
            },
            expected_outcome: ExpectedOutcome::Noop,
            expected_last_heartbeat_ms: Some(10),
        },
        Case {
            name: "missing node",
            seed: None,
            entry: HeartbeatCheckpointEntry {
                op_ms: 20_000,
                node_id: 1,
                expected_epoch: 1,
                expected_state: Some(NodeState::Live),
                last_heartbeat_ms: 20,
            },
            expected_outcome: ExpectedOutcome::NotFound,
            expected_last_heartbeat_ms: None,
        },
    ];

    for case in cases {
        let mgr = test_manager();
        if let Some(seed) = &case.seed {
            insert_node(&mgr, seed);
        }
        let outcome = mgr.apply_heartbeat_checkpoint(&case.entry).unwrap();
        assert_outcome(outcome, case.expected_outcome);
        match case.expected_last_heartbeat_ms {
            Some(expected) => assert_eq!(
                mgr.get_node(1).unwrap().last_heartbeat_ms,
                expected,
                "{}",
                case.name
            ),
            None => assert!(mgr.get_node(1).is_none(), "{}", case.name),
        }
    }
}

#[test]
fn apply_delete_node_cases() {
    struct Case {
        name: &'static str,
        seed: Option<NodeInfo>,
        entry: DeleteNodeEntry,
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
            name: "delete decommission node",
            seed: Some(decommission),
            entry: DeleteNodeEntry {
                op_ms: 20_000,
                node_id: 1,
                expected_epoch: 1,
                expected_state: Some(NodeState::Decommission),
            },
            expected_outcome: ExpectedOutcome::Applied,
            should_exist: false,
        },
        Case {
            name: "reject stale epoch",
            seed: Some(epoch2),
            entry: DeleteNodeEntry {
                op_ms: 20_000,
                node_id: 1,
                expected_epoch: 1,
                expected_state: Some(NodeState::Decommission),
            },
            expected_outcome: ExpectedOutcome::Stale,
            should_exist: true,
        },
        Case {
            name: "reject stale state",
            seed: Some(live),
            entry: DeleteNodeEntry {
                op_ms: 20_000,
                node_id: 1,
                expected_epoch: 1,
                expected_state: Some(NodeState::Decommission),
            },
            expected_outcome: ExpectedOutcome::Stale,
            should_exist: true,
        },
        Case {
            name: "missing node",
            seed: None,
            entry: DeleteNodeEntry {
                op_ms: 20_000,
                node_id: 1,
                expected_epoch: 1,
                expected_state: Some(NodeState::Decommission),
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
        let outcome = mgr.apply_delete_node(&case.entry).unwrap();
        assert_outcome(outcome, case.expected_outcome);
        assert_eq!(
            mgr.get_node(1).is_some(),
            case.should_exist,
            "{}",
            case.name
        );
    }
}

#[test]
fn apply_update_node_state_payload_update_preserves_meta_runtime_stats() {
    let mgr = test_manager();
    let mut node = make_node(1, NodeType::Meta, NodeState::Live);
    node.last_heartbeat_ms = 10;
    if let NodePayload::Meta(ref mut payload) = node.payload {
        payload.group_id = 10;
        payload.group_epoch = 1;
        payload.stats.inode_count = 42;
    }
    insert_node(&mgr, &node);

    let mut new_payload = match node.payload.clone() {
        NodePayload::Meta(payload) => payload,
        _ => unreachable!(),
    };
    new_payload.group_epoch = 2;
    new_payload.stats.inode_count = 0;

    let outcome = mgr
        .apply_update_node_state(&UpdateNodeStateEntry {
            op_ms: 20_000,
            node_id: 1,
            expected_epoch: 1,
            expected_state: Some(NodeState::Live),
            expected_last_heartbeat_ms: None,
            new_state: NodeState::Live,
            state_since_ms: node.state_since_ms,
            last_heartbeat_ms: Some(20_000),
            payload_update: Some(NodePayloadUpdate::Replace(NodePayload::Meta(new_payload))),
        })
        .unwrap();
    assert_outcome(outcome, ExpectedOutcome::Applied);

    let updated = mgr.get_node(1).unwrap();
    assert_eq!(updated.state, NodeState::Live);
    assert_eq!(updated.last_heartbeat_ms, 20_000);
    match updated.payload {
        NodePayload::Meta(payload) => {
            assert_eq!(payload.group_epoch, 2);
            assert_eq!(payload.stats.inode_count, 42);
        }
        _ => panic!("expected meta payload"),
    }
}
