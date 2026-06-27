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
use crate::pd::journal::entry::NodeEntry;
use curvine_common::state::{MetaNodePayload, NodeInfo, NodePayload, NodeState, NodeType};

#[derive(Debug, Clone, Copy)]
struct ExpectedNode {
    epoch: u64,
    state: NodeState,
    startup_time_ms: Option<u64>,
}

impl ExpectedNode {
    fn new(epoch: u64, state: NodeState) -> Self {
        Self {
            epoch,
            state,
            startup_time_ms: None,
        }
    }

    fn with_startup_time_ms(mut self, startup_time_ms: u64) -> Self {
        self.startup_time_ms = Some(startup_time_ms);
        self
    }
}

#[test]
fn apply_register_node_cases() {
    struct Case {
        name: &'static str,
        seed: Option<NodeInfo>,
        entry: NodeInfo,
        expected_outcome: ExpectedOutcome,
        expected_node: Option<ExpectedNode>,
    }

    let mut first = make_node(1, NodeType::Worker, NodeState::Starting);
    first.epoch = 1;
    first.base.startup_time_ms = 100;

    let mut lost = make_node(1, NodeType::Worker, NodeState::Lost);
    lost.base.startup_time_ms = 100;
    let mut replace_lost = make_node(1, NodeType::Worker, NodeState::Starting);
    replace_lost.epoch = 2;
    replace_lost.base.startup_time_ms = 100;

    let mut starting = make_node(1, NodeType::Worker, NodeState::Starting);
    starting.base.startup_time_ms = 100;
    let mut replace_starting_equal = make_node(1, NodeType::Worker, NodeState::Starting);
    replace_starting_equal.epoch = 2;
    replace_starting_equal.base.startup_time_ms = 100;

    let mut live = make_node(1, NodeType::Worker, NodeState::Live);
    live.base.startup_time_ms = 100;
    let mut replace_live_equal = make_node(1, NodeType::Worker, NodeState::Starting);
    replace_live_equal.epoch = 2;
    replace_live_equal.base.startup_time_ms = 100;

    let decommission = make_node(1, NodeType::Worker, NodeState::Decommission);
    let mut replace_decommission = make_node(1, NodeType::Worker, NodeState::Starting);
    replace_decommission.epoch = 2;
    replace_decommission.base.startup_time_ms = 200;

    let mut invalid_id = make_node(0, NodeType::Worker, NodeState::Starting);
    invalid_id.epoch = 1;

    let mut mismatch = make_node(2, NodeType::Worker, NodeState::Starting);
    mismatch.payload = NodePayload::Meta(MetaNodePayload::default());

    let mut replay_meta = make_node(3, NodeType::Meta, NodeState::Starting);
    replay_meta.epoch = 4;

    let cases = vec![
        Case {
            name: "first register",
            seed: None,
            entry: first,
            expected_outcome: ExpectedOutcome::Applied,
            expected_node: Some(
                ExpectedNode::new(1, NodeState::Starting).with_startup_time_ms(100),
            ),
        },
        Case {
            name: "replace lost without newer startup",
            seed: Some(lost),
            entry: replace_lost,
            expected_outcome: ExpectedOutcome::Applied,
            expected_node: Some(
                ExpectedNode::new(2, NodeState::Starting).with_startup_time_ms(100),
            ),
        },
        Case {
            name: "reject starting with equal startup",
            seed: Some(starting),
            entry: replace_starting_equal,
            expected_outcome: ExpectedOutcome::Stale,
            expected_node: Some(
                ExpectedNode::new(1, NodeState::Starting).with_startup_time_ms(100),
            ),
        },
        Case {
            name: "reject live with equal startup",
            seed: Some(live),
            entry: replace_live_equal,
            expected_outcome: ExpectedOutcome::Stale,
            expected_node: Some(ExpectedNode::new(1, NodeState::Live).with_startup_time_ms(100)),
        },
        Case {
            name: "reject existing decommission node",
            seed: Some(decommission),
            entry: replace_decommission,
            expected_outcome: ExpectedOutcome::Stale,
            expected_node: Some(ExpectedNode::new(1, NodeState::Decommission)),
        },
        Case {
            name: "reject invalid node id",
            seed: None,
            entry: invalid_id,
            expected_outcome: ExpectedOutcome::Stale,
            expected_node: None,
        },
        Case {
            name: "reject payload mismatch",
            seed: None,
            entry: mismatch,
            expected_outcome: ExpectedOutcome::Stale,
            expected_node: None,
        },
        Case {
            name: "allow replay missing meta with non-initial epoch",
            seed: None,
            entry: replay_meta,
            expected_outcome: ExpectedOutcome::Applied,
            expected_node: Some(ExpectedNode::new(4, NodeState::Starting)),
        },
    ];

    for case in cases {
        let mgr = test_manager();
        if let Some(seed) = &case.seed {
            insert_node(&mgr, seed);
        }
        let node_id = case.entry.base.node_id;
        let outcome = mgr
            .apply_register_node(&NodeEntry {
                op_ms: 20_000,
                info: case.entry,
            })
            .unwrap();
        assert_outcome(outcome, case.expected_outcome);

        match case.expected_node {
            Some(expected) => {
                let node = mgr
                    .get_node(node_id)
                    .unwrap_or_else(|| panic!("{}: expected node {} to exist", case.name, node_id));
                assert_eq!(node.epoch, expected.epoch, "{}", case.name);
                assert_eq!(node.state, expected.state, "{}", case.name);
                if let Some(startup_time_ms) = expected.startup_time_ms {
                    assert_eq!(node.base.startup_time_ms, startup_time_ms, "{}", case.name);
                }
            }
            None => assert!(mgr.get_node(node_id).is_none(), "{}", case.name),
        }
    }
}
