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
use curvine_common::state::{NodeState, NodeType};

#[test]
fn detect_heartbeat_timeout_ignores_non_live() {
    let mgr = test_manager();
    let mut node = make_node(1, NodeType::Worker, NodeState::Lost);
    node.last_heartbeat_ms = 1000;
    insert_node(&mgr, &node);

    let timed_out = mgr.detect_heartbeat_timeout(20_000, 5_000);
    assert!(timed_out.is_empty());

    let updated = mgr.get_node(1).unwrap();
    assert_eq!(updated.state, NodeState::Lost);
}

#[test]
fn offline_promotion_guard_cases() {
    struct Case {
        name: &'static str,
        states: Vec<NodeState>,
        expired_count: usize,
        expect_skip: bool,
    }

    let cases = vec![
        Case {
            name: "disabled below minimum node count",
            states: vec![NodeState::Lost; 9],
            expired_count: 9,
            expect_skip: false,
        },
        Case {
            name: "skip when promotion ratio exceeds default 35 percent",
            states: recoverable_states(4, 6),
            expired_count: 4,
            expect_skip: true,
        },
        Case {
            name: "allow when promotion ratio is below default 35 percent",
            states: recoverable_states(3, 7),
            expired_count: 3,
            expect_skip: false,
        },
        Case {
            name: "allow when promotion ratio equals default 35 percent",
            states: recoverable_states(7, 13),
            expired_count: 7,
            expect_skip: false,
        },
    ];

    for case in cases {
        let mgr = test_manager();
        insert_nodes(&mgr, &case.states);
        assert_eq!(
            mgr.should_skip_offline_promotion(case.expired_count),
            case.expect_skip,
            "{}",
            case.name
        );
    }
}

fn recoverable_states(lost: usize, live: usize) -> Vec<NodeState> {
    let mut states = vec![NodeState::Lost; lost];
    states.extend(std::iter::repeat(NodeState::Live).take(live));
    states
}

fn insert_nodes(mgr: &super::super::NodeManager, states: &[NodeState]) {
    for (index, state) in states.iter().enumerate() {
        let node = make_node((index + 1) as u32, NodeType::Worker, *state);
        insert_node(mgr, &node);
    }
}
