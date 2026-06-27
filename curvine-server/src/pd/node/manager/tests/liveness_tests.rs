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
