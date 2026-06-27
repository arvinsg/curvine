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
fn node_lookup_indexes_return_expected_nodes() {
    let mgr = test_manager();
    let seeds = [
        make_node(1, NodeType::Worker, NodeState::Live),
        make_node(2, NodeType::Worker, NodeState::Live),
        make_node(3, NodeType::Meta, NodeState::Live),
        make_node(4, NodeType::Task, NodeState::Lost),
    ];
    for node in &seeds {
        insert_node(&mgr, node);
    }

    let node = mgr.get_node(1).expect("node should exist");
    assert_eq!(node.base.node_id, 1);
    assert_eq!(node.base.node_type, NodeType::Worker);
    assert_eq!(node.state, NodeState::Live);

    let workers = mgr.get_nodes_by_type(NodeType::Worker);
    assert_eq!(workers.len(), 2);
    assert!(workers.iter().all(|n| n.base.node_type == NodeType::Worker));

    let metas = mgr.get_nodes_by_type(NodeType::Meta);
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].base.node_id, 3);

    let live = mgr.get_nodes_by_state(NodeState::Live);
    assert_eq!(live.len(), 3);
    assert!(live.iter().all(|n| n.state == NodeState::Live));

    let lost = mgr.get_nodes_by_state(NodeState::Lost);
    assert_eq!(lost.len(), 1);
    assert_eq!(lost[0].base.node_id, 4);
}

#[test]
fn restore_loads_from_store() {
    let kv = test_store();

    // Persist a node to the KvStore via the first manager
    let mgr1 = test_manager_with_store(kv.clone());
    let node = make_node(1, NodeType::Worker, NodeState::Live);
    insert_node_persisted(&kv, &node);
    drop(mgr1);

    // Create a fresh manager with the same backing store
    let mgr2 = test_manager_with_store(kv);
    assert!(mgr2.get_node(1).is_none(), "should be empty before restore");

    mgr2.restore().unwrap();

    let restored = mgr2.get_node(1).expect("node should be restored");
    assert_eq!(restored.base.node_id, 1);
    assert_eq!(restored.base.node_type, NodeType::Worker);
}
