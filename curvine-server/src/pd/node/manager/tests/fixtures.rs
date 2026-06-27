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

use super::super::NodeManager;
use crate::pd::journal::entry::UpdateNodeStateEntry;
use crate::pd::journal::ApplyOutcome;
use curvine_common::state::{
    MetaNodePayload, NodeAddress, NodeBase, NodeInfo, NodePayload, NodeState, NodeType,
    TaskNodePayload, WorkerNodePayload,
};
use std::sync::Arc;

pub(super) fn test_store() -> Arc<dyn crate::pd::store::KvStore> {
    Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new())
}

pub(super) fn test_manager_with_store(store: Arc<dyn crate::pd::store::KvStore>) -> NodeManager {
    crate::pd::pd_server::init_metrics_for_test();
    let node_store = Arc::new(crate::pd::node::NodeStore::new(store.clone()));
    let raft = curvine_common::raft::RaftClient::from_conf(
        curvine_common::conf::JournalConf::default().create_runtime(),
        &curvine_common::conf::JournalConf::default(),
    );
    let jc = Arc::new(crate::pd::journal::Client::new(raft));
    let config = Arc::new(crate::pd::config::ConfigManager::new(
        store,
        jc.clone(),
        std::collections::HashMap::new(),
    ));
    NodeManager::new(node_store, config, jc)
}

pub(super) fn test_manager() -> NodeManager {
    test_manager_with_store(test_store())
}

pub(super) fn make_node(id: u32, node_type: NodeType, state: NodeState) -> NodeInfo {
    let payload = match node_type {
        NodeType::Worker => NodePayload::Worker(WorkerNodePayload::default()),
        NodeType::Meta => NodePayload::Meta(MetaNodePayload::default()),
        NodeType::Task => NodePayload::Task(TaskNodePayload::default()),
    };
    NodeInfo {
        base: NodeBase {
            node_id: id,
            node_type,
            address: NodeAddress {
                hostname: format!("host-{}", id),
                ip: format!("10.0.0.{}", id),
                rpc_port: 8000 + id as u16,
                web_port: 9000 + id as u16,
            },
            ..Default::default()
        },
        state,
        epoch: 1,
        last_heartbeat_ms: orpc::common::LocalTime::mills(),
        state_since_ms: orpc::common::LocalTime::mills(),
        last_persist_ms: 0,
        sys_stats: Default::default(),
        payload,
    }
}

pub(super) fn insert_node(mgr: &NodeManager, node: &NodeInfo) {
    mgr.test_insert_node(node.clone());
}

pub(super) fn insert_node_persisted(kv: &Arc<dyn crate::pd::store::KvStore>, node: &NodeInfo) {
    crate::pd::node::NodeStore::new(kv.clone())
        .put(node)
        .expect("persist node");
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ExpectedOutcome {
    Applied,
    Noop,
    Stale,
    NotFound,
}

pub(super) fn assert_outcome(actual: ApplyOutcome, expected: ExpectedOutcome) {
    match (actual, expected) {
        (ApplyOutcome::Applied, ExpectedOutcome::Applied) => {}
        (ApplyOutcome::SkippedNoop, ExpectedOutcome::Noop) => {}
        (ApplyOutcome::SkippedStale { .. }, ExpectedOutcome::Stale) => {}
        (ApplyOutcome::NotFound { .. }, ExpectedOutcome::NotFound) => {}
        (actual, expected) => {
            panic!("unexpected outcome: actual={actual:?}, expected={expected:?}")
        }
    }
}

pub(super) fn update_entry(
    node_id: u32,
    expected_epoch: u64,
    expected_state: Option<NodeState>,
    expected_last_heartbeat_ms: Option<u64>,
    new_state: NodeState,
) -> UpdateNodeStateEntry {
    UpdateNodeStateEntry {
        op_ms: 20_000,
        node_id,
        expected_epoch,
        expected_state,
        expected_last_heartbeat_ms,
        new_state,
        state_since_ms: 20_000,
        last_heartbeat_ms: None,
        payload_update: None,
    }
}
