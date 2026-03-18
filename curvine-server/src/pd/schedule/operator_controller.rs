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

use super::operator::{BGCommands, BGOperator, OpStatus, OpStep};
use crate::pd::bg::BGManager;
use crate::pd::config::ConfigManager;
use dashmap::DashMap;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

/// Wrapper to make BinaryHeap a max-heap by priority then by oldest first
struct PriorityOperator(BGOperator);

impl PartialEq for PriorityOperator {
    fn eq(&self, other: &Self) -> bool {
        self.0.priority == other.0.priority && self.0.create_time_ms == other.0.create_time_ms
    }
}

impl Eq for PriorityOperator {}

impl PartialOrd for PriorityOperator {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriorityOperator {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.0.priority.cmp(&other.0.priority) {
            Ordering::Equal => other.0.create_time_ms.cmp(&self.0.create_time_ms),
            o => o,
        }
    }
}

pub struct OperatorController {
    waiting_operators: Mutex<BinaryHeap<PriorityOperator>>,
    running_operators: DashMap<u32, BGOperator>,
    config_manager: Arc<ConfigManager>,
    bg_manager: Arc<BGManager>,
    next_op_id: AtomicU64,
}

impl OperatorController {
    pub fn new(config_manager: Arc<ConfigManager>, bg_manager: Arc<BGManager>) -> Self {
        Self {
            waiting_operators: Mutex::new(BinaryHeap::new()),
            running_operators: DashMap::new(),
            config_manager,
            bg_manager,
            next_op_id: AtomicU64::new(1),
        }
    }

    pub fn add_operator(&self, op: BGOperator) -> bool {
        let max_waiting = self
            .config_manager
            .get_u32("pd.schedule.max_waiting_operators", 100);

        let mut queue = self.waiting_operators.lock().unwrap();
        if self.running_operators.contains_key(&op.bg_id) {
            return false;
        }
        if queue.len() >= max_waiting as usize {
            return false;
        }
        queue.push(PriorityOperator(op));
        true
    }

    /// Dispatch next batch of operators from waiting to running; returns those dispatched.
    pub fn dispatch_next(&self) -> Vec<BGOperator> {
        let mut queue = self.waiting_operators.lock().unwrap();
        let mut to_dispatch = Vec::new();
        while let Some(PriorityOperator(op)) = queue.pop() {
            if self.running_operators.contains_key(&op.bg_id) {
                continue;
            }
            let mut op = op;
            op.status = OpStatus::Running;
            self.running_operators.insert(op.bg_id, op.clone());
            to_dispatch.push(op);
        }
        to_dispatch
    }

    /// Build commands for a worker from running operators (add_bgs / remove_bgs).
    pub fn dispatch_to_worker(&self, worker_id: u32) -> BGCommands {
        let mut add_bgs = Vec::new();
        let mut remove_bgs = Vec::new();
        for mut entry in self.running_operators.iter_mut() {
            let op = entry.value_mut();
            let step = match op.steps.get(op.current_step) {
                Some(s) => s,
                None => continue,
            };
            match step {
                OpStep::AddReplica { worker_id: w } if *w == worker_id => {
                    if let Some(bg) = self.bg_manager.get_bg(op.bg_id) {
                        add_bgs.push(bg);
                    }
                }
                OpStep::RemoveReplica { worker_id: w } if *w == worker_id => {
                    remove_bgs.push(op.bg_id);
                }
                _ => {}
            }
        }
        BGCommands { add_bgs, remove_bgs }
    }

    /// Check progress and timeouts; remove completed or timed-out operators.
    pub fn check_progress(&self, now_ms: u64) {
        let timeout_ms = self
            .config_manager
            .get_u64("pd.schedule.operator_timeout_ms", 600_000);

        let mut to_remove = Vec::new();
        for mut entry in self.running_operators.iter_mut() {
            let op = entry.value_mut();
            if now_ms.saturating_sub(op.create_time_ms) > timeout_ms {
                op.status = OpStatus::Timeout;
                to_remove.push(op.bg_id);
                continue;
            }
            if op.current_step >= op.steps.len() {
                op.status = OpStatus::Success;
                to_remove.push(op.bg_id);
            }
        }
        for bg_id in to_remove {
            self.running_operators.remove(&bg_id);
        }
    }

    pub fn running_count(&self) -> usize {
        self.running_operators.len()
    }

    pub fn waiting_count(&self) -> usize {
        self.waiting_operators.lock().unwrap().len()
    }

    /// Generate next operator id
    pub fn next_operator_id(&self) -> u64 {
        self.next_op_id.fetch_add(1, AtomicOrdering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::node::NodeManager;
    use crate::pd::pool::PoolManager;

    fn test_controller() -> (OperatorController, Arc<ConfigManager>, Arc<BGManager>) {
        let store: Arc<dyn crate::pd::store::KvStore> =
            Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new());
        let raft = curvine_common::raft::RaftClient::from_conf(
            curvine_common::conf::JournalConf::default().create_runtime(),
            &curvine_common::conf::JournalConf::default(),
        );
        let jc = Arc::new(crate::pd::journal::Client::new(raft));
        let config = Arc::new(ConfigManager::new(
            store.clone(),
            jc.clone(),
            std::collections::HashMap::new(),
        ));
        let node_store = Arc::new(crate::pd::node::NodeStore::new(store.clone()));
        let node_mgr = Arc::new(NodeManager::new(node_store, config.clone(), jc.clone()));
        let pool_store = Arc::new(crate::pd::pool::PoolStore::new(store.clone()));
        let pool_mgr = Arc::new(PoolManager::new(pool_store, node_mgr, jc.clone()));
        let bg_store = Arc::new(crate::pd::bg::BGStore::new(store));
        let bg_mgr = Arc::new(BGManager::new(bg_store, pool_mgr, jc));
        let ctrl = OperatorController::new(config.clone(), bg_mgr.clone());
        (ctrl, config, bg_mgr)
    }

    #[test]
    fn add_and_dispatch_operator() {
        let (ctrl, _config, _bg_mgr) = test_controller();
        let op = BGOperator {
            id: 1,
            bg_id: 10,
            description: "test".to_string(),
            steps: vec![],
            current_step: 0,
            status: OpStatus::Pending,
            create_time_ms: 0,
            priority: 1,
        };
        assert!(ctrl.add_operator(op));
        assert_eq!(ctrl.waiting_count(), 1);
        let dispatched = ctrl.dispatch_next();
        assert_eq!(dispatched.len(), 1);
        assert_eq!(dispatched[0].bg_id, 10);
        assert_eq!(ctrl.waiting_count(), 0);
        assert_eq!(ctrl.running_count(), 1);
    }

    #[test]
    fn duplicate_bg_id_not_added_when_running() {
        let (ctrl, _config, _bg_mgr) = test_controller();
        let op = BGOperator {
            id: 1,
            bg_id: 10,
            description: "test".to_string(),
            steps: vec![],
            current_step: 0,
            status: OpStatus::Pending,
            create_time_ms: 0,
            priority: 1,
        };
        ctrl.add_operator(op.clone());
        ctrl.dispatch_next();
        assert!(!ctrl.add_operator(op));
    }
}
