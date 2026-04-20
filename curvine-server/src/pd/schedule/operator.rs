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

use curvine_common::state::{BGOpState, BlockGroupInfo, ReplicaState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::pd::bg::BGManager;

/// Operator kind: what type of scheduling operation this is
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperatorKind {
    Repair,
    Balance,
    LeaseTransfer,
    DecommissionRepair,
    Rebuild,
    Delete,
}

/// Operator class: distinguishes normal traffic from burst (rebuild) traffic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperatorClass {
    Normal,
    Burst,
}

/// Well-known operator priority levels, higher = more urgent.
pub struct OpPriority;
impl OpPriority {
    pub const LEASE_BALANCE: u32 = 40;
    pub const BG_BALANCE: u32 = 50;
    pub const OVER_REPLICA_REPAIR: u32 = 60;
    pub const REBUILD: u32 = 70;
    pub const ASSIGNMENT_SYNC: u32 = 80;
    pub const LEASE_VALIDITY_FIX: u32 = 80;
    pub const PLACEMENT_FIX: u32 = 90;
    pub const UNDER_REPLICA_REPAIR: u32 = 100;
    pub const DECOMMISSION_REPAIR: u32 = 120;
}

/// Operator status
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpStatus {
    Pending,
    Running,
    Success,
    Failed,
    Timeout,
    Cancelled,
    /// Replaced by a higher-priority operator for the same BG.
    Replaced,
}

impl OpStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            OpStatus::Pending => "pending",
            OpStatus::Running => "running",
            OpStatus::Success => "success",
            OpStatus::Failed => "failed",
            OpStatus::Timeout => "timeout",
            OpStatus::Cancelled => "cancelled",
            OpStatus::Replaced => "replaced",
        }
    }
}

/// A single step of an operator
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OpStep {
    AddReplica {
        worker_id: u32,
    },
    RemoveReplica {
        worker_id: u32,
    },
    TransferLease {
        from_worker: u32,
        to_worker: u32,
    },
    WaitReplicaReady {
        worker_id: u32,
        min_state: ReplicaState,
    },
}

impl OpStep {
    /// Check whether this step is finished based on the actual BG state.
    pub fn is_finish(&self, bg: &BlockGroupInfo, bg_manager: &BGManager) -> bool {
        match self {
            OpStep::AddReplica { worker_id } => bg.replica_set.contains(worker_id),
            OpStep::RemoveReplica { worker_id } => !bg.replica_set.contains(worker_id),
            OpStep::TransferLease { to_worker, .. } => bg
                .lease_owner
                .as_ref()
                .map(|l| l.node_id == *to_worker)
                .unwrap_or(false),
            OpStep::WaitReplicaReady {
                worker_id,
                min_state,
            } => bg_manager.get_replica_state(bg.bg_id, *worker_id) >= *min_state,
        }
    }
}

/// Operator: a sequence of steps applied to one BG
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BGOperator {
    pub id: u64,
    pub kind: OperatorKind,
    pub bg_id: u32,
    pub description: String,
    pub steps: Vec<OpStep>,
    pub current_step: usize,
    pub status: OpStatus,
    pub create_time_ms: u64,
    /// Timestamp when the current step started (for per-step timeout calculation)
    pub step_start_time_ms: u64,
    pub priority: u32,
    /// BG epoch at operator creation time, used for stale detection.
    pub bg_epoch: u64,
}

/// Fluent builder for constructing BGOperator instances.
pub struct OperatorBuilder {
    kind: OperatorKind,
    bg_id: u32,
    bg_epoch: u64,
    description: String,
    steps: Vec<OpStep>,
    priority: u32,
}

impl OperatorBuilder {
    pub fn new(kind: OperatorKind, bg_id: u32, description: impl Into<String>) -> Self {
        Self {
            kind,
            bg_id,
            bg_epoch: 0,
            description: description.into(),
            steps: Vec::new(),
            priority: 100,
        }
    }

    pub fn bg_epoch(mut self, epoch: u64) -> Self {
        self.bg_epoch = epoch;
        self
    }

    pub fn add_replica(mut self, worker_id: u32) -> Self {
        self.steps.push(OpStep::AddReplica { worker_id });
        self
    }

    pub fn remove_replica(mut self, worker_id: u32) -> Self {
        self.steps.push(OpStep::RemoveReplica { worker_id });
        self
    }

    pub fn transfer_lease(mut self, from_worker: u32, to_worker: u32) -> Self {
        self.steps.push(OpStep::TransferLease {
            from_worker,
            to_worker,
        });
        self
    }

    pub fn wait_replica_ready(mut self, worker_id: u32, min_state: ReplicaState) -> Self {
        self.steps.push(OpStep::WaitReplicaReady {
            worker_id,
            min_state,
        });
        self
    }

    pub fn priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    pub fn build(self) -> BGOperator {
        let now = orpc::common::LocalTime::mills();
        BGOperator {
            id: 0,
            kind: self.kind,
            bg_id: self.bg_id,
            bg_epoch: self.bg_epoch,
            description: self.description,
            steps: self.steps,
            current_step: 0,
            status: OpStatus::Pending,
            create_time_ms: now,
            step_start_time_ms: now,
            priority: self.priority,
        }
    }
}

impl BGOperator {
    /// Map operator kind to the corresponding BG operation state.
    pub fn bg_op_state(&self) -> BGOpState {
        match self.kind {
            OperatorKind::Repair | OperatorKind::DecommissionRepair => BGOpState::Recovering,
            OperatorKind::Balance | OperatorKind::Rebuild => BGOpState::Rebalancing,
            OperatorKind::LeaseTransfer => BGOpState::LeaseBalancing,
            OperatorKind::Delete => BGOpState::Deleting,
        }
    }

    /// Operator class for rate-limiting purposes.
    pub fn class(&self) -> OperatorClass {
        match self.kind {
            OperatorKind::Rebuild => OperatorClass::Burst,
            _ => OperatorClass::Normal,
        }
    }

    /// Compute the influence of this operator on each worker.
    pub fn compute_influence(&self) -> OpInfluence {
        let mut influence = OpInfluence::default();
        for step in &self.steps {
            match step {
                OpStep::AddReplica { worker_id } => {
                    *influence.bg_count_delta.entry(*worker_id).or_default() += 1;
                }
                OpStep::RemoveReplica { worker_id } => {
                    *influence.bg_count_delta.entry(*worker_id).or_default() -= 1;
                }
                OpStep::TransferLease {
                    from_worker,
                    to_worker,
                } => {
                    *influence
                        .leader_count_delta
                        .entry(*from_worker)
                        .or_default() -= 1;
                    *influence.leader_count_delta.entry(*to_worker).or_default() += 1;
                }
                OpStep::WaitReplicaReady { .. } => {}
            }
        }
        influence
    }
}

/// Tracks the in-flight influence of running operators on each worker.
#[derive(Debug, Clone, Default)]
pub struct OpInfluence {
    /// worker_id -> BG count change (positive = adding, negative = removing).
    pub bg_count_delta: HashMap<u32, i32>,
    /// worker_id -> leader count change.
    pub leader_count_delta: HashMap<u32, i32>,
}

/// Commands for a worker (add/remove BGs), returned via heartbeat response
#[derive(Debug, Clone, Default)]
pub struct BGCommands {
    pub add_bgs: Vec<BlockGroupInfo>,
    pub remove_bgs: Vec<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::state::{BGLease, BGState};

    fn test_bg_manager() -> std::sync::Arc<BGManager> {
        let store: std::sync::Arc<dyn crate::pd::store::KvStore> =
            std::sync::Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new());
        let raft = curvine_common::raft::RaftClient::from_conf(
            curvine_common::conf::JournalConf::default().create_runtime(),
            &curvine_common::conf::JournalConf::default(),
        );
        let jc = std::sync::Arc::new(crate::pd::journal::Client::new(raft));
        let config = std::sync::Arc::new(crate::pd::config::ConfigManager::new(
            store.clone(),
            jc.clone(),
            std::collections::HashMap::new(),
        ));
        let node_store = std::sync::Arc::new(crate::pd::node::NodeStore::new(store.clone()));
        let node_mgr = std::sync::Arc::new(crate::pd::node::NodeManager::new(
            node_store,
            config.clone(),
            jc.clone(),
        ));
        let pool_store = std::sync::Arc::new(crate::pd::pool::PoolStore::new(store.clone()));
        let pool_mgr = std::sync::Arc::new(crate::pd::pool::PoolManager::new(
            pool_store,
            node_mgr,
            jc.clone(),
        ));
        let bg_store = std::sync::Arc::new(crate::pd::bg::BGStore::new(store));
        std::sync::Arc::new(BGManager::new(
            bg_store,
            pool_mgr,
            jc,
            config,
            1024,
            vec![3],
            vec![],
        ))
    }

    fn make_bg(bg_id: u32, replica_set: Vec<u32>, lease_node: Option<u32>) -> BlockGroupInfo {
        BlockGroupInfo {
            bg_id,
            table_id: 1,
            bg_epoch: 1,
            replica_set,
            state: BGState::Active,
            op_state: BGOpState::Idle,
            lease_owner: lease_node.map(|n| BGLease {
                node_id: n,
                epoch: 1,
                grant_time_ms: 0,
            }),
            stats: Default::default(),
        }
    }

    #[test]
    fn add_replica_is_finish_when_worker_in_replica_set() {
        let mgr = test_bg_manager();
        let bg = make_bg(1, vec![10, 20, 30], None);
        assert!(OpStep::AddReplica { worker_id: 20 }.is_finish(&bg, &mgr));
    }

    #[test]
    fn add_replica_not_finish_when_worker_absent() {
        let mgr = test_bg_manager();
        let bg = make_bg(1, vec![10, 20], None);
        assert!(!OpStep::AddReplica { worker_id: 30 }.is_finish(&bg, &mgr));
    }

    #[test]
    fn remove_replica_is_finish_when_worker_absent() {
        let mgr = test_bg_manager();
        let bg = make_bg(1, vec![10, 20], None);
        assert!(OpStep::RemoveReplica { worker_id: 30 }.is_finish(&bg, &mgr));
    }

    #[test]
    fn remove_replica_not_finish_when_worker_still_present() {
        let mgr = test_bg_manager();
        let bg = make_bg(1, vec![10, 20, 30], None);
        assert!(!OpStep::RemoveReplica { worker_id: 20 }.is_finish(&bg, &mgr));
    }

    #[test]
    fn transfer_lease_is_finish_when_lease_matches_target() {
        let mgr = test_bg_manager();
        let bg = make_bg(1, vec![10, 20], Some(20));
        assert!(OpStep::TransferLease {
            from_worker: 10,
            to_worker: 20
        }
        .is_finish(&bg, &mgr));
    }

    #[test]
    fn transfer_lease_not_finish_when_lease_still_on_source() {
        let mgr = test_bg_manager();
        let bg = make_bg(1, vec![10, 20], Some(10));
        assert!(!OpStep::TransferLease {
            from_worker: 10,
            to_worker: 20
        }
        .is_finish(&bg, &mgr));
    }

    #[test]
    fn transfer_lease_not_finish_when_no_lease() {
        let mgr = test_bg_manager();
        let bg = make_bg(1, vec![10, 20], None);
        assert!(!OpStep::TransferLease {
            from_worker: 10,
            to_worker: 20
        }
        .is_finish(&bg, &mgr));
    }

    #[test]
    fn add_replica_empty_replica_set() {
        let mgr = test_bg_manager();
        let bg = make_bg(1, vec![], None);
        assert!(!OpStep::AddReplica { worker_id: 1 }.is_finish(&bg, &mgr));
    }

    #[test]
    fn remove_replica_empty_replica_set() {
        let mgr = test_bg_manager();
        let bg = make_bg(1, vec![], None);
        assert!(OpStep::RemoveReplica { worker_id: 1 }.is_finish(&bg, &mgr));
    }

    #[test]
    fn wait_replica_ready_finish_when_active() {
        let mgr = test_bg_manager();
        let bg = make_bg(1, vec![10], None);
        mgr.set_replica_state(1, 10, ReplicaState::Active);
        assert!(OpStep::WaitReplicaReady {
            worker_id: 10,
            min_state: ReplicaState::Active,
        }
        .is_finish(&bg, &mgr));
    }

    #[test]
    fn wait_replica_ready_not_finish_when_syncing() {
        let mgr = test_bg_manager();
        let bg = make_bg(1, vec![10], None);
        mgr.set_replica_state(1, 10, ReplicaState::Syncing);
        assert!(!OpStep::WaitReplicaReady {
            worker_id: 10,
            min_state: ReplicaState::Active,
        }
        .is_finish(&bg, &mgr));
    }

    #[test]
    fn wait_replica_ready_not_finish_when_pending() {
        let mgr = test_bg_manager();
        let bg = make_bg(1, vec![10], None);
        assert!(!OpStep::WaitReplicaReady {
            worker_id: 10,
            min_state: ReplicaState::Active,
        }
        .is_finish(&bg, &mgr));
    }

    fn make_operator(kind: OperatorKind) -> BGOperator {
        BGOperator {
            id: 1,
            kind,
            bg_id: 1,
            description: "test".to_string(),
            steps: vec![],
            current_step: 0,
            status: OpStatus::Pending,
            create_time_ms: 0,
            step_start_time_ms: 0,
            priority: 1,
            bg_epoch: 0,
        }
    }

    #[test]
    fn bg_op_state_repair() {
        assert_eq!(
            make_operator(OperatorKind::Repair).bg_op_state(),
            BGOpState::Recovering
        );
    }

    #[test]
    fn bg_op_state_decommission_repair() {
        assert_eq!(
            make_operator(OperatorKind::DecommissionRepair).bg_op_state(),
            BGOpState::Recovering
        );
    }

    #[test]
    fn bg_op_state_balance() {
        assert_eq!(
            make_operator(OperatorKind::Balance).bg_op_state(),
            BGOpState::Rebalancing
        );
    }

    #[test]
    fn bg_op_state_lease_transfer() {
        assert_eq!(
            make_operator(OperatorKind::LeaseTransfer).bg_op_state(),
            BGOpState::LeaseBalancing
        );
    }

    #[test]
    fn bg_op_state_delete() {
        assert_eq!(
            make_operator(OperatorKind::Delete).bg_op_state(),
            BGOpState::Deleting
        );
    }

    #[test]
    fn builder_defaults() {
        let op = OperatorBuilder::new(OperatorKind::Repair, 42, "test").build();
        assert_eq!(op.id, 0);
        assert_eq!(op.bg_id, 42);
        assert_eq!(op.status, OpStatus::Pending);
        assert_eq!(op.current_step, 0);
        assert!(op.create_time_ms > 0);
        assert_eq!(op.priority, 100);
        assert_eq!(op.bg_epoch, 0);
        assert!(op.steps.is_empty());
    }

    #[test]
    fn builder_fluent_chain() {
        let op = OperatorBuilder::new(OperatorKind::DecommissionRepair, 10, "decom")
            .bg_epoch(5)
            .add_replica(100)
            .transfer_lease(200, 100)
            .remove_replica(200)
            .priority(120)
            .build();
        assert_eq!(op.steps.len(), 3);
        assert_eq!(op.bg_epoch, 5);
        assert_eq!(op.priority, 120);
        assert_eq!(op.kind, OperatorKind::DecommissionRepair);
        assert_eq!(op.bg_id, 10);
        assert_eq!(op.description, "decom");
    }
}
