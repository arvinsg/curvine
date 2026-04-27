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
        expected_state: ReplicaState,
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
                expected_state,
            } => bg_manager.get_replica_state(bg.bg_id, *worker_id) == *expected_state,
        }
    }

    /// Whether this step type modifies BG state via Raft propose (increments bg_epoch).
    pub fn modifies_bg(&self) -> bool {
        matches!(
            self,
            OpStep::AddReplica { .. } | OpStep::RemoveReplica { .. } | OpStep::TransferLease { .. }
        )
    }

    /// Whether this step operates on the given worker (either endpoint for TransferLease).
    pub fn involves_worker(&self, worker_id: u32) -> bool {
        match self {
            OpStep::AddReplica { worker_id: w }
            | OpStep::RemoveReplica { worker_id: w }
            | OpStep::WaitReplicaReady { worker_id: w, .. } => *w == worker_id,
            OpStep::TransferLease {
                from_worker,
                to_worker,
            } => *from_worker == worker_id || *to_worker == worker_id,
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

    pub fn wait_replica_ready(mut self, worker_id: u32, expected_state: ReplicaState) -> Self {
        self.steps.push(OpStep::WaitReplicaReady {
            worker_id,
            expected_state,
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
    pub update_bgs: Vec<BlockGroupInfo>,
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
    fn is_finish_cases() {
        let mgr = test_bg_manager();

        // Set up replica states for WaitReplicaReady tests
        mgr.set_replica_state(100, 10, ReplicaState::Active);
        mgr.set_replica_state(101, 10, ReplicaState::Syncing);
        mgr.set_replica_state(102, 10, ReplicaState::Offline);

        let cases: Vec<(&str, OpStep, BlockGroupInfo, bool)> = vec![
            // AddReplica
            ("add: worker in set", OpStep::AddReplica { worker_id: 20 }, make_bg(1, vec![10, 20, 30], None), true),
            ("add: worker absent", OpStep::AddReplica { worker_id: 30 }, make_bg(2, vec![10, 20], None), false),
            ("add: empty set", OpStep::AddReplica { worker_id: 1 }, make_bg(3, vec![], None), false),
            // RemoveReplica
            ("remove: worker absent", OpStep::RemoveReplica { worker_id: 30 }, make_bg(4, vec![10, 20], None), true),
            ("remove: worker present", OpStep::RemoveReplica { worker_id: 20 }, make_bg(5, vec![10, 20, 30], None), false),
            ("remove: empty set", OpStep::RemoveReplica { worker_id: 1 }, make_bg(6, vec![], None), true),
            // TransferLease
            ("lease: matches target", OpStep::TransferLease { from_worker: 10, to_worker: 20 }, make_bg(7, vec![10, 20], Some(20)), true),
            ("lease: still on source", OpStep::TransferLease { from_worker: 10, to_worker: 20 }, make_bg(8, vec![10, 20], Some(10)), false),
            ("lease: no lease", OpStep::TransferLease { from_worker: 10, to_worker: 20 }, make_bg(9, vec![10, 20], None), false),
            // WaitReplicaReady
            ("wait: active=ok", OpStep::WaitReplicaReady { worker_id: 10, expected_state: ReplicaState::Active }, make_bg(100, vec![10], None), true),
            ("wait: syncing!=active", OpStep::WaitReplicaReady { worker_id: 10, expected_state: ReplicaState::Active }, make_bg(101, vec![10], None), false),
            ("wait: offline!=active", OpStep::WaitReplicaReady { worker_id: 10, expected_state: ReplicaState::Active }, make_bg(102, vec![10], None), false),
            ("wait: pending(default)", OpStep::WaitReplicaReady { worker_id: 10, expected_state: ReplicaState::Active }, make_bg(999, vec![10], None), false),
        ];

        for (name, step, bg, expected) in &cases {
            assert_eq!(
                step.is_finish(bg, &mgr),
                *expected,
                "case '{}' failed",
                name
            );
        }
    }

    #[test]
    fn bg_op_state_mapping() {
        let cases: Vec<(OperatorKind, BGOpState)> = vec![
            (OperatorKind::Repair, BGOpState::Recovering),
            (OperatorKind::DecommissionRepair, BGOpState::Recovering),
            (OperatorKind::Balance, BGOpState::Rebalancing),
            (OperatorKind::Rebuild, BGOpState::Rebalancing),
            (OperatorKind::LeaseTransfer, BGOpState::LeaseBalancing),
            (OperatorKind::Delete, BGOpState::Deleting),
        ];
        for (kind, expected) in cases {
            let op = BGOperator {
                id: 1, kind: kind.clone(), bg_id: 1, description: String::new(),
                steps: vec![], current_step: 0, status: OpStatus::Pending,
                create_time_ms: 0, step_start_time_ms: 0, priority: 1, bg_epoch: 0,
            };
            assert_eq!(op.bg_op_state(), expected, "kind {:?}", kind);
        }
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
            .wait_replica_ready(100, ReplicaState::Active)
            .transfer_lease(200, 100)
            .remove_replica(200)
            .priority(120)
            .build();
        assert_eq!(op.steps.len(), 4);
        assert_eq!(op.bg_epoch, 5);
        assert_eq!(op.priority, 120);
        assert_eq!(op.kind, OperatorKind::DecommissionRepair);
    }
}
