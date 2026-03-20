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

use curvine_common::state::{BGOpState, BlockGroupInfo};
use serde::{Deserialize, Serialize};

/// Operator kind: what type of scheduling operation this is
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperatorKind {
    Repair,
    Balance,
    LeaseTransfer,
    DecommissionRepair,
    Delete,
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
}

/// A single step of an operator
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OpStep {
    AddReplica { worker_id: u32 },
    RemoveReplica { worker_id: u32 },
    TransferLease { from_worker: u32, to_worker: u32 },
}

impl OpStep {
    /// Check whether this step is finished based on the actual BG state.
    /// Inspired by TiKV PD's `step.IsFinish(region)` pattern.
    pub fn is_finish(&self, bg: &BlockGroupInfo) -> bool {
        match self {
            OpStep::AddReplica { worker_id } => bg.replica_set.contains(worker_id),
            OpStep::RemoveReplica { worker_id } => !bg.replica_set.contains(worker_id),
            OpStep::TransferLease { to_worker, .. } => {
                bg.lease_owner
                    .as_ref()
                    .map(|l| l.node_id == *to_worker)
                    .unwrap_or(false)
            }
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
    /// Operators with bg_epoch=0 skip epoch-based staleness checks (backward compat).
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
        self.steps
            .push(OpStep::TransferLease { from_worker, to_worker });
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
            OperatorKind::Balance => BGOpState::Rebalancing,
            OperatorKind::LeaseTransfer => BGOpState::LeaseBalancing,
            OperatorKind::Delete => BGOpState::Deleting,
        }
    }
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
    use curvine_common::state::{BGLease, BGState, PlacementPolicy, BG_FLAG_NONE};

    fn make_bg(bg_id: u32, replica_set: Vec<u32>, lease_node: Option<u32>) -> BlockGroupInfo {
        BlockGroupInfo {
            bg_id,
            table_id: 1,
            bg_epoch: 1,
            lease_epoch: 1,
            replica_set,
            state: BGState::Active,
            flags: BG_FLAG_NONE,
            op_state: BGOpState::Idle,
            lease_owner: lease_node.map(|n| BGLease {
                node_id: n,
                expire_time_ms: 0,
            }),
            placement: PlacementPolicy::Default,
            stats: Default::default(),
        }
    }

    // ========== OpStep.is_finish tests ==========

    #[test]
    fn add_replica_is_finish_when_worker_in_replica_set() {
        let bg = make_bg(1, vec![10, 20, 30], None);
        assert!(OpStep::AddReplica { worker_id: 20 }.is_finish(&bg));
    }

    #[test]
    fn add_replica_not_finish_when_worker_absent() {
        let bg = make_bg(1, vec![10, 20], None);
        assert!(!OpStep::AddReplica { worker_id: 30 }.is_finish(&bg));
    }

    #[test]
    fn remove_replica_is_finish_when_worker_absent() {
        let bg = make_bg(1, vec![10, 20], None);
        assert!(OpStep::RemoveReplica { worker_id: 30 }.is_finish(&bg));
    }

    #[test]
    fn remove_replica_not_finish_when_worker_still_present() {
        let bg = make_bg(1, vec![10, 20, 30], None);
        assert!(!OpStep::RemoveReplica { worker_id: 20 }.is_finish(&bg));
    }

    #[test]
    fn transfer_lease_is_finish_when_lease_matches_target() {
        let bg = make_bg(1, vec![10, 20], Some(20));
        assert!(
            OpStep::TransferLease {
                from_worker: 10,
                to_worker: 20
            }
            .is_finish(&bg)
        );
    }

    #[test]
    fn transfer_lease_not_finish_when_lease_still_on_source() {
        let bg = make_bg(1, vec![10, 20], Some(10));
        assert!(
            !OpStep::TransferLease {
                from_worker: 10,
                to_worker: 20
            }
            .is_finish(&bg)
        );
    }

    #[test]
    fn transfer_lease_not_finish_when_no_lease() {
        let bg = make_bg(1, vec![10, 20], None);
        assert!(
            !OpStep::TransferLease {
                from_worker: 10,
                to_worker: 20
            }
            .is_finish(&bg)
        );
    }

    #[test]
    fn add_replica_empty_replica_set() {
        let bg = make_bg(1, vec![], None);
        assert!(!OpStep::AddReplica { worker_id: 1 }.is_finish(&bg));
    }

    #[test]
    fn remove_replica_empty_replica_set() {
        let bg = make_bg(1, vec![], None);
        assert!(OpStep::RemoveReplica { worker_id: 1 }.is_finish(&bg));
    }

    // ========== BGOperator.bg_op_state tests ==========

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

    // ========== OperatorBuilder tests ==========

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
