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

    /// P2.5: how much this step is expected to advance bg_epoch when it
    /// successfully applies via Raft. Used by `OperatorController::check_progress`
    /// to detect external mutations:
    ///
    ///   delta = bg.bg_epoch - op.origin_bg_epoch
    ///   consumed = sum of epoch_consumed() for steps[0..=current]
    ///   if delta > consumed → someone else mutated this BG → cancel + reschedule
    ///
    /// Mirrors TiKV PD's `Step::ConfVerChanged` (§14.2).
    pub fn epoch_consumed(&self) -> u64 {
        match self {
            // AddReplica / RemoveReplica / TransferLease each propose a single
            // BGUpdateEntry that bumps bg_epoch by 1.
            OpStep::AddReplica { .. }
            | OpStep::RemoveReplica { .. }
            | OpStep::TransferLease { .. } => 1,
            // WaitReplicaReady is a passive wait; no Raft entry, no epoch change.
            OpStep::WaitReplicaReady { .. } => 0,
        }
    }

    /// #3-C: Semantic safety check — does this step's precondition still
    /// hold against the current BG snapshot? Returns `Err(reason)` if the
    /// step would be unsafe or meaningless to execute; the operator
    /// controller cancels operators that fail this check.
    ///
    /// Distinct from:
    /// - `is_finish`: "has the effect already been achieved?"
    /// - epoch-delta stale detection: "did an external mutation advance bg_epoch
    ///   beyond what our step budget can explain?"
    ///
    /// `check_safety` catches cases where the step's preconditions are broken
    /// even without an epoch delta — e.g. a TransferLease whose target was
    /// removed out-of-band (replica_set shrunk but epoch attribution happens
    /// to balance). Mirrors TiKV PD's `OpStep::check_safety` (§14.2).
    pub fn check_safety(&self, bg: &BlockGroupInfo) -> Result<(), String> {
        match self {
            // AddReplica: no precondition beyond "not already added". If the
            // worker is already in the set, `is_finish` handles completion;
            // if not, the propose CAS handles racing adds.
            OpStep::AddReplica { .. } => Ok(()),

            // RemoveReplica: unsafe to remove the current lease owner — the
            // lease must be transferred first. `is_finish` handles the
            // "already removed" case.
            OpStep::RemoveReplica { worker_id } => {
                if bg.replica_set.contains(worker_id) {
                    if let Some(lease) = &bg.lease_owner {
                        if lease.node_id == *worker_id {
                            return Err(format!(
                                "cannot remove worker {}: it is the current lease owner (epoch {})",
                                worker_id, lease.epoch
                            ));
                        }
                    }
                }
                Ok(())
            }

            // TransferLease:
            // - target must be an actual replica; otherwise the lease would
            //   point at a node that doesn't hold the data
            // - the current lease must still live at `from_worker` OR have
            //   already reached `to_worker` (in which case `is_finish`
            //   completes us). Any other holder means someone else moved
            //   the lease — continuing would overwrite their decision.
            OpStep::TransferLease {
                from_worker,
                to_worker,
            } => {
                if !bg.replica_set.contains(to_worker) {
                    return Err(format!(
                        "cannot transfer lease to worker {}: not in replica_set {:?}",
                        to_worker, bg.replica_set
                    ));
                }
                if let Some(lease) = &bg.lease_owner {
                    if lease.node_id != *from_worker && lease.node_id != *to_worker {
                        return Err(format!(
                            "cannot transfer lease from worker {}: lease is held by worker {}",
                            from_worker, lease.node_id
                        ));
                    }
                }
                Ok(())
            }

            // WaitReplicaReady: passive wait. If we're waiting for an active
            // state the worker must still be a replica; Lost/Offline can
            // legitimately apply to a worker no longer in replica_set, so
            // we don't treat absence as unsafe there.
            OpStep::WaitReplicaReady {
                worker_id,
                expected_state,
            } => {
                let requires_membership = matches!(
                    expected_state,
                    ReplicaState::Active | ReplicaState::Syncing | ReplicaState::Pending
                );
                if requires_membership && !bg.replica_set.contains(worker_id) {
                    return Err(format!(
                        "cannot wait for worker {} to reach {:?}: not in replica_set {:?}",
                        worker_id, expected_state, bg.replica_set
                    ));
                }
                Ok(())
            }
        }
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
    /// P2.5: BG epoch observed at operator creation time. Replaces the
    /// pre-P2.5 `bg_epoch` field, which was used for in-place "white-stealing"
    /// (`op.bg_epoch += 1` on is_finish). The new contract: this stays
    /// constant across the operator's lifetime; check_progress compares
    /// `bg.bg_epoch - origin_bg_epoch` against `sum(steps.epoch_consumed())`.
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
        let pool_mgr = std::sync::Arc::new(crate::pd::pool::PoolManager::new(node_mgr));
        let bg_store = std::sync::Arc::new(crate::pd::bg::BGStore::new(store));
        let bg_mgr = std::sync::Arc::new(BGManager::new(
            bg_store,
            pool_mgr,
            jc,
            config,
            1024,
            vec![3],
            vec![],
        ));
        bg_mgr.test_disable_route_publish();
        bg_mgr
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
            (
                "add: worker in set",
                OpStep::AddReplica { worker_id: 20 },
                make_bg(1, vec![10, 20, 30], None),
                true,
            ),
            (
                "add: worker absent",
                OpStep::AddReplica { worker_id: 30 },
                make_bg(2, vec![10, 20], None),
                false,
            ),
            (
                "add: empty set",
                OpStep::AddReplica { worker_id: 1 },
                make_bg(3, vec![], None),
                false,
            ),
            // RemoveReplica
            (
                "remove: worker absent",
                OpStep::RemoveReplica { worker_id: 30 },
                make_bg(4, vec![10, 20], None),
                true,
            ),
            (
                "remove: worker present",
                OpStep::RemoveReplica { worker_id: 20 },
                make_bg(5, vec![10, 20, 30], None),
                false,
            ),
            (
                "remove: empty set",
                OpStep::RemoveReplica { worker_id: 1 },
                make_bg(6, vec![], None),
                true,
            ),
            // TransferLease
            (
                "lease: matches target",
                OpStep::TransferLease {
                    from_worker: 10,
                    to_worker: 20,
                },
                make_bg(7, vec![10, 20], Some(20)),
                true,
            ),
            (
                "lease: still on source",
                OpStep::TransferLease {
                    from_worker: 10,
                    to_worker: 20,
                },
                make_bg(8, vec![10, 20], Some(10)),
                false,
            ),
            (
                "lease: no lease",
                OpStep::TransferLease {
                    from_worker: 10,
                    to_worker: 20,
                },
                make_bg(9, vec![10, 20], None),
                false,
            ),
            // WaitReplicaReady
            (
                "wait: active=ok",
                OpStep::WaitReplicaReady {
                    worker_id: 10,
                    expected_state: ReplicaState::Active,
                },
                make_bg(100, vec![10], None),
                true,
            ),
            (
                "wait: syncing!=active",
                OpStep::WaitReplicaReady {
                    worker_id: 10,
                    expected_state: ReplicaState::Active,
                },
                make_bg(101, vec![10], None),
                false,
            ),
            (
                "wait: offline!=active",
                OpStep::WaitReplicaReady {
                    worker_id: 10,
                    expected_state: ReplicaState::Active,
                },
                make_bg(102, vec![10], None),
                false,
            ),
            (
                "wait: pending(default)",
                OpStep::WaitReplicaReady {
                    worker_id: 10,
                    expected_state: ReplicaState::Active,
                },
                make_bg(999, vec![10], None),
                false,
            ),
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
                id: 1,
                kind: kind.clone(),
                bg_id: 1,
                description: String::new(),
                steps: vec![],
                current_step: 0,
                status: OpStatus::Pending,
                create_time_ms: 0,
                step_start_time_ms: 0,
                priority: 1,
                bg_epoch: 0,
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
    fn check_safety_cases() {
        // AddReplica is always safe — the propose-time CAS handles races.
        assert!(OpStep::AddReplica { worker_id: 99 }
            .check_safety(&make_bg(1, vec![1, 2], Some(1)))
            .is_ok());

        // RemoveReplica: unsafe if the worker is the current lease owner.
        assert!(OpStep::RemoveReplica { worker_id: 1 }
            .check_safety(&make_bg(2, vec![1, 2], Some(1)))
            .is_err());
        // RemoveReplica: safe if the worker holds no lease.
        assert!(OpStep::RemoveReplica { worker_id: 2 }
            .check_safety(&make_bg(3, vec![1, 2], Some(1)))
            .is_ok());
        // RemoveReplica: worker already gone → still safe (is_finish completes).
        assert!(OpStep::RemoveReplica { worker_id: 99 }
            .check_safety(&make_bg(4, vec![1, 2], Some(1)))
            .is_ok());

        // TransferLease: target not in replica_set → unsafe.
        assert!(OpStep::TransferLease {
            from_worker: 1,
            to_worker: 99,
        }
        .check_safety(&make_bg(5, vec![1, 2], Some(1)))
        .is_err());
        // TransferLease: lease held by a third party → unsafe.
        assert!(OpStep::TransferLease {
            from_worker: 1,
            to_worker: 2,
        }
        .check_safety(&make_bg(6, vec![1, 2, 3], Some(3)))
        .is_err());
        // TransferLease: lease already at target → safe (is_finish completes).
        assert!(OpStep::TransferLease {
            from_worker: 1,
            to_worker: 2,
        }
        .check_safety(&make_bg(7, vec![1, 2], Some(2)))
        .is_ok());
        // TransferLease: normal path, lease at from_worker, target in set.
        assert!(OpStep::TransferLease {
            from_worker: 1,
            to_worker: 2,
        }
        .check_safety(&make_bg(8, vec![1, 2], Some(1)))
        .is_ok());

        // WaitReplicaReady for Active: worker absent from replica_set → unsafe.
        assert!(OpStep::WaitReplicaReady {
            worker_id: 99,
            expected_state: ReplicaState::Active,
        }
        .check_safety(&make_bg(9, vec![1, 2], Some(1)))
        .is_err());
        // WaitReplicaReady for Offline: absence is legitimate → safe.
        assert!(OpStep::WaitReplicaReady {
            worker_id: 99,
            expected_state: ReplicaState::Offline,
        }
        .check_safety(&make_bg(10, vec![1, 2], Some(1)))
        .is_ok());
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
