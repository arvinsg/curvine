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

use crate::pd::schedule::{BGOperator, ManagerContext, OpPriority, OperatorBuilder, OperatorKind};
use curvine_common::state::{BlockGroupInfo, ReplicaState};

pub struct LeaseValidityChecker;

impl LeaseValidityChecker {
    fn is_lease_invalid(bg: &BlockGroupInfo, ctx: &ManagerContext) -> bool {
        match bg.lease_owner.as_ref() {
            None => true,
            Some(lease) => {
                !bg.replica_set.contains(&lease.node_id)
                    || !ctx.pool_manager.is_worker_available(lease.node_id)
                    || ctx.bg_manager.get_replica_state(bg.bg_id, lease.node_id)
                        == ReplicaState::Offline
            }
        }
    }

    fn build_lease_transfer(bg: &BlockGroupInfo, ctx: &ManagerContext) -> Option<BGOperator> {
        let old_worker = bg.lease_owner.as_ref().map(|l| l.node_id).unwrap_or(0);
        let lease_counts = ctx.bg_manager.get_worker_lease_counts();

        // Select from Active replicas, excluding the current (invalid) owner
        let serving = ctx.bg_manager.get_serving_replicas(bg.bg_id);
        let candidates: Vec<u32> = serving.into_iter().filter(|&w| w != old_worker).collect();

        let new_owner = candidates
            .iter()
            .min_by_key(|&&w| (lease_counts.get(&w).copied().unwrap_or(0), w))
            .copied()?;

        Some(
            OperatorBuilder::new(
                OperatorKind::LeaseTransfer,
                bg.bg_id,
                format!(
                    "Transfer lease from {} to {} (validity fix)",
                    old_worker, new_owner
                ),
            )
            .bg_epoch(bg.bg_epoch)
            .transfer_lease(old_worker, new_owner)
            .priority(OpPriority::LEASE_VALIDITY_FIX)
            .build(),
        )
    }
}

impl super::Checker for LeaseValidityChecker {
    fn name(&self) -> &str {
        "lease-validity-checker"
    }

    fn check_bg(&self, bg: &BlockGroupInfo, ctx: &ManagerContext) -> Option<BGOperator> {
        if !Self::is_lease_invalid(bg, ctx) {
            return None;
        }
        Self::build_lease_transfer(bg, ctx)
    }

    fn priority(&self) -> u32 {
        super::CheckerPriority::LEASE_VALIDITY
    }
}

#[cfg(test)]
mod tests {
    use super::super::CheckerPriority;
    use super::*;
    use crate::pd::journal::BGEntry;
    use crate::pd::pool::POOL_ID_SSD;
    use crate::pd::schedule::checker::tests_common::{decompose, Fixture};
    use crate::pd::schedule::checker::Checker;
    use curvine_common::state::{BGLease, BGOpState, BGState, NodeState};

    #[test]
    fn name_and_priority() {
        let checker = LeaseValidityChecker;
        assert_eq!(checker.name(), "lease-validity-checker");
        assert_eq!(checker.priority(), CheckerPriority::LEASE_VALIDITY);
    }

    /// Extract the (from, to) of the first TransferLease step in the op.
    fn lease_transfer(op: &BGOperator) -> Option<(u32, u32)> {
        let (_, _, transfers) = decompose(op);
        transfers.first().copied()
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum LeaseOwnerSetup {
        /// lease_owner is this worker, which is Live + in replica_set + Active state.
        Healthy(u32),
        /// lease_owner is None.
        Missing,
        /// lease_owner set but not in replica_set.
        OutsideReplicaSet(u32),
        /// lease_owner is in replica_set but its worker is Lost (NodeState::Lost).
        OnLostWorker(u32),
        /// lease_owner is in replica_set but its replica state is Offline.
        OfflineReplica(u32),
    }

    #[derive(Debug)]
    enum Expect {
        None,
        Transfer {
            to: u32,
        },
        /// Accept any `to` in this set — used when lease_count tie-breaker by worker_id ambiguous
        TransferAny(Vec<u32>),
    }

    struct Case {
        name: &'static str,
        replica_set: Vec<u32>,
        /// (worker_id, active) — active=true makes the replica Active (serving).
        replicas_active: Vec<u32>,
        lease: LeaseOwnerSetup,
        expect: Expect,
    }

    fn cases() -> Vec<Case> {
        vec![
            Case {
                name: "healthy lease → None",
                replica_set: vec![100, 101, 102],
                replicas_active: vec![100, 101, 102],
                lease: LeaseOwnerSetup::Healthy(100),
                expect: Expect::None,
            },
            Case {
                name: "lease missing → transfer to an active replica",
                replica_set: vec![100, 101, 102],
                replicas_active: vec![100, 101, 102],
                lease: LeaseOwnerSetup::Missing,
                expect: Expect::Transfer { to: 100 },
            },
            Case {
                name: "lease owner not in replica_set → transfer",
                replica_set: vec![100, 101, 102],
                replicas_active: vec![100, 101, 102],
                lease: LeaseOwnerSetup::OutsideReplicaSet(999),
                expect: Expect::Transfer { to: 100 },
            },
            Case {
                name: "lease owner on Lost worker → transfer to other active replica",
                replica_set: vec![100, 101, 102],
                replicas_active: vec![100, 101, 102],
                lease: LeaseOwnerSetup::OnLostWorker(100),
                expect: Expect::TransferAny(vec![101, 102]),
            },
            Case {
                name: "lease owner replica marked Offline → transfer",
                replica_set: vec![100, 101, 102],
                replicas_active: vec![101, 102],
                lease: LeaseOwnerSetup::OfflineReplica(100),
                expect: Expect::TransferAny(vec![101, 102]),
            },
            Case {
                name: "invalid owner but no other active replica → None",
                replica_set: vec![100, 101],
                replicas_active: vec![],
                lease: LeaseOwnerSetup::OnLostWorker(100),
                expect: Expect::None,
            },
        ]
    }

    #[test]
    fn table_driven_check_bg() {
        for case in cases() {
            let f = Fixture::new();
            // Register all replicas as workers; we'll mark some Lost per-case.
            for &w in &case.replica_set {
                f.add_worker(w, POOL_ID_SSD, &[]);
            }
            // Extra candidate for OutsideReplicaSet case.
            if let LeaseOwnerSetup::OutsideReplicaSet(_) = case.lease {}
            let table_id = f.insert_table(POOL_ID_SSD, 3);

            let (lease_owner_override, lost_worker) = match case.lease {
                LeaseOwnerSetup::Healthy(w) => (Some(w), None),
                LeaseOwnerSetup::Missing => (None, None),
                LeaseOwnerSetup::OutsideReplicaSet(w) => (Some(w), None),
                LeaseOwnerSetup::OnLostWorker(w) => (Some(w), Some(w)),
                LeaseOwnerSetup::OfflineReplica(w) => (Some(w), None),
            };

            let bg_id = 1u32;
            let bg = BlockGroupInfo {
                bg_id,
                table_id,
                bg_epoch: 1,
                replica_set: case.replica_set.clone(),
                state: BGState::Active,
                op_state: BGOpState::Idle,
                lease_owner: lease_owner_override.map(|w| BGLease {
                    node_id: w,
                    epoch: 1,
                    grant_time_ms: 0,
                }),
                stats: Default::default(),
            };
            f.ctx
                .bg_manager
                .apply_create_bg(&BGEntry {
                    op_ms: 0,
                    info: bg.clone(),
                })
                .unwrap();

            // Mark Active replicas (only these are "serving" candidates).
            let active: Vec<(u32, ReplicaState)> = case
                .replicas_active
                .iter()
                .map(|&w| (w, ReplicaState::Active))
                .collect();
            f.set_replica_states(bg_id, &active);

            // Apply Lost state if configured.
            if let Some(w) = lost_worker {
                f.set_worker_state(w, NodeState::Lost);
            }
            // Mark the lease owner's replica Offline if configured.
            if let LeaseOwnerSetup::OfflineReplica(w) = case.lease {
                f.set_replica_states(bg_id, &[(w, ReplicaState::Offline)]);
            }

            let op = LeaseValidityChecker.check_bg(&bg, &f.ctx);
            match case.expect {
                Expect::None => {
                    assert!(op.is_none(), "{}: expected None, got {:?}", case.name, op);
                }
                Expect::Transfer { to } => {
                    let op = op.unwrap_or_else(|| panic!("{}: expected op", case.name));
                    assert_eq!(
                        op.priority,
                        OpPriority::LEASE_VALIDITY_FIX,
                        "{}: priority",
                        case.name
                    );
                    let (_, actual_to) = lease_transfer(&op)
                        .unwrap_or_else(|| panic!("{}: no TransferLease step", case.name));
                    assert_eq!(actual_to, to, "{}: transfer target", case.name);
                }
                Expect::TransferAny(allowed) => {
                    let op = op.unwrap_or_else(|| panic!("{}: expected op", case.name));
                    let (_, actual_to) = lease_transfer(&op)
                        .unwrap_or_else(|| panic!("{}: no TransferLease step", case.name));
                    assert!(
                        allowed.contains(&actual_to),
                        "{}: transfer target {} not in {:?}",
                        case.name,
                        actual_to,
                        allowed
                    );
                }
            }
        }
    }

    #[test]
    fn prefers_worker_with_lowest_lease_count() {
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102, 103], POOL_ID_SSD);
        let table_id = f.insert_table(POOL_ID_SSD, 3);

        // Load up lease counts on 101 and 103 by creating BGs whose lease_owner is them.
        for (bg_id, owner) in [
            (10, 101),
            (11, 101),
            (12, 101),
            (13, 102),
            (14, 103),
            (15, 103),
        ] {
            f.insert_bg(bg_id, table_id, vec![owner], Some(owner));
        }

        // Now the BG under test: owner=100 (invalid), candidates = {101, 102, 103}, all Active.
        f.insert_bg(1, table_id, vec![100, 101, 102, 103], Some(100));
        f.set_replica_states(
            1,
            &[
                (101, ReplicaState::Active),
                (102, ReplicaState::Active),
                (103, ReplicaState::Active),
            ],
        );
        // Make owner 100 invalid via Lost state.
        f.set_worker_state(100, NodeState::Lost);

        let bg = f.ctx.bg_manager.get_bg(1).unwrap();
        let op = LeaseValidityChecker.check_bg(&bg, &f.ctx).unwrap();
        let (_, to) = lease_transfer(&op).unwrap();
        assert_eq!(to, 102, "lowest-lease candidate should be picked");
    }
}
