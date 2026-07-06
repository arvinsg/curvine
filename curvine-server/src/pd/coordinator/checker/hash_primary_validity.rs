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

use crate::pd::coordinator::policy::is_hash_repair_candidate;
use crate::pd::coordinator::{
    BGOperator, CoordinatorContext, OpPriority, OperatorBuilder, OperatorKind,
};
use curvine_common::state::{BGKind, BlockGroupInfo, ReplicaState};

pub struct HashPrimaryValidityChecker;

impl HashPrimaryValidityChecker {
    fn is_primary_invalid(bg: &BlockGroupInfo, ctx: &CoordinatorContext) -> bool {
        !bg.replica_set.contains(&bg.primary.node_id)
            || !ctx.pool_manager.is_worker_available(bg.primary.node_id)
            || ctx
                .bgtable_manager
                .bg()
                .get_replica_state(bg.kind, bg.bg_id, bg.primary.node_id)
                != ReplicaState::Active
    }

    fn build_primary_transfer(bg: &BlockGroupInfo, ctx: &CoordinatorContext) -> Option<BGOperator> {
        let old_worker = bg.primary.node_id;
        let primary_counts = ctx.bgtable_manager.bg().worker_primary_counts(bg.kind, None);

        // Select from Active replicas, excluding the current (invalid) owner
        let serving = ctx.bgtable_manager.bg().active_isr_workers(bg.kind, bg.bg_id);
        let candidates: Vec<u32> = serving.into_iter().filter(|&w| w != old_worker).collect();

        let new_owner = candidates
            .iter()
            .min_by_key(|&&w| (primary_counts.get(&w).copied().unwrap_or(0), w))
            .copied()?;

        Some(
            OperatorBuilder::new(
                bg.kind,
                OperatorKind::PrimaryTransfer,
                bg.bg_id,
                format!(
                    "Transfer primary from {} to {} (validity fix)",
                    old_worker, new_owner
                ),
            )
            .bg_epoch(bg.bg_epoch)
            .transfer_primary(old_worker, new_owner)
            .priority(OpPriority::PRIMARY_VALIDITY_FIX)
            .build(),
        )
    }
}

impl super::Checker for HashPrimaryValidityChecker {
    fn name(&self) -> &str {
        "hash-primary-validity-checker"
    }

    fn supported_kinds(&self) -> &[BGKind] {
        &[BGKind::Hash]
    }

    fn check_bg(&self, bg: &BlockGroupInfo, ctx: &CoordinatorContext) -> Option<BGOperator> {
        if !is_hash_repair_candidate(bg) {
            return None;
        }
        if !Self::is_primary_invalid(bg, ctx) {
            return None;
        }
        Self::build_primary_transfer(bg, ctx)
    }

    fn priority(&self) -> u32 {
        super::CheckerPriority::PRIMARY_VALIDITY
    }
}

#[cfg(test)]
mod tests {
    use super::super::CheckerPriority;
    use super::*;
    use crate::pd::coordinator::checker::tests_common::{decompose, Fixture};
    use crate::pd::coordinator::checker::Checker;
    use curvine_common::state::StorageType;
    use curvine_common::state::{BGKind, BGOpState, BGPrimary, BGState, NodeState};

    #[test]
    fn name_and_priority() {
        let checker = HashPrimaryValidityChecker;
        assert_eq!(checker.name(), "hash-primary-validity-checker");
        assert_eq!(checker.priority(), CheckerPriority::PRIMARY_VALIDITY);
    }

    /// Extract the (from, to) of the first TransferPrimary step in the op.
    fn primary_transfer(op: &BGOperator) -> Option<(u32, u32)> {
        let (_, _, transfers) = decompose(op);
        transfers.first().copied()
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum PrimarySetup {
        /// primary is this worker, which is Live + in replica_set + Active state.
        Healthy(u32),
        /// primary is in replica_set but its worker is Lost (NodeState::Lost).
        OnLostWorker(u32),
        /// primary is in replica_set but its replica state is Offline.
        OfflineReplica(u32),
    }

    #[derive(Debug)]
    enum Expect {
        None,
        /// Accept any `to` in this set — used when primary_count tie-breaker by worker_id ambiguous
        TransferAny(Vec<u32>),
    }

    struct Case {
        name: &'static str,
        replica_set: Vec<u32>,
        /// (worker_id, active) — active=true makes the replica Active (serving).
        replicas_active: Vec<u32>,
        primary: PrimarySetup,
        expect: Expect,
    }

    fn cases() -> Vec<Case> {
        vec![
            Case {
                name: "healthy primary → None",
                replica_set: vec![100, 101, 102],
                replicas_active: vec![100, 101, 102],
                primary: PrimarySetup::Healthy(100),
                expect: Expect::None,
            },
            Case {
                name: "primary on Lost worker → transfer to other active replica",
                replica_set: vec![100, 101, 102],
                replicas_active: vec![100, 101, 102],
                primary: PrimarySetup::OnLostWorker(100),
                expect: Expect::TransferAny(vec![101, 102]),
            },
            Case {
                name: "primary replica marked Offline → transfer",
                replica_set: vec![100, 101, 102],
                replicas_active: vec![101, 102],
                primary: PrimarySetup::OfflineReplica(100),
                expect: Expect::TransferAny(vec![101, 102]),
            },
            Case {
                name: "invalid owner but no other active replica → None",
                replica_set: vec![100, 101],
                replicas_active: vec![],
                primary: PrimarySetup::OnLostWorker(100),
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
                f.add_worker(w, StorageType::Ssd, &[]);
            }
            let table_id = f.insert_table(StorageType::Ssd, 3);

            let (primary_override, lost_worker) = match case.primary {
                PrimarySetup::Healthy(w) => (Some(w), None),
                PrimarySetup::OnLostWorker(w) => (Some(w), Some(w)),
                PrimarySetup::OfflineReplica(w) => (Some(w), None),
            };

            let bg_id = 1u64;
            let bg = BlockGroupInfo {
                bg_id,
                table_id,
                kind: BGKind::Hash,
                bg_epoch: 1,
                replica_set: case.replica_set.clone(),
                isr: case.replica_set.clone(),
                state: BGState::Active,
                op_state: BGOpState::Idle,
                primary: BGPrimary {
                    node_id: primary_override.unwrap_or_else(|| case.replica_set[0]),
                    epoch: 1,
                    grant_time_ms: 0,
                },
                stats: Default::default(),
                replicas: Default::default(),
            };
            f.ctx
                .bgtable_manager
                .test_seed_bg(bg.clone())
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
            // Mark the primary replica non-serving if configured.
            if let PrimarySetup::OfflineReplica(w) = case.primary {
                f.set_replica_states(bg_id, &[(w, ReplicaState::Syncing)]);
            }

            let op = HashPrimaryValidityChecker.check_bg(&bg, &f.ctx);
            match case.expect {
                Expect::None => {
                    assert!(op.is_none(), "{}: expected None, got {:?}", case.name, op);
                }
                Expect::TransferAny(allowed) => {
                    let op = op.unwrap_or_else(|| panic!("{}: expected op", case.name));
                    let (_, actual_to) = primary_transfer(&op)
                        .unwrap_or_else(|| panic!("{}: no TransferPrimary step", case.name));
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
    fn prefers_worker_with_lowest_primary_count() {
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102, 103], StorageType::Ssd);
        let table_id = f.insert_table(StorageType::Ssd, 3);

        // Load up primary counts on 101 and 103 by creating BGs whose primary is them.
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

        let bg = f.ctx.bgtable_manager.bg().get_bg(BGKind::Hash, 1).unwrap();
        let op = HashPrimaryValidityChecker.check_bg(&bg, &f.ctx).unwrap();
        let (_, to) = primary_transfer(&op).unwrap();
        assert_eq!(to, 102, "lowest-primary candidate should be picked");
    }
}
