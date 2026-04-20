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

use crate::pd::schedule::operator::{BGOperator, OpPriority, OperatorBuilder, OperatorKind};
use crate::pd::schedule::ManagerContext;
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
        let lease_counts = ctx.bg_manager.get_worker_lease_counts();

        let serving = ctx.bg_manager.get_serving_replicas(bg.bg_id);
        let new_owner = serving
            .iter()
            .min_by_key(|&&w| (lease_counts.get(&w).copied().unwrap_or(0), w))
            .copied()?;

        let old_worker = bg.lease_owner.as_ref().map(|l| l.node_id).unwrap_or(0);
        if new_owner == old_worker {
            return None;
        }

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
    use super::*;
    use crate::pd::schedule::checker::Checker;
    use curvine_common::state::{BGLease, BGOpState, BGState};

    fn test_ctx() -> std::sync::Arc<ManagerContext> {
        crate::pd::schedule::checker::tests_common::test_context(
            std::collections::HashMap::new(),
        )
    }

    fn make_bg(bg_id: u32, table_id: u32, replica_set: Vec<u32>) -> BlockGroupInfo {
        let leader = replica_set.first().copied().unwrap_or(0);
        BlockGroupInfo {
            bg_id,
            table_id,
            bg_epoch: 1,
            replica_set,
            state: BGState::Assigned,
            op_state: BGOpState::Idle,
            lease_owner: Some(BGLease {
                node_id: leader,
                epoch: 1,
                grant_time_ms: 0,
            }),
            stats: Default::default(),
        }
    }

    #[test]
    fn no_op_for_bg_with_no_available_workers() {
        let ctx = test_ctx();
        let checker = LeaseValidityChecker;
        let bg = make_bg(1, 0x0001_0001, vec![100, 101]);
        ctx.bg_manager
            .apply_create_bg(&crate::pd::journal::entry::BGEntry {
                op_ms: 0,
                info: bg.clone(),
            })
            .unwrap();
        assert!(checker.check_bg(&bg, &ctx).is_none());
    }

    #[test]
    fn name_and_priority() {
        let checker = LeaseValidityChecker;
        assert_eq!(checker.name(), "lease-validity-checker");
        assert_eq!(
            checker.priority(),
            super::super::CheckerPriority::LEASE_VALIDITY
        );
    }
}
