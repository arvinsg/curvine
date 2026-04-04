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

use super::CheckerContext;
use crate::pd::schedule::operator::{BGOperator, OperatorBuilder, OperatorKind};
use crate::pd::schedule::CoordinatorContext;
use curvine_common::state::BlockGroupInfo;
use std::sync::Arc;

/// LeaseValidityChecker: detects invalid leases and generates LeaseTransfer operators.
pub struct LeaseValidityChecker {
    ctx: Arc<CoordinatorContext>,
}

impl LeaseValidityChecker {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        Self { ctx }
    }

    fn is_lease_invalid(&self, bg: &BlockGroupInfo, ctx: &CheckerContext<'_>) -> bool {
        match bg.lease_owner.as_ref() {
            None => true,
            Some(lease) => {
                !bg.replica_set.contains(&lease.node_id)
                    || !ctx.pool_manager.is_worker_available(lease.node_id)
            }
        }
    }

    fn build_lease_transfer(
        &self,
        bg: &BlockGroupInfo,
        ctx: &CheckerContext<'_>,
    ) -> Option<BGOperator> {
        let lease_counts = ctx.bg_manager.get_worker_lease_counts();

        let new_owner = bg
            .replica_set
            .iter()
            .filter(|w| ctx.pool_manager.is_worker_available(**w))
            .min_by_key(|&&w| {
                (
                    lease_counts.get(&w).copied().unwrap_or(0),
                    w,
                )
            })
            .copied()?;

        let old_worker = bg.lease_owner.as_ref().map(|l| l.node_id).unwrap_or(0);
        if new_owner == old_worker {
            return None;
        }

        Some(
            OperatorBuilder::new(
                OperatorKind::LeaseTransfer,
                bg.bg_id,
                format!("Transfer lease from {} to {} (validity fix)", old_worker, new_owner),
            )
            .bg_epoch(bg.bg_epoch)
            .transfer_lease(old_worker, new_owner)
            .priority(80)
            .build(),
        )
    }
}

impl super::Checker for LeaseValidityChecker {
    fn name(&self) -> &str {
        "lease-validity-checker"
    }

    fn check_bg(&self, bg: &BlockGroupInfo, ctx: &CheckerContext<'_>) -> Option<BGOperator> {
        if !self.is_lease_invalid(bg, ctx) {
            return None;
        }
        self.build_lease_transfer(bg, ctx)
    }

    fn priority(&self) -> u32 {
        10
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::schedule::checker::{Checker, CheckerContext};
    use curvine_common::state::{BGLease, BGOpState, BGState};

    fn test_ctx() -> Arc<CoordinatorContext> {
        crate::pd::schedule::checker::tests_common::test_coordinator_context(
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

    fn checker_ctx(ctx: &CoordinatorContext) -> CheckerContext<'_> {
        CheckerContext {
            pool_manager: &ctx.pool_manager,
            bg_manager: &ctx.bg_manager,
            node_manager: &ctx.node_manager,
            config_manager: &ctx.config_manager,
        }
    }

    #[test]
    fn no_op_for_bg_with_no_available_workers() {
        let ctx = test_ctx();
        let checker = LeaseValidityChecker::new(ctx.clone());
        let bg = make_bg(1, 0x0001_0001, vec![100, 101]);
        ctx.bg_manager
            .apply_create_bg(&crate::pd::journal::entry::BGEntry { op_ms: 0, info: bg.clone() })
            .unwrap();
        let cctx = checker_ctx(&ctx);
        assert!(checker.check_bg(&bg, &cctx).is_none());
    }

    #[test]
    fn name_and_priority() {
        let ctx = test_ctx();
        let checker = LeaseValidityChecker::new(ctx);
        assert_eq!(checker.name(), "lease-validity-checker");
        assert_eq!(checker.priority(), 10);
    }
}
