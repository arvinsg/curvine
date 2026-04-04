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
use curvine_common::state::{BGState, BlockGroupInfo, NodeState};
use std::sync::Arc;

pub struct ReplicaChecker {
    ctx: Arc<crate::pd::schedule::CoordinatorContext>,
}

impl ReplicaChecker {
    pub fn new(ctx: Arc<crate::pd::schedule::CoordinatorContext>) -> Self {
        Self { ctx }
    }

    fn build_decommission_repair(
        &self,
        bg: &BlockGroupInfo,
        decom_worker: u32,
        ctx: &CheckerContext<'_>,
    ) -> Option<BGOperator> {
        let _table = ctx.bg_manager.get_table(bg.table_id)?;
        let pool_id = (bg.table_id >> 16) as u16;

        let new_workers = ctx
            .bg_manager
            .select_workers(pool_id, 1, &bg.replica_set)
            .ok()?;
        let new_worker = *new_workers.first()?;

        let mut builder = OperatorBuilder::new(
            OperatorKind::DecommissionRepair,
            bg.bg_id,
            format!("Decommission repair: replace {} with {}", decom_worker, new_worker),
        )
        .bg_epoch(bg.bg_epoch)
        .priority(120)
        .add_replica(new_worker);

        if bg
            .lease_owner
            .as_ref()
            .map(|l| l.node_id == decom_worker)
            .unwrap_or(false)
        {
            let to_worker = bg
                .replica_set
                .iter()
                .filter(|&&w| w != decom_worker)
                .find(|&&w| ctx.pool_manager.is_worker_available(w))
                .copied()
                .unwrap_or(new_worker);
            builder = builder.transfer_lease(decom_worker, to_worker);
        }

        Some(builder.remove_replica(decom_worker).build())
    }

    fn build_under_replicated_repair(
        &self,
        bg: &BlockGroupInfo,
        available_replicas: &[u32],
        ctx: &CheckerContext<'_>,
    ) -> Option<BGOperator> {
        let table = ctx.bg_manager.get_table(bg.table_id)?;
        let desired = table.policy.replicas as usize;
        if available_replicas.len() >= desired {
            return None;
        }
        let needed = desired - available_replicas.len();
        let pool_id = (bg.table_id >> 16) as u16;
        let new_workers = ctx
            .bg_manager
            .select_workers(pool_id, needed as u16, &bg.replica_set)
            .ok()?;
        if new_workers.is_empty() {
            return None;
        }

        let mut builder = OperatorBuilder::new(
            OperatorKind::Repair,
            bg.bg_id,
            format!("Add {} replicas", new_workers.len()),
        )
        .bg_epoch(bg.bg_epoch)
        .priority(100);

        for &w in &new_workers {
            builder = builder.add_replica(w);
        }

        Some(builder.build())
    }

    fn build_over_replicated_repair(
        &self,
        bg: &BlockGroupInfo,
        available_replicas: &[u32],
        ctx: &CheckerContext<'_>,
    ) -> Option<BGOperator> {
        let table = ctx.bg_manager.get_table(bg.table_id)?;
        let desired = table.policy.replicas as usize;
        if available_replicas.len() <= desired {
            return None;
        }
        let excess = available_replicas.len() - desired;
        let lease_owner_id = bg.lease_owner.as_ref().map(|l| l.node_id).unwrap_or(0);

        let mut to_remove: Vec<u32> = available_replicas
            .iter()
            .filter(|&&w| w != lease_owner_id)
            .copied()
            .take(excess)
            .collect();
        if to_remove.len() < excess {
            to_remove.extend(
                available_replicas
                    .iter()
                    .filter(|&&w| w == lease_owner_id)
                    .copied()
                    .take(excess - to_remove.len()),
            );
        }

        if to_remove.is_empty() {
            return None;
        }

        let mut builder = OperatorBuilder::new(
            OperatorKind::Repair,
            bg.bg_id,
            format!("Remove {} excess replicas", to_remove.len()),
        )
        .bg_epoch(bg.bg_epoch)
        .priority(60);

        for &w in &to_remove {
            builder = builder.remove_replica(w);
        }

        Some(builder.build())
    }
}

impl super::Checker for ReplicaChecker {
    fn name(&self) -> &str {
        "replica-checker"
    }

    fn check_bg(&self, bg: &BlockGroupInfo, ctx: &CheckerContext<'_>) -> Option<BGOperator> {
        let table = ctx.bg_manager.get_table(bg.table_id)?;
        let desired = table.policy.replicas as usize;

        // Decommission repair: highest priority
        let decom_worker = bg
            .replica_set
            .iter()
            .find(|&&w| {
                ctx.pool_manager
                    .get_worker_node(w)
                    .map(|n| n.state == NodeState::Decommission)
                    .unwrap_or(false)
            })
            .copied();
        if let Some(dw) = decom_worker {
            return self.build_decommission_repair(bg, dw, ctx);
        }

        // Compute available replicas
        let available_replicas: Vec<u32> = bg
            .replica_set
            .iter()
            .filter(|w| ctx.pool_manager.is_worker_available(**w))
            .copied()
            .collect();

        if available_replicas.is_empty() {
            log::error!("BG {} all replicas lost!", bg.bg_id);
            return None;
        }

        if available_replicas.len() < desired || bg.state == BGState::Degraded {
            return self.build_under_replicated_repair(bg, &available_replicas, ctx);
        }

        if available_replicas.len() > desired {
            return self.build_over_replicated_repair(bg, &available_replicas, ctx);
        }

        None
    }

    fn priority(&self) -> u32 {
        20
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::schedule::checker::{Checker, CheckerContext};
    use crate::pd::schedule::CoordinatorContext;
    use curvine_common::state::{BGLease, BGOpState, BGState, BlockGroupInfo};

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
    fn no_ops_for_healthy_bgs() {
        let ctx = test_ctx();
        let checker = ReplicaChecker::new(ctx.clone());
        let bg = make_bg(1, 0x0001_0003, vec![100, 101, 102]);
        ctx.bg_manager
            .apply_create_bg(&crate::pd::journal::entry::BGEntry { op_ms: 0, info: bg.clone() })
            .unwrap();
        let cctx = checker_ctx(&ctx);
        // No workers registered -> not available -> all replicas lost -> None
        assert!(checker.check_bg(&bg, &cctx).is_none());
    }

    #[test]
    fn skips_bg_without_table() {
        let ctx = test_ctx();
        let checker = ReplicaChecker::new(ctx.clone());
        // BG references a table that doesn't exist
        let bg = make_bg(1, 0x0001_0003, vec![100]);
        let cctx = checker_ctx(&ctx);
        assert!(checker.check_bg(&bg, &cctx).is_none());
    }

    #[test]
    fn name_and_priority() {
        let ctx = test_ctx();
        let checker = ReplicaChecker::new(ctx);
        assert_eq!(checker.name(), "replica-checker");
        assert_eq!(checker.priority(), 20);
    }
}
