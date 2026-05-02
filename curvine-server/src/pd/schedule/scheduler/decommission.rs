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

use super::Scheduler;
use crate::pd::schedule::{BGOperator, ManagerContext, OpPriority, OperatorBuilder, OperatorKind};
use curvine_common::state::{BGOpState, BlockGroupInfo, NodeState, ReplicaState};
use std::time::Duration;

pub struct DecommissionScheduler;

impl DecommissionScheduler {
    fn build_migration_op(
        bg: &BlockGroupInfo,
        decom_worker: u32,
        ctx: &ManagerContext,
    ) -> Option<BGOperator> {
        let new_workers = ctx.bg_manager.select_replacement_workers(bg, 1).ok()?;
        let new_worker = *new_workers.first()?;

        let mut builder = OperatorBuilder::new(
            OperatorKind::DecommissionRepair,
            bg.bg_id,
            format!(
                "Decommission migration: replace {} with {}",
                decom_worker, new_worker
            ),
        )
        .bg_epoch(bg.bg_epoch)
        .priority(OpPriority::DECOMMISSION_REPAIR)
        .add_replica(new_worker)
        .wait_replica_ready(new_worker, ReplicaState::Active);

        if bg
            .lease_owner
            .as_ref()
            .map(|l| l.node_id == decom_worker)
            .unwrap_or(false)
        {
            let to_worker = ctx.pick_lease_fallback(bg, decom_worker, new_worker);
            builder = builder.transfer_lease(decom_worker, to_worker);
        }

        Some(builder.remove_replica(decom_worker).build())
    }
}

impl Scheduler for DecommissionScheduler {
    fn name(&self) -> &str {
        "decommission-scheduler"
    }

    fn schedule(&self, ctx: &ManagerContext) -> Vec<BGOperator> {
        let mut ops = Vec::new();
        let decommission_nodes = ctx
            .node_manager
            .get_nodes_by_state(NodeState::Decommission);

        for node in decommission_nodes {
            let node_id = node.base.node_id;
            let bgs = ctx.bg_manager.get_bgs_on_worker(node_id);

            if !bgs.is_empty() {
                for bg in bgs {
                    if bg.op_state != BGOpState::Idle {
                        continue;
                    }
                    if let Some(op) = Self::build_migration_op(&bg, node_id, ctx) {
                        ops.push(op);
                    }
                }
            } else if !ctx
                .operator_controller
                .has_running_operators_for_node(node_id)
            {
                log::info!(
                    "Node {} decommission complete (no BGs, no operators)",
                    node_id
                );
                if let Err(e) = ctx.node_manager.finish_decommission(node_id) {
                    log::error!("Failed to finish decommission for node {}: {}", node_id, e);
                }
            }
        }

        ops
    }

    fn is_schedule_allowed(&self, _ctx: &ManagerContext) -> bool {
        true
    }

    fn min_interval(&self) -> Duration {
        Duration::from_secs(5)
    }

    fn next_interval(&self, current: Duration) -> Duration {
        current
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::pool::POOL_ID_SSD;
    use crate::pd::schedule::checker::tests_common::{decompose, Fixture};
    use crate::pd::schedule::Scheduler;

    #[test]
    fn name_and_type() {
        let s = DecommissionScheduler;
        assert_eq!(s.name(), "decommission-scheduler");
    }

    #[test]
    fn is_schedule_allowed_is_true() {
        let f = Fixture::new();
        assert!(DecommissionScheduler.is_schedule_allowed(&f.ctx));
    }

    #[test]
    fn no_ops_when_no_decommission_nodes() {
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102], POOL_ID_SSD);
        assert!(DecommissionScheduler.schedule(&f.ctx).is_empty());
    }

    /// Seed a 3-replica table, 4 live workers, put BGs on the first 2, then mark worker 100
    /// as Decommission. Returns (table_id, bg_ids).
    fn seed_decommission_fixture(
        f: &Fixture,
        bg_count: u32,
        lease_owner: u32,
    ) -> (u32, Vec<u32>) {
        f.add_workers(&[100, 101, 102, 103], POOL_ID_SSD);
        let table_id = f.insert_table(POOL_ID_SSD, 3);
        let bg_ids: Vec<u32> = (0..bg_count).map(|i| 3_000 + i).collect();
        for &bg_id in &bg_ids {
            // Worker 100 is in replica_set; this is the one being decommissioned.
            f.insert_bg(bg_id, table_id, vec![100, 101, 102], Some(lease_owner));
            f.activate_all_replicas(bg_id);
        }
        f.set_table_buckets(table_id, &bg_ids);
        f.set_worker_state(100, NodeState::Decommission);
        (table_id, bg_ids)
    }

    #[test]
    fn produces_one_migration_op_per_bg() {
        let f = Fixture::new();
        let (_, bg_ids) = seed_decommission_fixture(&f, 3, 101);

        let ops = DecommissionScheduler.schedule(&f.ctx);
        assert_eq!(
            ops.len(),
            bg_ids.len(),
            "one migration op per BG on decommission node"
        );
        for op in &ops {
            assert_eq!(op.priority, OpPriority::DECOMMISSION_REPAIR);
            let (add, remove, _) = decompose(op);
            assert_eq!(add.len(), 1, "one AddReplica");
            assert_eq!(remove, vec![100], "removes the decommissioning worker");
            // Target must be a live worker that isn't already in replica_set.
            assert_eq!(add[0], 103, "target is the only worker not in replica_set");
        }
    }

    #[test]
    fn lease_owner_on_decommission_node_triggers_lease_transfer() {
        // lease_owner = 100 (the decommissioning worker) → op must include TransferLease.
        let f = Fixture::new();
        seed_decommission_fixture(&f, 1, 100);

        let ops = DecommissionScheduler.schedule(&f.ctx);
        assert_eq!(ops.len(), 1);
        let (_, _, transfer) = decompose(&ops[0]);
        assert_eq!(transfer.len(), 1, "lease owner on decom node → TransferLease");
        assert_eq!(transfer[0].0, 100, "transfer from the decommissioning worker");
    }

    #[test]
    fn lease_owner_elsewhere_has_no_lease_transfer() {
        // lease_owner = 101 (not the decommission target) → no TransferLease step.
        let f = Fixture::new();
        seed_decommission_fixture(&f, 1, 101);

        let ops = DecommissionScheduler.schedule(&f.ctx);
        assert_eq!(ops.len(), 1);
        let (_, _, transfer) = decompose(&ops[0]);
        assert!(transfer.is_empty(), "lease owner not on decom node → no TransferLease");
    }

    #[test]
    fn non_idle_bgs_are_skipped() {
        let f = Fixture::new();
        let (_, bg_ids) = seed_decommission_fixture(&f, 3, 101);
        for &bg_id in &bg_ids {
            f.ctx.bg_manager.set_op_state(bg_id, BGOpState::Recovering);
        }

        let ops = DecommissionScheduler.schedule(&f.ctx);
        assert!(ops.is_empty(), "all BGs non-Idle → no migration ops");
    }
}
