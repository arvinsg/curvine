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
use crate::pd::coordinator::policy::is_hash_repair_candidate;
use crate::pd::coordinator::{
    BGOperator, CoordinatorContext, OpPriority, OperatorBuilder, OperatorKind,
};
use curvine_common::state::{BGKind, BlockGroupInfo, NodeState, ReplicaState};
use std::time::Duration;

/// Scheduler responsible for evacuating BGs off nodes that are leaving the
/// cluster, both gracefully (NodeState::Decommission) and unexpectedly
/// (NodeState::Offline).
pub struct HashDecommissionScheduler;

impl HashDecommissionScheduler {
    pub fn new() -> Self {
        Self
    }

    fn build_migration_op(
        bg: &BlockGroupInfo,
        leaving_worker: u32,
        ctx: &CoordinatorContext,
    ) -> Option<BGOperator> {
        let new_workers = ctx
            .bgtable_manager
            .hash_placement()
            .select_replacement_workers(bg, 1)
            .ok()?;
        let new_worker = *new_workers.first()?;

        if new_worker == leaving_worker {
            log::error!(
                "select_replacement_workers returned the leaving worker {} for bg {}",
                leaving_worker,
                bg.bg_id
            );
            return None;
        }

        let mut builder = OperatorBuilder::new(
            BGKind::Hash,
            OperatorKind::DecommissionRepair,
            bg.bg_id,
            format!(
                "Migrate BG off leaving worker {}: replace with {}",
                leaving_worker, new_worker
            ),
        )
        .bg_epoch(bg.bg_epoch)
        .priority(OpPriority::DECOMMISSION_REPAIR)
        .add_replica(new_worker)
        .wait_replica_ready(new_worker, ReplicaState::Active);

        if bg.primary.node_id == leaving_worker {
            let to_worker = ctx.pick_primary_fallback(bg, leaving_worker, new_worker);
            builder = builder.transfer_primary(leaving_worker, to_worker);
        }

        Some(builder.remove_replica(leaving_worker).build())
    }

    /// Build per-BG migration operators for one leaving worker.
    fn migration_ops_for_worker(node_id: u32, ctx: &CoordinatorContext) -> Vec<BGOperator> {
        let mut ops = Vec::new();
        for bg in ctx.bgtable_manager.bg().bgs_on_worker(BGKind::Hash, node_id, None) {
            if !is_hash_repair_candidate(&bg) {
                continue;
            }
            if let Some(op) = Self::build_migration_op(&bg, node_id, ctx) {
                ops.push(op);
            }
        }
        ops
    }
}

impl Default for HashDecommissionScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Scheduler for HashDecommissionScheduler {
    fn name(&self) -> &str {
        "hash-decommission-scheduler"
    }

    fn schedule(&self, ctx: &CoordinatorContext) -> Vec<BGOperator> {
        let mut ops = Vec::new();

        // Offline nodes: migrate BGs but never call finish_decommission.
        for node in ctx.node_manager.get_nodes_by_state(NodeState::Offline) {
            ops.extend(Self::migration_ops_for_worker(node.base.node_id, ctx));
        }

        // Decommission nodes: same migration, plus terminal finish_decommission
        // when all BGs are drained and no operators remain.
        for node in ctx.node_manager.get_nodes_by_state(NodeState::Decommission) {
            let node_id = node.base.node_id;
            let bgs = ctx.bgtable_manager.bg().bgs_on_worker(BGKind::Hash, node_id, None);
            if !bgs.is_empty() {
                ops.extend(Self::migration_ops_for_worker(node_id, ctx));
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

    fn is_schedule_allowed(&self, _ctx: &CoordinatorContext) -> bool {
        true
    }

    fn min_interval(&self) -> Duration {
        Duration::from_secs(1)
    }

    fn next_interval(&self, current: Duration) -> Duration {
        current
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::coordinator::checker::tests_common::{decompose, Fixture};
    use crate::pd::coordinator::Scheduler;
    use curvine_common::state::{BGKind, BGOpState, StorageType};

    #[test]
    fn name_and_type() {
        let s = HashDecommissionScheduler::new();
        assert_eq!(s.name(), "hash-decommission-scheduler");
    }

    #[test]
    fn is_schedule_allowed_is_true() {
        let f = Fixture::new();
        assert!(HashDecommissionScheduler::new().is_schedule_allowed(&f.ctx));
    }

    #[test]
    fn no_ops_when_no_leaving_nodes() {
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102], StorageType::Ssd);
        assert!(HashDecommissionScheduler::new().schedule(&f.ctx).is_empty());
    }

    /// Seed a 3-replica table, 4 live workers, put BGs on the first 3, then mark worker 100
    /// as the given state. Returns (table_id, bg_ids).
    fn seed_leaving_node(
        f: &Fixture,
        node_state: NodeState,
        bg_count: u32,
        primary: u32,
    ) -> (
        curvine_common::state::TableId,
        Vec<curvine_common::state::BgId>,
    ) {
        f.add_workers(&[100, 101, 102, 103], StorageType::Ssd);
        let table_id = f.insert_table(StorageType::Ssd, 3);
        let bg_ids: Vec<curvine_common::state::BgId> =
            (0..bg_count).map(|i| 3_000 + i as u64).collect();
        for &bg_id in &bg_ids {
            f.insert_bg(bg_id, table_id, vec![100, 101, 102], Some(primary));
            f.activate_all_replicas(bg_id);
        }
        f.set_table_buckets(table_id, &bg_ids);
        f.set_worker_state(100, node_state);
        (table_id, bg_ids)
    }

    #[test]
    fn decommission_node_produces_one_migration_op_per_bg() {
        let f = Fixture::new();
        let (_, bg_ids) = seed_leaving_node(&f, NodeState::Decommission, 3, 101);

        let ops = HashDecommissionScheduler::new().schedule(&f.ctx);
        assert_eq!(ops.len(), bg_ids.len());
        for op in &ops {
            assert_eq!(op.priority, OpPriority::DECOMMISSION_REPAIR);
            let (add, remove, _) = decompose(op);
            assert_eq!(add.len(), 1);
            assert_eq!(remove, vec![100]);
            assert_eq!(add[0], 103);
        }
    }

    #[test]
    fn offline_node_produces_one_migration_op_per_bg() {
        let f = Fixture::new();
        let (_, bg_ids) = seed_leaving_node(&f, NodeState::Offline, 3, 101);

        let ops = HashDecommissionScheduler::new().schedule(&f.ctx);
        assert_eq!(
            ops.len(),
            bg_ids.len(),
            "Offline nodes should be evacuated like Decommission"
        );
        for op in &ops {
            let (add, remove, _) = decompose(op);
            assert_eq!(add.len(), 1);
            assert_eq!(remove, vec![100]);
        }
    }

    #[test]
    fn primary_on_leaving_node_triggers_primary_transfer() {
        for state in [NodeState::Decommission, NodeState::Offline] {
            let f = Fixture::new();
            seed_leaving_node(&f, state, 1, 100); // primary = 100 = leaving

            let ops = HashDecommissionScheduler::new().schedule(&f.ctx);
            assert_eq!(ops.len(), 1, "{:?}", state);
            let (_, _, transfer) = decompose(&ops[0]);
            assert_eq!(transfer.len(), 1, "{:?}: primary transfer expected", state);
            assert_eq!(transfer[0].0, 100);
        }
    }

    #[test]
    fn primary_elsewhere_has_no_primary_transfer() {
        let f = Fixture::new();
        seed_leaving_node(&f, NodeState::Decommission, 1, 101);

        let ops = HashDecommissionScheduler::new().schedule(&f.ctx);
        assert_eq!(ops.len(), 1);
        let (_, _, transfer) = decompose(&ops[0]);
        assert!(transfer.is_empty());
    }

    #[test]
    fn non_idle_bgs_are_skipped() {
        let f = Fixture::new();
        let (_, bg_ids) = seed_leaving_node(&f, NodeState::Decommission, 3, 101);
        for &bg_id in &bg_ids {
            f.ctx
                .bgtable_manager
                .bg()
                .set_op_state(BGKind::Hash, bg_id, BGOpState::Repairing);
        }

        let ops = HashDecommissionScheduler::new().schedule(&f.ctx);
        assert!(ops.is_empty());
    }

    #[test]
    fn no_replacement_worker_available_means_no_op() {
        // 3 workers, replica_count=3, all BGs on [100,101,102] — the only live
        // candidate that's not in replica_set is... none. select_replacement_workers
        // returns empty, build_migration_op returns None.
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102], StorageType::Ssd);
        let table_id = f.insert_table(StorageType::Ssd, 3);
        f.insert_bg(3_000, table_id, vec![100, 101, 102], Some(101));
        f.activate_all_replicas(3_000);
        f.set_table_buckets(table_id, &[3_000]);
        f.set_worker_state(100, NodeState::Offline);

        let ops = HashDecommissionScheduler::new().schedule(&f.ctx);
        // Without a replacement candidate we skip this BG rather than producing
        // a degenerate op. The self-migration guard defends further: even if
        // the planner ever returned 100 itself, build_migration_op would reject.
        assert!(ops.is_empty());
    }
}
