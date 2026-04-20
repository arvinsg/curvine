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
use crate::pd::schedule::operator::{BGOperator, OpPriority, OperatorBuilder, OperatorKind};
use crate::pd::schedule::ManagerContext;
use curvine_common::state::{BGOpState, ReplicaState};
use std::time::Duration;

pub struct DecommissionScheduler;

impl DecommissionScheduler {
    fn build_migration_op(
        bg: &curvine_common::state::BlockGroupInfo,
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
}

impl Scheduler for DecommissionScheduler {
    fn name(&self) -> &str {
        "decommission-scheduler"
    }

    fn schedule(&self, ctx: &ManagerContext) -> Vec<BGOperator> {
        let mut ops = Vec::new();
        let decommission_nodes = ctx
            .node_manager
            .get_nodes_by_state(curvine_common::state::NodeState::Decommission);

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
    use crate::pd::schedule::scheduler::Scheduler;

    #[test]
    fn name_and_type() {
        let s = DecommissionScheduler;
        assert_eq!(s.name(), "decommission-scheduler");
    }
}
