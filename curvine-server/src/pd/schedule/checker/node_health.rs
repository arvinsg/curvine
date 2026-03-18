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

use super::{CheckResult, CheckerContext};
use crate::pd::schedule::operator::{BGOperator, OpStatus, OpStep};
use crate::pd::schedule::CoordinatorContext;
use curvine_common::state::{NodeState, NodeType};
use dashmap::DashMap;
use std::sync::Arc;

pub struct NodeHealthChecker {
    ctx: Arc<CoordinatorContext>,
    processed_lost_nodes: DashMap<u32, u64>,
}

impl NodeHealthChecker {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        Self {
            ctx,
            processed_lost_nodes: DashMap::new(),
        }
    }
}

impl super::Checker for NodeHealthChecker {
    fn name(&self) -> &str {
        "node-health-checker"
    }

    fn interval_ms(&self) -> u64 {
        self.ctx
            .config_manager
            .get_u64("pd.schedule.node_check_interval_ms", 10_000)
    }

    fn check(&self, ctx: &CheckerContext<'_>) -> CheckResult {
        let now = orpc::common::LocalTime::mills();
        let mut result = CheckResult::default();

        // 1. Detect heartbeat timeouts: Live → Lost in memory
        let timeout = ctx.node_manager.heartbeat_timeout_ms();
        let newly_lost = ctx.node_manager.detect_heartbeat_timeout(now, timeout);
        for &node_id in &newly_lost {
            if let Err(e) = ctx.node_manager.persist_node(node_id) {
                log::warn!("persist node {} Lost state failed: {}", node_id, e);
            }
        }

        // 2. Process Lost workers
        let recovery_window = self.ctx.config_manager.get_u64(
            crate::pd::config::keys::PD_NODE_LOST_RECOVERY_WINDOW_MS,
            crate::pd::config::keys::PD_NODE_LOST_RECOVERY_WINDOW_MS_DEFAULT,
        );

        let lost_workers: Vec<_> = ctx
            .node_manager
            .get_nodes_by_type(NodeType::Worker)
            .into_iter()
            .filter(|n| n.state == NodeState::Lost)
            .collect();

        for worker in &lost_workers {
            let node_id = worker.base.node_id;

            if let Some(entry) = self.processed_lost_nodes.get(&node_id) {
                let lost_time = *entry;
                if now.saturating_sub(lost_time) > recovery_window {
                    if let Err(e) = self.handle_node_offline(ctx, node_id) {
                        log::error!("handle_node_offline {} failed: {}", node_id, e);
                    }
                }
                continue;
            }

            self.processed_lost_nodes.insert(node_id, now);

            let affected_bgs = ctx.bg_manager.get_bgs_on_worker(node_id);
            for bg in &affected_bgs {
                if let Err(e) =
                    self.ctx
                        .propose_bg_state(bg.bg_id, curvine_common::state::BGState::Degraded)
                {
                    log::warn!("propose_bg_state Degraded {} failed: {}", bg.bg_id, e);
                }
            }
            if !affected_bgs.is_empty() {
                log::warn!(
                    "Worker {} marked Lost, {} BGs degraded",
                    node_id,
                    affected_bgs.len()
                );
            }
        }

        // 3. Lease expiry check (merged from LeaseChecker)
        let expired = ctx.bg_manager.get_bgs_with_expired_lease(now);
        for bg in expired {
            let new_owner = bg
                .replica_set
                .iter()
                .find(|w| ctx.pool_manager.is_worker_available(**w))
                .copied();
            if let Some(new_worker) = new_owner {
                let old_worker = bg.lease_owner.node_id;
                if new_worker != old_worker {
                    let op = BGOperator {
                        id: 0,
                        bg_id: bg.bg_id,
                        description: format!("Transfer lease to {}", new_worker),
                        steps: vec![OpStep::TransferLease {
                            from_worker: old_worker,
                            to_worker: new_worker,
                        }],
                        current_step: 0,
                        status: OpStatus::Pending,
                        create_time_ms: now,
                        priority: 80,
                    };
                    result.bg_operators.push(op);
                }
            }
        }

        self.cleanup_recovered_nodes(ctx);

        result
    }
}

impl NodeHealthChecker {
    fn handle_node_offline(
        &self,
        ctx: &CheckerContext<'_>,
        node_id: u32,
    ) -> curvine_common::FsResult<()> {
        log::error!(
            "Worker {} exceeded recovery window, marking Offline",
            node_id
        );

        ctx.node_manager
            .update_state_and_persist(node_id, NodeState::Offline)?;

        self.ctx.pool_manager.remove_worker_from_pools(node_id)?;

        self.processed_lost_nodes.remove(&node_id);
        Ok(())
    }

    fn cleanup_recovered_nodes(&self, ctx: &CheckerContext<'_>) {
        let recovered: Vec<u32> = self
            .processed_lost_nodes
            .iter()
            .filter(|entry| {
                let node_id = *entry.key();
                ctx.node_manager
                    .get_node(node_id)
                    .map(|n| n.state != NodeState::Lost)
                    .unwrap_or(false)
            })
            .map(|entry| *entry.key())
            .collect();

        for node_id in recovered {
            self.processed_lost_nodes.remove(&node_id);
            log::info!("Worker {} recovered from Lost state", node_id);
        }
    }
}
