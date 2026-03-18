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

use super::operator::RebuildReason;
use super::CoordinatorContext;
use crate::pd::node::NodeEvent;
use curvine_common::state::{NodeState, NodeType};
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct RebuildTask {
    pub pool_id: u16,
    pub reason: RebuildReason,
    pub scheduled_time_ms: u64,
}

pub struct RebuildScheduler {
    ctx: Arc<CoordinatorContext>,
    pending_rebuilds: DashMap<u16, RebuildTask>,
}

impl RebuildScheduler {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        Self {
            ctx,
            pending_rebuilds: DashMap::new(),
        }
    }

    pub fn schedule_rebuild(&self, pool_ids: Vec<u16>, reason: RebuildReason) {
        let auto_enabled = self
            .ctx
            .config_manager
            .get_bool("pd.bg.rebuild.auto_enabled", true);

        if !auto_enabled {
            log::info!(
                "Auto rebuild disabled, skipping rebuild for pools {:?}",
                pool_ids
            );
            return;
        }

        let cooldown = self
            .ctx
            .config_manager
            .get_u64("pd.bg.rebuild.cooldown_ms", 60_000);
        let scheduled_time = orpc::common::LocalTime::mills() + cooldown;

        for pool_id in pool_ids {
            self.pending_rebuilds
                .entry(pool_id)
                .and_modify(|task| {
                    task.reason = merge_reasons(&task.reason, &reason);
                    task.scheduled_time_ms = task.scheduled_time_ms.max(scheduled_time);
                })
                .or_insert(RebuildTask {
                    pool_id,
                    reason: reason.clone(),
                    scheduled_time_ms: scheduled_time,
                });
        }
    }

    pub async fn check_and_rebuild(&self) {
        let now = orpc::common::LocalTime::mills();

        // BGTable initialization: pools with workers but no table
        self.check_table_initialization();

        let ready: Vec<(u16, RebuildTask)> = self
            .pending_rebuilds
            .iter()
            .filter(|e| e.value().scheduled_time_ms <= now)
            .map(|e| (*e.key(), e.value().clone()))
            .collect();

        for (pool_id, task) in ready {
            self.pending_rebuilds.remove(&pool_id);
            if let Err(e) = self.execute_rebuild(pool_id, &task.reason) {
                log::error!("Rebuild BGTable for pool {} failed: {}", pool_id, e);
            }
        }
    }

    /// Check if any active pool lacks a BGTable and create one.
    fn check_table_initialization(&self) {
        let active_pools = self.ctx.pool_manager.list_active_pools();
        let bucket_count = self
            .ctx
            .config_manager
            .get_u32("pd.bg.default_bucket_count", 1024);
        let replica_count = self
            .ctx
            .config_manager
            .get_u32("pd.bg.default_replica_count", 3) as u16;

        for pool in active_pools {
            if self.ctx.bg_manager.has_table_for_pool(pool.pool_id) {
                continue;
            }
            let workers: Vec<u32> = pool
                .workers
                .iter()
                .copied()
                .filter(|w| self.ctx.pool_manager.is_worker_available(*w))
                .collect();
            if workers.len() < replica_count as usize {
                continue;
            }
            log::info!(
                "Initializing BGTable for pool {} with {} buckets, {} replicas, {} workers",
                pool.pool_id,
                bucket_count,
                replica_count,
                workers.len()
            );
            if let Err(e) = self.ctx.bg_manager.create_table(
                pool.pool_id,
                bucket_count,
                replica_count,
                &workers,
            ) {
                log::error!(
                    "Failed to create BGTable for pool {}: {}",
                    pool.pool_id,
                    e
                );
            }
        }
    }

    fn execute_rebuild(
        &self,
        pool_id: u16,
        reason: &RebuildReason,
    ) -> curvine_common::FsResult<()> {
        log::info!(
            "Rebuilding BGTable for pool {} (reason: {:?})",
            pool_id,
            reason
        );

        self.ctx.bg_manager.rebuild_tables_for_pool(pool_id)?;

        log::info!("BGTable for pool {} rebuild completed", pool_id);
        Ok(())
    }
}

impl super::checker::Scheduler for RebuildScheduler {
    fn name(&self) -> &str {
        "rebuild-scheduler"
    }

    fn on_event(&self, event: &NodeEvent) {
        match event {
            NodeEvent::Registered {
                node_type: NodeType::Worker,
                pool_ids,
                ..
            } => {
                if !pool_ids.is_empty() {
                    self.schedule_rebuild(
                        pool_ids.clone(),
                        RebuildReason::NodeJoined {
                            node_ids: vec![],
                        },
                    );
                }
            }
            NodeEvent::StateChanged {
                node_id,
                node_type: NodeType::Worker,
                new_state: NodeState::Offline,
                ..
            } => {
                let pool_ids = self.ctx.pool_manager.get_pools_by_worker(*node_id);
                if !pool_ids.is_empty() {
                    self.schedule_rebuild(
                        pool_ids,
                        RebuildReason::NodeRemoved {
                            node_ids: vec![*node_id],
                        },
                    );
                }
            }
            _ => {}
        }
    }

    fn tick(&self) {
        self.check_table_initialization();
    }
}

fn merge_reasons(existing: &RebuildReason, new: &RebuildReason) -> RebuildReason {
    match (existing, new) {
        (
            RebuildReason::NodeJoined { node_ids: ids1 },
            RebuildReason::NodeJoined { node_ids: ids2 },
        ) => {
            let mut merged = ids1.clone();
            merged.extend(ids2.iter().copied());
            RebuildReason::NodeJoined { node_ids: merged }
        }
        (
            RebuildReason::NodeRemoved { node_ids: ids1 },
            RebuildReason::NodeRemoved { node_ids: ids2 },
        ) => {
            let mut merged = ids1.clone();
            merged.extend(ids2.iter().copied());
            RebuildReason::NodeRemoved { node_ids: merged }
        }
        _ => new.clone(),
    }
}
