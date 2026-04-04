use super::{Scheduler, SchedulerContext};
use crate::pd::node::{NodeEvent, NodeEventType};
use crate::pd::schedule::operator::BGOperator;
use crate::pd::schedule::CoordinatorContext;
use curvine_common::state::NodeType;
use curvine_common::FsResult;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;

/// Reason for BGTable rebuild.
#[derive(Clone, Debug)]
pub enum RebuildReason {
    NodeJoined { node_ids: Vec<u32> },
    NodeRemoved { node_ids: Vec<u32> },
    Manual,
}

#[derive(Clone)]
pub struct RebuildTask {
    pub pool_id: u16,
    pub reason: RebuildReason,
    pub scheduled_time_ms: u64,
}

/// BGTable scheduler: handles initial table creation and event-driven rebuild
/// with cooldown and reason merging.
pub struct BGTableScheduler {
    ctx: Arc<CoordinatorContext>,
    pending_rebuilds: DashMap<u16, RebuildTask>,
}

impl BGTableScheduler {
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

    /// Check if any active pool lacks a BGTable and create one.
    pub fn check_table_initialization(&self) {
        let active_pools = self.ctx.pool_manager.list_active_pools();
        let bucket_count = self.ctx.bg_manager.bucket_count();
        let replica_counts = self.ctx.bg_manager.replica_counts().to_vec();

        for pool in active_pools {
            for &replica_count in &replica_counts {
                let table_id = ((pool.pool_id as u32) << 16) | (replica_count as u32);
                if self.ctx.bg_manager.get_table(table_id).is_some() {
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

    fn check_and_execute_rebuilds(&self) {
        let now = orpc::common::LocalTime::mills();
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
}

impl Scheduler for BGTableScheduler {
    fn name(&self) -> &str {
        "bg-table-scheduler"
    }

    fn scheduler_type(&self) -> &str {
        "bg-table"
    }

    fn schedule(&self, _ctx: &SchedulerContext<'_>) -> Vec<BGOperator> {
        self.check_table_initialization();
        self.check_and_execute_rebuilds();
        Vec::new() // BGTableScheduler doesn't produce operators
    }

    fn is_schedule_allowed(&self, _ctx: &SchedulerContext<'_>) -> bool {
        true
    }

    fn min_interval(&self) -> Duration {
        Duration::from_secs(5) // Same as old rebuild_loop
    }

    fn next_interval(&self, current: Duration) -> Duration {
        current // Fixed interval for table management
    }

    fn on_event(&self, event: &NodeEvent) {
        match event.event_type {
            NodeEventType::Registered if event.node_type == NodeType::Worker => {
                let pool_ids = self.ctx.pool_manager.get_pools_by_worker(event.node_id);
                if !pool_ids.is_empty() {
                    self.schedule_rebuild(
                        pool_ids,
                        RebuildReason::NodeJoined {
                            node_ids: vec![event.node_id],
                        },
                    );
                }
            }
            NodeEventType::Offline if event.node_type == NodeType::Worker => {
                let pool_ids = self.ctx.pool_manager.get_pools_by_worker(event.node_id);
                if !pool_ids.is_empty() {
                    self.schedule_rebuild(
                        pool_ids,
                        RebuildReason::NodeRemoved {
                            node_ids: vec![event.node_id],
                        },
                    );
                }
            }
            NodeEventType::DecommissionFinished if event.node_type == NodeType::Worker => {
                let pool_ids = self.ctx.pool_manager.get_pools_by_worker(event.node_id);
                if !pool_ids.is_empty() {
                    self.schedule_rebuild(
                        pool_ids,
                        RebuildReason::NodeRemoved {
                            node_ids: vec![event.node_id],
                        },
                    );
                }
            }
            _ => {}
        }
    }

    fn encode_config(&self) -> FsResult<serde_json::Value> {
        Ok(serde_json::json!({
            "type": self.scheduler_type(),
            "pending_rebuilds": self.pending_rebuilds.len(),
        }))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_same_type_joined() {
        let a = RebuildReason::NodeJoined { node_ids: vec![1] };
        let b = RebuildReason::NodeJoined { node_ids: vec![2] };
        match merge_reasons(&a, &b) {
            RebuildReason::NodeJoined { node_ids } => assert_eq!(node_ids, vec![1, 2]),
            _ => panic!("expected NodeJoined"),
        }
    }

    #[test]
    fn merge_same_type_removed() {
        let a = RebuildReason::NodeRemoved { node_ids: vec![3] };
        let b = RebuildReason::NodeRemoved {
            node_ids: vec![4, 5],
        };
        match merge_reasons(&a, &b) {
            RebuildReason::NodeRemoved { node_ids } => assert_eq!(node_ids, vec![3, 4, 5]),
            _ => panic!("expected NodeRemoved"),
        }
    }

    #[test]
    fn merge_different_types_uses_new() {
        let a = RebuildReason::NodeJoined { node_ids: vec![1] };
        let b = RebuildReason::Manual;
        match merge_reasons(&a, &b) {
            RebuildReason::Manual => {}
            _ => panic!("expected Manual"),
        }
    }

    #[test]
    fn name_and_type() {
        let ctx = crate::pd::schedule::checker::tests_common::test_coordinator_context(
            std::collections::HashMap::new(),
        );
        let s = BGTableScheduler::new(ctx);
        assert_eq!(s.name(), "bg-table-scheduler");
        assert_eq!(s.scheduler_type(), "bg-table");
    }
}
