use super::Scheduler;
use crate::pd::node::{NodeEvent, NodeEventType};
use crate::pd::schedule::operator::{BGOperator, OpPriority, OperatorBuilder, OperatorKind};
use crate::pd::schedule::ManagerContext;
use curvine_common::state::{BGOpState, NodeType, ReplicaState};
use dashmap::DashMap;
use std::collections::HashSet;
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
///
/// `check_table_initialization` proposes new tables directly via Raft (no concurrent
/// state to conflict with — new table). Rebuild produces `Rebuild` operators that
/// flow through the operator pipeline (with Burst-class rate limiting for fast
/// post-expansion rebalance).
pub struct BGTableScheduler {
    ctx: Arc<ManagerContext>,
    pending_rebuilds: DashMap<u16, RebuildTask>,
}

impl BGTableScheduler {
    pub fn new(ctx: Arc<ManagerContext>) -> Self {
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
                    log::warn!(
                        "No enough workers for pool {} with {} buckets, {} replicas, {} workers",
                        pool.pool_id,
                        bucket_count,
                        replica_count,
                        workers.len()
                    );
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
                    log::error!("Failed to create BGTable for pool {}: {}", pool.pool_id, e);
                }
            }
        }
    }

    /// Compute rebuild diff for the pool's tables and produce Rebuild operators.
    fn compute_rebuild_operators(&self, pool_id: u16) -> Vec<BGOperator> {
        let mut ops = Vec::new();
        let table_ids: Vec<u32> = self
            .ctx
            .bg_manager
            .list_tables()
            .into_iter()
            .filter(|t| t.pool_id() == pool_id)
            .map(|t| t.table_id)
            .collect();

        for table_id in table_ids {
            let diff = match self.ctx.bg_manager.compute_rebuild_diff(table_id) {
                Ok(d) => d,
                Err(e) => {
                    log::error!("compute_rebuild_diff for table {} failed: {}", table_id, e);
                    continue;
                }
            };

            for (new_bg, old_replicas) in diff {
                // Skip BGs already under operation.
                if new_bg.op_state != BGOpState::Idle {
                    continue;
                }
                let old_set: HashSet<u32> = old_replicas.iter().copied().collect();
                let new_set: HashSet<u32> = new_bg.replica_set.iter().copied().collect();
                let added: Vec<u32> = new_set.difference(&old_set).copied().collect();
                let removed: Vec<u32> = old_set.difference(&new_set).copied().collect();
                if added.is_empty() && removed.is_empty() {
                    continue;
                }

                // Safety: limit single-round replacements to preserve Active replica count.
                let max_replace = std::cmp::max(1, old_replicas.len() / 2);
                let replace_count = std::cmp::min(added.len(), max_replace).min(removed.len());
                let added = &added
                    [..std::cmp::min(added.len(), replace_count.max(added.len().min(max_replace)))];
                let removed = &removed[..std::cmp::min(removed.len(), replace_count.max(1))];

                let mut builder = OperatorBuilder::new(
                    OperatorKind::Rebuild,
                    new_bg.bg_id,
                    format!(
                        "Rebuild bg {}: add {:?}, remove {:?}",
                        new_bg.bg_id, added, removed
                    ),
                )
                .bg_epoch(new_bg.bg_epoch.saturating_sub(1))
                .priority(OpPriority::REBUILD);

                for w in added {
                    builder = builder.add_replica(*w);
                    builder = builder.wait_replica_ready(*w, ReplicaState::Active);
                }

                // Lease transfer if owner changed.
                if let Some(new_lease) = &new_bg.lease_owner {
                    let old_owner = old_replicas
                        .iter()
                        .find(|w| !new_set.contains(w))
                        .copied()
                        .unwrap_or(0);
                    if old_owner != 0 && old_owner != new_lease.node_id {
                        builder = builder.transfer_lease(old_owner, new_lease.node_id);
                    }
                }

                for w in removed {
                    builder = builder.remove_replica(*w);
                }

                ops.push(builder.build());
            }
        }
        ops
    }

    fn check_and_execute_rebuilds(&self) -> Vec<BGOperator> {
        let now = orpc::common::LocalTime::mills();
        let ready: Vec<(u16, RebuildTask)> = self
            .pending_rebuilds
            .iter()
            .filter(|e| e.value().scheduled_time_ms <= now)
            .map(|e| (*e.key(), e.value().clone()))
            .collect();

        let mut all_ops = Vec::new();
        for (pool_id, task) in ready {
            self.pending_rebuilds.remove(&pool_id);
            log::info!(
                "Rebuilding BGTable for pool {} (reason: {:?})",
                pool_id,
                task.reason
            );
            all_ops.extend(self.compute_rebuild_operators(pool_id));
        }
        all_ops
    }
}

impl Scheduler for BGTableScheduler {
    fn name(&self) -> &str {
        "bg-table-scheduler"
    }

    fn schedule(&self, _ctx: &ManagerContext) -> Vec<BGOperator> {
        self.check_table_initialization();
        self.check_and_execute_rebuilds()
    }

    fn is_schedule_allowed(&self, _ctx: &ManagerContext) -> bool {
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
        let ctx = crate::pd::schedule::checker::tests_common::test_context(
            std::collections::HashMap::new(),
        );
        let s = BGTableScheduler::new(ctx);
        assert_eq!(s.name(), "bg-table-scheduler");
    }
}
