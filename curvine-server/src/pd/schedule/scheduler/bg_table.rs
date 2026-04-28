use super::Scheduler;
use crate::pd::config::keys;
use crate::pd::node::{NodeEvent, NodeEventType};
use crate::pd::schedule::{BGOperator, ManagerContext, OpPriority, OperatorBuilder, OperatorKind};
use curvine_common::state::{BGOpState, NodeType, ReplicaState};
use dashmap::DashMap;
use std::cmp::{max, min};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const BGTABLE_MIN_INTERVAL: Duration = Duration::from_secs(5);
const BGTABLE_MAX_INTERVAL: Duration = Duration::from_secs(60);
const BGTABLE_BACKOFF_FACTOR: f64 = 1.5;

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
    pub generation: u64,
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
    next_generation: AtomicU64,
}

impl BGTableScheduler {
    pub fn new(ctx: Arc<ManagerContext>) -> Self {
        Self {
            ctx,
            pending_rebuilds: DashMap::new(),
            next_generation: AtomicU64::new(1),
        }
    }

    pub fn schedule_rebuild(&self, pool_ids: Vec<u16>, reason: RebuildReason) {
        let auto_enabled = self.ctx.config_manager.get_bool(
            keys::PD_BG_REBUILD_AUTO_ENABLED,
            keys::PD_BG_REBUILD_AUTO_ENABLED_DEFAULT,
        );

        if !auto_enabled {
            log::info!(
                "Auto rebuild disabled, skipping rebuild for pools {:?}",
                pool_ids
            );
            return;
        }

        let cooldown = self.ctx.config_manager.get_u64(
            keys::PD_BG_REBUILD_COOLDOWN_MS,
            keys::PD_BG_REBUILD_COOLDOWN_MS_DEFAULT,
        );
        let scheduled_time = orpc::common::LocalTime::mills() + cooldown;

        for pool_id in pool_ids {
            self.pending_rebuilds
                .entry(pool_id)
                .and_modify(|task| {
                    task.reason = merge_reasons(&task.reason, &reason);
                    task.scheduled_time_ms = task.scheduled_time_ms.max(scheduled_time);
                    task.generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
                })
                .or_insert_with(|| RebuildTask {
                    pool_id,
                    reason: reason.clone(),
                    scheduled_time_ms: scheduled_time,
                    generation: self.next_generation.fetch_add(1, Ordering::Relaxed),
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
                let max_replace = max(1, old_replicas.len() / 2);
                let replace_count = min(added.len(), max_replace).min(removed.len());
                let added =
                    &added[..min(added.len(), replace_count.max(added.len().min(max_replace)))];
                let removed = &removed[..min(removed.len(), replace_count.max(1))];

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
        let ready: Vec<(u16, u64, RebuildReason)> = self
            .pending_rebuilds
            .iter()
            .filter(|e| e.value().scheduled_time_ms <= now)
            .map(|e| (*e.key(), e.value().generation, e.value().reason.clone()))
            .collect();

        let mut all_ops = Vec::new();
        for (pool_id, gen, reason) in ready {
            let removed = self
                .pending_rebuilds
                .remove_if(&pool_id, |_, v| v.generation == gen);
            if removed.is_none() {
                log::debug!(
                    "pool {}: rebuild task updated concurrently (gen mismatch), deferring",
                    pool_id
                );
                continue;
            }
            log::info!(
                "Rebuilding BGTable for pool {} (reason: {:?})",
                pool_id,
                reason
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
        BGTABLE_MIN_INTERVAL
    }

    fn next_interval(&self, current: Duration) -> Duration {
        let ms = (current.as_millis() as f64 * BGTABLE_BACKOFF_FACTOR) as u64;
        Duration::from_millis(ms).min(BGTABLE_MAX_INTERVAL)
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
    use crate::pd::pool::POOL_ID_SSD;
    use crate::pd::schedule::checker::tests_common::{test_context, Fixture};
    use std::collections::HashMap;

    #[test]
    fn name_and_type() {
        let ctx = test_context(HashMap::new());
        let s = BGTableScheduler::new(ctx);
        assert_eq!(s.name(), "bg-table-scheduler");
    }

    #[test]
    fn interval_backoff() {
        let ctx = test_context(HashMap::new());
        let s = BGTableScheduler::new(ctx);
        // min_interval = 5s; next_interval grows by 1.5x up to 60s.
        assert_eq!(s.min_interval(), Duration::from_secs(5));
        let i1 = s.next_interval(s.min_interval());
        assert!(i1 > Duration::from_secs(5) && i1 <= Duration::from_secs(60));
        // Converges to the cap.
        let mut cur = s.min_interval();
        for _ in 0..20 {
            cur = s.next_interval(cur);
        }
        assert_eq!(cur, Duration::from_secs(60));
    }

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

    fn scheduler_for(f: &Fixture) -> BGTableScheduler {
        BGTableScheduler::new(f.ctx.clone())
    }

    #[test]
    fn table_init_skips_when_not_enough_workers() {
        // Only 2 workers but replica_count=3 → skip; no Raft propose attempted.
        let f = Fixture::new();
        f.add_workers(&[100, 101], POOL_ID_SSD);
        let s = scheduler_for(&f);

        s.check_table_initialization();

        let table_id = ((POOL_ID_SSD as u32) << 16) | 3;
        assert!(
            f.ctx.bg_manager.get_table(table_id).is_none(),
            "table must not be initialized with only 2 workers for replica_count=3"
        );
    }

    #[test]
    fn table_init_skips_when_table_already_exists() {
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102], POOL_ID_SSD);
        let existing_id = f.insert_table(POOL_ID_SSD, 3);
        let s = scheduler_for(&f);

        // Should see the existing table and short-circuit without touching Raft.
        s.check_table_initialization();

        assert!(f.ctx.bg_manager.get_table(existing_id).is_some());
    }

    #[test]
    fn schedule_rebuild_stores_task_with_cooldown() {
        let f = Fixture::new();
        let s = scheduler_for(&f);
        let before = orpc::common::LocalTime::mills();

        s.schedule_rebuild(
            vec![POOL_ID_SSD],
            RebuildReason::NodeJoined {
                node_ids: vec![100],
            },
        );

        let task = s.pending_rebuilds.get(&POOL_ID_SSD).expect("pending task");
        // Default cooldown is 60_000 ms.
        assert!(
            task.scheduled_time_ms >= before + 60_000,
            "task.scheduled_time_ms should be at least now+cooldown"
        );
        matches!(task.reason, RebuildReason::NodeJoined { .. });
    }

    #[test]
    fn schedule_rebuild_merges_same_pool() {
        let f = Fixture::new();
        let s = scheduler_for(&f);

        s.schedule_rebuild(
            vec![POOL_ID_SSD],
            RebuildReason::NodeJoined {
                node_ids: vec![100],
            },
        );
        s.schedule_rebuild(
            vec![POOL_ID_SSD],
            RebuildReason::NodeJoined {
                node_ids: vec![101],
            },
        );

        let task = s.pending_rebuilds.get(&POOL_ID_SSD).unwrap();
        match &task.reason {
            RebuildReason::NodeJoined { node_ids } => assert_eq!(node_ids, &vec![100, 101]),
            _ => panic!("expected merged NodeJoined"),
        }
    }

    #[test]
    fn schedule_rebuild_takes_later_scheduled_time() {
        let f = Fixture::new();
        let s = scheduler_for(&f);

        s.schedule_rebuild(vec![POOL_ID_SSD], RebuildReason::Manual);
        let first = s
            .pending_rebuilds
            .get(&POOL_ID_SSD)
            .unwrap()
            .scheduled_time_ms;

        // Call again — should not decrease scheduled_time_ms.
        std::thread::sleep(std::time::Duration::from_millis(5));
        s.schedule_rebuild(vec![POOL_ID_SSD], RebuildReason::Manual);
        let second = s
            .pending_rebuilds
            .get(&POOL_ID_SSD)
            .unwrap()
            .scheduled_time_ms;

        assert!(second >= first, "scheduled_time_ms must be monotonic (max)");
    }

    #[test]
    fn schedule_rebuild_disabled_by_config() {
        let f = Fixture::with_overrides(
            [(keys::PD_BG_REBUILD_AUTO_ENABLED, "false")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        let s = scheduler_for(&f);

        s.schedule_rebuild(vec![POOL_ID_SSD], RebuildReason::Manual);

        assert!(
            s.pending_rebuilds.is_empty(),
            "auto_enabled=false must skip pending insertion"
        );
    }

    fn make_event(event_type: NodeEventType, node_type: NodeType, node_id: u32) -> NodeEvent {
        NodeEvent {
            event_type,
            node_id,
            node_type,
            old_state: None,
            new_state: None,
            epoch: 1,
            event_time_ms: 0,
        }
    }

    struct EventCase {
        name: &'static str,
        event_type: NodeEventType,
        node_type: NodeType,
        expect_pending: bool,
        expect_reason_matches_removed: bool,
    }

    fn event_cases() -> Vec<EventCase> {
        vec![
            EventCase {
                name: "Worker Registered → NodeJoined",
                event_type: NodeEventType::Registered,
                node_type: NodeType::Worker,
                expect_pending: true,
                expect_reason_matches_removed: false,
            },
            EventCase {
                name: "Worker Offline → NodeRemoved",
                event_type: NodeEventType::Offline,
                node_type: NodeType::Worker,
                expect_pending: true,
                expect_reason_matches_removed: true,
            },
            EventCase {
                name: "Worker DecommissionFinished → NodeRemoved",
                event_type: NodeEventType::DecommissionFinished,
                node_type: NodeType::Worker,
                expect_pending: true,
                expect_reason_matches_removed: true,
            },
            EventCase {
                name: "Worker Lost → no-op",
                event_type: NodeEventType::Lost,
                node_type: NodeType::Worker,
                expect_pending: false,
                expect_reason_matches_removed: false,
            },
            EventCase {
                name: "Meta Registered → no-op",
                event_type: NodeEventType::Registered,
                node_type: NodeType::Meta,
                expect_pending: false,
                expect_reason_matches_removed: false,
            },
        ]
    }

    #[test]
    fn on_event_dispatch_table_driven() {
        for case in event_cases() {
            let f = Fixture::new();
            // Register worker 100 and add to pool so get_pools_by_worker returns SSD.
            f.add_worker(100, POOL_ID_SSD, &[]);
            let s = scheduler_for(&f);

            let event = make_event(case.event_type, case.node_type, 100);
            s.on_event(&event);

            if case.expect_pending {
                let task = s.pending_rebuilds.get(&POOL_ID_SSD);
                assert!(task.is_some(), "{}: expected a pending task", case.name);
                if case.expect_reason_matches_removed {
                    matches!(task.unwrap().reason, RebuildReason::NodeRemoved { .. });
                }
            } else {
                assert!(
                    s.pending_rebuilds.is_empty(),
                    "{}: expected no pending task, got {:?}",
                    case.name,
                    s.pending_rebuilds
                        .iter()
                        .map(|e| *e.key())
                        .collect::<Vec<_>>(),
                );
            }
        }
    }

    #[test]
    fn check_and_execute_skips_future_tasks() {
        let f = Fixture::new();
        let s = scheduler_for(&f);

        // Default cooldown is 60s → scheduled_time > now, task should stay pending.
        s.schedule_rebuild(vec![POOL_ID_SSD], RebuildReason::Manual);
        let before = s.pending_rebuilds.len();

        let ops = s.check_and_execute_rebuilds();
        assert!(ops.is_empty(), "future-scheduled tasks produce no ops");
        assert_eq!(
            s.pending_rebuilds.len(),
            before,
            "task must remain in pending until its scheduled_time"
        );
    }

    #[test]
    fn check_and_execute_releases_ready_tasks() {
        let f = Fixture::new();
        let s = scheduler_for(&f);

        // Manually insert a task whose scheduled_time is in the past.
        s.pending_rebuilds.insert(
            POOL_ID_SSD,
            RebuildTask {
                pool_id: POOL_ID_SSD,
                reason: RebuildReason::Manual,
                scheduled_time_ms: 0, // already past
                generation: 1,
            },
        );

        let _ops = s.check_and_execute_rebuilds();
        assert!(
            s.pending_rebuilds.is_empty(),
            "ready task must be removed from pending"
        );
    }

    #[test]
    fn check_and_execute_defers_when_generation_changed() {
        // Simulates the race: snapshot captures gen=G, but before remove_if runs a
        // concurrent on_event bumps the entry's generation. The snapshot should be
        // skipped and the entry retained for the next cycle.
        let f = Fixture::new();
        let s = scheduler_for(&f);

        // Seed a "ready" task at gen=5.
        s.pending_rebuilds.insert(
            POOL_ID_SSD,
            RebuildTask {
                pool_id: POOL_ID_SSD,
                reason: RebuildReason::NodeJoined {
                    node_ids: vec![100],
                },
                scheduled_time_ms: 0,
                generation: 5,
            },
        );
        // Prime the counter past gen=5 so the next write produces gen>5.
        s.next_generation.store(6, Ordering::Relaxed);

        // Manually mimic the ordering we're guarding: perform the iter-snapshot now,
        // then race in a `schedule_rebuild` that bumps generation, then run the remove.
        let snapshot_gen = s
            .pending_rebuilds
            .get(&POOL_ID_SSD)
            .map(|e| e.value().generation)
            .unwrap();
        assert_eq!(snapshot_gen, 5);

        // Concurrent write: bumps generation to 6.
        s.schedule_rebuild(
            vec![POOL_ID_SSD],
            RebuildReason::NodeRemoved {
                node_ids: vec![999],
            },
        );
        assert!(s.pending_rebuilds.get(&POOL_ID_SSD).unwrap().generation > snapshot_gen);

        // Now invoke check_and_execute: since the entry's gen != snapshot gen, the
        // remove_if predicate fails → entry stays in pending.
        let ops = s.check_and_execute_rebuilds();
        assert!(
            ops.is_empty(),
            "no op dispatched when generation mismatched"
        );
        assert!(
            s.pending_rebuilds.contains_key(&POOL_ID_SSD),
            "mismatched-generation entry must survive for next cycle"
        );
    }
}
