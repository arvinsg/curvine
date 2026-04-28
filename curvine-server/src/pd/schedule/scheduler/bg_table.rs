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
use crate::pd::config::keys;
use crate::pd::node::{NodeEvent, NodeEventType};
use crate::pd::schedule::{BGOperator, ManagerContext, OpPriority, OperatorBuilder, OperatorKind};
use curvine_common::state::{gen_table_id, BGOpState, NodeType, ReplicaState};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BGTABLE_MIN_INTERVAL: Duration = Duration::from_secs(5);
const BGTABLE_MAX_INTERVAL: Duration = Duration::from_secs(60);
const BGTABLE_BACKOFF_FACTOR: f64 = 1.5;

#[derive(Clone, Debug)]
pub struct PendingPoolRebuild {
    pub scheduled_time_ms: u64,
}

/// BGTable scheduler: two responsibilities keyed by `pool_id`:
///
/// 1. **Initialize** — when a pool has no BGTable for some configured `replica_count`,
///    wait for `cooldown` of quiescence (no further node joins) before creating it.
///
/// 2. **Rebuild** — when a new worker joins a pool that already has a table, wait
///    `cooldown` of quiescence and then re-plan placement via the rebuild diff.
pub struct BGTableScheduler {
    ctx: Arc<ManagerContext>,
    pending: Mutex<HashMap<u16, PendingPoolRebuild>>,
}

impl BGTableScheduler {
    pub fn new(ctx: Arc<ManagerContext>) -> Self {
        Self {
            ctx,
            pending: Mutex::new(HashMap::new()),
        }
    }

    fn cooldown_ms(&self) -> u64 {
        self.ctx
            .config_manager
            .get_u64(keys::PD_BG_REBUILD_COOLDOWN_MS)
    }

    fn enqueue(&self, pool_id: u16) {
        let deadline = orpc::common::LocalTime::mills() + self.cooldown_ms();
        let mut guard = self.pending.lock().unwrap();
        let entry = guard.entry(pool_id).or_insert(PendingPoolRebuild {
            scheduled_time_ms: deadline,
        });
        entry.scheduled_time_ms = entry.scheduled_time_ms.max(deadline);
    }

    /// Manual trigger.
    pub fn request_rebuild(&self, pool_ids: Vec<u16>) {
        for pool_id in pool_ids {
            self.enqueue(pool_id);
        }
    }

    fn has_table(&self, pool_id: u16, replica_count: u16) -> bool {
        self.ctx
            .bg_manager
            .get_table(gen_table_id(pool_id, replica_count))
            .is_some()
    }

    /// Has any table for any configured replica_count on this pool?
    fn has_any_table(&self, pool_id: u16) -> bool {
        self.ctx
            .bg_manager
            .replica_counts()
            .iter()
            .any(|&rc| self.has_table(pool_id, rc))
    }

    fn bootstrap_missing_tables(&self) {
        let deadline = orpc::common::LocalTime::mills() + self.cooldown_ms();
        let pools_needing_init: Vec<u16> = self
            .ctx
            .pool_manager
            .list_active_pools()
            .into_iter()
            .filter(|p| !self.has_any_table(p.pool_id))
            .map(|p| p.pool_id)
            .collect();

        let mut guard = self.pending.lock().unwrap();
        for pool_id in pools_needing_init {
            if let Entry::Vacant(v) = guard.entry(pool_id) {
                log::debug!("bootstrap: seeding Init for pool {}", pool_id);
                v.insert(PendingPoolRebuild {
                    scheduled_time_ms: deadline,
                });
            }
        }
    }

    fn check_and_execute(&self) -> Vec<BGOperator> {
        let now = orpc::common::LocalTime::mills();
        let auto_enabled = self.auto_rebuild_enabled();

        let pure_init_pools = self.pools_needing_only_init();
        let drained = self.drain_ready_pools(now, auto_enabled, &pure_init_pools);
        self.dispatch_drained_pools(drained, auto_enabled)
    }

    fn auto_rebuild_enabled(&self) -> bool {
        self.ctx
            .config_manager
            .get_bool(keys::PD_BG_REBUILD_AUTO_ENABLED)
    }

    fn pools_needing_only_init(&self) -> HashSet<u16> {
        let pending_pool_ids: Vec<u16> = {
            let guard = self.pending.lock().unwrap();
            guard.keys().copied().collect()
        };
        pending_pool_ids
            .into_iter()
            .filter(|&pid| !self.has_any_table(pid))
            .collect()
    }

    fn drain_ready_pools(
        &self,
        now: u64,
        auto_enabled: bool,
        pure_init_pools: &HashSet<u16>,
    ) -> Vec<u16> {
        let mut guard = self.pending.lock().unwrap();
        let ready: Vec<u16> = guard
            .iter()
            .filter(|(pid, v)| {
                v.scheduled_time_ms <= now && (auto_enabled || pure_init_pools.contains(pid))
            })
            .map(|(k, _)| *k)
            .collect();
        for k in &ready {
            guard.remove(k);
        }
        ready
    }

    fn dispatch_drained_pools(&self, pool_ids: Vec<u16>, auto_enabled: bool) -> Vec<BGOperator> {
        let mut ops = Vec::new();
        for pool_id in pool_ids {
            for &rc in self.ctx.bg_manager.replica_counts() {
                if self.has_table(pool_id, rc) {
                    if auto_enabled {
                        ops.extend(self.build_rebuild_ops(gen_table_id(pool_id, rc)));
                    }
                } else {
                    self.try_initialize_table(pool_id, rc);
                }
            }
        }
        ops
    }

    fn try_initialize_table(&self, pool_id: u16, replica_count: u16) {
        if self.has_table(pool_id, replica_count) {
            return;
        }
        let pool = match self.ctx.pool_manager.get_pool(pool_id) {
            Ok(p) => p,
            Err(e) => {
                log::warn!("init pool={}: get_pool failed: {}", pool_id, e);
                return;
            }
        };
        let workers: Vec<u32> = pool
            .workers
            .iter()
            .copied()
            .filter(|w| self.ctx.pool_manager.is_worker_available(*w))
            .collect();
        if workers.len() < replica_count as usize {
            log::warn!(
                "init pool={} replica_count={}: only {} available workers; will retry on next event/scan",
                pool_id,
                replica_count,
                workers.len()
            );
            return;
        }
        let bucket_count = self.ctx.bg_manager.bucket_count();
        log::info!(
            "Initializing BGTable pool={} buckets={} replica_count={} workers={}",
            pool_id,
            bucket_count,
            replica_count,
            workers.len()
        );
        if let Err(e) =
            self.ctx
                .bg_manager
                .create_table(pool_id, bucket_count, replica_count, &workers)
        {
            log::error!(
                "create_table pool={} replica_count={} failed: {}",
                pool_id,
                replica_count,
                e
            );
        }
    }

    fn build_rebuild_ops(&self, table_id: u32) -> Vec<BGOperator> {
        let mut ops = Vec::new();
        let diff = match self.ctx.bg_manager.compute_rebuild_diff(table_id) {
            Ok(d) => d,
            Err(e) => {
                log::error!("compute_rebuild_diff table={} failed: {}", table_id, e);
                return ops;
            }
        };

        for (new_bg, old_replicas) in diff {
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

            for w in &added {
                builder = builder.add_replica(*w);
                builder = builder.wait_replica_ready(*w, ReplicaState::Active);
            }

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

            for w in &removed {
                builder = builder.remove_replica(*w);
            }

            ops.push(builder.build());
        }
        ops
    }
}

impl Scheduler for BGTableScheduler {
    fn name(&self) -> &str {
        "bg-table-scheduler"
    }

    fn schedule(&self, _ctx: &ManagerContext) -> Vec<BGOperator> {
        self.bootstrap_missing_tables();
        self.check_and_execute()
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
        if event.node_type != NodeType::Worker {
            return;
        }
        if event.event_type != NodeEventType::Registered {
            return;
        }
        for pool_id in self.ctx.pool_manager.get_pools_by_worker(event.node_id) {
            self.enqueue(pool_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::pool::POOL_ID_SSD;
    use crate::pd::schedule::checker::tests_common::{test_context, Fixture};

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
        assert_eq!(s.min_interval(), Duration::from_secs(5));
        let i1 = s.next_interval(s.min_interval());
        assert!(i1 > Duration::from_secs(5) && i1 <= Duration::from_secs(60));
        let mut cur = s.min_interval();
        for _ in 0..20 {
            cur = s.next_interval(cur);
        }
        assert_eq!(cur, Duration::from_secs(60));
    }

    fn scheduler_for(f: &Fixture) -> BGTableScheduler {
        BGTableScheduler::new(f.ctx.clone())
    }

    fn pending_deadline(s: &BGTableScheduler, pool_id: u16) -> Option<u64> {
        s.pending
            .lock()
            .unwrap()
            .get(&pool_id)
            .map(|v| v.scheduled_time_ms)
    }

    #[test]
    fn enqueue_inserts_with_now_plus_cooldown() {
        let f = Fixture::new();
        let s = scheduler_for(&f);
        let before = orpc::common::LocalTime::mills();

        s.enqueue(POOL_ID_SSD);

        let deadline = pending_deadline(&s, POOL_ID_SSD).unwrap();
        assert!(deadline >= before + 60_000, "default cooldown is 60s");
    }

    #[test]
    fn enqueue_pushes_deadline_forward() {
        let f = Fixture::new();
        let s = scheduler_for(&f);

        s.enqueue(POOL_ID_SSD);
        let t1 = pending_deadline(&s, POOL_ID_SSD).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(5));
        s.enqueue(POOL_ID_SSD);
        let t2 = pending_deadline(&s, POOL_ID_SSD).unwrap();

        assert!(t2 > t1, "subsequent event must push deadline forward");
    }

    #[test]
    fn bootstrap_seeds_pools_without_tables() {
        let f = Fixture::new();
        f.add_worker(100, POOL_ID_SSD, &[]);
        let s = scheduler_for(&f);

        s.bootstrap_missing_tables();

        assert!(pending_deadline(&s, POOL_ID_SSD).is_some());
    }

    #[test]
    fn bootstrap_does_not_bump_existing_deadline() {
        let f = Fixture::new();
        f.add_worker(100, POOL_ID_SSD, &[]);
        let s = scheduler_for(&f);

        s.bootstrap_missing_tables();
        let t1 = pending_deadline(&s, POOL_ID_SSD).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(5));
        s.bootstrap_missing_tables();
        let t2 = pending_deadline(&s, POOL_ID_SSD).unwrap();

        assert_eq!(t1, t2, "bootstrap must be idempotent w.r.t. deadline");
    }

    #[test]
    fn bootstrap_skips_pools_with_any_table() {
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102], POOL_ID_SSD);
        f.insert_table(POOL_ID_SSD, 3);
        let s = scheduler_for(&f);

        s.bootstrap_missing_tables();

        assert!(s.pending.lock().unwrap().is_empty());
    }

    #[test]
    fn check_and_execute_skips_future_deadlines() {
        let f = Fixture::new();
        let s = scheduler_for(&f);
        s.enqueue(POOL_ID_SSD);
        let before = pending_deadline(&s, POOL_ID_SSD);

        let ops = s.check_and_execute();

        assert!(ops.is_empty());
        assert_eq!(
            pending_deadline(&s, POOL_ID_SSD),
            before,
            "future deadline must stay in pending"
        );
    }

    #[test]
    fn check_and_execute_drains_ready_entry() {
        // 1 worker (< replica_count=3) so try_initialize drops early → no Raft call.
        let f = Fixture::new();
        f.add_worker(100, POOL_ID_SSD, &[]);
        let s = scheduler_for(&f);
        s.pending.lock().unwrap().insert(
            POOL_ID_SSD,
            PendingPoolRebuild {
                scheduled_time_ms: 0,
            },
        );

        s.check_and_execute();

        assert!(
            !s.pending.lock().unwrap().contains_key(&POOL_ID_SSD),
            "ready entry must be drained"
        );
    }

    #[test]
    fn check_and_execute_preserves_rebuild_when_auto_disabled() {
        let f = Fixture::with_overrides(
            [(keys::PD_BG_REBUILD_AUTO_ENABLED, "false")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        f.add_workers(&[100, 101, 102], POOL_ID_SSD);
        f.insert_table(POOL_ID_SSD, 3); // pool has a table → rebuild, not init
        let s = scheduler_for(&f);
        s.pending.lock().unwrap().insert(
            POOL_ID_SSD,
            PendingPoolRebuild {
                scheduled_time_ms: 0,
            },
        );

        let ops = s.check_and_execute();

        assert!(
            ops.is_empty(),
            "rebuild suppressed under auto_enabled=false"
        );
        assert!(
            s.pending.lock().unwrap().contains_key(&POOL_ID_SSD),
            "entry preserved so it fires once auto_enabled flips back on"
        );
    }

    #[test]
    fn check_and_execute_dispatches_pure_init_even_when_auto_disabled() {
        // auto_enabled=false but no table exists → pure init → still dispatched.
        // 1 worker (< replica_count=3) so try_initialize drops without Raft.
        let f = Fixture::with_overrides(
            [(keys::PD_BG_REBUILD_AUTO_ENABLED, "false")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        f.add_worker(100, POOL_ID_SSD, &[]);
        let s = scheduler_for(&f);
        s.pending.lock().unwrap().insert(
            POOL_ID_SSD,
            PendingPoolRebuild {
                scheduled_time_ms: 0,
            },
        );

        s.check_and_execute();

        assert!(
            !s.pending.lock().unwrap().contains_key(&POOL_ID_SSD),
            "pure init must dispatch even under auto_enabled=false"
        );
    }

    #[test]
    fn fresh_event_after_drain_creates_new_entry() {
        // Atomic drain + new event = fresh entry for next cycle; no lost update.
        let f = Fixture::new();
        f.insert_table(POOL_ID_SSD, 3);
        let s = scheduler_for(&f);

        s.pending.lock().unwrap().insert(
            POOL_ID_SSD,
            PendingPoolRebuild {
                scheduled_time_ms: 0,
            },
        );
        let _ = s.check_and_execute();
        assert!(s.pending.lock().unwrap().is_empty());

        s.enqueue(POOL_ID_SSD);
        assert!(pending_deadline(&s, POOL_ID_SSD).is_some());
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

    #[test]
    fn worker_registered_enqueues_pool() {
        let f = Fixture::new();
        f.add_worker(100, POOL_ID_SSD, &[]);
        let s = scheduler_for(&f);

        s.on_event(&make_event(
            NodeEventType::Registered,
            NodeType::Worker,
            100,
        ));

        assert!(pending_deadline(&s, POOL_ID_SSD).is_some());
    }

    #[test]
    fn worker_offline_is_ignored_by_bg_table() {
        // Offline / DecommissionFinished belong to DecommissionScheduler.
        let f = Fixture::new();
        f.add_worker(100, POOL_ID_SSD, &[]);
        let s = scheduler_for(&f);

        s.on_event(&make_event(NodeEventType::Offline, NodeType::Worker, 100));
        s.on_event(&make_event(
            NodeEventType::DecommissionFinished,
            NodeType::Worker,
            100,
        ));

        assert!(s.pending.lock().unwrap().is_empty());
    }

    #[test]
    fn meta_events_are_ignored() {
        let f = Fixture::new();
        f.add_worker(100, POOL_ID_SSD, &[]);
        let s = scheduler_for(&f);

        s.on_event(&make_event(NodeEventType::Registered, NodeType::Meta, 100));

        assert!(s.pending.lock().unwrap().is_empty());
    }

    #[test]
    fn multiple_registrations_collapse_to_one_entry() {
        let f = Fixture::new();
        f.add_worker(100, POOL_ID_SSD, &[]);
        let s = scheduler_for(&f);

        s.on_event(&make_event(
            NodeEventType::Registered,
            NodeType::Worker,
            100,
        ));
        f.add_worker(101, POOL_ID_SSD, &[]);
        s.on_event(&make_event(
            NodeEventType::Registered,
            NodeType::Worker,
            101,
        ));

        assert_eq!(s.pending.lock().unwrap().len(), 1);
    }

    #[test]
    fn request_rebuild_enqueues_given_pools() {
        let f = Fixture::new();
        let s = scheduler_for(&f);

        s.request_rebuild(vec![POOL_ID_SSD]);

        assert!(pending_deadline(&s, POOL_ID_SSD).is_some());
    }
}
