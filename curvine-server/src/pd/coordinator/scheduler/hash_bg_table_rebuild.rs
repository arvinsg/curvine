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
use crate::pd::coordinator::policy::is_hash_table_rebuild_candidate;
use crate::pd::coordinator::{
    BGOperator, CoordinatorContext, CoordinatorEvent, OpPriority, OperatorBuilder, OperatorKind,
};
use curvine_common::state::{BGKind, ReplicaState, StorageType, TableId};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BGTABLE_MIN_INTERVAL: Duration = Duration::from_secs(5);
const BGTABLE_MAX_INTERVAL: Duration = Duration::from_secs(60);
const BGTABLE_BACKOFF_FACTOR: f64 = 1.5;

#[derive(Clone, Debug)]
pub struct PendingPoolRebuild {
    pub scheduled_time_ms: u64,
}

/// BGTable scheduler: two responsibilities keyed by `pool_type`:
///
/// 1. **Initialize** — when a pool has no BGTable for some configured `replica_count`,
///    wait for `cooldown` of quiescence (no further node joins) before creating it.
///
/// 2. **Rebuild** — when a new worker joins a pool that already has a table, wait
///    `cooldown` of quiescence and then re-plan placement via the rebuild diff.
pub struct HashBGTableRebuildScheduler {
    ctx: Arc<CoordinatorContext>,
    pending: Mutex<HashMap<StorageType, PendingPoolRebuild>>,
    pool_generations: Mutex<HashMap<StorageType, u64>>,
}

impl HashBGTableRebuildScheduler {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        Self {
            ctx,
            pending: Mutex::new(HashMap::new()),
            pool_generations: Mutex::new(HashMap::new()),
        }
    }

    fn cooldown_ms(&self) -> u64 {
        self.ctx
            .config_manager
            .get_u64(keys::PD_BG_REBUILD_COOLDOWN_MS)
    }

    fn enqueue(&self, pool_type: StorageType) {
        let deadline = orpc::common::LocalTime::mills() + self.cooldown_ms();
        let mut guard = self.pending.lock().unwrap();
        let entry = guard.entry(pool_type).or_insert(PendingPoolRebuild {
            scheduled_time_ms: deadline,
        });
        entry.scheduled_time_ms = entry.scheduled_time_ms.max(deadline);
    }

    /// Manual trigger.
    pub fn request_rebuild(&self, pool_types: Vec<StorageType>) {
        for pool_type in pool_types {
            self.enqueue(pool_type);
        }
    }

    fn table_ids_for_pool(
        &self,
        pool_type: StorageType,
        replica_count: Option<u16>,
    ) -> Vec<TableId> {
        self.ctx
            .bgtable_manager
            .list_tables()
            .into_iter()
            .filter(|table| table.kind() == BGKind::Hash)
            .filter(|table| table.storage_type() == pool_type)
            .filter(|table| {
                replica_count
                    .map(|rc| table.replica_count() == rc)
                    .unwrap_or(true)
            })
            .map(|table| table.table_id())
            .collect()
    }

    fn pool_generation(&self, pool_type: StorageType) -> u64 {
        let workers = self.ctx.pool_manager.get_workers_in_pool(pool_type);
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        pool_type.hash(&mut hasher);
        workers.hash(&mut hasher);
        hasher.finish()
    }

    fn detect_pool_membership_changes(&self) {
        let pools = self.ctx.pool_manager.list_active_pools();
        let mut generations = self.pool_generations.lock().unwrap();
        for pool in pools {
            let pool_type = pool.media;
            let generation = self.pool_generation(pool_type);
            let changed = generations
                .insert(pool_type, generation)
                .map(|old| old != generation)
                .unwrap_or(true);
            if changed {
                log::info!(
                    "HashBGTableRebuildScheduler detected pool membership generation change pool_type={}, generation={}",
                    pool_type,
                    generation
                );
                drop(generations);
                self.enqueue(pool_type);
                generations = self.pool_generations.lock().unwrap();
            }
        }
    }

    fn check_and_execute(&self) -> Vec<BGOperator> {
        if !self.auto_rebuild_enabled() {
            return Vec::new();
        }
        let now = orpc::common::LocalTime::mills();
        let drained = self.drain_ready_pools(now);
        self.dispatch_drained_pools(drained)
    }

    fn auto_rebuild_enabled(&self) -> bool {
        self.ctx
            .config_manager
            .get_bool(keys::PD_BG_REBUILD_AUTO_ENABLED)
    }

    fn drain_ready_pools(&self, now: u64) -> Vec<StorageType> {
        let mut guard = self.pending.lock().unwrap();
        let ready: Vec<StorageType> = guard
            .iter()
            .filter(|(_, v)| v.scheduled_time_ms <= now)
            .map(|(k, _)| *k)
            .collect();
        for k in &ready {
            guard.remove(k);
        }
        ready
    }

    fn dispatch_drained_pools(&self, pool_types: Vec<StorageType>) -> Vec<BGOperator> {
        let mut ops = Vec::new();
        for pool_type in pool_types {
            for table_id in self.table_ids_for_pool(pool_type, None) {
                ops.extend(self.build_rebuild_ops(table_id));
            }
        }
        ops
    }

    fn build_rebuild_ops(&self, table_id: curvine_common::state::TableId) -> Vec<BGOperator> {
        let mut ops = Vec::new();
        let diff = match self
            .ctx
            .bgtable_manager
            .hash_placement()
            .compute_rebuild_diff(table_id)
        {
            Ok(d) => d,
            Err(e) => {
                log::error!("compute_rebuild_diff table={} failed: {}", table_id, e);
                return ops;
            }
        };

        for (new_bg, old_replicas) in diff {
            if !is_hash_table_rebuild_candidate(&new_bg) {
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
                BGKind::Hash,
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

            let old_primary = self
                .ctx
                .bgtable_manager
                .bg()
                .get_bg(BGKind::Hash, new_bg.bg_id)
                .map(|bg| bg.primary.node_id)
                .unwrap_or(0);
            if old_primary != 0 && old_primary != new_bg.primary.node_id {
                builder = builder.transfer_primary(old_primary, new_bg.primary.node_id);
            }

            for w in &removed {
                builder = builder.remove_replica(*w);
            }

            ops.push(builder.build());
        }
        ops
    }
}

impl Scheduler for HashBGTableRebuildScheduler {
    fn name(&self) -> &str {
        "hash-bg-table-rebuild-scheduler"
    }

    fn schedule(&self, _ctx: &CoordinatorContext) -> Vec<BGOperator> {
        self.detect_pool_membership_changes();
        self.check_and_execute()
    }

    fn is_schedule_allowed(&self, _ctx: &CoordinatorContext) -> bool {
        true
    }

    fn min_interval(&self) -> Duration {
        BGTABLE_MIN_INTERVAL
    }

    fn next_interval(&self, current: Duration) -> Duration {
        let ms = (current.as_millis() as f64 * BGTABLE_BACKOFF_FACTOR) as u64;
        Duration::from_millis(ms).min(BGTABLE_MAX_INTERVAL)
    }

    fn on_event(&self, event: &CoordinatorEvent) {
        if let CoordinatorEvent::WorkerJoinedPools { pool_types, .. } = event {
            for &pool_type in pool_types {
                self.enqueue(pool_type);
            }
        }
    }

    fn on_leader_start(&self) {
        self.pending.lock().unwrap().clear();
        self.pool_generations.lock().unwrap().clear();
        log::info!("HashBGTableRebuildScheduler leader-start cooldown state reset");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::coordinator::checker::tests_common::{test_context, Fixture};
    use curvine_common::state::StorageType;

    #[test]
    fn name_and_type() {
        let ctx = test_context(HashMap::new());
        let s = HashBGTableRebuildScheduler::new(ctx);
        assert_eq!(s.name(), "hash-bg-table-rebuild-scheduler");
    }

    #[test]
    fn interval_backoff() {
        let ctx = test_context(HashMap::new());
        let s = HashBGTableRebuildScheduler::new(ctx);
        assert_eq!(s.min_interval(), Duration::from_secs(5));
        let i1 = s.next_interval(s.min_interval());
        assert!(i1 > Duration::from_secs(5) && i1 <= Duration::from_secs(60));
        let mut cur = s.min_interval();
        for _ in 0..20 {
            cur = s.next_interval(cur);
        }
        assert_eq!(cur, Duration::from_secs(60));
    }

    fn scheduler_for(f: &Fixture) -> HashBGTableRebuildScheduler {
        HashBGTableRebuildScheduler::new(f.ctx.clone())
    }

    fn pending_deadline(s: &HashBGTableRebuildScheduler, pool_type: StorageType) -> Option<u64> {
        s.pending
            .lock()
            .unwrap()
            .get(&pool_type)
            .map(|v| v.scheduled_time_ms)
    }

    #[test]
    fn enqueue_inserts_with_now_plus_cooldown() {
        let f = Fixture::new();
        let s = scheduler_for(&f);
        let before = orpc::common::LocalTime::mills();

        s.enqueue(StorageType::Ssd);

        let deadline = pending_deadline(&s, StorageType::Ssd).unwrap();
        assert!(deadline >= before + 60_000, "default cooldown is 60s");
    }

    #[test]
    fn enqueue_pushes_deadline_forward() {
        let f = Fixture::new();
        let s = scheduler_for(&f);

        s.enqueue(StorageType::Ssd);
        let t1 = pending_deadline(&s, StorageType::Ssd).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(5));
        s.enqueue(StorageType::Ssd);
        let t2 = pending_deadline(&s, StorageType::Ssd).unwrap();

        assert!(t2 > t1, "subsequent event must push deadline forward");
    }

    #[test]
    fn check_and_execute_skips_future_deadlines() {
        let f = Fixture::new();
        let s = scheduler_for(&f);
        s.enqueue(StorageType::Ssd);
        let before = pending_deadline(&s, StorageType::Ssd);

        let ops = s.check_and_execute();

        assert!(ops.is_empty());
        assert_eq!(
            pending_deadline(&s, StorageType::Ssd),
            before,
            "future deadline must stay in pending"
        );
    }

    #[test]
    fn check_and_execute_drains_ready_entry() {
        // 1 worker (< replica_count=3) so try_initialize drops early → no Raft call.
        let f = Fixture::new();
        f.add_worker(100, StorageType::Ssd, &[]);
        let s = scheduler_for(&f);
        s.pending.lock().unwrap().insert(
            StorageType::Ssd,
            PendingPoolRebuild {
                scheduled_time_ms: 0,
            },
        );

        s.check_and_execute();

        assert!(
            !s.pending.lock().unwrap().contains_key(&StorageType::Ssd),
            "ready entry must be drained"
        );
    }

    #[test]
    fn table_scan_is_independent_of_global_replica_counts() {
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102], StorageType::Ssd);
        // Fixture BGManager is configured with global replica_counts=[3], but
        // namespace-created tables may carry their own replica_count. The
        // scheduler must discover existing tables by table metadata.
        let table_id = f.insert_table(StorageType::Ssd, 2);

        let s = scheduler_for(&f);

        assert_eq!(s.table_ids_for_pool(StorageType::Ssd, None), vec![table_id]);
        assert_eq!(
            s.table_ids_for_pool(StorageType::Ssd, Some(2)),
            vec![table_id]
        );
        assert!(s.table_ids_for_pool(StorageType::Ssd, Some(3)).is_empty());
    }

    #[test]
    fn check_and_execute_preserves_rebuild_when_auto_disabled() {
        let f = Fixture::with_overrides(
            [(keys::PD_BG_REBUILD_AUTO_ENABLED, "false")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        f.add_workers(&[100, 101, 102], StorageType::Ssd);
        f.insert_table(StorageType::Ssd, 3); // pool has a table → rebuild, not init
        let s = scheduler_for(&f);
        s.pending.lock().unwrap().insert(
            StorageType::Ssd,
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
            s.pending.lock().unwrap().contains_key(&StorageType::Ssd),
            "entry preserved so it fires once auto_enabled flips back on"
        );
    }

    #[test]
    fn check_and_execute_preserves_missing_namespace_table_when_auto_disabled() {
        // Namespace creation owns table initialization. With no existing table,
        // scheduler has no pure-init path and preserves the event while auto rebuild is off.
        let f = Fixture::with_overrides(
            [(keys::PD_BG_REBUILD_AUTO_ENABLED, "false")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        f.add_worker(100, StorageType::Ssd, &[]);
        let s = scheduler_for(&f);
        s.pending.lock().unwrap().insert(
            StorageType::Ssd,
            PendingPoolRebuild {
                scheduled_time_ms: 0,
            },
        );

        s.check_and_execute();

        assert!(
            s.pending.lock().unwrap().contains_key(&StorageType::Ssd),
            "no table initialization is dispatched by HashBGTableRebuildScheduler"
        );
    }

    #[test]
    fn fresh_event_after_drain_creates_new_entry() {
        // Atomic drain + new event = fresh entry for next cycle; no lost update.
        let f = Fixture::new();
        f.insert_table(StorageType::Ssd, 3);
        let s = scheduler_for(&f);

        s.pending.lock().unwrap().insert(
            StorageType::Ssd,
            PendingPoolRebuild {
                scheduled_time_ms: 0,
            },
        );
        let _ = s.check_and_execute();
        assert!(s.pending.lock().unwrap().is_empty());

        s.enqueue(StorageType::Ssd);
        assert!(pending_deadline(&s, StorageType::Ssd).is_some());
    }

    fn worker_joined_event(pool_types: Vec<StorageType>) -> CoordinatorEvent {
        CoordinatorEvent::WorkerJoinedPools {
            worker_id: 100,
            node_epoch: 1,
            pool_types,
            event_time_ms: 0,
        }
    }

    #[test]
    fn detect_pool_membership_changes_enqueues_only_on_generation_change() {
        let f = Fixture::new();
        let s = scheduler_for(&f);

        s.detect_pool_membership_changes();
        assert!(s.pending.lock().unwrap().is_empty());

        f.add_worker(100, StorageType::Ssd, &[]);
        s.detect_pool_membership_changes();
        assert!(pending_deadline(&s, StorageType::Ssd).is_some());

        s.pending.lock().unwrap().clear();
        s.detect_pool_membership_changes();
        assert!(
            s.pending.lock().unwrap().is_empty(),
            "unchanged pool generation must not enqueue again"
        );

        f.add_worker(101, StorageType::Ssd, &[]);
        s.detect_pool_membership_changes();
        assert!(pending_deadline(&s, StorageType::Ssd).is_some());
    }

    #[test]
    fn worker_registered_enqueues_pool() {
        let f = Fixture::new();
        f.add_worker(100, StorageType::Ssd, &[]);
        let s = scheduler_for(&f);

        s.on_event(&worker_joined_event(vec![StorageType::Ssd]));

        assert!(pending_deadline(&s, StorageType::Ssd).is_some());
    }

    #[test]
    fn worker_offline_is_ignored_by_bg_table() {
        // Offline / DecommissionFinished belong to HashDecommissionScheduler.
        let f = Fixture::new();
        f.add_worker(100, StorageType::Ssd, &[]);
        let s = scheduler_for(&f);

        s.on_event(&CoordinatorEvent::WorkerOffline {
            worker_id: 100,
            node_epoch: 1,
            event_time_ms: 0,
        });
        s.on_event(&CoordinatorEvent::WorkerDecommissionFinished {
            worker_id: 100,
            node_epoch: 1,
            event_time_ms: 0,
        });

        assert!(s.pending.lock().unwrap().is_empty());
    }

    #[test]
    fn joined_event_without_changed_pools_is_ignored() {
        let f = Fixture::new();
        let s = scheduler_for(&f);

        s.on_event(&worker_joined_event(vec![]));

        assert!(s.pending.lock().unwrap().is_empty());
    }

    #[test]
    fn multiple_registrations_collapse_to_one_entry() {
        let f = Fixture::new();
        f.add_worker(100, StorageType::Ssd, &[]);
        let s = scheduler_for(&f);

        s.on_event(&worker_joined_event(vec![StorageType::Ssd]));
        f.add_worker(101, StorageType::Ssd, &[]);
        s.on_event(&CoordinatorEvent::WorkerJoinedPools {
            worker_id: 101,
            node_epoch: 1,
            pool_types: vec![StorageType::Ssd],
            event_time_ms: 0,
        });

        assert_eq!(s.pending.lock().unwrap().len(), 1);
    }

    #[test]
    fn request_rebuild_enqueues_given_pools() {
        let f = Fixture::new();
        let s = scheduler_for(&f);

        s.request_rebuild(vec![StorageType::Ssd]);

        assert!(pending_deadline(&s, StorageType::Ssd).is_some());
    }
}
