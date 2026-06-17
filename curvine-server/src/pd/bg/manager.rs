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

use super::id_allocator::IdAllocator;
use super::placement::{
    create_policy, PlacementContext, PlacementRule, RebuildOptions, WorkerLoadSnapshot,
};
use super::state_machine;
use super::{BGStore, BGTable, BGTableStats};
use crate::pd::config::{keys, ConfigManager};
use crate::pd::journal::entry::{
    BGDeleteEntry, BGEntry, BGUpdateEntry, BatchBGEntry, BumpTableEpochEntry, TableEpochUpdate,
};
use crate::pd::journal::{self, ApplyOutcome, PdEntry};
use crate::pd::pool::PoolManager;
use curvine_common::state::{
    gen_table_id, BGLease, BGOpState, BGState, BGStats, BGTableSummary, BlockGroupInfo,
    BlockGroupInfoView, NodeState, ReplicaInfo, ReplicaState, StorageType, WorkerBGReport,
};
use curvine_common::{FsError, FsResult};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

pub struct BGManager {
    /// P5.2: tables stored as `Arc<BGTable>` so `get_table()` and route-
    /// publish paths return cheap shared references instead of cloning ~hundred-
    /// bucket structs on every read.
    tables: RwLock<HashMap<u32, Arc<BGTable>>>,
    /// P5.2: BGs stored as `Arc<BlockGroupInfo>` so scheduler / checker /
    /// operator hot-paths share read references without copying. Mutation
    /// paths construct a new `BlockGroupInfo`, wrap with `Arc::new`, and
    /// insert — readers holding old `Arc`s see the previous snapshot until
    /// they refresh.
    bgs: RwLock<HashMap<u32, Arc<BlockGroupInfo>>>,
    worker_to_bgs: RwLock<HashMap<u32, HashSet<u32>>>,
    /// Latest leader-observed lifecycle states, used by scheduler/operator.
    observed_replica_states: RwLock<HashMap<u32, HashMap<u32, ReplicaState>>>,
    /// Full client route view published by a committed table_epoch bump.
    published_routes: RwLock<HashMap<u32, BGTableSummary>>,
    dirty_route_tables: RwLock<HashSet<u32>>,
    route_ready: AtomicBool,
    /// Test-only: when true, apply_*_bg paths skip `try_flush_dirty_route_tables`
    /// to avoid blocking on a Raft propose when the journal client has no
    /// real cluster behind it. Production code must leave this false so that
    /// BumpTableEpoch entries are proposed (P4.2 requires the entry path to
    /// be the single source of truth for table.epoch).
    route_publish_disabled: AtomicBool,
    store: Arc<BGStore>,
    pool_manager: Arc<PoolManager>,
    journal_client: Arc<journal::Client>,
    config_manager: Arc<ConfigManager>,
    id_allocator: IdAllocator,
    bucket_count: u32,
    replica_counts: Vec<u16>,
    location_labels: Vec<String>,
}

impl BGManager {
    pub fn new(
        store: Arc<BGStore>,
        pool_manager: Arc<PoolManager>,
        journal_client: Arc<journal::Client>,
        config_manager: Arc<ConfigManager>,
        bucket_count: u32,
        replica_counts: Vec<u16>,
        location_labels: Vec<String>,
    ) -> Self {
        let id_allocator = IdAllocator::new(store.clone(), journal_client.clone());
        Self {
            tables: RwLock::new(HashMap::new()),
            bgs: RwLock::new(HashMap::new()),
            worker_to_bgs: RwLock::new(HashMap::new()),
            observed_replica_states: RwLock::new(HashMap::new()),
            published_routes: RwLock::new(HashMap::new()),
            dirty_route_tables: RwLock::new(HashSet::new()),
            route_ready: AtomicBool::new(false),
            route_publish_disabled: AtomicBool::new(false),
            store,
            pool_manager,
            journal_client,
            config_manager,
            id_allocator,
            bucket_count,
            replica_counts,
            location_labels,
        }
    }

    fn balance_policy_strategy(&self) -> String {
        self.config_manager.get_string(keys::PD_BG_BALANCE_POLICY)
    }

    fn rebuild_tolerant_ratio(&self) -> f64 {
        self.config_manager
            .get_u32(keys::PD_BG_REBUILD_TOLERANT_RATIO_BPS) as f64
            / 10_000.0
    }

    pub fn restore(&self) -> FsResult<()> {
        let tables = self.store.list_tables()?;
        let bgs = self.store.list_all()?;
        let mut t = self.tables.write().unwrap();
        let mut b = self.bgs.write().unwrap();
        let mut w2b = self.worker_to_bgs.write().unwrap();
        t.clear();
        b.clear();
        w2b.clear();
        self.reset_runtime_route_state();
        for table in tables {
            t.insert(table.table_id, Arc::new(table));
        }
        for bg in bgs {
            for &wid in &bg.replica_set {
                w2b.entry(wid).or_default().insert(bg.bg_id);
            }
            b.insert(bg.bg_id, Arc::new(bg));
        }
        self.id_allocator.restore()?;
        Ok(())
    }

    pub fn reset_runtime_route_state_after_snapshot(&self) {
        self.reset_runtime_route_state();
        log::info!("BG runtime route state reset after snapshot restore");
    }

    fn reset_runtime_route_state(&self) {
        self.observed_replica_states.write().unwrap().clear();
        self.published_routes.write().unwrap().clear();
        self.dirty_route_tables.write().unwrap().clear();
        self.route_ready.store(false, Ordering::Release);
    }

    /// Scheduler/lifecycle view: missing runtime state is conservative Pending.
    pub fn get_replica_state(&self, bg_id: u32, worker_id: u32) -> ReplicaState {
        replica_state_in(
            self.observed_replica_states.read().unwrap().get(&bg_id),
            worker_id,
        )
    }

    pub fn set_replica_state(&self, bg_id: u32, worker_id: u32, state: ReplicaState) {
        self.observed_replica_states
            .write()
            .unwrap()
            .entry(bg_id)
            .or_default()
            .insert(worker_id, state);
    }

    /// Called when this PD becomes leader. Runtime state is discarded, then a
    /// table_epoch bump publishes the failover route view (Live replica_set default Active).
    ///
    /// P4.2: do NOT fail-fast on incomplete route publish. Some tables may
    /// remain dirty if the BumpTableEpoch entry is racing with concurrent BG
    /// updates; the next try_flush will pick them up. route_ready is still
    /// set so the leader can serve client RPCs against whatever subset of
    /// tables successfully published.
    pub fn on_leader_start(&self) -> FsResult<()> {
        let table_ids: HashSet<u32> = self.tables.read().unwrap().keys().copied().collect();
        self.reset_runtime_route_state();
        if table_ids.is_empty() {
            self.route_ready.store(true, Ordering::Release);
            log::info!("BG runtime route state reset on leader start, no tables to publish");
            return Ok(());
        }
        self.mark_route_tables_dirty(&table_ids);
        if let Err(e) = self.flush_dirty_route_tables(Some(&table_ids)) {
            log::warn!(
                "leader route epoch bump partial: {}; remaining tables will be \
                 picked up by next try_flush_dirty_route_tables",
                e
            );
        }
        let remaining: HashSet<u32> = self.dirty_route_tables.read().unwrap().clone();
        let still_dirty = remaining.intersection(&table_ids).count();
        if still_dirty > 0 {
            log::warn!(
                "leader start: {} table(s) still dirty after publish attempt; \
                 client routes for those tables will be served at the next bump",
                still_dirty
            );
        }
        self.route_ready.store(true, Ordering::Release);
        log::info!(
            "BG runtime route state reset on leader start, published table_epochs \
             for {} tables ({} still dirty)",
            table_ids.len() - still_dirty,
            still_dirty
        );
        Ok(())
    }

    /// Apply detailed replica reports from a worker heartbeat. Observed states are
    /// leader-runtime only; client-visible changes are published by table_epoch bump.
    pub fn apply_replica_reports(
        &self,
        worker_id: u32,
        reports: &[WorkerBGReport],
    ) -> FsResult<usize> {
        let mut changed_tables = HashSet::new();
        let mut changed = 0usize;
        {
            let bgs = self.bgs.read().unwrap();
            let mut observed = self.observed_replica_states.write().unwrap();
            for report in reports {
                let Some(bg) = bgs.get(&report.bg_id) else {
                    log::warn!(
                        "worker {} reported unknown bg_id={}; skip replica report",
                        worker_id,
                        report.bg_id
                    );
                    continue;
                };
                if !bg.replica_set.contains(&worker_id) {
                    log::warn!(
                        "worker {} reported bg_id={} but is not in replica_set; skip replica report",
                        worker_id,
                        report.bg_id
                    );
                    continue;
                }
                let states = observed.entry(report.bg_id).or_default();
                let old = states
                    .get(&worker_id)
                    .copied()
                    .unwrap_or(ReplicaState::Pending);
                if old == report.state {
                    continue;
                }
                states.insert(worker_id, report.state);
                changed += 1;
                if old.shifts_client_view(report.state) {
                    changed_tables.insert(bg.table_id);
                }
            }
        }
        self.mark_route_tables_dirty(&changed_tables);
        self.try_flush_dirty_route_tables("replica report route publish");
        if changed > 0 {
            log::info!(
                "Applied observed replica reports worker_id={}, changed={}, route_changed_tables={}",
                worker_id,
                changed,
                changed_tables.len()
            );
        }
        Ok(changed)
    }

    pub fn promote_pending_replicas(&self, worker_id: u32, bg_ids: &[u32]) -> FsResult<usize> {
        let mut changed_tables = HashSet::new();
        let mut changed = 0usize;
        {
            let bgs = self.bgs.read().unwrap();
            let mut observed = self.observed_replica_states.write().unwrap();
            for &bg_id in bg_ids {
                let Some(bg) = bgs.get(&bg_id) else {
                    log::warn!(
                        "worker {} requested promote for unknown bg_id={}; skip",
                        worker_id,
                        bg_id
                    );
                    continue;
                };
                if !bg.replica_set.contains(&worker_id) {
                    log::warn!(
                        "worker {} requested promote for bg_id={} but is not in replica_set; skip",
                        worker_id,
                        bg_id
                    );
                    continue;
                }
                let states = observed.entry(bg_id).or_default();
                let old = states
                    .get(&worker_id)
                    .copied()
                    .unwrap_or(ReplicaState::Pending);
                if old != ReplicaState::Pending {
                    continue;
                }
                states.insert(worker_id, ReplicaState::Active);
                changed += 1;
                if old.shifts_client_view(ReplicaState::Active) {
                    changed_tables.insert(bg.table_id);
                }
            }
        }
        self.mark_route_tables_dirty(&changed_tables);
        self.try_flush_dirty_route_tables("replica promote route publish");
        if changed > 0 {
            log::info!(
                "Promoted observed replicas worker_id={}, changed={}, route_changed_tables={}",
                worker_id,
                changed,
                changed_tables.len()
            );
        }
        Ok(changed)
    }

    pub fn mark_replicas_lost(&self, worker_id: u32) -> FsResult<usize> {
        self.mark_worker_replicas(worker_id, ReplicaState::Lost)
    }

    pub fn mark_replicas_offline(&self, worker_id: u32) -> FsResult<usize> {
        self.mark_worker_replicas(worker_id, ReplicaState::Offline)
    }

    fn mark_worker_replicas(&self, worker_id: u32, new_state: ReplicaState) -> FsResult<usize> {
        self.mark_workers_replicas(&[(worker_id, new_state)])
    }

    pub fn mark_workers_replicas(&self, worker_states: &[(u32, ReplicaState)]) -> FsResult<usize> {
        let target_states: HashMap<u32, ReplicaState> = worker_states.iter().copied().collect();
        if target_states.is_empty() {
            self.try_flush_dirty_route_tables("mark workers replicas noop route publish retry");
            return Ok(0);
        }

        let worker_bg_ids: Vec<(u32, ReplicaState, Vec<u32>)> = {
            let w2b = self.worker_to_bgs.read().unwrap();
            target_states
                .iter()
                .map(|(&worker_id, &new_state)| {
                    let bg_ids = w2b
                        .get(&worker_id)
                        .map(|ids| ids.iter().copied().collect())
                        .unwrap_or_default();
                    (worker_id, new_state, bg_ids)
                })
                .collect()
        };

        let mut changed_tables = HashSet::new();
        let mut changed = 0usize;
        {
            let bgs = self.bgs.read().unwrap();
            let mut observed = self.observed_replica_states.write().unwrap();
            for (worker_id, new_state, bg_ids) in worker_bg_ids {
                for bg_id in bg_ids {
                    let Some(bg) = bgs.get(&bg_id) else {
                        log::warn!(
                            "worker_to_bgs contains missing bg_id={} for worker_id={}; skip mark replica",
                            bg_id,
                            worker_id
                        );
                        continue;
                    };
                    if !bg.replica_set.contains(&worker_id) {
                        log::warn!(
                            "worker_to_bgs contains stale bg_id={} for worker_id={} not in replica_set; skip mark replica",
                            bg_id,
                            worker_id
                        );
                        continue;
                    }
                    let states = observed.entry(bg.bg_id).or_default();
                    let old = states
                        .get(&worker_id)
                        .copied()
                        .unwrap_or(ReplicaState::Pending);
                    if old == new_state {
                        continue;
                    }
                    states.insert(worker_id, new_state);
                    changed += 1;
                    // NodeState Live->Lost/Offline changes route visibility even when observed state was missing.
                    changed_tables.insert(bg.table_id);
                }
            }
        }
        self.mark_route_tables_dirty(&changed_tables);
        self.try_flush_dirty_route_tables("mark workers replicas route publish");
        if changed > 0 || !changed_tables.is_empty() {
            log::info!(
                "Marked worker replicas worker_count={}, changed={}, affected_tables={}",
                target_states.len(),
                changed,
                changed_tables.len()
            );
        }
        Ok(changed)
    }

    fn mark_route_tables_dirty(&self, table_ids: &HashSet<u32>) {
        if table_ids.is_empty() {
            return;
        }
        self.dirty_route_tables
            .write()
            .unwrap()
            .extend(table_ids.iter().copied());
    }

    fn flush_dirty_route_tables(&self, only: Option<&HashSet<u32>>) -> FsResult<()> {
        let mut last_target = HashSet::new();
        for attempt in 0..3 {
            let target = self.dirty_route_target(only);
            if target.is_empty() {
                return Ok(());
            }
            last_target = target.clone();

            let updates = self.build_table_epoch_updates(&target);
            self.clear_dirty_tables_without_epoch_update(&target, &updates);
            if updates.is_empty() {
                return Ok(());
            }

            self.journal_client
                .propose(PdEntry::BumpTableEpoch(BumpTableEpochEntry {
                    op_ms: orpc::common::LocalTime::mills(),
                    updates,
                }))?;

            if !self.has_dirty_route_tables(&target) {
                return Ok(());
            }
            log::warn!(
                "route table epoch bump attempt {} still has dirty tables: {:?}",
                attempt + 1,
                self.dirty_route_target(Some(&target))
            );
        }
        let remaining = self.dirty_route_target(Some(&last_target));
        if remaining.is_empty() {
            return Ok(());
        }
        log::warn!(
            "route table epoch bump failed to clear dirty tables after retries: {:?}",
            remaining
        );
        Err(FsError::common(format!(
            "route table epoch bump failed for tables {:?}",
            remaining
        )))
    }

    pub fn retry_dirty_route_publish(&self) -> FsResult<()> {
        if self.route_publish_disabled.load(Ordering::Acquire) {
            return Ok(());
        }
        self.flush_dirty_route_tables(None)
    }

    fn try_flush_dirty_route_tables(&self, reason: &str) {
        if self.route_publish_disabled.load(Ordering::Acquire) {
            return;
        }
        if let Err(e) = self.flush_dirty_route_tables(None) {
            log::warn!(
                "{} failed; dirty route tables kept for retry: {}",
                reason,
                e
            );
        }
    }

    fn dirty_route_target(&self, only: Option<&HashSet<u32>>) -> HashSet<u32> {
        let dirty = self.dirty_route_tables.read().unwrap();
        match only {
            Some(only) => dirty.intersection(only).copied().collect(),
            None => dirty.iter().copied().collect(),
        }
    }

    fn has_dirty_route_tables(&self, table_ids: &HashSet<u32>) -> bool {
        let dirty = self.dirty_route_tables.read().unwrap();
        table_ids.iter().any(|table_id| dirty.contains(table_id))
    }

    fn clear_dirty_tables_without_epoch_update(
        &self,
        target: &HashSet<u32>,
        updates: &[TableEpochUpdate],
    ) {
        let update_table_ids: HashSet<u32> = updates.iter().map(|u| u.table_id).collect();
        let missing: Vec<u32> = target.difference(&update_table_ids).copied().collect();
        if missing.is_empty() {
            return;
        }
        log::warn!(
            "clear dirty route tables without epoch update because tables are missing: {:?}",
            missing
        );
        let mut dirty = self.dirty_route_tables.write().unwrap();
        let mut published = self.published_routes.write().unwrap();
        for table_id in missing {
            dirty.remove(&table_id);
            published.remove(&table_id);
        }
    }

    fn build_table_epoch_updates(&self, table_ids: &HashSet<u32>) -> Vec<TableEpochUpdate> {
        let tables = self.tables.read().unwrap();
        table_ids
            .iter()
            .filter_map(|table_id| {
                let Some(table) = tables.get(table_id) else {
                    log::warn!("skip table_epoch bump for missing table_id={}", table_id);
                    return None;
                };
                Some(TableEpochUpdate {
                    table_id: *table_id,
                    expected_epoch: table.epoch,
                    new_epoch: table.epoch.saturating_add(1),
                })
            })
            .collect()
    }

    fn publish_observed_route_for_table(&self, table_id: u32) -> bool {
        let table = match self.tables.read().unwrap().get(&table_id).cloned() {
            Some(table) => table,
            None => {
                log::warn!("publish route for missing table_id={}; skip", table_id);
                return false;
            }
        };
        let bgs = self.bgs.read().unwrap();
        let observed = self.observed_replica_states.read().unwrap();
        let mut buckets = Vec::with_capacity(table.buckets.len());
        for &bg_id in &table.buckets {
            let Some(bg) = bgs.get(&bg_id) else {
                log::warn!(
                    "publish route for table_id={} skipped because bg_id={} is missing",
                    table_id,
                    bg_id
                );
                return false;
            };
            let states = observed.get(&bg.bg_id);
            let visible: Vec<u32> = bg
                .replica_set
                .iter()
                .copied()
                .filter(|&wid| self.route_replica_state(states, wid) == ReplicaState::Active)
                .collect();
            let mut filtered_bg: BlockGroupInfo = (**bg).clone();
            filtered_bg.replica_set = visible;
            buckets.push(Self::block_group_info_to_view(
                &filtered_bg,
                &self.pool_manager,
            ));
        }

        let summary = BGTableSummary {
            table_id: table.table_id,
            bucket_count: table.bucket_count,
            epoch: table.epoch,
            buckets,
            last_rebuild_ms: table.last_rebuild_ms,
        };
        self.published_routes
            .write()
            .unwrap()
            .insert(table_id, summary);
        self.dirty_route_tables.write().unwrap().remove(&table_id);
        true
    }

    /// Serving view for scheduler/operator: only observed Active replicas in replica_set.
    pub fn get_serving_replicas(&self, bg_id: u32) -> Vec<u32> {
        let bgs = self.bgs.read().unwrap();
        let bg = match bgs.get(&bg_id) {
            Some(bg) => bg,
            None => return vec![],
        };
        let observed = self.observed_replica_states.read().unwrap();
        let states = observed.get(&bg_id);
        bg.replica_set
            .iter()
            .filter(|&&wid| replica_state_in(states, wid) == ReplicaState::Active)
            .copied()
            .collect()
    }

    /// Resident view for scheduler/operator: non-Offline observed replicas in replica_set.
    pub fn get_resident_replicas(&self, bg_id: u32) -> Vec<u32> {
        let bgs = self.bgs.read().unwrap();
        let bg = match bgs.get(&bg_id) {
            Some(bg) => bg,
            None => return vec![],
        };
        let observed = self.observed_replica_states.read().unwrap();
        let states = observed.get(&bg_id);
        bg.replica_set
            .iter()
            .filter(|&&wid| replica_state_in(states, wid) != ReplicaState::Offline)
            .copied()
            .collect()
    }

    /// Propose Raft removal of a worker from a BG's replica_set.
    pub fn propose_remove_replica(&self, bg_id: u32, worker_id: u32) -> FsResult<()> {
        let bg = self
            .get_bg(bg_id)
            .ok_or_else(|| FsError::common(format!("bg {} not found", bg_id)))?;
        let new_rs: Vec<u32> = bg
            .replica_set
            .iter()
            .filter(|&&w| w != worker_id)
            .copied()
            .collect();
        let entry = BGUpdateEntry {
            op_ms: orpc::common::LocalTime::mills(),
            bg_id,
            state: None,
            replica_set: Some(new_rs),
            lease_owner: None,
            expected_bg_epoch: bg.bg_epoch,
            new_bg_epoch: bg.bg_epoch.saturating_add(1),
            bump_table_epoch: Some(bg.table_id),
        };
        self.propose_update_bg(entry, "propose_remove_replica")
    }

    /// Propose Raft addition of a worker to a BG's replica_set.
    pub fn propose_add_replica(&self, bg_id: u32, worker_id: u32) -> FsResult<()> {
        let bg = self
            .get_bg(bg_id)
            .ok_or_else(|| FsError::common(format!("bg {} not found", bg_id)))?;
        if bg.replica_set.contains(&worker_id) {
            return Ok(());
        }
        let mut new_rs = bg.replica_set.clone();
        new_rs.push(worker_id);
        let entry = BGUpdateEntry {
            op_ms: orpc::common::LocalTime::mills(),
            bg_id,
            state: None,
            replica_set: Some(new_rs),
            lease_owner: None,
            expected_bg_epoch: bg.bg_epoch,
            new_bg_epoch: bg.bg_epoch.saturating_add(1),
            bump_table_epoch: Some(bg.table_id),
        };
        self.propose_update_bg(entry, "propose_add_replica")
    }

    /// Propose Raft transfer of lease owner.
    pub fn propose_transfer_lease(
        &self,
        bg_id: u32,
        from_worker: u32,
        to_worker: u32,
    ) -> FsResult<()> {
        let bg = self
            .get_bg(bg_id)
            .ok_or_else(|| FsError::common(format!("bg {} not found", bg_id)))?;
        let current_owner = bg.lease_owner.as_ref().map(|l| l.node_id).unwrap_or(0);
        if current_owner != from_worker {
            return Ok(());
        }
        let new_epoch = bg
            .lease_owner
            .as_ref()
            .map(|l| l.epoch.saturating_add(1))
            .unwrap_or(1);
        let lease = BGLease {
            node_id: to_worker,
            epoch: new_epoch,
            grant_time_ms: orpc::common::LocalTime::mills(),
        };
        let entry = BGUpdateEntry {
            op_ms: orpc::common::LocalTime::mills(),
            bg_id,
            state: None,
            replica_set: None,
            lease_owner: Some(lease),
            expected_bg_epoch: bg.bg_epoch,
            new_bg_epoch: bg.bg_epoch.saturating_add(1),
            bump_table_epoch: Some(bg.table_id),
        };
        self.propose_update_bg(entry, "propose_transfer_lease")
    }

    /// Common BG update propose path: leader-fenced + ApplyOutcome dispatch.
    /// Per §17 contract, propose path does NOT retry on stale; the upper-layer
    /// scheduler (operator/checker) is responsible for re-planning on a fresh
    /// snapshot.
    fn propose_update_bg(&self, entry: BGUpdateEntry, kind: &str) -> FsResult<()> {
        let expected_epoch = entry.expected_bg_epoch;
        let bg_id = entry.bg_id;
        let outcome = self
            .journal_client
            .propose_as_leader_with_result(PdEntry::UpdateBG(entry))?;
        match outcome {
            ApplyOutcome::Applied | ApplyOutcome::SkippedNoop => Ok(()),
            ApplyOutcome::SkippedStale { reason } => {
                log::warn!(
                    "{} bg_id={} returned Stale (expected_bg_epoch={}): {}",
                    kind,
                    bg_id,
                    expected_epoch,
                    reason
                );
                Err(FsError::stale_entry("update_bg", expected_epoch, reason))
            }
            ApplyOutcome::NotFound { reason } => Err(FsError::not_found(reason)),
        }
    }

    pub fn apply_bump_table_epoch(&self, entry: &BumpTableEpochEntry) -> FsResult<()> {
        self.apply_bump_table_epoch_with_role(entry, true)
    }

    pub fn apply_bump_table_epoch_with_role(
        &self,
        entry: &BumpTableEpochEntry,
        is_leader: bool,
    ) -> FsResult<()> {
        let mut applied_tables = Vec::new();
        for update in &entry.updates {
            if self.apply_table_epoch_update(update)? {
                applied_tables.push(update.table_id);
            }
        }
        if is_leader {
            for table_id in &applied_tables {
                if !self.publish_observed_route_for_table(*table_id) {
                    log::warn!(
                        "BumpTableEpoch applied but failed to publish route table_id={}",
                        table_id
                    );
                }
            }
        }
        log::info!(
            "BumpTableEpoch applied updates={}, changed_tables={}, is_leader={}",
            entry.updates.len(),
            applied_tables.len(),
            is_leader
        );
        Ok(())
    }

    pub fn apply_create_bg(&self, entry: &BGEntry) -> FsResult<()> {
        self.apply_create_bg_with_role(entry, true)
    }

    pub fn apply_create_bg_with_role(&self, entry: &BGEntry, is_leader: bool) -> FsResult<()> {
        let info = &entry.info;
        let table_exists = {
            let tables = self.tables.read().unwrap();
            let mut bgs = self.bgs.write().unwrap();
            let mut w2b = self.worker_to_bgs.write().unwrap();

            self.store.put(info)?;
            if is_leader {
                self.seed_runtime_pending_for_bg(info);
            }
            bgs.insert(info.bg_id, Arc::new(info.clone()));
            for &wid in &info.replica_set {
                w2b.entry(wid).or_default().insert(info.bg_id);
            }

            tables.contains_key(&info.table_id)
        };

        // P4.2: do NOT bump table.epoch in-place. Mark the table dirty so the
        // leader's next heartbeat-driven try_flush proposes a BumpTableEpoch
        // entry through Raft.
        //
        // Critical (#1 fix): apply_*_bg runs inside the Raft state-machine
        // apply path. Calling try_flush_dirty_route_tables here would
        // synchronously block_on_send_propose into the same runtime → panic /
        // deadlock. The flush is driven by heartbeat handlers
        // (apply_replica_reports / mark_workers_replicas / etc.) and the stats
        // scheduler's retry_dirty_route_publish.
        if is_leader && table_exists {
            let mut dirty = HashSet::new();
            dirty.insert(info.table_id);
            self.mark_route_tables_dirty(&dirty);
        }
        Ok(())
    }

    pub fn apply_update_bg(&self, entry: &BGUpdateEntry) -> FsResult<ApplyOutcome> {
        self.apply_update_bg_with_role(entry, true)
    }

    pub fn apply_update_bg_with_role(
        &self,
        entry: &BGUpdateEntry,
        is_leader: bool,
    ) -> FsResult<ApplyOutcome> {
        let (outcome, mutation) = {
            let mut bgs = self.bgs.write().unwrap();
            let mut w2b = self.worker_to_bgs.write().unwrap();
            self.apply_single_update(entry, is_leader, &mut bgs, &mut w2b)?
        };

        let Some((table_id, _old, _new)) = mutation else {
            return Ok(outcome);
        };

        // P4.2 + #1: do NOT bump in-place AND do NOT propose synchronously
        // from apply path. Just mark dirty; heartbeat / stats path drives flush.
        if is_leader && entry.bump_table_epoch.is_some() {
            let mut dirty = HashSet::new();
            dirty.insert(table_id);
            self.mark_route_tables_dirty(&dirty);
        }
        Ok(outcome)
    }

    /// Apply one `BGUpdateEntry` to persisted metadata and indexes.
    ///
    /// Returns `(ApplyOutcome, Option<(table_id, old_replica_set, new_replica_set)>)`:
    /// the mutation tuple is `Some` only when `outcome == Applied`.
    /// CAS contract (P2.1):
    ///   - bg not found → returned as Err (state machine corruption)
    ///   - existing.bg_epoch != entry.expected_bg_epoch → SkippedStale
    ///   - entry.new_bg_epoch <= existing.bg_epoch → SkippedNoop (idempotent)
    ///   - otherwise → Applied + mutation tuple
    fn apply_single_update(
        &self,
        entry: &BGUpdateEntry,
        is_leader_runtime: bool,
        bgs: &mut HashMap<u32, Arc<BlockGroupInfo>>,
        w2b: &mut HashMap<u32, HashSet<u32>>,
    ) -> FsResult<(ApplyOutcome, Option<(u32, Vec<u32>, Vec<u32>)>)> {
        let existing_arc = bgs
            .get(&entry.bg_id)
            .cloned()
            .ok_or_else(|| FsError::common(format!("bg {} not found for update", entry.bg_id)))?;
        let mut info: BlockGroupInfo = (*existing_arc).clone();

        if info.bg_epoch != entry.expected_bg_epoch {
            log::warn!(
                "Apply UpdateBG skipped: bg_id={} stale, current_epoch={}, \
                 entry_expected_epoch={}, entry_new_epoch={}",
                entry.bg_id,
                info.bg_epoch,
                entry.expected_bg_epoch,
                entry.new_bg_epoch
            );
            return Ok((
                ApplyOutcome::stale(format!(
                    "bg_epoch mismatch: current={}, expected={}",
                    info.bg_epoch, entry.expected_bg_epoch
                )),
                None,
            ));
        }

        if entry.new_bg_epoch <= info.bg_epoch {
            log::warn!(
                "Apply UpdateBG noop: bg_id={} non-monotonic, current_epoch={}, \
                 entry_new_epoch={}",
                entry.bg_id,
                info.bg_epoch,
                entry.new_bg_epoch
            );
            return Ok((ApplyOutcome::SkippedNoop, None));
        }

        if let Some(s) = entry.state {
            state_machine::validate_transition(info.state, s)?;
        }

        // P2.4: lease.epoch must be strictly monotonic. Even though bg_epoch
        // CAS already protects most paths, lease epoch is also a per-BG version
        // counter that scheduler/operator depend on for correct lease lifecycle
        // tracking. Reject malformed entries that try to install an older or
        // equal lease.epoch under a higher bg_epoch.
        if let Some(ref new_lease) = entry.lease_owner {
            if let Some(ref old_lease) = info.lease_owner {
                if new_lease.epoch <= old_lease.epoch {
                    log::warn!(
                        "Apply UpdateBG skipped: bg_id={} non-monotonic lease epoch, \
                         current_lease_epoch={}, entry_lease_epoch={}",
                        entry.bg_id,
                        old_lease.epoch,
                        new_lease.epoch
                    );
                    return Ok((
                        ApplyOutcome::stale(format!(
                            "lease.epoch non-monotonic: current={}, entry={}",
                            old_lease.epoch, new_lease.epoch
                        )),
                        None,
                    ));
                }
            }
        }

        let old_replica_set = info.replica_set.clone();

        info.bg_epoch = entry.new_bg_epoch;
        if let Some(s) = entry.state {
            info.state = s;
        }
        if let Some(ref new_rs) = entry.replica_set {
            info.replica_set = new_rs.clone();
        }
        if let Some(ref lease) = entry.lease_owner {
            info.lease_owner = Some(lease.clone());
        }

        self.store.put(&info)?;
        let table_id = info.table_id;
        let new_replica_set = info.replica_set.clone();
        if is_leader_runtime && entry.replica_set.is_some() {
            self.sync_runtime_for_replica_set_change(
                entry.bg_id,
                &old_replica_set,
                &new_replica_set,
            );
        }
        bgs.insert(entry.bg_id, Arc::new(info));

        if entry.replica_set.is_some() {
            let old_set: HashSet<u32> = old_replica_set.iter().copied().collect();
            let new_set: HashSet<u32> = new_replica_set.iter().copied().collect();
            for &removed in old_set.difference(&new_set) {
                if let Some(set) = w2b.get_mut(&removed) {
                    set.remove(&entry.bg_id);
                    if set.is_empty() {
                        w2b.remove(&removed);
                    }
                }
            }
            for &added in new_set.difference(&old_set) {
                w2b.entry(added).or_default().insert(entry.bg_id);
            }
        }

        Ok((
            ApplyOutcome::Applied,
            Some((table_id, old_replica_set, new_replica_set)),
        ))
    }

    pub fn apply_delete_bg(&self, entry: &BGDeleteEntry) -> FsResult<ApplyOutcome> {
        self.apply_delete_bg_with_role(entry, true)
    }

    /// Apply DeleteBG with CAS guard (P2.2).
    ///
    /// - bg not found → `NotFound`
    /// - existing.bg_epoch != entry.expected_bg_epoch → `SkippedStale`
    /// - otherwise → delete + Applied; uses `info.table_id` (the BG's actual
    ///   table) for `bump_table_epoch_for_publish`, ignoring the
    ///   potentially stale `entry.table_id`.
    pub fn apply_delete_bg_with_role(
        &self,
        entry: &BGDeleteEntry,
        is_leader: bool,
    ) -> FsResult<ApplyOutcome> {
        // Read-then-CAS under write lock to avoid TOCTOU.
        let bg_to_delete = {
            let mut bgs_write = self.bgs.write().unwrap();
            let Some(existing) = bgs_write.get(&entry.bg_id).cloned() else {
                log::warn!("Apply DeleteBG skipped: bg_id={} not present", entry.bg_id);
                return Ok(ApplyOutcome::not_found(format!(
                    "bg {} not present",
                    entry.bg_id
                )));
            };
            if existing.bg_epoch != entry.expected_bg_epoch {
                log::warn!(
                    "Apply DeleteBG skipped: bg_id={} stale, current_epoch={}, \
                     entry_expected_epoch={}",
                    entry.bg_id,
                    existing.bg_epoch,
                    entry.expected_bg_epoch
                );
                return Ok(ApplyOutcome::stale(format!(
                    "bg_epoch mismatch: current={}, expected={}",
                    existing.bg_epoch, entry.expected_bg_epoch
                )));
            }
            // CAS passed — actually remove from in-memory index.
            bgs_write.remove(&entry.bg_id);
            existing
        };
        // Persist deletion to store.
        self.store.delete(entry.bg_id)?;

        // Use the BG's authoritative table_id, not entry.table_id (which may
        // be stale if the BG moved between tables — guards against §4.6 bug).
        let table_id = bg_to_delete.table_id;

        let mut w2b = self.worker_to_bgs.write().unwrap();
        for &wid in &bg_to_delete.replica_set {
            if let Some(set) = w2b.get_mut(&wid) {
                set.remove(&entry.bg_id);
                if set.is_empty() {
                    w2b.remove(&wid);
                }
            }
        }
        drop(w2b);

        // P4.2 + #1: mark dirty only; heartbeat / stats path drives the flush.
        if is_leader {
            self.observed_replica_states
                .write()
                .unwrap()
                .remove(&entry.bg_id);
            let mut dirty = HashSet::new();
            dirty.insert(table_id);
            self.mark_route_tables_dirty(&dirty);
        }
        Ok(ApplyOutcome::Applied)
    }

    pub fn apply_batch_bg(&self, entry: &BatchBGEntry) -> FsResult<ApplyOutcome> {
        self.apply_batch_bg_with_role(entry, true)
    }

    /// Apply a batch of BG operations from Raft.
    ///
    /// **Best-effort batch, NOT atomic** (#4 doc fix). Each entry is applied
    /// independently with its own CAS:
    ///   - `creates` whose `bg_id` already exists are skipped (warn).
    ///   - `updates` go through `apply_single_update` per-entry CAS; some may
    ///     `SkippedStale` while others apply.
    ///   - If `expected_table_absent` is set and the table already exists,
    ///     the **whole batch** is rejected as `SkippedStale` (the only group-
    ///     level guard, used by `create_table` to prevent table_id collisions).
    ///
    /// Why this is acceptable for our usage:
    ///   - `create_table`: `expected_table_absent=true` rejects the whole
    ///     batch on collision; on success all `creates` insert (no `bg_id`
    ///     collisions because the IdAllocator hands out fresh ids).
    ///   - `rebuild_table`: per-update CAS may partially apply. The BG
    ///     scheduler/checker is convergent — operators retry uncovered BGs
    ///     on the next patrol with a fresh snapshot.
    ///
    /// `ApplyOutcome::Applied` is returned with mutation counts in the log,
    /// not in the outcome payload (callers don't currently need them). If
    /// future code needs per-batch detail, extend `ApplyOutcome` with a new
    /// variant.
    ///
    /// P2.3 + P4.2 + #1: only mark route table dirty when at least one
    /// mutation actually landed (no phantom publish). Mark dirty only — do
    /// NOT propose `BumpTableEpoch` from inside apply (would deadlock; #1).
    pub fn apply_batch_bg_with_role(
        &self,
        entry: &BatchBGEntry,
        is_leader: bool,
    ) -> FsResult<ApplyOutcome> {
        let initial_publish_table_id = entry.table.as_ref().map(|table| table.table_id);

        // P2.3: table-create existence guard. Reject the whole batch if a
        // concurrent create already installed the table — prevents both the
        // table overwrite AND the orphan BG creation that pre-P2.3 produced.
        if let Some(ref table) = entry.table {
            if entry.expected_table_absent
                && self.tables.read().unwrap().contains_key(&table.table_id)
            {
                log::warn!(
                    "Apply BatchBG rejected: table_id={} already exists; \
                     `expected_table_absent` set, treating creates+updates as Stale",
                    table.table_id
                );
                return Ok(ApplyOutcome::stale(format!(
                    "table {} already exists",
                    table.table_id
                )));
            }
            self.store.put_table(table)?;
            self.tables
                .write()
                .unwrap()
                .insert(table.table_id, Arc::new(table.clone()));
        }

        // P2.3: count actual mutations to drive the route-publish decision.
        let mut applied_creates = 0usize;
        let mut applied_updates = 0usize;
        let table_created = entry.table.is_some();
        {
            let mut bgs = self.bgs.write().unwrap();
            let mut w2b = self.worker_to_bgs.write().unwrap();

            for bg in &entry.creates {
                if bgs.contains_key(&bg.bg_id) {
                    log::warn!(
                        "Apply BatchBG create skipped: bg_id={} already exists",
                        bg.bg_id
                    );
                    continue;
                }
                self.store.put(bg)?;
                if is_leader {
                    self.seed_runtime_pending_for_bg(bg);
                }
                bgs.insert(bg.bg_id, Arc::new(bg.clone()));
                for &wid in &bg.replica_set {
                    w2b.entry(wid).or_default().insert(bg.bg_id);
                }
                applied_creates += 1;
            }

            for update in &entry.updates {
                let (outcome, mutation) =
                    self.apply_single_update(update, is_leader, &mut bgs, &mut w2b)?;
                if matches!(outcome, ApplyOutcome::Applied) && mutation.is_some() {
                    applied_updates += 1;
                }
            }
        }

        if let Some(next_id) = entry.next_bg_id {
            self.store.set_next_bg_id(next_id)?;
        }

        // P2.3 + P4.2 + #1: only mark dirty when at least one mutation actually
        // landed (no phantom publish). table_created counts as a route change.
        // Do NOT try_flush from apply path — heartbeat / stats path drives flush.
        let any_mutation = table_created || applied_creates > 0 || applied_updates > 0;
        if is_leader && any_mutation {
            let mut dirty: HashSet<u32> = HashSet::new();
            if let Some(table_id) = entry.bump_table_epoch {
                dirty.insert(table_id);
            }
            if let Some(table_id) = initial_publish_table_id {
                dirty.insert(table_id);
            }
            if !dirty.is_empty() {
                self.mark_route_tables_dirty(&dirty);
            }
        }

        log::info!(
            "Apply BatchBG completed: table_created={}, applied_creates={}, \
             applied_updates={}, total_creates={}, total_updates={}",
            table_created,
            applied_creates,
            applied_updates,
            entry.creates.len(),
            entry.updates.len()
        );
        Ok(ApplyOutcome::Applied)
    }

    /// Propose a batch BG operation via Raft, with leader fence + ApplyOutcome
    /// translation (#3 fix). Pre-#3 used plain `propose()` which discarded the
    /// outcome — concurrent `create_table` would silently SkippedStale and
    /// callers thought their table-create succeeded.
    pub fn propose_batch_bg(&self, entry: BatchBGEntry) -> FsResult<()> {
        let table_id_for_log = entry
            .table
            .as_ref()
            .map(|t| t.table_id)
            .or(entry.bump_table_epoch);
        let outcome = self
            .journal_client
            .propose_as_leader_with_result(PdEntry::BatchBG(entry))?;
        match outcome {
            ApplyOutcome::Applied | ApplyOutcome::SkippedNoop => Ok(()),
            ApplyOutcome::SkippedStale { reason } => {
                log::warn!(
                    "BatchBG returned Stale (table_id={:?}): {}",
                    table_id_for_log,
                    reason
                );
                Err(FsError::stale_entry(
                    "batch_bg",
                    format!("table_id={:?}", table_id_for_log),
                    reason,
                ))
            }
            ApplyOutcome::NotFound { reason } => Err(FsError::not_found(reason)),
        }
    }

    /// Create a new BGTable for a pool. Uses the placement algorithm to assign BGs
    /// to workers, then proposes the entire result as a single BatchBG Raft entry.
    pub fn create_table(
        &self,
        pool_id: u16,
        bucket_count: u32,
        replica_count: u16,
        _workers: &[u32],
    ) -> FsResult<()> {
        let table_id = gen_table_id(pool_id, replica_count);

        if self.tables.read().unwrap().contains_key(&table_id) {
            return Err(FsError::common(format!(
                "table already exists for pool {} replicas {}",
                pool_id, replica_count
            )));
        }

        let next_bg_id = self.id_allocator.alloc(bucket_count)?;

        let stub_table = BGTable {
            table_id,
            bucket_count: 0,
            buckets: vec![],
            epoch: 0,
            create_time_ms: 0,
            last_rebuild_ms: 0,
            stats: BGTableStats::default(),
        };
        let inputs = self.prepare_placement_inputs(pool_id, &stub_table, true)?;

        let tolerant = self.rebuild_tolerant_ratio();
        let ctx = PlacementContext {
            workers: &inputs.workers,
            bucket_count,
            replica_count,
            tolerant_ratio: tolerant,
            lease_tolerant_ratio: tolerant,
        };
        let mut st = inputs.policy.prepare(&ctx)?;

        let result = super::placement::build_table(
            table_id,
            bucket_count,
            replica_count,
            next_bg_id,
            &ctx,
            &inputs.rule,
            inputs.policy.as_ref(),
            &mut st,
        )?;

        let entry = BatchBGEntry {
            op_ms: orpc::common::LocalTime::mills(),
            table: Some(result.table),
            creates: result.bgs,
            updates: vec![],
            next_bg_id: None,       // ID already advanced by IdAllocator's realloc
            bump_table_epoch: None, // new table carries its initial epoch
            // P2.3: refuse to overwrite a concurrently-created table.
            expected_table_absent: true,
        };
        self.propose_batch_bg(entry)
    }

    /// Check if a BGTable exists for the given pool_id.
    pub fn has_table_for_pool(&self, pool_id: u16) -> bool {
        self.tables
            .read()
            .unwrap()
            .values()
            .any(|t| t.pool_id() == pool_id)
    }

    // P4.2 (removed): `bump_table_epoch_for_publish` and
    // `bump_table_epoch_for_publish_locked` previously bumped table.epoch
    // in-place inside `apply_*_bg`. They are gone — all table.epoch bumps
    // now go through the `BumpTableEpoch` Raft entry (single source of
    // truth, with explicit CAS in `apply_table_epoch_update`). apply_*_bg
    // only marks the table dirty + try_flush.

    fn apply_table_epoch_update(&self, update: &TableEpochUpdate) -> FsResult<bool> {
        let table_arc = match self.tables.read().unwrap().get(&update.table_id).cloned() {
            Some(t) => t,
            None => {
                log::warn!(
                    "BumpTableEpoch for missing table_id={}, expected_epoch={}, new_epoch={}; skip",
                    update.table_id,
                    update.expected_epoch,
                    update.new_epoch
                );
                return Ok(false);
            }
        };
        if table_arc.epoch != update.expected_epoch {
            log::warn!(
                "stale BumpTableEpoch table_id={}, expected_epoch={}, current_epoch={}, new_epoch={}; skip",
                update.table_id,
                update.expected_epoch,
                table_arc.epoch,
                update.new_epoch
            );
            return Ok(false);
        }
        if update.new_epoch <= table_arc.epoch {
            log::warn!(
                "non-increasing BumpTableEpoch table_id={}, current_epoch={}, new_epoch={}; skip",
                update.table_id,
                table_arc.epoch,
                update.new_epoch
            );
            return Ok(false);
        }
        let mut table: BGTable = (*table_arc).clone();
        table.epoch = update.new_epoch;
        self.store.put_table(&table)?;
        self.tables
            .write()
            .unwrap()
            .insert(update.table_id, Arc::new(table));
        Ok(true)
    }

    fn seed_runtime_pending_for_bg(&self, bg: &BlockGroupInfo) {
        let mut observed = self.observed_replica_states.write().unwrap();
        let observed_states = observed.entry(bg.bg_id).or_default();
        for &wid in &bg.replica_set {
            observed_states.entry(wid).or_insert(ReplicaState::Pending);
        }
    }

    fn sync_runtime_for_replica_set_change(
        &self,
        bg_id: u32,
        old_replica_set: &[u32],
        new_replica_set: &[u32],
    ) {
        let old_set: HashSet<u32> = old_replica_set.iter().copied().collect();
        let new_set: HashSet<u32> = new_replica_set.iter().copied().collect();
        let mut observed = self.observed_replica_states.write().unwrap();
        let observed_states = observed.entry(bg_id).or_default();
        for &added in new_set.difference(&old_set) {
            observed_states
                .entry(added)
                .or_insert(ReplicaState::Pending);
        }
        for &removed in old_set.difference(&new_set) {
            observed_states.remove(&removed);
        }
        if observed_states.is_empty() {
            observed.remove(&bg_id);
        }
    }

    pub fn get_bg(&self, bg_id: u32) -> Option<Arc<BlockGroupInfo>> {
        self.bgs.read().unwrap().get(&bg_id).cloned()
    }

    pub fn get_table(&self, table_id: u32) -> Option<Arc<BGTable>> {
        self.tables.read().unwrap().get(&table_id).cloned()
    }

    /// Lookup bg_id by table_id and key, then return BlockGroupInfo if present.
    pub fn lookup_bg(&self, table_id: u32, key: &[u8]) -> Option<Arc<BlockGroupInfo>> {
        let tables = self.tables.read().unwrap();
        let table = tables.get(&table_id)?;
        let bg_id = table.lookup(key);
        drop(tables);
        self.bgs.read().unwrap().get(&bg_id).cloned()
    }

    pub fn list_tables(&self) -> Vec<Arc<BGTable>> {
        self.tables.read().unwrap().values().cloned().collect()
    }

    /// Per-table published route epoch map: table_id -> epoch. Used in heartbeat responses.
    /// Keep this in sync with `build_table_summary()`: both expose only the
    /// last published route snapshot, never a newer in-flight table epoch.
    pub fn get_table_epochs(&self) -> HashMap<u32, u64> {
        if !self.route_ready.load(Ordering::Acquire) {
            return HashMap::new();
        }
        self.published_routes
            .read()
            .unwrap()
            .iter()
            .map(|(&id, summary)| (id, summary.epoch))
            .collect()
    }

    pub fn list_bgs(&self) -> Vec<Arc<BlockGroupInfo>> {
        self.bgs.read().unwrap().values().cloned().collect()
    }

    /// Set BG operation state (runtime-only).
    pub fn set_op_state(&self, bg_id: u32, op_state: BGOpState) {
        let mut bgs = self.bgs.write().unwrap();
        if let Some(bg) = bgs.get_mut(&bg_id) {
            Arc::make_mut(bg).op_state = op_state;
        }
    }

    /// Update per-BG stats from worker heartbeat.
    pub fn update_bg_stats(&self, bg_stats: &HashMap<u32, BGStats>) {
        let now = orpc::common::LocalTime::mills();
        let mut bgs = self.bgs.write().unwrap();
        for (bg_id, stats) in bg_stats {
            if let Some(bg) = bgs.get_mut(bg_id) {
                let bg_mut = Arc::make_mut(bg);
                bg_mut.stats = stats.clone();
                bg_mut.stats.last_report_ms = now;
            }
        }
    }

    pub fn get_table_stats(&self, table_id: u32) -> BGTableStats {
        self.tables
            .read()
            .unwrap()
            .get(&table_id)
            .map(|t| t.stats.clone())
            .unwrap_or_default()
    }

    pub fn refresh_table_stats(&self) {
        // Phase 1: read-only aggregation.
        let mut aggregates: HashMap<u32, BGTableStats> = HashMap::new();
        {
            let tables = self.tables.read().unwrap();
            let bgs = self.bgs.read().unwrap();
            for (&table_id, table) in tables.iter() {
                let mut agg = BGTableStats::default();
                for &bg_id in &table.buckets {
                    if let Some(bg) = bgs.get(&bg_id) {
                        agg.used_bytes += bg.stats.used_bytes;
                        agg.free_bytes += bg.stats.free_bytes;
                        agg.block_count += bg.stats.block_count;
                        agg.last_report_ms = agg.last_report_ms.max(bg.stats.last_report_ms);
                    }
                }
                aggregates.insert(table_id, agg);
            }
        }
        // Phase 2: short write lock to publish.
        let mut tables = self.tables.write().unwrap();
        for (table_id, agg) in aggregates {
            if let Some(t) = tables.get_mut(&table_id) {
                Arc::make_mut(t).stats = agg;
            }
        }
    }

    /// BGs that have this worker in replica_set (uses worker_to_bgs index).
    pub fn get_bgs_on_worker(&self, worker_id: u32) -> Vec<Arc<BlockGroupInfo>> {
        let w2b = self.worker_to_bgs.read().unwrap();
        let Some(bg_ids) = w2b.get(&worker_id) else {
            return Vec::new();
        };
        let bgs = self.bgs.read().unwrap();
        bg_ids
            .iter()
            .filter_map(|bg_id| bgs.get(bg_id).cloned())
            .collect()
    }

    /// BGs in the given state.
    pub fn get_bgs_by_state(&self, state: BGState) -> Vec<Arc<BlockGroupInfo>> {
        self.bgs
            .read()
            .unwrap()
            .values()
            .filter(|bg| bg.state == state)
            .cloned()
            .collect()
    }

    /// Expand BlockGroupInfo to view (replica_set with address and state). BG module owns this.
    fn block_group_info_to_view(
        bg: &BlockGroupInfo,
        pool_manager: &PoolManager,
    ) -> BlockGroupInfoView {
        let replica_set: Vec<ReplicaInfo> = bg
            .replica_set
            .iter()
            .filter_map(|&node_id| {
                pool_manager
                    .get_worker_node(node_id)
                    .map(|node| ReplicaInfo {
                        node_id,
                        address: node.base.address,
                        state: node.state,
                        labels: node.base.labels,
                    })
            })
            .collect();
        BlockGroupInfoView {
            bg_id: bg.bg_id,
            table_id: bg.table_id,
            bg_epoch: bg.bg_epoch,
            replica_set,
            state: bg.state,
            op_state: bg.op_state,
            lease_owner: bg.lease_owner.clone(),
        }
    }

    fn route_replica_state(
        &self,
        states: Option<&HashMap<u32, ReplicaState>>,
        worker_id: u32,
    ) -> ReplicaState {
        if let Some(state) = states.and_then(|m| m.get(&worker_id)).copied() {
            return state;
        }
        match self
            .pool_manager
            .get_worker_node(worker_id)
            .map(|n| n.state)
        {
            Some(NodeState::Live) => ReplicaState::Active,
            _ => ReplicaState::Pending,
        }
    }

    /// Build client-facing summary for the given table.
    ///
    /// Client route reads only a published full-route snapshot. Live `tables`/`bgs`
    /// may already contain newer Raft-applied metadata, but clients keep seeing
    /// the last published table_epoch view until a publish atomically replaces it.
    pub fn build_table_summary(&self, table_id: u32) -> Option<BGTableSummary> {
        if !self.route_ready.load(Ordering::Acquire) {
            return None;
        }
        self.published_routes
            .read()
            .unwrap()
            .get(&table_id)
            .cloned()
    }

    /// Build per-table WorkerLoadSnapshot map for the given table.
    ///
    /// When `init` is true (new table creation), all actual counts are zero.
    /// When false, counts are derived from existing BGs in the table.
    fn build_worker_snapshots(
        &self,
        table: &BGTable,
        media: StorageType,
        init: bool,
    ) -> HashMap<u32, WorkerLoadSnapshot> {
        let pool_id = table.pool_id();
        let live_workers = self.pool_manager.get_live_workers(pool_id);

        let table_bgs: Vec<Arc<BlockGroupInfo>> = if init {
            vec![]
        } else {
            let bgs = self.bgs.read().unwrap();
            table
                .buckets
                .iter()
                .filter_map(|&id| bgs.get(&id).cloned())
                .collect()
        };

        live_workers
            .iter()
            .map(|&wid| {
                let (bg_count, lease_count) = if init {
                    (0, 0)
                } else {
                    let bg = table_bgs
                        .iter()
                        .filter(|b| b.replica_set.contains(&wid))
                        .count() as u32;
                    let lease = table_bgs
                        .iter()
                        .filter(|b| b.lease_owner.as_ref().map(|l| l.node_id) == Some(wid))
                        .count() as u32;
                    (bg, lease)
                };
                let labels = self.pool_manager.get_worker_labels(wid).unwrap_or_default();
                let (capacity, used) = self
                    .pool_manager
                    .get_worker_storage_stats(wid, media)
                    .unwrap_or((0, 0));
                (
                    wid,
                    WorkerLoadSnapshot {
                        worker_id: wid,
                        actual_bg: bg_count,
                        actual_lease: lease_count,
                        pending_bg_add: 0,
                        pending_bg_remove: 0,
                        pending_lease_in: 0,
                        pending_lease_out: 0,
                        capacity_bytes: capacity as u64,
                        used_bytes: used as u64,
                        labels,
                    },
                )
            })
            .collect()
    }

    /// Select replacement workers for an existing BG.
    /// Builds a per-table snapshot and runs the full Rule → Policy pipeline.
    /// Existing replicas are excluded. Returns up to `count` distinct workers.
    pub fn select_replacement_workers(
        &self,
        bg: &BlockGroupInfo,
        count: u16,
    ) -> FsResult<Vec<u32>> {
        let table = self
            .tables
            .read()
            .unwrap()
            .get(&bg.table_id)
            .cloned()
            .ok_or_else(|| FsError::common(format!("table {} not found", bg.table_id)))?;
        let inputs = self.prepare_placement_inputs(table.pool_id(), &table, false)?;

        let tolerant = self.rebuild_tolerant_ratio();
        let ctx = PlacementContext {
            workers: &inputs.workers,
            bucket_count: table.bucket_count,
            replica_count: table.replica_count(),
            tolerant_ratio: tolerant,
            lease_tolerant_ratio: tolerant,
        };
        let worker_labels = ctx.worker_labels();
        let constrained = inputs.rule.filter(&ctx.worker_ids(), &worker_labels);
        let mut st = inputs.policy.prepare(&ctx)?;

        let mut selected: Vec<u32> = Vec::with_capacity(count as usize);
        let mut exclude: HashSet<u32> = bg.replica_set.iter().copied().collect();

        for _ in 0..count {
            let current: Vec<u32> = bg
                .replica_set
                .iter()
                .copied()
                .chain(selected.iter().copied())
                .collect();

            let picked = super::placement::select_with_fallback(
                &ctx,
                &mut st,
                &inputs.rule,
                inputs.policy.as_ref(),
                &constrained,
                &worker_labels,
                &current,
                &exclude,
            );
            let Some(picked) = picked else { break };

            selected.push(picked);
            exclude.insert(picked);
            st.record_bg_change(None, picked);
        }

        if selected.len() < count as usize {
            return Err(FsError::common(format!(
                "selector returned {} workers for BG {}, need {}",
                selected.len(),
                bg.bg_id,
                count
            )));
        }
        Ok(selected)
    }

    /// Used by the scheduler to inspect what a rebuild would change.
    pub fn compute_rebuild_diff(&self, table_id: u32) -> FsResult<Vec<(BlockGroupInfo, Vec<u32>)>> {
        Ok(self
            .plan_rebuild(table_id)?
            .map(|p| p.changes)
            .unwrap_or_default())
    }

    /// Plan and persist: propose a Raft batch entry that applies the rebuild.
    pub fn rebuild_table(&self, table_id: u32) -> FsResult<()> {
        let Some(plan) = self.plan_rebuild(table_id)? else {
            return Ok(());
        };
        if plan.changes.is_empty() {
            return Ok(());
        }
        let entry = self.build_rebuild_batch_entry(&plan);
        self.propose_batch_bg(entry)
    }

    /// Plan-only side of rebuild: read table + existing BGs,
    /// run the placement algorithm, return the diff.
    /// Returns `None` if the table has no existing BGs to rebuild.
    fn plan_rebuild(&self, table_id: u32) -> FsResult<Option<RebuildPlan>> {
        let table_arc = {
            let tables = self.tables.read().unwrap();
            tables
                .get(&table_id)
                .cloned()
                .ok_or_else(|| FsError::common(format!("table {} not found", table_id)))?
        };
        let table: BGTable = (*table_arc).clone();

        let existing_bgs: Vec<BlockGroupInfo> = {
            let bgs = self.bgs.read().unwrap();
            table
                .buckets
                .iter()
                .filter(|&&id| id != 0)
                .filter_map(|id| bgs.get(id).map(|arc| (**arc).clone()))
                .collect()
        };
        if existing_bgs.is_empty() {
            return Ok(None);
        }

        let inputs = self.prepare_placement_inputs(table.pool_id(), &table, false)?;
        let tolerant = self.rebuild_tolerant_ratio();
        let ctx = PlacementContext {
            workers: &inputs.workers,
            bucket_count: table.bucket_count,
            replica_count: table.replica_count(),
            tolerant_ratio: tolerant,
            lease_tolerant_ratio: tolerant,
        };
        let mut st = inputs.policy.prepare(&ctx)?;

        let result = super::placement::rebuild_table(
            &table,
            &existing_bgs,
            &ctx,
            &inputs.rule,
            inputs.policy.as_ref(),
            &mut st,
            &RebuildOptions::default(),
        )?;

        let old_by_id: HashMap<u32, Vec<u32>> = existing_bgs
            .iter()
            .map(|bg| (bg.bg_id, bg.replica_set.clone()))
            .collect();
        let changes = result
            .updated_bgs
            .into_iter()
            .map(|bg| {
                let old = old_by_id.get(&bg.bg_id).cloned().unwrap_or_default();
                (bg, old)
            })
            .collect();
        Ok(Some(RebuildPlan { table, changes }))
    }

    /// Serialize a planned rebuild into a single batched journal entry,
    /// bumping the table's epoch.
    fn build_rebuild_batch_entry(&self, plan: &RebuildPlan) -> BatchBGEntry {
        let now = orpc::common::LocalTime::mills();
        let updates = plan
            .changes
            .iter()
            .map(|(new_bg, _old)| {
                // The placement planner produced new BG state with bg_epoch
                // already bumped (= current + 1). expected_bg_epoch must point
                // to the version we read at plan time.
                let expected = new_bg.bg_epoch.saturating_sub(1);
                BGUpdateEntry {
                    op_ms: now,
                    bg_id: new_bg.bg_id,
                    state: None,
                    replica_set: Some(new_bg.replica_set.clone()),
                    lease_owner: new_bg.lease_owner.clone(),
                    expected_bg_epoch: expected,
                    new_bg_epoch: new_bg.bg_epoch,
                    bump_table_epoch: None,
                }
            })
            .collect();
        BatchBGEntry {
            op_ms: now,
            table: None,
            creates: vec![],
            updates,
            next_bg_id: None,
            bump_table_epoch: Some(plan.table.table_id),
            expected_table_absent: false,
        }
    }

    /// Rebuild all tables for a pool, calls rebuild_table for each table in the pool.
    pub fn rebuild_tables_for_pool(&self, pool_id: u16) -> FsResult<()> {
        let table_ids: Vec<u32> = self
            .list_tables()
            .into_iter()
            .filter(|t| t.pool_id() == pool_id)
            .map(|t| t.table_id)
            .collect();
        for table_id in table_ids {
            if let Err(e) = self.rebuild_table(table_id) {
                log::error!("Failed to rebuild table {}: {}", table_id, e);
            }
        }
        Ok(())
    }

    /// Build placement rule from global config + static location labels.
    pub fn placement_rule(&self) -> PlacementRule {
        let policy_name = self.config_manager.get_string(keys::PD_BG_PLACEMENT_POLICY);
        let min_iso = self
            .config_manager
            .get_string(keys::PD_BG_MIN_ISOLATION_LEVEL);
        let min_isolation_level = if min_iso.is_empty() {
            None
        } else {
            Some(min_iso)
        };

        match policy_name.as_str() {
            keys::PD_BG_PLACEMENT_POLICY_TOPOLOGY_AWARE => PlacementRule {
                id: keys::PD_BG_PLACEMENT_POLICY_TOPOLOGY_AWARE.into(),
                label_constraints: vec![],
                location_labels: self.location_labels.clone(),
                min_isolation_level,
            },
            _ => PlacementRule {
                min_isolation_level,
                ..PlacementRule::default_rule()
            },
        }
    }

    /// Count leases per worker across all BGs (used by LeaseValidityChecker).
    pub fn get_worker_lease_counts(&self) -> HashMap<u32, u32> {
        let bgs = self.bgs.read().unwrap();
        let mut counts: HashMap<u32, u32> = HashMap::new();
        for bg in bgs.values() {
            if let Some(ref lease) = bg.lease_owner {
                *counts.entry(lease.node_id).or_default() += 1;
            }
        }
        counts
    }

    pub fn bucket_count(&self) -> u32 {
        self.bucket_count
    }

    pub fn replica_counts(&self) -> &[u16] {
        &self.replica_counts
    }

    /// Assemble the worker snapshot, placement rule, and balance policy for `pool_id`.
    fn prepare_placement_inputs(
        &self,
        pool_id: u16,
        table_for_snapshot: &BGTable,
        for_create: bool,
    ) -> FsResult<PlacementInputs> {
        let pool = self.pool_manager.get_pool(pool_id)?;
        let workers = self.build_worker_snapshots(table_for_snapshot, pool.media, for_create);
        let rule = self.placement_rule();
        let policy = create_policy(&self.balance_policy_strategy());
        Ok(PlacementInputs {
            workers,
            rule,
            policy,
        })
    }
}

/// Resolve a per-replica state from `states`, defaulting to Pending.
fn replica_state_in(states: Option<&HashMap<u32, ReplicaState>>, wid: u32) -> ReplicaState {
    states
        .and_then(|m| m.get(&wid))
        .copied()
        .unwrap_or(ReplicaState::Pending)
}

/// the Rule → Policy pipeline.
struct PlacementInputs {
    workers: HashMap<u32, WorkerLoadSnapshot>,
    rule: PlacementRule,
    policy: Box<dyn super::placement::PlacementPolicy>,
}

/// Output of `plan_rebuild`: the table being rebuilt and the per-BG
/// diff (`new_bg`, `old_replica_set`) for every BG that actually changed
struct RebuildPlan {
    table: BGTable,
    changes: Vec<(BlockGroupInfo, Vec<u32>)>,
}

#[cfg(test)]
impl BGManager {
    /// Insert a BGTable directly into the in-memory index, bypassing Raft propose.
    pub fn test_insert_table(&self, table: super::BGTable) {
        self.tables
            .write()
            .unwrap()
            .insert(table.table_id, Arc::new(table));
    }

    pub fn test_insert_node(&self, node: curvine_common::state::NodeInfo) {
        self.pool_manager.test_insert_node(node);
    }

    pub fn test_clear_observed_replica_states(&self) {
        self.reset_runtime_route_state();
    }

    pub fn test_publish_table(&self, table_id: u32) {
        self.publish_observed_route_for_table(table_id);
        self.route_ready.store(true, Ordering::Release);
    }

    /// Test-only: skip the leader-side `try_flush_dirty_route_tables` propose.
    /// Set this in tests that have no real Raft cluster behind `journal_client`,
    /// so apply_*_bg paths don't block on a propose timeout.
    pub fn test_disable_route_publish(&self) {
        self.route_publish_disabled.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::config::ConfigManager;
    use crate::pd::node::{NodeManager, NodeStore};
    use crate::pd::pool::PoolStore;
    use crate::pd::store::memory_kv_engine::MemoryKvEngine;
    use crate::pd::store::KvStore;
    use curvine_common::conf::JournalConf;
    use curvine_common::raft::RaftClient;
    use curvine_common::state::{BGLease, BGState};

    fn test_manager() -> BGManager {
        let store: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
        let bg_store = Arc::new(BGStore::new(store));
        let pool_store = Arc::new(PoolStore::new(Arc::new(MemoryKvEngine::new())));
        let node_store = Arc::new(NodeStore::new(Arc::new(MemoryKvEngine::new())));
        let journal_conf = JournalConf::default();
        let rt = journal_conf.create_runtime();
        let raft = RaftClient::from_conf(rt, &journal_conf);
        let jc = Arc::new(journal::Client::new(raft));
        let config_manager = Arc::new(ConfigManager::new(
            Arc::new(MemoryKvEngine::new()),
            jc.clone(),
            HashMap::new(),
        ));
        let node_manager: Arc<NodeManager> = Arc::new(NodeManager::new(
            node_store,
            config_manager.clone(),
            jc.clone(),
        ));
        let pool_manager = Arc::new(PoolManager::new(pool_store, node_manager, jc.clone()));
        let mgr = BGManager::new(
            bg_store,
            pool_manager,
            jc,
            config_manager,
            1024,
            vec![3],
            vec![],
        );
        // Tests have no real Raft cluster — disable route publish so apply
        // paths don't block on a propose timeout (P4.2).
        mgr.test_disable_route_publish();
        mgr
    }

    fn make_bg(bg_id: u32, table_id: u32, replica_set: Vec<u32>) -> BlockGroupInfo {
        let leader = replica_set.first().copied().unwrap_or(0);
        BlockGroupInfo {
            bg_id,
            table_id,
            bg_epoch: 1,
            replica_set,
            state: BGState::Assigned,
            op_state: Default::default(),
            lease_owner: Some(BGLease {
                node_id: leader,
                epoch: 1,
                grant_time_ms: 0,
            }),
            stats: Default::default(),
        }
    }

    #[test]
    fn apply_create_bg_and_get() {
        let mgr = test_manager();
        let info = make_bg(1, 10, vec![100, 101, 102]);
        mgr.apply_create_bg(&BGEntry {
            op_ms: 0,
            info: info.clone(),
        })
        .unwrap();
        let got = mgr.get_bg(1).unwrap();
        assert_eq!(got.bg_id, 1);
        assert_eq!(got.replica_set, vec![100, 101, 102]);
    }

    #[test]
    fn apply_update_bg_state() {
        let mgr = test_manager();
        let info = make_bg(2, 10, vec![200, 201]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        mgr.apply_update_bg(&BGUpdateEntry {
            op_ms: 1,
            bg_id: 2,
            state: Some(BGState::Degraded),
            replica_set: None,
            lease_owner: None,
            expected_bg_epoch: 1,
            new_bg_epoch: 2,
            bump_table_epoch: None,
        })
        .unwrap();
        let got = mgr.get_bg(2).unwrap();
        assert_eq!(got.state, BGState::Degraded);
    }

    #[test]
    fn apply_update_bg_replica_set() {
        let mgr = test_manager();
        let info = make_bg(3, 10, vec![300]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        mgr.apply_update_bg(&BGUpdateEntry {
            op_ms: 1,
            bg_id: 3,
            state: None,
            replica_set: Some(vec![301, 302]),
            lease_owner: None,
            expected_bg_epoch: 1,
            new_bg_epoch: 2,
            bump_table_epoch: None,
        })
        .unwrap();
        let got = mgr.get_bg(3).unwrap();
        assert_eq!(got.replica_set, vec![301, 302]);
    }

    #[test]
    fn apply_update_bg_lease_owner() {
        let mgr = test_manager();
        let info = make_bg(5, 10, vec![500, 501]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        mgr.apply_update_bg(&BGUpdateEntry {
            op_ms: 1,
            bg_id: 5,
            state: None,
            replica_set: None,
            lease_owner: Some(BGLease {
                node_id: 501,
                epoch: 2,
                grant_time_ms: 99_000,
            }),
            expected_bg_epoch: 1,
            new_bg_epoch: 2,
            bump_table_epoch: None,
        })
        .unwrap();
        let got = mgr.get_bg(5).unwrap();
        assert_eq!(got.lease_owner.as_ref().unwrap().node_id, 501);
        assert_eq!(got.lease_owner.as_ref().unwrap().grant_time_ms, 99_000);
    }

    #[test]
    fn apply_delete_bg() {
        let mgr = test_manager();
        let info = make_bg(4, 10, vec![400]);
        mgr.test_insert_table(BGTable {
            table_id: 10,
            bucket_count: 1,
            buckets: vec![4],
            epoch: 0,
            create_time_ms: 0,
            last_rebuild_ms: 0,
            stats: BGTableStats::default(),
        });
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        assert!(mgr.get_bg(4).is_some());
        mgr.apply_delete_bg(&BGDeleteEntry {
            op_ms: 0,
            bg_id: 4,
            table_id: 10,
            expected_bg_epoch: 1,
        })
        .unwrap();
        assert!(mgr.get_bg(4).is_none());
    }

    #[test]
    fn restore_loads_bgs() {
        let mgr = test_manager();
        let info = make_bg(5, 10, vec![500]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        let bgs_before = mgr.list_bgs();
        assert_eq!(bgs_before.len(), 1);
        mgr.restore().unwrap();
        let bgs_after = mgr.list_bgs();
        assert_eq!(bgs_after.len(), 1);
        assert_eq!(bgs_after[0].bg_id, 5);
    }

    // ========== Epoch correctness tests ==========

    #[test]
    fn bg_epoch_increments_on_replica_set_change() {
        let mgr = test_manager();
        let info = make_bg(10, 1, vec![1, 2, 3]);
        mgr.apply_create_bg(&BGEntry {
            op_ms: 0,
            info: info.clone(),
        })
        .unwrap();
        assert_eq!(mgr.get_bg(10).unwrap().bg_epoch, 1);

        mgr.apply_update_bg(&BGUpdateEntry {
            op_ms: 1,
            bg_id: 10,
            state: None,
            replica_set: Some(vec![1, 2, 4]),
            lease_owner: None,
            expected_bg_epoch: 1,
            new_bg_epoch: 2,
            bump_table_epoch: None,
        })
        .unwrap();
        assert_eq!(mgr.get_bg(10).unwrap().bg_epoch, 2);

        // Second replica change should increment again
        mgr.apply_update_bg(&BGUpdateEntry {
            op_ms: 2,
            bg_id: 10,
            state: None,
            replica_set: Some(vec![1, 5, 4]),
            lease_owner: None,
            expected_bg_epoch: 2,
            new_bg_epoch: 3,
            bump_table_epoch: None,
        })
        .unwrap();
        assert_eq!(mgr.get_bg(10).unwrap().bg_epoch, 3);
    }

    #[test]
    fn lease_epoch_increments_on_lease_change() {
        let mgr = test_manager();
        let info = make_bg(11, 1, vec![1, 2]);
        mgr.apply_create_bg(&BGEntry {
            op_ms: 0,
            info: info.clone(),
        })
        .unwrap();
        assert_eq!(
            mgr.get_bg(11).unwrap().lease_owner.as_ref().unwrap().epoch,
            1
        );

        mgr.apply_update_bg(&BGUpdateEntry {
            op_ms: 1,
            bg_id: 11,
            state: None,
            replica_set: None,
            lease_owner: Some(BGLease {
                node_id: 2,
                epoch: 2,
                grant_time_ms: 100_000,
            }),
            expected_bg_epoch: 1,
            new_bg_epoch: 2,
            bump_table_epoch: None,
        })
        .unwrap();
        assert_eq!(
            mgr.get_bg(11).unwrap().lease_owner.as_ref().unwrap().epoch,
            2
        );
    }

    #[test]
    fn state_change_increments_bg_epoch() {
        let mgr = test_manager();
        let info = make_bg(12, 1, vec![1]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        let before = mgr.get_bg(12).unwrap();

        mgr.apply_update_bg(&BGUpdateEntry {
            op_ms: 1,
            bg_id: 12,
            state: Some(BGState::Degraded),
            replica_set: None,
            lease_owner: None,
            expected_bg_epoch: before.bg_epoch,
            new_bg_epoch: before.bg_epoch + 1,
            bump_table_epoch: None,
        })
        .unwrap();
        let after = mgr.get_bg(12).unwrap();
        assert_eq!(after.bg_epoch, before.bg_epoch + 1);
        // Lease epoch should not change
        assert_eq!(
            before.lease_owner.as_ref().unwrap().epoch,
            after.lease_owner.as_ref().unwrap().epoch
        );
    }

    #[test]
    fn invalid_state_transition_rejected() {
        let mgr = test_manager();
        // BG starts at Assigned state
        let info = make_bg(13, 1, vec![1]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();

        // Assigned -> Recovering is not valid (must go through Degraded first)
        let result = mgr.apply_update_bg(&BGUpdateEntry {
            op_ms: 1,
            bg_id: 13,
            state: Some(BGState::Recovering),
            replica_set: None,
            lease_owner: None,
            expected_bg_epoch: 1,
            new_bg_epoch: 2,
            bump_table_epoch: None,
        });
        assert!(result.is_err());
    }

    // ========== Worker-to-BG index tests ==========

    #[test]
    fn worker_to_bg_index_updated_on_replica_change() {
        let mgr = test_manager();
        let info = make_bg(20, 1, vec![1, 2]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();

        // Worker 1 and 2 should have bg 20
        assert_eq!(mgr.get_bgs_on_worker(1).len(), 1);
        assert_eq!(mgr.get_bgs_on_worker(2).len(), 1);
        assert_eq!(mgr.get_bgs_on_worker(3).len(), 0);

        // Change replica_set: remove worker 2, add worker 3
        mgr.apply_update_bg(&BGUpdateEntry {
            op_ms: 1,
            bg_id: 20,
            state: None,
            replica_set: Some(vec![1, 3]),
            lease_owner: None,
            expected_bg_epoch: 1,
            new_bg_epoch: 2,
            bump_table_epoch: None,
        })
        .unwrap();

        assert_eq!(mgr.get_bgs_on_worker(1).len(), 1);
        assert_eq!(mgr.get_bgs_on_worker(2).len(), 0); // removed
        assert_eq!(mgr.get_bgs_on_worker(3).len(), 1); // added
    }

    #[test]
    fn worker_to_bg_index_cleaned_on_delete() {
        let mgr = test_manager();
        let info = make_bg(21, 1, vec![1, 2]);
        mgr.test_insert_table(BGTable {
            table_id: 1,
            bucket_count: 1,
            buckets: vec![21],
            epoch: 0,
            create_time_ms: 0,
            last_rebuild_ms: 0,
            stats: BGTableStats::default(),
        });
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        assert_eq!(mgr.get_bgs_on_worker(1).len(), 1);

        mgr.apply_delete_bg(&BGDeleteEntry {
            op_ms: 0,
            bg_id: 21,
            table_id: 1,
            expected_bg_epoch: 1,
        })
        .unwrap();
        assert_eq!(mgr.get_bgs_on_worker(1).len(), 0);
        assert_eq!(mgr.get_bgs_on_worker(2).len(), 0);
    }

    // ========== set_op_state tests ==========

    #[test]
    fn set_op_state_updates_runtime_state() {
        let mgr = test_manager();
        let info = make_bg(30, 1, vec![1]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        assert_eq!(mgr.get_bg(30).unwrap().op_state, BGOpState::Idle);

        mgr.set_op_state(30, BGOpState::Recovering);
        assert_eq!(mgr.get_bg(30).unwrap().op_state, BGOpState::Recovering);

        mgr.set_op_state(30, BGOpState::Idle);
        assert_eq!(mgr.get_bg(30).unwrap().op_state, BGOpState::Idle);
    }

    #[test]
    fn set_op_state_nonexistent_bg_is_noop() {
        let mgr = test_manager();
        // Should not panic
        mgr.set_op_state(999, BGOpState::Recovering);
    }

    fn seed_active_replica(mgr: &BGManager, bg_id: u32, worker_id: u32) {
        mgr.apply_create_bg(&BGEntry {
            op_ms: 0,
            info: make_bg(bg_id, 0x0001_0003, vec![worker_id]),
        })
        .unwrap();
        mgr.set_replica_state(bg_id, worker_id, ReplicaState::Active);
    }

    fn make_worker_node(id: u32, state: NodeState) -> curvine_common::state::NodeInfo {
        curvine_common::state::NodeInfo {
            base: curvine_common::state::NodeBase {
                node_id: id,
                node_type: curvine_common::state::NodeType::Worker,
                address: curvine_common::state::NodeAddress {
                    hostname: format!("worker-{}", id),
                    ip: format!("10.0.0.{}", id),
                    rpc_port: 8000 + id as u16,
                    web_port: 9000 + id as u16,
                },
                labels: HashMap::from([
                    ("az".to_string(), format!("az-{}", id % 2)),
                    ("rack".to_string(), format!("rack-{}", id % 4)),
                ]),
                ..Default::default()
            },
            state,
            epoch: 1,
            last_heartbeat_ms: 0,
            state_since_ms: 0,
            last_persist_ms: 0,
            sys_stats: Default::default(),
            payload: curvine_common::state::NodePayload::Worker(Default::default()),
        }
    }

    #[test]
    fn route_view_hides_explicit_pending_live_replica() {
        let mgr = test_manager();
        mgr.test_insert_node(make_worker_node(100, NodeState::Live));
        mgr.test_insert_table(BGTable {
            table_id: 10,
            bucket_count: 1,
            buckets: vec![1],
            epoch: 1,
            create_time_ms: 0,
            last_rebuild_ms: 0,
            stats: BGTableStats::default(),
        });
        mgr.apply_create_bg(&BGEntry {
            op_ms: 0,
            info: make_bg(1, 10, vec![100]),
        })
        .unwrap();
        mgr.test_publish_table(10);

        let summary = mgr.build_table_summary(10).unwrap();
        assert!(summary.buckets[0].replica_set.is_empty());
    }

    #[test]
    fn route_view_defaults_missing_live_replica_to_active_after_failover() {
        let mgr = test_manager();
        mgr.test_insert_node(make_worker_node(100, NodeState::Live));
        mgr.test_insert_table(BGTable {
            table_id: 10,
            bucket_count: 1,
            buckets: vec![1],
            epoch: 1,
            create_time_ms: 0,
            last_rebuild_ms: 0,
            stats: BGTableStats::default(),
        });
        mgr.apply_create_bg(&BGEntry {
            op_ms: 0,
            info: make_bg(1, 10, vec![100]),
        })
        .unwrap();
        mgr.test_clear_observed_replica_states();
        mgr.test_publish_table(10);

        let summary = mgr.build_table_summary(10).unwrap();
        assert_eq!(summary.buckets[0].replica_set.len(), 1);
        let replica = &summary.buckets[0].replica_set[0];
        assert_eq!(replica.node_id, 100);
        assert_eq!(replica.labels.get("az"), Some(&"az-0".to_string()));
        assert_eq!(replica.labels.get("rack"), Some(&"rack-0".to_string()));
    }

    #[test]
    fn scheduler_view_defaults_missing_replica_state_to_pending() {
        let mgr = test_manager();
        mgr.test_insert_node(make_worker_node(100, NodeState::Live));
        mgr.apply_create_bg(&BGEntry {
            op_ms: 0,
            info: make_bg(1, 10, vec![100]),
        })
        .unwrap();
        mgr.test_clear_observed_replica_states();

        assert_eq!(mgr.get_replica_state(1, 100), ReplicaState::Pending);
    }

    #[test]
    fn runtime_replica_state_defaults_pending_and_can_be_set() {
        let mgr = test_manager();
        assert_eq!(mgr.get_replica_state(1, 100), ReplicaState::Pending);
        seed_active_replica(&mgr, 1, 100);
        assert_eq!(mgr.get_replica_state(1, 100), ReplicaState::Active);
        mgr.set_replica_state(1, 100, ReplicaState::Lost);
        assert_eq!(mgr.get_replica_state(1, 100), ReplicaState::Lost);
    }

    #[test]
    fn apply_bump_table_epoch_updates_epoch() {
        let mgr = test_manager();
        let table = BGTable {
            table_id: 10,
            bucket_count: 1,
            buckets: vec![1],
            epoch: 1,
            create_time_ms: 0,
            last_rebuild_ms: 0,
            stats: BGTableStats::default(),
        };
        mgr.test_insert_table(table);
        mgr.apply_bump_table_epoch(&BumpTableEpochEntry {
            op_ms: 10,
            updates: vec![TableEpochUpdate {
                table_id: 10,
                expected_epoch: 1,
                new_epoch: 2,
            }],
        })
        .unwrap();
        assert_eq!(mgr.get_table(10).unwrap().epoch, 2);

        // Idempotent/stale bumps do not move the epoch backwards.
        mgr.apply_bump_table_epoch(&BumpTableEpochEntry {
            op_ms: 11,
            updates: vec![TableEpochUpdate {
                table_id: 10,
                expected_epoch: 1,
                new_epoch: 2,
            }],
        })
        .unwrap();
        assert_eq!(mgr.get_table(10).unwrap().epoch, 2);
    }

    #[test]
    fn hidden_report_updates_runtime_without_epoch_bump() {
        let mgr = test_manager();
        mgr.apply_create_bg(&BGEntry {
            op_ms: 0,
            info: make_bg(1, 0x0001_0003, vec![100]),
        })
        .unwrap();

        let changed = mgr
            .apply_replica_reports(
                100,
                &[WorkerBGReport {
                    bg_id: 1,
                    state: ReplicaState::Syncing,
                    stats: BGStats::default(),
                }],
            )
            .unwrap();
        assert_eq!(changed, 1);
        assert_eq!(mgr.get_replica_state(1, 100), ReplicaState::Syncing);
    }

    #[test]
    fn report_transition_to_active_updates_runtime() {
        let mgr = test_manager();
        mgr.apply_create_bg(&BGEntry {
            op_ms: 0,
            info: make_bg(1, 0x0001_0003, vec![100]),
        })
        .unwrap();

        let changed = mgr
            .apply_replica_reports(
                100,
                &[WorkerBGReport {
                    bg_id: 1,
                    state: ReplicaState::Active,
                    stats: BGStats::default(),
                }],
            )
            .unwrap();
        assert_eq!(changed, 1);
        assert_eq!(mgr.get_replica_state(1, 100), ReplicaState::Active);
    }

    /// Regression: the batch path used to update `worker_to_bgs` but skipped
    /// `observed_replica_states`, leaking stale states for removed workers.

    #[test]
    fn mark_workers_replicas_marks_only_target_worker_lost() {
        let mgr = test_manager();
        let info = make_bg(30_000, 10, vec![100, 101]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        mgr.set_replica_state(30_000, 100, ReplicaState::Active);
        mgr.set_replica_state(30_000, 101, ReplicaState::Active);

        let changed = mgr
            .mark_workers_replicas(&[(100, ReplicaState::Lost)])
            .unwrap();

        assert_eq!(changed, 1);
        assert_eq!(mgr.get_replica_state(30_000, 100), ReplicaState::Lost);
        assert_eq!(mgr.get_replica_state(30_000, 101), ReplicaState::Active);
    }

    #[test]
    fn apply_batch_bg_syncs_observed_replica_states_on_replica_change() {
        let mgr = test_manager();
        let info = make_bg(20, 10, vec![100, 101, 102]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        // Seed all three replicas as Active so we can tell them apart from
        // defaulted Pending later.
        for wid in [100, 101, 102] {
            mgr.set_replica_state(20, wid, ReplicaState::Active);
        }

        // Batch update: drop 102, add 103.
        let batch = BatchBGEntry {
            op_ms: 1,
            table: None,
            creates: vec![],
            updates: vec![BGUpdateEntry {
                op_ms: 1,
                bg_id: 20,
                state: None,
                replica_set: Some(vec![100, 101, 103]),
                lease_owner: None,
                expected_bg_epoch: 1,
                new_bg_epoch: 2,
                bump_table_epoch: None,
            }],
            next_bg_id: None,
            bump_table_epoch: None,
            expected_table_absent: false,
        };
        mgr.apply_batch_bg(&batch).unwrap();

        // Survivors keep Active state.
        assert_eq!(mgr.get_replica_state(20, 100), ReplicaState::Active);
        assert_eq!(mgr.get_replica_state(20, 101), ReplicaState::Active);
        // Removed replica's state is gone (defaults to Pending).
        assert_eq!(mgr.get_replica_state(20, 102), ReplicaState::Pending);
        // New replica is seeded Pending.
        assert_eq!(mgr.get_replica_state(20, 103), ReplicaState::Pending);
    }

    // =========================================================================
    // REGRESSION-BASELINE tests (P0.4 from docs/pd-raft-consistency.md §15).
    //
    // These tests fix the CURRENT (buggy) behavior in writing so that the bugs
    // are visible on every test run. After P2 (BG CAS化), these tests must be
    // UPDATED to reflect the new contracts:
    //   - apply_batch_bg refuses to overwrite an existing table (P2.3)
    //   - apply_batch_bg only bumps table_epoch when at least one update was
    //     actually Applied (no假发布) (P2.3)
    //   - apply_update_bg with stale expected_bg_epoch returns SkippedStale
    //     (instead of silent skip on `bg_epoch >= new_bg_epoch`) (P2.1)
    // =========================================================================

    /// REGRESSION-BASELINE: concurrent create_table for the same (pool,
    /// replica_count) key produces a table_id collision in apply: the second
    /// BatchBG silently overwrites the table mapping but does NOT remove the
    /// first batch's BGs from `bgs` / `worker_to_bgs`, leaving them as orphans.
    ///
    /// Expected post-P2.3: BatchBG with `table_create.expected_table_absent=true`
    /// must SkippedStale when the table already exists; orphan BGs cannot occur.
    #[test]
    fn baseline_concurrent_batch_create_same_table_orphans_first_bgs() {
        use crate::pd::bg::BGTable;
        let mgr = test_manager();
        let table_id = curvine_common::state::gen_table_id(1, 3);

        // Path A: build table with bg_id range 100..103.
        let table_a = BGTable {
            table_id,
            bucket_count: 3,
            buckets: vec![100, 101, 102],
            epoch: 0,
            create_time_ms: 0,
            last_rebuild_ms: 0,
            stats: BGTableStats::default(),
        };
        let creates_a: Vec<BlockGroupInfo> = (100..103)
            .map(|id| make_bg(id, table_id, vec![1, 2, 3]))
            .collect();

        // Path B: build same table with disjoint bg_id range 200..203.
        let table_b = BGTable {
            table_id,
            bucket_count: 3,
            buckets: vec![200, 201, 202],
            epoch: 0,
            create_time_ms: 0,
            last_rebuild_ms: 0,
            stats: BGTableStats::default(),
        };
        let creates_b: Vec<BlockGroupInfo> = (200..203)
            .map(|id| make_bg(id, table_id, vec![4, 5, 6]))
            .collect();

        // Apply A first, then B.
        mgr.apply_batch_bg(&BatchBGEntry {
            op_ms: 1,
            table: Some(table_a.clone()),
            creates: creates_a,
            updates: vec![],
            next_bg_id: None,
            bump_table_epoch: None,
            expected_table_absent: false,
        })
        .unwrap();
        mgr.apply_batch_bg(&BatchBGEntry {
            op_ms: 2,
            table: Some(table_b.clone()),
            creates: creates_b,
            updates: vec![],
            next_bg_id: None,
            bump_table_epoch: None,
            expected_table_absent: false,
        })
        .unwrap();

        // BUG: table now points to range 200..203 (B won), but range 100..103
        // is still in `bgs` map → orphan BGs.
        let table = mgr.get_table(table_id).unwrap();
        assert_eq!(table.buckets, vec![200, 201, 202]);

        for orphan_id in 100..103 {
            assert!(
                mgr.get_bg(orphan_id).is_some(),
                "BASELINE: bg_id={} should be orphaned in current code; \
                 after P2.3, table_already_exists guard rejects path B and no orphan exists.",
                orphan_id
            );
        }

        // Worker indexes from path A also leak: workers 1/2/3 still appear to
        // own BGs that the table no longer references.
        for orphan_worker in [1, 2, 3] {
            let bgs_on_worker = mgr.get_bgs_on_worker(orphan_worker);
            assert!(
                !bgs_on_worker.is_empty(),
                "BASELINE: worker {} should still index orphan BGs",
                orphan_worker
            );
        }
    }

    /// REGRESSION (post-P4.2): batch updates that are all stale no longer
    /// silently bump table.epoch via the in-place path. The batch marks the
    /// table dirty and tries to flush a `BumpTableEpoch` entry; in this test
    /// env the journal_client has no real Raft cluster, so try_flush fails
    /// silently and table.epoch stays put.
    ///
    /// In production (real Raft), the BumpTableEpoch entry WILL still apply,
    /// advancing the epoch even if no BG was mutated. P2.3 mutation counting
    /// would prevent that "phantom publish" — it's still pending. For now,
    /// this test verifies that P4.2 unblocks the in-place bug at least in
    /// the no-Raft path.
    #[test]
    fn batch_bg_stale_updates_no_inplace_bump() {
        use crate::pd::bg::BGTable;
        let mgr = test_manager();
        let table_id = 42;
        mgr.test_insert_table(BGTable {
            table_id,
            bucket_count: 1,
            buckets: vec![1],
            epoch: 5,
            create_time_ms: 0,
            last_rebuild_ms: 0,
            stats: BGTableStats::default(),
        });
        let mut bg = make_bg(1, table_id, vec![100, 101]);
        bg.bg_epoch = 7;
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info: bg })
            .unwrap();
        let initial_table_epoch = mgr.get_table(table_id).unwrap().epoch;

        let batch = BatchBGEntry {
            op_ms: 1,
            table: None,
            creates: vec![],
            updates: vec![BGUpdateEntry {
                op_ms: 1,
                bg_id: 1,
                state: None,
                replica_set: Some(vec![999]),
                lease_owner: None,
                expected_bg_epoch: 6,
                new_bg_epoch: 8,
                bump_table_epoch: None,
            }],
            next_bg_id: None,
            bump_table_epoch: Some(table_id),
            expected_table_absent: false,
        };
        mgr.apply_batch_bg(&batch).unwrap();

        // P4.2 fix: no more in-place bump. table.epoch unchanged in test env.
        let final_epoch = mgr.get_table(table_id).unwrap().epoch;
        assert_eq!(
            final_epoch, initial_table_epoch,
            "P4.2: table.epoch must not be bumped in-place; BumpTableEpoch entry \
             would have done so via Raft, but here the journal client has no cluster."
        );
        // BG itself unchanged.
        let bg_after = mgr.get_bg(1).unwrap();
        assert_eq!(bg_after.replica_set, vec![100, 101]);
        assert_eq!(bg_after.bg_epoch, 7);
    }

    /// REGRESSION (post-P2.1): apply_update_bg with mismatching expected_bg_epoch
    /// now returns `Ok(ApplyOutcome::SkippedStale { reason })` instead of
    /// silent Ok(()). Pre-P2.1 (now removed): apply skipped without surfacing.
    #[test]
    fn apply_update_bg_returns_stale_on_epoch_mismatch() {
        let mgr = test_manager();
        let mut bg = make_bg(50, 1, vec![100]);
        bg.bg_epoch = 5;
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info: bg })
            .unwrap();

        // Stale entry: proposer thought bg_epoch was 4, but it's 5.
        let entry = BGUpdateEntry {
            op_ms: 1,
            bg_id: 50,
            state: None,
            replica_set: Some(vec![200]),
            lease_owner: None,
            expected_bg_epoch: 4, // STALE: doesn't match current 5
            new_bg_epoch: 5,
            bump_table_epoch: None,
        };
        let outcome = mgr.apply_update_bg(&entry).unwrap();
        assert!(
            matches!(outcome, ApplyOutcome::SkippedStale { .. }),
            "expected SkippedStale, got {:?}",
            outcome
        );
        // BG was not mutated.
        let bg_after = mgr.get_bg(50).unwrap();
        assert_eq!(bg_after.replica_set, vec![100]);
        assert_eq!(bg_after.bg_epoch, 5);
    }

    /// REGRESSION (post-P2.1): apply_update_bg with non-monotonic new_bg_epoch
    /// returns SkippedNoop (idempotent skip). expected_bg_epoch matches but
    /// new_bg_epoch <= current.
    #[test]
    fn apply_update_bg_returns_noop_on_non_monotonic_new_epoch() {
        let mgr = test_manager();
        let mut bg = make_bg(51, 1, vec![100]);
        bg.bg_epoch = 5;
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info: bg })
            .unwrap();

        let entry = BGUpdateEntry {
            op_ms: 1,
            bg_id: 51,
            state: None,
            replica_set: Some(vec![200]),
            lease_owner: None,
            expected_bg_epoch: 5, // matches current
            new_bg_epoch: 5,      // not greater than current
            bump_table_epoch: None,
        };
        let outcome = mgr.apply_update_bg(&entry).unwrap();
        assert_eq!(outcome, ApplyOutcome::SkippedNoop);
        // BG was not mutated.
        let bg_after = mgr.get_bg(51).unwrap();
        assert_eq!(bg_after.replica_set, vec![100]);
        assert_eq!(bg_after.bg_epoch, 5);
    }
}
