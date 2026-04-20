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
use super::{BGStore, BGTable};
use crate::pd::config::{keys, ConfigManager};
use crate::pd::journal::entry::{BGDeleteEntry, BGEntry, BGUpdateEntry, BatchBGEntry};
use crate::pd::journal::{self, PdEntry};
use crate::pd::pool::PoolManager;
use curvine_common::state::{
    BGOpState, BGStats, BGTableSummary, BlockGroupInfo, BlockGroupInfoView, ReplicaInfo,
    ReplicaState, WorkerBGReport,
};
use curvine_common::{FsError, FsResult};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

pub struct BGManager {
    tables: RwLock<HashMap<u32, BGTable>>,
    bgs: RwLock<HashMap<u32, BlockGroupInfo>>,
    worker_to_bgs: RwLock<HashMap<u32, HashSet<u32>>>,
    replica_states: RwLock<HashMap<u32, HashMap<u32, ReplicaState>>>,
    extra_remove_bgs: RwLock<HashMap<u32, Vec<u32>>>,
    epoch_dirty: AtomicBool,
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
            replica_states: RwLock::new(HashMap::new()),
            extra_remove_bgs: RwLock::new(HashMap::new()),
            epoch_dirty: AtomicBool::new(false),
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
        self.config_manager.get_string(
            keys::PD_BG_BALANCE_POLICY,
            keys::PD_BG_BALANCE_POLICY_DEFAULT,
        )
    }

    fn rebuild_tolerant_ratio(&self) -> f64 {
        self.config_manager.get_u32(
            keys::PD_BG_REBUILD_TOLERANT_RATIO_BPS,
            keys::PD_BG_REBUILD_TOLERANT_RATIO_BPS_DEFAULT,
        ) as f64
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
        for table in tables {
            t.insert(table.table_id, table);
        }
        for bg in bgs {
            for &wid in &bg.replica_set {
                w2b.entry(wid).or_default().insert(bg.bg_id);
            }
            b.insert(bg.bg_id, bg);
        }
        self.id_allocator.restore()?;
        Ok(())
    }

    pub fn get_replica_state(&self, bg_id: u32, worker_id: u32) -> ReplicaState {
        self.replica_states
            .read()
            .unwrap()
            .get(&bg_id)
            .and_then(|m| m.get(&worker_id))
            .copied()
            .unwrap_or(ReplicaState::Pending)
    }

    pub fn set_replica_state(&self, bg_id: u32, worker_id: u32, state: ReplicaState) {
        self.replica_states
            .write()
            .unwrap()
            .entry(bg_id)
            .or_default()
            .insert(worker_id, state);
    }

    /// Update replica states from worker heartbeat bg_reports.
    pub fn update_replica_states_from_reports(&self, worker_id: u32, reports: &[WorkerBGReport]) {
        let mut rs = self.replica_states.write().unwrap();
        for report in reports {
            let old = rs
                .get(&report.bg_id)
                .and_then(|m| m.get(&worker_id))
                .copied()
                .unwrap_or(ReplicaState::Pending);
            rs.entry(report.bg_id)
                .or_default()
                .insert(worker_id, report.state);
            // Detect Active set change
            if (old == ReplicaState::Active) != (report.state == ReplicaState::Active) {
                self.epoch_dirty.store(true, Ordering::Relaxed);
            }
        }
    }

    pub fn update_replica_states_from_bg_ids(&self, worker_id: u32, bg_ids: &[u32]) {
        let mut rs = self.replica_states.write().unwrap();
        for &bg_id in bg_ids {
            let old = rs
                .get(&bg_id)
                .and_then(|m| m.get(&worker_id))
                .copied()
                .unwrap_or(ReplicaState::Pending);
            rs.entry(bg_id)
                .or_default()
                .entry(worker_id)
                .and_modify(|s| {
                    if *s == ReplicaState::Pending {
                        *s = ReplicaState::Active;
                    }
                })
                .or_insert(ReplicaState::Active);
            if old != ReplicaState::Active {
                let new = rs.get(&bg_id).and_then(|m| m.get(&worker_id)).copied();
                if new == Some(ReplicaState::Active) {
                    self.epoch_dirty.store(true, Ordering::Relaxed);
                }
            }
        }
    }

    /// Mark all replicas on a worker as Offline.
    pub fn mark_worker_offline(&self, worker_id: u32) {
        let mut rs = self.replica_states.write().unwrap();
        for states in rs.values_mut() {
            if let Some(s) = states.get_mut(&worker_id) {
                if *s == ReplicaState::Active {
                    self.epoch_dirty.store(true, Ordering::Relaxed);
                }
                *s = ReplicaState::Offline;
            }
        }
    }

    /// Serving view: only Active replicas in replica_set.
    pub fn get_serving_replicas(&self, bg_id: u32) -> Vec<u32> {
        let bgs = self.bgs.read().unwrap();
        let bg = match bgs.get(&bg_id) {
            Some(bg) => bg,
            None => return vec![],
        };
        let rs = self.replica_states.read().unwrap();
        let states = rs.get(&bg_id);
        bg.replica_set
            .iter()
            .filter(|&&wid| {
                states
                    .and_then(|m| m.get(&wid))
                    .copied()
                    .unwrap_or(ReplicaState::Pending)
                    == ReplicaState::Active
            })
            .copied()
            .collect()
    }

    /// Resident view: non-Offline replicas in replica_set (Active + Syncing + Pending).
    pub fn get_resident_replicas(&self, bg_id: u32) -> Vec<u32> {
        let bgs = self.bgs.read().unwrap();
        let bg = match bgs.get(&bg_id) {
            Some(bg) => bg,
            None => return vec![],
        };
        let rs = self.replica_states.read().unwrap();
        let states = rs.get(&bg_id);
        bg.replica_set
            .iter()
            .filter(|&&wid| {
                states
                    .and_then(|m| m.get(&wid))
                    .copied()
                    .unwrap_or(ReplicaState::Pending)
                    != ReplicaState::Offline
            })
            .copied()
            .collect()
    }

    /// Record extra BGs that a worker holds but PD doesn't expect.
    pub fn add_extra_remove_bg(&self, worker_id: u32, bg_id: u32) {
        self.extra_remove_bgs
            .write()
            .unwrap()
            .entry(worker_id)
            .or_default()
            .push(bg_id);
    }

    /// Drain extra remove_bgs for a worker (called during heartbeat response).
    pub fn drain_extra_remove_bgs(&self, worker_id: u32) -> Vec<u32> {
        self.extra_remove_bgs
            .write()
            .unwrap()
            .remove(&worker_id)
            .unwrap_or_default()
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
        let entry = crate::pd::journal::entry::BGUpdateEntry {
            op_ms: orpc::common::LocalTime::mills(),
            bg_id,
            state: None,
            replica_set: Some(new_rs),
            lease_owner: None,
            new_bg_epoch: bg.bg_epoch.saturating_add(1),
            new_table_epoch: None,
        };
        self.journal_client
            .propose(crate::pd::journal::PdEntry::UpdateBG(entry))
    }

    /// Flush table_epoch bump if Active set has changed (coalesce window).
    /// Bumps all tables that have dirty BGs and persists the Active snapshot.
    pub fn flush_table_epoch_if_dirty(&self) {
        if !self.epoch_dirty.swap(false, Ordering::Relaxed) {
            return;
        }
        // Bump epoch for all tables (simple: one global dirty flag covers all tables)
        let tables: Vec<BGTable> = self.tables.read().unwrap().values().cloned().collect();
        for table in &tables {
            let new_epoch = table.epoch.saturating_add(1);
            if let Err(e) = self.set_table_epoch(table.table_id, new_epoch) {
                log::error!(
                    "Failed to bump table_epoch for table {}: {}",
                    table.table_id,
                    e
                );
            }
        }
        // Persist Active snapshot for leader-switch recovery
        self.persist_active_snapshot();
    }

    /// Persist current Active replica set for each table.
    /// Snapshot key: bg:active_snapshot:{table_id}
    fn persist_active_snapshot(&self) {
        let rs = self.replica_states.read().unwrap();
        let tables = self.tables.read().unwrap();
        let bgs = self.bgs.read().unwrap();
        for table in tables.values() {
            let mut snapshot: HashMap<u32, Vec<u32>> = HashMap::new();
            for &bg_id in &table.buckets {
                if let Some(bg) = bgs.get(&bg_id) {
                    let active: Vec<u32> = bg
                        .replica_set
                        .iter()
                        .filter(|&&wid| {
                            rs.get(&bg_id)
                                .and_then(|m| m.get(&wid))
                                .copied()
                                .unwrap_or(ReplicaState::Pending)
                                == ReplicaState::Active
                        })
                        .copied()
                        .collect();
                    if !active.is_empty() {
                        snapshot.insert(bg_id, active);
                    }
                }
            }
            let key = format!("active_snapshot:{}", table.table_id);
            let value = serde_json::to_vec(&snapshot).unwrap_or_default();
            if let Err(e) = self.store.put_raw(&key, &value) {
                log::error!(
                    "Failed to persist Active snapshot for table {}: {}",
                    table.table_id,
                    e
                );
            }
        }
    }

    /// Restore Active snapshot after leader switch.
    /// Called after restore() to initialize replica_states from persisted snapshot.
    pub fn restore_active_snapshot(&self) {
        let tables = self.tables.read().unwrap();
        let bgs = self.bgs.read().unwrap();
        let mut rs = self.replica_states.write().unwrap();
        for table in tables.values() {
            let key = format!("active_snapshot:{}", table.table_id);
            let snapshot: HashMap<u32, Vec<u32>> = match self.store.get_raw(&key) {
                Ok(Some(data)) => serde_json::from_slice(&data).unwrap_or_default(),
                _ => continue,
            };
            for &bg_id in &table.buckets {
                if let Some(bg) = bgs.get(&bg_id) {
                    let active_set: HashSet<u32> = snapshot
                        .get(&bg_id)
                        .map(|v| v.iter().copied().collect())
                        .unwrap_or_default();
                    let states = rs.entry(bg_id).or_default();
                    for &wid in &bg.replica_set {
                        if active_set.contains(&wid) {
                            states.insert(wid, ReplicaState::Active);
                        } else {
                            states.entry(wid).or_insert(ReplicaState::Pending);
                        }
                    }
                }
            }
        }
    }

    pub fn apply_create_bg(&self, entry: &BGEntry) -> FsResult<()> {
        let info = &entry.info;
        self.store.put(&info)?;
        self.bgs.write().unwrap().insert(info.bg_id, info.clone());
        let mut w2b = self.worker_to_bgs.write().unwrap();
        for &wid in &info.replica_set {
            w2b.entry(wid).or_default().insert(info.bg_id);
        }
        // Initialize replica states to Pending
        let mut rs = self.replica_states.write().unwrap();
        let entry_states = rs.entry(info.bg_id).or_default();
        for &wid in &info.replica_set {
            entry_states.entry(wid).or_insert(ReplicaState::Pending);
        }
        Ok(())
    }

    pub fn apply_update_bg(&self, entry: &BGUpdateEntry) -> FsResult<()> {
        let mut info = self
            .bgs
            .read()
            .unwrap()
            .get(&entry.bg_id)
            .cloned()
            .ok_or_else(|| FsError::common(format!("bg {} not found for update", entry.bg_id)))?;

        if info.bg_epoch >= entry.new_bg_epoch {
            return Ok(());
        }

        // Validate state transition before any mutation.
        if let Some(s) = entry.state {
            state_machine::validate_transition(info.state, s)?;
        }

        let old_replica_set = info.replica_set.clone();

        info.bg_epoch = entry.new_bg_epoch;
        if let Some(s) = entry.state {
            info.state = s;
        }
        if let Some(ref rs) = entry.replica_set {
            info.replica_set = rs.clone();
        }
        if let Some(ref lease) = entry.lease_owner {
            info.lease_owner = Some(lease.clone());
        }

        self.store.put(&info)?;
        self.bgs.write().unwrap().insert(entry.bg_id, info.clone());

        if entry.replica_set.is_some() {
            let old_set: HashSet<u32> = old_replica_set.iter().copied().collect();
            let new_set: HashSet<u32> = info.replica_set.iter().copied().collect();
            let mut w2b = self.worker_to_bgs.write().unwrap();
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
            // Sync replica_states: add Pending for new workers, remove departed workers
            let mut rs = self.replica_states.write().unwrap();
            let states = rs.entry(entry.bg_id).or_default();
            for &added in new_set.difference(&old_set) {
                states.entry(added).or_insert(ReplicaState::Pending);
            }
            for &removed in old_set.difference(&new_set) {
                states.remove(&removed);
            }
        }

        if let Some(new_epoch) = entry.new_table_epoch {
            self.set_table_epoch(info.table_id, new_epoch)?;
        }
        Ok(())
    }

    pub fn apply_delete_bg(&self, entry: &BGDeleteEntry) -> FsResult<()> {
        self.store.delete(entry.bg_id)?;
        let removed = self.bgs.write().unwrap().remove(&entry.bg_id);
        if let Some(bg) = removed {
            let mut w2b = self.worker_to_bgs.write().unwrap();
            for &wid in &bg.replica_set {
                if let Some(set) = w2b.get_mut(&wid) {
                    set.remove(&entry.bg_id);
                    if set.is_empty() {
                        w2b.remove(&wid);
                    }
                }
            }
        }
        self.replica_states.write().unwrap().remove(&entry.bg_id);
        self.set_table_epoch(entry.table_id, entry.new_table_epoch)?;
        Ok(())
    }

    /// Apply a batch of BG operations atomically (from Raft).
    /// Table (if any) is created/updated first, then creates, then updates.
    /// All epochs (BG, table, lease) come from the entry — apply never mints.
    pub fn apply_batch_bg(&self, entry: &BatchBGEntry) -> FsResult<()> {
        if let Some(ref table) = entry.table {
            self.store.put_table(table)?;
            self.tables
                .write()
                .unwrap()
                .insert(table.table_id, table.clone());
        }

        for bg in &entry.creates {
            self.store.put(bg)?;
            self.bgs.write().unwrap().insert(bg.bg_id, bg.clone());
            let mut w2b = self.worker_to_bgs.write().unwrap();
            for &wid in &bg.replica_set {
                w2b.entry(wid).or_default().insert(bg.bg_id);
            }
        }

        for update in &entry.updates {
            let mut info = match self.bgs.read().unwrap().get(&update.bg_id).cloned() {
                Some(bg) => bg,
                None => continue,
            };
            if info.bg_epoch >= update.new_bg_epoch {
                continue; // idempotent skip
            }
            if let Some(s) = update.state {
                state_machine::validate_transition(info.state, s)?;
            }

            let old_replica_set = info.replica_set.clone();
            info.bg_epoch = update.new_bg_epoch;
            if let Some(s) = update.state {
                info.state = s;
            }
            if let Some(ref rs) = update.replica_set {
                info.replica_set = rs.clone();
            }
            if let Some(ref lease) = update.lease_owner {
                info.lease_owner = Some(lease.clone());
            }
            self.store.put(&info)?;
            self.bgs.write().unwrap().insert(update.bg_id, info.clone());
            if update.replica_set.is_some() {
                let old_set: HashSet<u32> = old_replica_set.iter().copied().collect();
                let new_set: HashSet<u32> = info.replica_set.iter().copied().collect();
                let mut w2b = self.worker_to_bgs.write().unwrap();
                for &removed in old_set.difference(&new_set) {
                    if let Some(set) = w2b.get_mut(&removed) {
                        set.remove(&update.bg_id);
                        if set.is_empty() {
                            w2b.remove(&removed);
                        }
                    }
                }
                for &added in new_set.difference(&old_set) {
                    w2b.entry(added).or_default().insert(update.bg_id);
                }
            }
        }

        if let Some((tid, new_epoch)) = entry.new_table_epoch {
            self.set_table_epoch(tid, new_epoch)?;
        }

        if let Some(next_id) = entry.next_bg_id {
            self.store.set_next_bg_id(next_id)?;
        }

        Ok(())
    }

    /// Propose a batch BG operation via Raft.
    pub fn propose_batch_bg(&self, entry: BatchBGEntry) -> FsResult<()> {
        self.journal_client.propose(PdEntry::BatchBG(entry))
    }

    /// Create a new BGTable for a pool. Uses the placement algorithm to assign BGs
    /// to workers, then proposes the entire result as a single BatchBG Raft entry.
    pub fn create_table(
        &self,
        pool_id: u16,
        bucket_count: u32,
        replica_count: u16,
        workers: &[u32],
    ) -> FsResult<()> {
        let table_id = (pool_id as u32) << 16 | (replica_count as u32);

        if self.tables.read().unwrap().contains_key(&table_id) {
            return Err(FsError::common(format!(
                "table already exists for pool {} replicas {}",
                pool_id, replica_count
            )));
        }

        let pool = self.pool_manager.get_pool(pool_id)?;

        let next_bg_id = self.id_allocator.alloc(bucket_count)?;

        let stub_table = BGTable {
            table_id,
            bucket_count: 0,
            buckets: vec![],
            epoch: 0,
            create_time_ms: 0,
            last_rebuild_ms: 0,
        };
        let worker_snapshots = self.build_worker_snapshots(&stub_table, pool.media, true);

        let tolerant = self.rebuild_tolerant_ratio();
        let ctx = PlacementContext {
            workers: &worker_snapshots,
            bucket_count,
            replica_count,
            tolerant_ratio: tolerant,
            lease_tolerant_ratio: tolerant,
        };

        let rule = self.placement_rule();
        let balance_policy = create_policy(&self.balance_policy_strategy());
        let mut st = balance_policy.prepare(&ctx)?;

        let result = super::placement::build_table(
            table_id,
            bucket_count,
            replica_count,
            next_bg_id,
            &ctx,
            &rule,
            balance_policy.as_ref(),
            &mut st,
        )?;

        let entry = BatchBGEntry {
            op_ms: orpc::common::LocalTime::mills(),
            table: Some(result.table),
            creates: result.bgs,
            updates: vec![],
            next_bg_id: None,      // ID already advanced by IdAllocator's realloc
            new_table_epoch: None, // new table carries its initial epoch
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

    fn set_table_epoch(&self, table_id: u32, new_epoch: u64) -> FsResult<()> {
        let mut table = match self.tables.read().unwrap().get(&table_id).cloned() {
            Some(t) => t,
            None => return Ok(()),
        };
        if table.epoch >= new_epoch {
            return Ok(());
        }
        table.epoch = new_epoch;
        self.store.put_table(&table)?;
        self.tables.write().unwrap().insert(table_id, table);
        Ok(())
    }

    pub fn get_bg(&self, bg_id: u32) -> Option<BlockGroupInfo> {
        self.bgs.read().unwrap().get(&bg_id).cloned()
    }

    pub fn get_table(&self, table_id: u32) -> Option<BGTable> {
        self.tables.read().unwrap().get(&table_id).cloned()
    }

    /// Lookup bg_id by table_id and key, then return BlockGroupInfo if present.
    pub fn lookup_bg(&self, table_id: u32, key: &[u8]) -> Option<BlockGroupInfo> {
        let tables = self.tables.read().unwrap();
        let table = tables.get(&table_id)?;
        let bg_id = table.lookup(key);
        drop(tables);
        self.bgs.read().unwrap().get(&bg_id).cloned()
    }

    pub fn list_tables(&self) -> Vec<BGTable> {
        self.tables.read().unwrap().values().cloned().collect()
    }

    /// Per-table epoch map: table_id -> epoch. Used in heartbeat responses.
    pub fn get_table_epochs(&self) -> HashMap<u32, u64> {
        self.tables
            .read()
            .unwrap()
            .iter()
            .map(|(&id, t)| (id, t.epoch))
            .collect()
    }

    pub fn list_bgs(&self) -> Vec<BlockGroupInfo> {
        self.bgs.read().unwrap().values().cloned().collect()
    }

    /// Set BG operation state (runtime-only).
    pub fn set_op_state(&self, bg_id: u32, op_state: BGOpState) {
        if let Some(bg) = self.bgs.write().unwrap().get_mut(&bg_id) {
            bg.op_state = op_state;
        }
    }

    /// Update per-BG stats from worker heartbeat.
    pub fn update_bg_stats(&self, bg_stats: &HashMap<u32, BGStats>) {
        let now = orpc::common::LocalTime::mills();
        let mut bgs = self.bgs.write().unwrap();
        for (bg_id, stats) in bg_stats {
            if let Some(bg) = bgs.get_mut(bg_id) {
                bg.stats = stats.clone();
                bg.stats.last_report_ms = now;
            }
        }
    }

    /// Aggregate stats for a table from its BGs (runtime computation).
    pub fn compute_table_stats(&self, table_id: u32) -> BGStats {
        let tables = self.tables.read().unwrap();
        let table = match tables.get(&table_id) {
            Some(t) => t,
            None => return BGStats::default(),
        };
        let bgs = self.bgs.read().unwrap();
        let mut agg = BGStats::default();
        for &bg_id in &table.buckets {
            if let Some(bg) = bgs.get(&bg_id) {
                agg.used_bytes += bg.stats.used_bytes;
                agg.free_bytes += bg.stats.free_bytes;
                agg.block_count += bg.stats.block_count;
                agg.last_report_ms = agg.last_report_ms.max(bg.stats.last_report_ms);
            }
        }
        agg
    }

    /// BGs that have this worker in replica_set (uses worker_to_bgs index).
    pub fn get_bgs_on_worker(&self, worker_id: u32) -> Vec<BlockGroupInfo> {
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
    pub fn get_bgs_by_state(&self, state: curvine_common::state::BGState) -> Vec<BlockGroupInfo> {
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
                    .get_worker_address_and_state(node_id)
                    .map(|(address, state)| ReplicaInfo {
                        node_id,
                        address,
                        state,
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

    /// Build client-facing summary (buckets as BlockGroupInfoView) for the given table.
    pub fn build_table_summary(&self, table_id: u32) -> Option<BGTableSummary> {
        let table = self.tables.read().unwrap().get(&table_id).cloned()?;
        let bgs = self.bgs.read().unwrap();
        let rs = self.replica_states.read().unwrap();
        let buckets: Vec<_> = table
            .buckets
            .iter()
            .filter_map(|&bg_id| bgs.get(&bg_id).cloned())
            .map(|bg| {
                // Route filtering: only Active replicas visible to Client
                let serving: Vec<u32> = bg
                    .replica_set
                    .iter()
                    .filter(|&&wid| {
                        rs.get(&bg.bg_id)
                            .and_then(|m| m.get(&wid))
                            .copied()
                            .unwrap_or(ReplicaState::Pending)
                            == ReplicaState::Active
                    })
                    .copied()
                    .collect();
                let mut filtered_bg = bg.clone();
                filtered_bg.replica_set = serving;
                block_group_info_to_view(&filtered_bg, &self.pool_manager)
            })
            .collect();
        drop(rs);
        drop(bgs);
        if buckets.len() != table.buckets.len() {
            return None;
        }
        Some(BGTableSummary {
            table_id: table.table_id,
            bucket_count: table.bucket_count,
            epoch: table.epoch,
            buckets,
            last_rebuild_ms: table.last_rebuild_ms,
        })
    }

    /// Build per-table WorkerLoadSnapshot map for the given table.
    ///
    /// When `init` is true (new table creation), all actual counts are zero.
    /// When false, counts are derived from existing BGs in the table.
    fn build_worker_snapshots(
        &self,
        table: &BGTable,
        media: curvine_common::state::StorageType,
        init: bool,
    ) -> HashMap<u32, WorkerLoadSnapshot> {
        let pool_id = table.pool_id();
        let live_workers = self.pool_manager.get_live_workers(pool_id);

        let table_bgs: Vec<BlockGroupInfo> = if init {
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

        let pool_id = table.pool_id();
        let pool = self.pool_manager.get_pool(pool_id)?;
        let worker_snapshots = self.build_worker_snapshots(&table, pool.media, false);

        let tolerant = self.rebuild_tolerant_ratio();
        let ctx = PlacementContext {
            workers: &worker_snapshots,
            bucket_count: table.bucket_count,
            replica_count: table.replica_count(),
            tolerant_ratio: tolerant,
            lease_tolerant_ratio: tolerant,
        };

        let rule = self.placement_rule();
        let worker_labels = ctx.worker_labels();
        let constrained = rule.filter(&ctx.worker_ids(), &worker_labels);

        let balance_policy = create_policy(&self.balance_policy_strategy());
        let mut st = balance_policy.prepare(&ctx)?;

        let mut selected: Vec<u32> = Vec::with_capacity(count as usize);
        let mut exclude: HashSet<u32> = bg.replica_set.iter().copied().collect();

        for _ in 0..count {
            let current: Vec<u32> = bg
                .replica_set
                .iter()
                .copied()
                .chain(selected.iter().copied())
                .collect();

            let hard_filtered = if let Some(ref min_level) = rule.min_isolation_level {
                let f = super::placement::filter_min_isolation(
                    &constrained,
                    &current,
                    min_level,
                    &rule.location_labels,
                    &worker_labels,
                );
                if f.is_empty() {
                    constrained.clone()
                } else {
                    f
                }
            } else {
                constrained.clone()
            };

            let best = super::placement::best_isolation_candidates(
                &hard_filtered,
                &current,
                &rule.location_labels,
                &worker_labels,
            );

            let mut targets = match balance_policy.select_bg_targets(&ctx, &st, &best, 1, &exclude)
            {
                Ok(v) => v,
                Err(_) => vec![],
            };
            if targets.is_empty() {
                targets =
                    match balance_policy.select_bg_targets(&ctx, &st, &constrained, 1, &exclude) {
                        Ok(v) => v,
                        Err(_) => vec![],
                    };
            }
            if targets.is_empty() {
                let fallback: Vec<u32> = constrained
                    .iter()
                    .copied()
                    .filter(|w| !exclude.contains(w))
                    .collect();
                if !fallback.is_empty() {
                    targets = match balance_policy.select_bg_targets(
                        &ctx,
                        &st,
                        &fallback,
                        1,
                        &HashSet::new(),
                    ) {
                        Ok(v) => v,
                        Err(_) => vec![],
                    };
                }
            }
            if targets.is_empty() {
                break;
            }

            let picked = targets[0];
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

    /// Rebuild table buckets: for each bucket, verify the BG's replica_set workers are still alive.
    pub fn compute_rebuild_diff(&self, table_id: u32) -> FsResult<Vec<(BlockGroupInfo, Vec<u32>)>> {
        let table = {
            let tables = self.tables.read().unwrap();
            tables
                .get(&table_id)
                .cloned()
                .ok_or_else(|| FsError::common(format!("table {} not found", table_id)))?
        };

        let existing_bgs: Vec<BlockGroupInfo> = {
            let bgs = self.bgs.read().unwrap();
            table
                .buckets
                .iter()
                .filter_map(|&bg_id| {
                    if bg_id == 0 {
                        return None;
                    }
                    bgs.get(&bg_id).cloned()
                })
                .collect()
        };

        if existing_bgs.is_empty() {
            return Ok(vec![]);
        }

        let pool = self.pool_manager.get_pool(table.pool_id())?;
        let worker_snapshots = self.build_worker_snapshots(&table, pool.media, false);

        let tolerant = self.rebuild_tolerant_ratio();
        let ctx = PlacementContext {
            workers: &worker_snapshots,
            bucket_count: table.bucket_count,
            replica_count: table.replica_count(),
            tolerant_ratio: tolerant,
            lease_tolerant_ratio: tolerant,
        };

        let rule = self.placement_rule();
        let balance_policy = create_policy(&self.balance_policy_strategy());
        let mut st = balance_policy.prepare(&ctx)?;

        let options = RebuildOptions::default();
        let result = super::placement::rebuild_table(
            &table,
            &existing_bgs,
            &ctx,
            &rule,
            balance_policy.as_ref(),
            &mut st,
            &options,
        )?;

        // Pair each updated BG with its OLD replica_set so caller can compute diff.
        let old_by_id: HashMap<u32, Vec<u32>> = existing_bgs
            .iter()
            .map(|bg| (bg.bg_id, bg.replica_set.clone()))
            .collect();
        Ok(result
            .updated_bgs
            .into_iter()
            .map(|bg| {
                let old = old_by_id.get(&bg.bg_id).cloned().unwrap_or_default();
                (bg, old)
            })
            .collect())
    }

    pub fn rebuild_table(&self, table_id: u32) -> FsResult<()> {
        let table = {
            let tables = self.tables.read().unwrap();
            tables
                .get(&table_id)
                .cloned()
                .ok_or_else(|| FsError::common(format!("table {} not found", table_id)))?
        };

        let existing_bgs: Vec<BlockGroupInfo> = {
            let bgs = self.bgs.read().unwrap();
            table
                .buckets
                .iter()
                .filter_map(|&bg_id| {
                    if bg_id == 0 {
                        return None;
                    }
                    bgs.get(&bg_id).cloned()
                })
                .collect()
        };

        if existing_bgs.is_empty() {
            return Ok(());
        }

        let pool = self.pool_manager.get_pool(table.pool_id())?;
        let worker_snapshots = self.build_worker_snapshots(&table, pool.media, false);

        let tolerant = self.rebuild_tolerant_ratio();
        let ctx = PlacementContext {
            workers: &worker_snapshots,
            bucket_count: table.bucket_count,
            replica_count: table.replica_count(),
            tolerant_ratio: tolerant,
            lease_tolerant_ratio: tolerant,
        };

        let rule = self.placement_rule();
        let balance_policy = create_policy(&self.balance_policy_strategy());
        let mut st = balance_policy.prepare(&ctx)?;

        let options = RebuildOptions::default();
        let result = super::placement::rebuild_table(
            &table,
            &existing_bgs,
            &ctx,
            &rule,
            balance_policy.as_ref(),
            &mut st,
            &options,
        )?;

        if result.updated_bgs.is_empty() {
            return Ok(());
        }

        let updates: Vec<BGUpdateEntry> = result
            .updated_bgs
            .iter()
            .map(|bg| BGUpdateEntry {
                op_ms: orpc::common::LocalTime::mills(),
                bg_id: bg.bg_id,
                state: None,
                replica_set: Some(bg.replica_set.clone()),
                lease_owner: bg.lease_owner.clone(),
                new_bg_epoch: bg.bg_epoch,
                new_table_epoch: None,
            })
            .collect();

        let new_table_epoch = Some((table.table_id, table.epoch.saturating_add(1)));

        let entry = BatchBGEntry {
            op_ms: orpc::common::LocalTime::mills(),
            table: None,
            creates: vec![],
            updates,
            next_bg_id: None,
            new_table_epoch,
        };
        self.propose_batch_bg(entry)
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
        let policy_name = self.config_manager.get_string(
            keys::PD_BG_PLACEMENT_POLICY,
            keys::PD_BG_PLACEMENT_POLICY_DEFAULT,
        );
        let min_iso = self.config_manager.get_string(
            keys::PD_BG_MIN_ISOLATION_LEVEL,
            keys::PD_BG_MIN_ISOLATION_LEVEL_DEFAULT,
        );
        let min_isolation_level = if min_iso.is_empty() {
            None
        } else {
            Some(min_iso)
        };

        match policy_name.as_str() {
            "topology_aware" => PlacementRule {
                id: "topology_aware".into(),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::state::{BGLease, BGState};

    fn test_manager() -> BGManager {
        let store: Arc<dyn crate::pd::store::KvStore> =
            Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new());
        let bg_store = Arc::new(BGStore::new(store));
        let pool_store = Arc::new(crate::pd::pool::PoolStore::new(Arc::new(
            crate::pd::store::memory_kv_engine::MemoryKvEngine::new(),
        )));
        let node_store = Arc::new(crate::pd::node::NodeStore::new(Arc::new(
            crate::pd::store::memory_kv_engine::MemoryKvEngine::new(),
        )));
        let journal_conf = curvine_common::conf::JournalConf::default();
        let rt = journal_conf.create_runtime();
        let raft = curvine_common::raft::RaftClient::from_conf(rt, &journal_conf);
        let jc = Arc::new(crate::pd::journal::Client::new(raft));
        let config_manager = Arc::new(crate::pd::config::ConfigManager::new(
            Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new()),
            jc.clone(),
            std::collections::HashMap::new(),
        ));
        let node_manager: Arc<crate::pd::node::NodeManager> = Arc::new(
            crate::pd::node::NodeManager::new(node_store, config_manager.clone(), jc.clone()),
        );
        let pool_manager = Arc::new(PoolManager::new(pool_store, node_manager, jc.clone()));
        BGManager::new(
            bg_store,
            pool_manager,
            jc,
            config_manager,
            1024,
            vec![3],
            vec![],
        )
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
            new_bg_epoch: 2,
            new_table_epoch: None,
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
            new_bg_epoch: 2,
            new_table_epoch: None,
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
            new_bg_epoch: 2,
            new_table_epoch: None,
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
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        assert!(mgr.get_bg(4).is_some());
        mgr.apply_delete_bg(&BGDeleteEntry {
            op_ms: 0,
            bg_id: 4,
            table_id: 10,
            new_table_epoch: 1,
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
            new_bg_epoch: 2,
            new_table_epoch: None,
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
            new_bg_epoch: 3,
            new_table_epoch: None,
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
            new_bg_epoch: 2,
            new_table_epoch: None,
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
            new_bg_epoch: before.bg_epoch + 1,
            new_table_epoch: None,
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
            new_bg_epoch: 2,
            new_table_epoch: None,
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
            new_bg_epoch: 2,
            new_table_epoch: None,
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
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        assert_eq!(mgr.get_bgs_on_worker(1).len(), 1);

        mgr.apply_delete_bg(&BGDeleteEntry {
            op_ms: 0,
            bg_id: 21,
            table_id: 1,
            new_table_epoch: 1,
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
}
