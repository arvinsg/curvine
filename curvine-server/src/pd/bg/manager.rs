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

use super::placement::{
    create_policy, create_selector, select_workers_for_bg, PlacementContext, PlacementRule,
    RebuildOptions, WorkerCandidate, WorkerLoadSnapshot,
};
use super::state_machine;
use super::{BGStore, BGTable};
use crate::pd::journal::entry::{BGEntry, BGUpdateEntry, BatchBGEntry};
use crate::pd::journal::{self, PdEntry};
use crate::pd::pool::PoolManager;
use curvine_common::state::{
    BGOpState, BGStats, BGTableSummary, BlockGroupInfo, BlockGroupInfoView, ReplicaInfo,
};
use curvine_common::{FsError, FsResult};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

/// Expand BlockGroupInfo to view (replica_set with address and state). BG module owns this.
fn block_group_info_to_view(bg: &BlockGroupInfo, pool_manager: &PoolManager) -> BlockGroupInfoView {
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

/// Pre-allocates BG ID ranges to reduce Raft proposal frequency.
pub struct IdAllocator {
    store: Arc<BGStore>,
    journal_client: Arc<journal::Client>,
    /// Next available ID in the current pre-allocated range.
    next: AtomicU32,
    /// End of the current pre-allocated range (exclusive).
    end: AtomicU32,
    /// Pre-allocation batch size.
    step: u32,
    /// Serialize realloc operations.
    alloc_lock: Mutex<()>,
}

impl IdAllocator {
    const DEFAULT_STEP: u32 = 4096;

    pub fn new(store: Arc<BGStore>, journal_client: Arc<journal::Client>) -> Self {
        Self {
            store,
            journal_client,
            next: AtomicU32::new(0),
            end: AtomicU32::new(0),
            step: Self::DEFAULT_STEP,
            alloc_lock: Mutex::new(()),
        }
    }

    /// Initialize from persisted state. Called during restore.
    pub fn restore(&self) -> FsResult<()> {
        let base = self.store.get_next_bg_id()?;
        // Start with an empty range; first alloc() will trigger realloc.
        self.next.store(base, Ordering::SeqCst);
        self.end.store(base, Ordering::SeqCst);
        Ok(())
    }

    /// Allocate `count` contiguous IDs. Returns the starting ID.
    pub fn alloc(&self, count: u32) -> FsResult<u32> {
        loop {
            let next = self.next.load(Ordering::SeqCst);
            let end = self.end.load(Ordering::SeqCst);
            if next + count <= end {
                if self
                    .next
                    .compare_exchange(next, next + count, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    return Ok(next);
                }
                continue; // CAS failed, retry
            }
            // Range exhausted, need to pre-allocate more
            self.realloc(count)?;
        }
    }

    fn realloc(&self, min_count: u32) -> FsResult<()> {
        let _lock = self.alloc_lock.lock().unwrap();
        // Double-check: another thread may have reallocated
        let next = self.next.load(Ordering::SeqCst);
        let end = self.end.load(Ordering::SeqCst);
        if next + min_count <= end {
            return Ok(());
        }
        let alloc_size = self.step.max(min_count);
        let base = self.store.get_next_bg_id()?;
        let new_end = base + alloc_size;

        // Persist the new end via Raft so all nodes agree.
        let entry = BatchBGEntry {
            op_ms: orpc::common::LocalTime::mills(),
            table: None,
            creates: vec![],
            updates: vec![],
            next_bg_id: Some(new_end),
        };
        self.journal_client.propose(PdEntry::BatchBG(entry))?;

        self.next.store(base, Ordering::SeqCst);
        self.end.store(new_end, Ordering::SeqCst);
        Ok(())
    }
}

pub struct BGManager {
    tables: RwLock<HashMap<u32, BGTable>>,
    bgs: RwLock<HashMap<u32, BlockGroupInfo>>,
    worker_to_bgs: RwLock<HashMap<u32, HashSet<u32>>>,
    suspect_bgs: RwLock<HashMap<u32, SuspectEntry>>,
    store: Arc<BGStore>,
    pool_manager: Arc<PoolManager>,
    journal_client: Arc<journal::Client>,
    id_allocator: IdAllocator,
    bucket_count: u32,
    replica_counts: Vec<u16>,
    location_labels: Vec<String>,
    selector_strategy: RwLock<String>,
}

/// Tracks a suspect BG for priority checking.
struct SuspectEntry {
    added_ms: u64,
    check_count: u32,
}

impl BGManager {
    pub fn new(
        store: Arc<BGStore>,
        pool_manager: Arc<PoolManager>,
        journal_client: Arc<journal::Client>,
        bucket_count: u32,
        replica_counts: Vec<u16>,
        location_labels: Vec<String>,
    ) -> Self {
        let id_allocator = IdAllocator::new(store.clone(), journal_client.clone());
        Self {
            tables: RwLock::new(HashMap::new()),
            bgs: RwLock::new(HashMap::new()),
            worker_to_bgs: RwLock::new(HashMap::new()),
            suspect_bgs: RwLock::new(HashMap::new()),
            store,
            pool_manager,
            journal_client,
            id_allocator,
            bucket_count,
            replica_counts,
            location_labels,
            selector_strategy: RwLock::new("quota".to_string()),
        }
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

    // TODO: 存在问题，apply 过程不应该涉及 epoch 修改
    pub fn apply_create_bg(&self, entry: &BGEntry) -> FsResult<()> {
        let info = &entry.info;
        self.store.put(&info)?;
        self.bgs.write().unwrap().insert(info.bg_id, info.clone());
        let mut w2b = self.worker_to_bgs.write().unwrap();
        for &wid in &info.replica_set {
            w2b.entry(wid).or_default().insert(info.bg_id);
        }
        Ok(())
    }

    pub fn apply_update_bg(&self, entry: &BGUpdateEntry) -> FsResult<()> {
        // 1. Read + clone
        let mut info = self
            .bgs
            .read()
            .unwrap()
            .get(&entry.bg_id)
            .cloned()
            .ok_or_else(|| FsError::common(format!("bg {} not found for update", entry.bg_id)))?;

        // if entry carries a bg_epoch and the current epoch is already ahead, this entry was already applied.
        if let Some(entry_epoch) = entry.bg_epoch {
            if info.bg_epoch > entry_epoch {
                return Ok(());
            }
        }

        let old_replica_set = info.replica_set.clone();

        // 2. Apply mutations to clone
        if let Some(s) = entry.state {
            state_machine::validate_transition(info.state, s)?;
            info.state = s;
            info.bg_epoch += 1;
        }
        if let Some(ref rs) = entry.replica_set {
            info.replica_set = rs.clone();
            info.bg_epoch += 1;
        }
        if let Some(ref lease) = entry.lease_owner {
            let old_epoch = info.lease_owner.as_ref().map(|l| l.epoch).unwrap_or(0);
            let mut new_lease = lease.clone();
            new_lease.epoch = old_epoch + 1;
            info.lease_owner = Some(new_lease);
        }

        // 3. Store first
        self.store.put(&info)?;

        // 4. Memory update
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
        }
        // Bump table epoch only on topology changes (replica_set)
        if entry.replica_set.is_some() {
            self.bump_table_epoch(info.table_id)?;
        }
        Ok(())
    }

    pub fn apply_delete_bg(&self, bg_id: u32) -> FsResult<()> {
        let table_id = self.bgs.read().unwrap().get(&bg_id).map(|b| b.table_id);
        // Store first
        self.store.delete(bg_id)?;
        // Then update memory
        let removed = self.bgs.write().unwrap().remove(&bg_id);
        if let Some(bg) = removed {
            let mut w2b = self.worker_to_bgs.write().unwrap();
            for &wid in &bg.replica_set {
                if let Some(set) = w2b.get_mut(&wid) {
                    set.remove(&bg_id);
                    if set.is_empty() {
                        w2b.remove(&wid);
                    }
                }
            }
        }
        if let Some(tid) = table_id {
            let _ = self.bump_table_epoch(tid);
        }
        Ok(())
    }

    // TODO: 和上面一样，存在相同的问题
    /// Apply a batch of BG operations atomically (from Raft).
    /// Table is created/updated first, then creates, then updates.
    /// Table epoch is bumped once at the end only if topology changed
    /// (table/BG creates or replica_set updates).
    pub fn apply_batch_bg(&self, entry: &BatchBGEntry) -> FsResult<()> {
        let mut table_id_to_bump = None;

        if let Some(ref table) = entry.table {
            self.store.put_table(table)?;
            table_id_to_bump = Some(table.table_id);
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
            if table_id_to_bump.is_none() {
                table_id_to_bump = Some(bg.table_id);
            }
        }

        for update in &entry.updates {
            let mut info = match self.bgs.read().unwrap().get(&update.bg_id).cloned() {
                Some(bg) => bg,
                None => continue,
            };
            if let Some(s) = update.state {
                info.state = s;
                info.bg_epoch += 1;
            }
            let old_replica_set = info.replica_set.clone();
            if let Some(ref rs) = update.replica_set {
                info.replica_set = rs.clone();
                info.bg_epoch += 1;
            }
            if let Some(ref lease) = update.lease_owner {
                let old_epoch = info.lease_owner.as_ref().map(|l| l.epoch).unwrap_or(0);
                let mut new_lease = lease.clone();
                new_lease.epoch = old_epoch + 1;
                info.lease_owner = Some(new_lease);
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
            // Only bump table epoch on topology changes (replica_set)
            if update.replica_set.is_some() && table_id_to_bump.is_none() {
                table_id_to_bump = Some(info.table_id);
            }
        }

        if let Some(tid) = table_id_to_bump {
            self.bump_table_epoch(tid)?;
        }

        // Atomically advance the BG ID counter if specified.
        if let Some(next_id) = entry.next_bg_id {
            self.store.set_next_bg_id(next_id)?;
        }

        Ok(())
    }

    /// Propose a BG update via Raft.
    pub fn propose_update_bg(&self, entry: BGUpdateEntry) -> FsResult<()> {
        self.journal_client.propose(PdEntry::UpdateBG(entry))
    }

    /// Propose a batch BG operation via Raft.
    pub fn propose_batch_bg(&self, entry: BatchBGEntry) -> FsResult<()> {
        self.journal_client.propose(PdEntry::BatchBG(entry))
    }

    /// Create a new BGTable for a pool. Uses the placement algorithm to assign BGs
    /// to workers, then proposes the entire result as a single BatchBG Raft entry.
    /// BG IDs are pre-allocated via the IdAllocator for performance.
    pub fn create_table(
        &self,
        pool_id: u16,
        bucket_count: u32,
        replica_count: u16,
        workers: &[u32],
    ) -> FsResult<()> {
        use curvine_common::state::{BlockGroupPolicy, PlacementPolicy};

        let table_id = (pool_id as u32) << 16 | (replica_count as u32);

        if self.tables.read().unwrap().contains_key(&table_id) {
            return Err(FsError::common(format!(
                "table already exists for pool {} replicas {}",
                pool_id, replica_count
            )));
        }

        let pool = self.pool_manager.get_pool(pool_id)?;

        // Pre-allocate IDs via Raft (no direct RocksDB write).
        let next_bg_id = self.id_allocator.alloc(bucket_count)?;

        let bg_policy = BlockGroupPolicy {
            storage_type: pool.media,
            replicas: replica_count,
            placement: PlacementPolicy::Default,
        };

        // Build snapshot for the new table (all workers start at 0 BGs for this table).
        let worker_snapshots: std::collections::HashMap<u32, WorkerLoadSnapshot> = workers
            .iter()
            .map(|&wid| {
                let labels = self.pool_manager.get_worker_labels(wid).unwrap_or_default();
                let (capacity, used) = self
                    .pool_manager
                    .get_worker_storage_stats(wid, pool.media)
                    .unwrap_or((0, 0));
                (
                    wid,
                    WorkerLoadSnapshot {
                        worker_id: wid,
                        actual_bg: 0,
                        actual_lease: 0,
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
            .collect();

        let ctx = PlacementContext {
            workers: &worker_snapshots,
            bucket_count,
            replica_count,
            tolerant_ratio: 0.1,
            lease_tolerant_ratio: 0.1,
        };

        let rules = self.get_pool_placement_rules(pool_id);
        let rule = rules
            .first()
            .cloned()
            .unwrap_or_else(PlacementRule::default_rule);
        let strategy = self.selector_strategy.read().unwrap().clone();
        let balance_policy = create_policy("quota");
        let mut st = balance_policy.prepare(&ctx)?;
        let mut selector = create_selector(&strategy);
        selector.init_from_policy(&ctx, &st);

        let result = super::placement::build_table(
            table_id,
            bucket_count,
            replica_count,
            bg_policy,
            next_bg_id,
            &ctx,
            &rule,
            balance_policy.as_ref(),
            &mut st,
            selector.as_mut(),
        )?;

        let entry = BatchBGEntry {
            op_ms: orpc::common::LocalTime::mills(),
            table: Some(result.table),
            creates: result.bgs,
            updates: vec![],
            next_bg_id: None, // ID already advanced by IdAllocator's realloc
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

    fn bump_table_epoch(&self, table_id: u32) -> FsResult<()> {
        let mut table = match self.tables.read().unwrap().get(&table_id).cloned() {
            Some(t) => t,
            None => return Ok(()),
        };
        table.inc_epoch();
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

    // ========== Suspect BG tracking ==========

    /// Mark a BG as suspect (needs priority checking by checkers).
    pub fn mark_suspect(&self, bg_id: u32) {
        let now = orpc::common::LocalTime::mills();
        self.suspect_bgs
            .write()
            .unwrap()
            .entry(bg_id)
            .or_insert(SuspectEntry {
                added_ms: now,
                check_count: 0,
            });
    }

    /// Mark all BGs on a worker as suspect.
    pub fn mark_worker_bgs_suspect(&self, worker_id: u32) {
        let bg_ids: Vec<u32> = self
            .worker_to_bgs
            .read()
            .unwrap()
            .get(&worker_id)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        let now = orpc::common::LocalTime::mills();
        let mut suspects = self.suspect_bgs.write().unwrap();
        for bg_id in bg_ids {
            suspects.entry(bg_id).or_insert(SuspectEntry {
                added_ms: now,
                check_count: 0,
            });
        }
    }

    /// Get all suspect BGs, incrementing check counts and expiring stale entries.
    pub fn take_suspect_bgs(&self, max_checks: u32, ttl_ms: u64) -> Vec<BlockGroupInfo> {
        let now = orpc::common::LocalTime::mills();
        let mut suspects = self.suspect_bgs.write().unwrap();
        let mut expired = Vec::new();
        let mut result_ids = Vec::new();

        for (bg_id, entry) in suspects.iter_mut() {
            if now.saturating_sub(entry.added_ms) > ttl_ms || entry.check_count >= max_checks {
                expired.push(*bg_id);
                continue;
            }
            entry.check_count += 1;
            result_ids.push(*bg_id);
        }
        for id in expired {
            suspects.remove(&id);
        }
        drop(suspects);

        let bgs = self.bgs.read().unwrap();
        result_ids
            .iter()
            .filter_map(|id| bgs.get(id).cloned())
            .collect()
    }

    /// Clear a BG from the suspect set (checker found no issue).
    pub fn clear_suspect(&self, bg_id: u32) {
        self.suspect_bgs.write().unwrap().remove(&bg_id);
    }

    /// Number of suspect BGs.
    pub fn suspect_count(&self) -> usize {
        self.suspect_bgs.read().unwrap().len()
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

    /// Build client-facing summary (buckets as BlockGroupInfoView) for the given table.
    pub fn build_table_summary(&self, table_id: u32) -> Option<BGTableSummary> {
        let table = self.tables.read().unwrap().get(&table_id).cloned()?;
        let bgs = self.bgs.read().unwrap();
        let buckets: Vec<_> = table
            .buckets
            .iter()
            .filter_map(|&bg_id| bgs.get(&bg_id).cloned())
            .map(|bg| block_group_info_to_view(&bg, &self.pool_manager))
            .collect();
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

    /// Select workers for BG replica placement.
    /// Combines worker_to_bgs (BG load) + pool storage stats + placement rules.
    pub fn select_workers(&self, pool_id: u16, count: u16, exclude: &[u32]) -> FsResult<Vec<u32>> {
        let pool = self.pool_manager.get_pool(pool_id)?;
        let exclude_set: HashSet<u32> = exclude.iter().copied().collect();
        let live_workers = self.pool_manager.get_live_workers(pool_id);
        let allocatable: Vec<u32> = live_workers
            .into_iter()
            .filter(|w| !exclude_set.contains(w))
            .collect();

        let n = count as usize;
        if allocatable.len() < n {
            return Err(FsError::common(format!(
                "not enough workers in pool {}: need {} have {}",
                pool_id,
                n,
                allocatable.len()
            )));
        }

        let w2b = self.worker_to_bgs.read().unwrap();
        let candidates: Vec<WorkerCandidate> = allocatable
            .iter()
            .map(|&wid| {
                let bg_count = w2b.get(&wid).map(|s| s.len() as u32).unwrap_or(0);
                let (capacity_bytes, used_bytes) = self
                    .pool_manager
                    .get_worker_storage_stats(wid, pool.media)
                    .unwrap_or((0, 0));
                let labels = self.pool_manager.get_worker_labels(wid).unwrap_or_default();
                WorkerCandidate {
                    worker_id: wid,
                    bg_count,
                    lease_count: 0,
                    capacity_bytes,
                    used_bytes,
                    labels,
                }
            })
            .collect();
        drop(w2b);

        let rules = self.get_pool_placement_rules(pool_id);
        let worker_labels = self
            .pool_manager
            .get_workers_labels(&allocatable.iter().copied().collect::<Vec<_>>());
        let strategy = self.selector_strategy.read().unwrap().clone();
        let mut selector = create_selector(&strategy);
        let result = select_workers_for_bg(
            &candidates,
            n,
            &exclude_set,
            exclude,
            &rules,
            selector.as_mut(),
            &worker_labels,
        );

        if result.len() < n {
            return Err(FsError::common(format!(
                "selector returned {} workers, need {}",
                result.len(),
                n
            )));
        }

        Ok(result)
    }

    // TODO:
    /// Rebuild table buckets: for each bucket, verify the BG's replica_set workers are still alive.
    /// Rebuild a BGTable incrementally using the unified policy framework.
    /// All changes are batched into a single Raft entry for consistency.
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

        // Build per-table snapshot.
        let live_workers = self.pool_manager.get_live_workers(table.pool_id());
        let pool = self.pool_manager.get_pool(table.pool_id())?;
        let w2b = self.worker_to_bgs.read().unwrap();

        let worker_snapshots: std::collections::HashMap<u32, WorkerLoadSnapshot> = live_workers
            .iter()
            .map(|&wid| {
                // Count per-table BG and lease for this worker.
                let table_bg_count = existing_bgs
                    .iter()
                    .filter(|bg| bg.replica_set.contains(&wid))
                    .count() as u32;
                let table_lease_count = existing_bgs
                    .iter()
                    .filter(|bg| bg.lease_owner.as_ref().map(|l| l.node_id) == Some(wid))
                    .count() as u32;
                let labels = self.pool_manager.get_worker_labels(wid).unwrap_or_default();
                let (capacity, used) = self
                    .pool_manager
                    .get_worker_storage_stats(wid, pool.media)
                    .unwrap_or((0, 0));
                (
                    wid,
                    WorkerLoadSnapshot {
                        worker_id: wid,
                        actual_bg: table_bg_count,
                        actual_lease: table_lease_count,
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
            .collect();
        drop(w2b);

        let ctx = PlacementContext {
            workers: &worker_snapshots,
            bucket_count: table.bucket_count,
            replica_count: table.replica_count(),
            tolerant_ratio: 0.1,
            lease_tolerant_ratio: 0.1,
        };

        let rules = self.get_pool_placement_rules(table.pool_id());
        let rule = rules
            .first()
            .cloned()
            .unwrap_or_else(PlacementRule::default_rule);
        let strategy = self.selector_strategy.read().unwrap().clone();
        let balance_policy = create_policy("quota");
        let mut st = balance_policy.prepare(&ctx)?;
        let mut selector = create_selector(&strategy);
        selector.init_from_policy(&ctx, &st);

        let options = RebuildOptions::default();
        let result = super::placement::rebuild_table(
            &table,
            &existing_bgs,
            &ctx,
            &rule,
            balance_policy.as_ref(),
            &mut st,
            selector.as_mut(),
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
                bg_epoch: Some(bg.bg_epoch - 1),
            })
            .collect();

        let entry = BatchBGEntry {
            op_ms: orpc::common::LocalTime::mills(),
            table: None,
            creates: vec![],
            updates,
            next_bg_id: None,
        };
        self.propose_batch_bg(entry)
    }

    /// Rebuild all tables for a pool (e.g. after node join/remove). Calls rebuild_table for each table in the pool.
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

    /// Get placement rules for a pool. Derives placement from the table's policy.
    pub fn get_pool_placement_rules(&self, pool_id: u16) -> Vec<PlacementRule> {
        // Find the table for this pool to get its placement policy
        let placement = self
            .tables
            .read()
            .unwrap()
            .values()
            .find(|t| t.pool_id() == pool_id)
            .map(|t| t.policy.placement)
            .unwrap_or(curvine_common::state::PlacementPolicy::Default);
        // TODO: load custom rules from ConfigManager KV when supported
        vec![PlacementRule::from_placement_policy(
            placement,
            &self.location_labels,
        )]
    }

    // TODO: 应该通过动态参数设置
    /// Switch the worker selection strategy at runtime.
    pub fn set_selector_strategy(&self, name: &str) -> FsResult<()> {
        match name {
            "quota" | "normalized" | "random" => {
                *self.selector_strategy.write().unwrap() = name.to_string();
                Ok(())
            }
            _ => Err(FsError::common(format!(
                "unknown selector strategy: {}",
                name
            ))),
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
            crate::pd::node::NodeManager::new(node_store, config_manager, jc.clone()),
        );
        let pool_manager = Arc::new(PoolManager::new(pool_store, node_manager, jc.clone()));
        BGManager::new(bg_store, pool_manager, jc, 1024, vec![3], vec![])
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
            bg_epoch: None,
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
            bg_epoch: None,
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
                epoch: 0,
                grant_time_ms: 99_000,
            }),
            bg_epoch: None,
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
        mgr.apply_delete_bg(4).unwrap();
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
            bg_epoch: None,
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
            bg_epoch: None,
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
                epoch: 0,
                grant_time_ms: 100_000,
            }),
            bg_epoch: None,
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
            bg_epoch: None,
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
            bg_epoch: None,
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
            bg_epoch: None,
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

        mgr.apply_delete_bg(21).unwrap();
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

    // ========== Suspect BG tests ==========

    #[test]
    fn mark_and_take_suspect_bgs() {
        let mgr = test_manager();
        let info1 = make_bg(60, 1, vec![1]);
        let info2 = make_bg(61, 1, vec![2]);
        mgr.apply_create_bg(&BGEntry {
            op_ms: 0,
            info: info1,
        })
        .unwrap();
        mgr.apply_create_bg(&BGEntry {
            op_ms: 0,
            info: info2,
        })
        .unwrap();

        assert_eq!(mgr.suspect_count(), 0);

        mgr.mark_suspect(60);
        mgr.mark_suspect(61);
        assert_eq!(mgr.suspect_count(), 2);

        // take_suspect_bgs returns all suspect BGs and increments check_count
        let suspects = mgr.take_suspect_bgs(5, 300_000);
        assert_eq!(suspects.len(), 2);
        // After take, count still 2 (entries are not removed, just incremented)
        assert_eq!(mgr.suspect_count(), 2);
    }

    #[test]
    fn suspect_expires_after_max_checks() {
        let mgr = test_manager();
        let info = make_bg(70, 1, vec![1]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();

        mgr.mark_suspect(70);
        // max_checks=3: take increments check_count each call.
        // After 3 takes, check_count=3; on the 4th take it's expired (>= max_checks).
        let _ = mgr.take_suspect_bgs(3, 300_000); // check_count: 0->1
        assert_eq!(mgr.suspect_count(), 1);
        let _ = mgr.take_suspect_bgs(3, 300_000); // check_count: 1->2
        assert_eq!(mgr.suspect_count(), 1);
        let _ = mgr.take_suspect_bgs(3, 300_000); // check_count: 2->3
        assert_eq!(mgr.suspect_count(), 1);
        let _ = mgr.take_suspect_bgs(3, 300_000); // check_count=3 >= max_checks=3 -> expired
        assert_eq!(mgr.suspect_count(), 0);
    }

    #[test]
    fn clear_suspect_removes_entry() {
        let mgr = test_manager();
        let info = make_bg(80, 1, vec![1]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();

        mgr.mark_suspect(80);
        assert_eq!(mgr.suspect_count(), 1);

        mgr.clear_suspect(80);
        assert_eq!(mgr.suspect_count(), 0);
    }

    #[test]
    fn mark_worker_bgs_suspect_marks_all_bgs_on_worker() {
        let mgr = test_manager();
        let info1 = make_bg(90, 1, vec![1, 2]);
        let info2 = make_bg(91, 1, vec![1, 3]);
        let info3 = make_bg(92, 1, vec![3, 4]); // not on worker 1
        mgr.apply_create_bg(&BGEntry {
            op_ms: 0,
            info: info1,
        })
        .unwrap();
        mgr.apply_create_bg(&BGEntry {
            op_ms: 0,
            info: info2,
        })
        .unwrap();
        mgr.apply_create_bg(&BGEntry {
            op_ms: 0,
            info: info3,
        })
        .unwrap();

        mgr.mark_worker_bgs_suspect(1);
        // BGs 90 and 91 are on worker 1, BG 92 is not
        assert_eq!(mgr.suspect_count(), 2);
    }
}
