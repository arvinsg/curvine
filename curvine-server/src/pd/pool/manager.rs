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

use super::{PoolIndex, PoolStore};
use crate::pd::journal::entry::PoolEntry;
use crate::pd::journal::{self, ApplyOutcome, PdEntry};
use crate::pd::node::NodeManager;
use curvine_common::state::{NodeAddress, NodeInfo, NodePayload, NodeState, NodeType, StorageSpec};
use curvine_common::state::{PoolInfo, PoolStats, StorageType};
use curvine_common::{FsError, FsResult};
use orpc::common::LocalTime;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

pub const POOL_ID_MEM: u16 = 1;
pub const POOL_ID_SSD: u16 = 2;
pub const POOL_ID_HDD: u16 = 3;

fn pool_id_for_media(media: StorageType) -> Option<u16> {
    match media {
        StorageType::Mem => Some(POOL_ID_MEM),
        StorageType::Ssd => Some(POOL_ID_SSD),
        StorageType::Hdd => Some(POOL_ID_HDD),
        _ => None,
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolAssignmentResult {
    pub target_pool_ids: Vec<u16>,
    pub changed_pool_ids: Vec<u16>,
}

impl PoolAssignmentResult {
    pub fn is_empty(&self) -> bool {
        self.target_pool_ids.is_empty()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolReconcileResult {
    pub changed_pool_ids: Vec<u16>,
}

/// Outcome of `propose_pool_mutate`. Decouples the noop-vs-applied distinction
/// from FsError so callers iterating over multiple pools can collect changes
/// without try-propagating each apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolMutateResult {
    Applied,
    /// Either mutator chose no-op (target state already correct) or apply
    /// returned `SkippedNoop`. Caller treats this as success but excludes
    /// the pool from `changed_pool_ids`.
    Skipped,
}

pub struct PoolManager {
    index: Arc<RwLock<PoolIndex>>,
    store: Arc<PoolStore>,
    node_manager: Arc<NodeManager>,
    journal_client: Arc<journal::Client>,
    /// Single global write lock that serializes the three propose entry points
    /// (P1.2): event_loop assign/remove, patrol PoolMembershipChecker, and
    /// leader full_reconcile. With only 3 default pools, fine-grained locking
    /// brings no measurable benefit; CAS in `apply_save_pool` (P1.1) is the
    /// authoritative correctness layer, this lock is the optimization that
    /// keeps stale-retry rare.
    write_lock: Mutex<()>,
}

impl PoolManager {
    pub fn new(
        store: Arc<PoolStore>,
        node_manager: Arc<NodeManager>,
        journal_client: Arc<journal::Client>,
    ) -> Self {
        Self {
            index: Arc::new(RwLock::new(PoolIndex::new())),
            store,
            node_manager,
            journal_client,
            write_lock: Mutex::new(()),
        }
    }

    /// Restore from store.
    pub fn restore(&self) -> FsResult<()> {
        let pools = self.store.list_pools()?;
        let mut index = self.index.write().unwrap();
        index.clear();
        for info in pools {
            index.insert_pool(info);
        }
        Ok(())
    }

    /// Ensure default pools exist. Must be called on the leader after startup.
    /// Each missing pool is proposed with epoch=0 / expected_epoch=0; if a
    /// concurrent leader already created it, apply CAS rejects this entry as
    /// SkippedStale and we treat it as success (pool now exists).
    pub fn ensure_default_pools(&self) -> FsResult<()> {
        let defaults = [
            (POOL_ID_MEM, "mem_pool", StorageType::Mem),
            (POOL_ID_SSD, "ssd_pool", StorageType::Ssd),
            (POOL_ID_HDD, "hdd_pool", StorageType::Hdd),
        ];
        let _g = self.write_lock.lock().unwrap();
        for (pool_id, name, media) in defaults {
            if self.index.read().unwrap().get_pool(pool_id).is_some() {
                continue;
            }
            let info = PoolInfo::new(pool_id, name.to_string(), media);
            let entry = PoolEntry {
                op_ms: LocalTime::mills(),
                info,
                expected_epoch: 0,
            };
            let outcome = self
                .journal_client
                .propose_as_leader_with_result(PdEntry::SavePool(entry))?;
            match outcome {
                ApplyOutcome::Applied | ApplyOutcome::SkippedNoop => {}
                ApplyOutcome::SkippedStale { reason } => {
                    log::warn!(
                        "ensure_default_pools: create pool_id={} stale ({}) — \
                         likely created concurrently; treating as success",
                        pool_id,
                        reason
                    );
                }
                ApplyOutcome::NotFound { reason } => {
                    return Err(FsError::not_found(format!(
                        "ensure_default_pools pool_id={} unexpected NotFound: {}",
                        pool_id, reason
                    )));
                }
            }
        }
        Ok(())
    }

    /// Assign worker to pools based on storage_specs (unique storage_type -> pool).
    /// Returns target pools and pools actually changed by this call.
    pub fn assign_worker_to_pools(
        &self,
        worker_id: u32,
        storage_specs: &std::collections::HashMap<String, StorageSpec>,
    ) -> FsResult<PoolAssignmentResult> {
        let mut pool_ids = HashSet::new();
        for spec in storage_specs.values() {
            if let Some(pid) = pool_id_for_media(spec.storage_type) {
                pool_ids.insert(pid);
            }
        }
        let mut target_pool_ids: Vec<u16> = pool_ids.into_iter().collect();
        target_pool_ids.sort_unstable();
        if target_pool_ids.is_empty() {
            return Ok(PoolAssignmentResult::default());
        }

        // Single global write_lock serializes the three propose entry points
        // (P1.2). Inside the lock we read fresh pool epoch and propose with
        // matching expected_epoch — apply CAS guarantees no two SavePool
        // entries based on the same snapshot can both Applied.
        let _g = self.write_lock.lock().unwrap();
        let mut changed_pool_ids = Vec::new();
        for &pid in &target_pool_ids {
            match self.propose_pool_mutate(pid, |info| {
                if info.workers.insert(worker_id) {
                    Some("assign_worker_to_pools")
                } else {
                    None
                }
            })? {
                PoolMutateResult::Applied => changed_pool_ids.push(pid),
                PoolMutateResult::Skipped => {}
            }
        }

        if !changed_pool_ids.is_empty() {
            log::info!(
                "assigned worker {} to pools {:?} (targets {:?})",
                worker_id,
                changed_pool_ids,
                target_pool_ids
            );
        }
        Ok(PoolAssignmentResult {
            target_pool_ids,
            changed_pool_ids,
        })
    }

    /// Reconcile pool membership for a full worker snapshot. This is pool-centric:
    /// each changed pool is proposed once, avoiding O(worker) raft round-trips during leader start.
    pub fn reconcile_worker_pool_membership(
        &self,
        workers: &[NodeInfo],
    ) -> FsResult<PoolReconcileResult> {
        let desired_by_pool = self.compute_desired_workers(workers);

        let _g = self.write_lock.lock().unwrap();
        let mut changed_pool_ids = Vec::new();
        let pool_ids: Vec<u16> = self
            .index
            .read()
            .unwrap()
            .all_pool_ids()
            .into_iter()
            .collect();
        for pool_id in pool_ids {
            let desired = desired_by_pool
                .get(&pool_id)
                .cloned()
                .unwrap_or_default();
            match self.propose_pool_mutate(pool_id, |info| {
                if info.workers != desired {
                    info.workers = desired.clone();
                    Some("reconcile_worker_pool_membership")
                } else {
                    None
                }
            })? {
                PoolMutateResult::Applied => changed_pool_ids.push(pool_id),
                PoolMutateResult::Skipped => {}
            }
        }
        changed_pool_ids.sort_unstable();

        if !changed_pool_ids.is_empty() {
            log::info!(
                "reconciled worker pool membership, changed_pool_ids={:?}",
                changed_pool_ids
            );
        }
        Ok(PoolReconcileResult { changed_pool_ids })
    }

    /// Compute the desired worker set per pool from a full node snapshot.
    /// Pure read: no propose, no lock dependencies.
    fn compute_desired_workers(&self, workers: &[NodeInfo]) -> HashMap<u16, HashSet<u32>> {
        let mut desired_by_pool: HashMap<u16, HashSet<u32>> = HashMap::new();
        for node in workers {
            let NodePayload::Worker(payload) = &node.payload else {
                log::warn!(
                    "reconcile_worker_pool_membership: node {} payload is not Worker; skip",
                    node.base.node_id
                );
                continue;
            };
            if !matches!(
                node.state,
                NodeState::Starting | NodeState::Live | NodeState::Lost
            ) {
                continue;
            }
            for spec in payload.storage_specs.values() {
                if let Some(pid) = pool_id_for_media(spec.storage_type) {
                    desired_by_pool
                        .entry(pid)
                        .or_default()
                        .insert(node.base.node_id);
                }
            }
        }
        // Sanity warn for desired pools that aren't registered.
        let existing: HashSet<u16> = self
            .index
            .read()
            .unwrap()
            .all_pool_ids()
            .into_iter()
            .collect();
        for pid in desired_by_pool.keys() {
            if !existing.contains(pid) {
                log::warn!(
                    "reconcile_worker_pool_membership: desired pool {} does not exist; \
                     workers will not be assigned",
                    pid
                );
            }
        }
        desired_by_pool
    }

    /// Remove worker from all pools (e.g. on worker offline).
    pub fn remove_worker_from_pools(&self, worker_id: u32) -> FsResult<()> {
        let pool_ids: Vec<u16> = {
            let index = self.index.read().unwrap();
            match index.get_pools_by_worker(worker_id) {
                Some(ids) => ids.iter().copied().collect(),
                None => return Ok(()),
            }
        };

        let _g = self.write_lock.lock().unwrap();
        for pool_id in pool_ids {
            self.propose_pool_mutate(pool_id, |info| {
                if info.workers.remove(&worker_id) {
                    Some("remove_worker_from_pools")
                } else {
                    None
                }
            })?;
        }
        Ok(())
    }

    /// Internal helper: read fresh pool snapshot, apply `mutator`, propose with
    /// CAS, and translate the `ApplyOutcome` into a `PoolMutateResult`.
    ///
    /// `mutator` returns `Some(reason)` to indicate a desired mutation, or
    /// `None` if the pool already matches the target state (no propose).
    /// Caller MUST hold `write_lock` before calling.
    fn propose_pool_mutate(
        &self,
        pool_id: u16,
        mutator: impl FnOnce(&mut PoolInfo) -> Option<&'static str>,
    ) -> FsResult<PoolMutateResult> {
        let pool = match self.index.read().unwrap().get_pool(pool_id) {
            Some(p) => p.clone(),
            None => {
                log::warn!(
                    "propose_pool_mutate: pool {} does not exist; skipping",
                    pool_id
                );
                return Ok(PoolMutateResult::Skipped);
            }
        };

        let mut updated = pool.clone();
        let reason = match mutator(&mut updated) {
            Some(r) => r,
            None => return Ok(PoolMutateResult::Skipped),
        };

        let expected_epoch = pool.epoch;
        updated.epoch = expected_epoch.saturating_add(1);
        let entry = PoolEntry {
            op_ms: LocalTime::mills(),
            info: updated,
            expected_epoch,
        };
        let outcome = self
            .journal_client
            .propose_as_leader_with_result(PdEntry::SavePool(entry))?;
        match outcome {
            ApplyOutcome::Applied => Ok(PoolMutateResult::Applied),
            ApplyOutcome::SkippedNoop => Ok(PoolMutateResult::Skipped),
            ApplyOutcome::SkippedStale { reason: stale_reason } => {
                // §17 contract: do not retry in propose path. Surface the stale
                // outcome to upper-layer scheduling (patrol will reschedule).
                log::warn!(
                    "propose_pool_mutate ({}) for pool {} returned Stale: {}",
                    reason,
                    pool_id,
                    stale_reason
                );
                Err(FsError::stale_entry(
                    "save_pool",
                    expected_epoch,
                    stale_reason,
                ))
            }
            ApplyOutcome::NotFound { reason: nf_reason } => {
                Err(FsError::not_found(nf_reason))
            }
        }
    }

    /// Raft apply callback for SavePool.
    ///
    /// CAS contract (P1.1):
    /// - target pool not found → `NotFound`
    /// - `existing.epoch != entry.expected_epoch` → `SkippedStale`
    /// - `entry.info.epoch != existing.epoch + 1` → `SkippedStale` (non-monotonic)
    /// - otherwise → mutate, `Applied`
    ///
    /// Pre-P1.1 SavePool entries (no `expected_epoch` field) decode with
    /// `expected_epoch=0`. They are accepted only when applied to a pool whose
    /// current epoch is also 0 — which matches first-time `ensure_default_pools`.
    pub fn apply_save_pool(&self, entry: &PoolEntry) -> FsResult<ApplyOutcome> {
        let mut index = self.index.write().unwrap();
        match index.get_pool(entry.info.pool_id).cloned() {
            None => {
                // First-time create: only accept if entry's expected_epoch is 0
                // and info.epoch is also 0. This protects against rogue entries
                // claiming arbitrary starting epochs.
                if entry.expected_epoch != 0 || entry.info.epoch != 0 {
                    log::warn!(
                        "Apply SavePool create skipped: pool_id={} not present \
                         but expected_epoch={} info.epoch={} (both must be 0 \
                         for first-time create)",
                        entry.info.pool_id,
                        entry.expected_epoch,
                        entry.info.epoch
                    );
                    return Ok(ApplyOutcome::stale("non-zero epoch on create"));
                }
                self.store.put_pool(&entry.info)?;
                index.insert_pool(entry.info.clone());
                log::info!(
                    "Apply SavePool created pool_id={}, workers={}, epoch=0",
                    entry.info.pool_id,
                    entry.info.workers.len()
                );
                Ok(ApplyOutcome::Applied)
            }
            Some(existing) => {
                if existing.epoch != entry.expected_epoch {
                    log::warn!(
                        "Apply SavePool skipped: pool_id={} stale, current_epoch={}, \
                         entry_expected_epoch={}, entry_info_epoch={}",
                        entry.info.pool_id,
                        existing.epoch,
                        entry.expected_epoch,
                        entry.info.epoch
                    );
                    return Ok(ApplyOutcome::stale(format!(
                        "epoch mismatch: current={}, expected={}",
                        existing.epoch, entry.expected_epoch
                    )));
                }
                if entry.info.epoch != existing.epoch.saturating_add(1) {
                    log::warn!(
                        "Apply SavePool skipped: pool_id={} non-monotonic, \
                         current_epoch={}, entry_info_epoch={}",
                        entry.info.pool_id,
                        existing.epoch,
                        entry.info.epoch
                    );
                    return Ok(ApplyOutcome::stale(format!(
                        "non-monotonic: current={}, info.epoch={}",
                        existing.epoch, entry.info.epoch
                    )));
                }
                self.store.put_pool(&entry.info)?;
                index.insert_pool(entry.info.clone());
                log::info!(
                    "Apply SavePool updated pool_id={}, workers={}, epoch={}->{}",
                    entry.info.pool_id,
                    entry.info.workers.len(),
                    existing.epoch,
                    entry.info.epoch
                );
                Ok(ApplyOutcome::Applied)
            }
        }
    }

    pub fn get_pool_by_media(&self, media: StorageType) -> FsResult<PoolInfo> {
        let index = self.index.read().unwrap();
        index
            .get_pool_by_media(media)
            .cloned()
            .ok_or_else(|| FsError::common(format!("no pool for media {:?}", media)))
    }

    pub fn get_pool(&self, pool_id: u16) -> FsResult<PoolInfo> {
        let index = self.index.read().unwrap();
        index
            .get_pool(pool_id)
            .cloned()
            .ok_or_else(|| FsError::common(format!("pool {} not found", pool_id)))
    }

    /// List pools that have at least one worker (active = worker count > 0).
    pub fn list_active_pools(&self) -> Vec<PoolInfo> {
        let index = self.index.read().unwrap();
        index
            .list_pools()
            .into_iter()
            .filter(|p| !p.workers.is_empty())
            .cloned()
            .collect()
    }

    pub fn get_workers_in_pool(&self, pool_id: u16) -> Vec<u32> {
        let index = self.index.read().unwrap();
        index
            .get_pool(pool_id)
            .map(|p| p.workers.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Get pool IDs that contain this worker.
    pub fn get_pools_by_worker(&self, worker_id: u32) -> Vec<u16> {
        let index = self.index.read().unwrap();
        index
            .get_pools_by_worker(worker_id)
            .map(|s: &std::collections::HashSet<u16>| s.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Update pool stats (in-memory only). For use by Scheduler to periodically refresh.
    pub fn update_pool_stats(&self, pool_id: u16, stats: PoolStats) -> FsResult<()> {
        let mut index = self.index.write().unwrap();
        index.update_pool_stats(pool_id, stats);
        Ok(())
    }

    /// Refresh stats for all pools by aggregating worker storage_stats.
    /// For each pool, sums capacity/available/used from workers whose storage_specs
    /// match the pool's media type.
    pub fn refresh_pool_stats(&self) {
        let pool_ids = {
            let index = self.index.read().unwrap();
            index.all_pool_ids()
        };

        for pool_id in pool_ids {
            let (workers, media) = {
                let index = self.index.read().unwrap();
                match index.get_pool(pool_id) {
                    Some(pool) => (pool.workers.clone(), pool.media),
                    None => continue,
                }
            };

            let mut stats = PoolStats::default();
            for worker_id in &workers {
                let Some(node) = self.node_manager.get_node(*worker_id) else {
                    continue;
                };
                let NodePayload::Worker(ref payload) = node.payload else {
                    continue;
                };
                // Find storage_ids that match this pool's media type
                let matching_ids: Vec<&String> = payload
                    .storage_specs
                    .iter()
                    .filter(|(_, spec)| spec.storage_type == media)
                    .map(|(id, _)| id)
                    .collect();
                for sid in matching_ids {
                    if let Some(ss) = payload.storage_stats.get(sid) {
                        stats.capacity_bytes += ss.capacity as u64;
                        stats.available_bytes += ss.available as u64;
                        stats.used_bytes += ss.fs_used as u64;
                        stats.block_count += ss.block_num as u64;
                    }
                }
            }

            let mut index = self.index.write().unwrap();
            index.update_pool_stats(pool_id, stats);
        }
    }

    /// Get live workers in a pool: pool.workers ∩ {Live nodes}.
    pub fn get_live_workers(&self, pool_id: u16) -> Vec<u32> {
        let index = self.index.read().unwrap();
        let workers = match index.get_pool(pool_id) {
            Some(pool) => pool.workers.clone(),
            None => return Vec::new(),
        };
        drop(index);
        workers
            .into_iter()
            .filter(|&wid| {
                self.node_manager
                    .get_node(wid)
                    .map(|n| n.state == NodeState::Live)
                    .unwrap_or(false)
            })
            .collect()
    }

    /// Get worker storage stats for a specific media type.
    /// Returns (capacity_bytes, used_bytes) summed across all dirs of the given media.
    pub fn get_worker_storage_stats(
        &self,
        worker_id: u32,
        media: StorageType,
    ) -> Option<(u64, u64)> {
        let node = self.node_manager.get_node(worker_id)?;
        let NodePayload::Worker(ref p) = node.payload else {
            return None;
        };
        let mut cap = 0u64;
        let mut used = 0u64;
        for (sid, spec) in &p.storage_specs {
            if spec.storage_type == media {
                if let Some(ss) = p.storage_stats.get(sid) {
                    cap += ss.capacity as u64;
                    used += ss.fs_used as u64;
                }
            }
        }
        Some((cap, used))
    }

    /// Get labels for a single worker.
    pub fn get_worker_labels(
        &self,
        worker_id: u32,
    ) -> Option<std::collections::HashMap<String, String>> {
        self.node_manager
            .get_node(worker_id)
            .map(|n| n.base.labels.clone())
    }

    /// Check if a worker is available: present in any pool and node state is Live.
    pub fn is_worker_available(&self, worker_id: u32) -> bool {
        let index = self.index.read().unwrap();
        let in_pool = index.get_pools_by_worker(worker_id).is_some();
        drop(index);
        if !in_pool {
            return false;
        }
        self.node_manager
            .get_node(worker_id)
            .map(|n| n.state == NodeState::Live)
            .unwrap_or(false)
    }

    /// Returns None if the node does not exist or is not a Worker.
    pub fn get_worker_node(&self, worker_id: u32) -> Option<NodeInfo> {
        let node = self.node_manager.get_node(worker_id)?;
        if node.base.node_type == NodeType::Worker {
            Some(node)
        } else {
            None
        }
    }

    /// Get labels for a set of workers.
    pub fn get_workers_labels(
        &self,
        worker_ids: &[u32],
    ) -> std::collections::HashMap<u32, std::collections::HashMap<String, String>> {
        worker_ids
            .iter()
            .filter_map(|&wid| {
                self.node_manager
                    .get_node(wid)
                    .map(|n| (wid, n.base.labels.clone()))
            })
            .collect()
    }

    /// Get worker address and state for BG view building.
    pub fn get_worker_address_and_state(&self, worker_id: u32) -> Option<(NodeAddress, NodeState)> {
        self.node_manager
            .get_node(worker_id)
            .map(|n| (n.base.address.clone(), n.state))
    }
}

#[cfg(test)]
impl PoolManager {
    pub fn test_insert_node(&self, node: NodeInfo) {
        self.node_manager.test_insert_node(node);
    }

    /// Test helper: install/replace a pool's in-memory state, bypassing CAS.
    /// Computes the correct epoch (existing.epoch + 1, or 0 for first-create)
    /// so callers don't have to manage it. Use this for test setup; regression
    /// tests that exercise CAS itself should call apply_save_pool directly.
    pub fn test_install_pool(&self, mut info: PoolInfo) -> FsResult<ApplyOutcome> {
        let expected_epoch = self
            .index
            .read()
            .unwrap()
            .get_pool(info.pool_id)
            .map(|p| p.epoch)
            .unwrap_or(0);
        info.epoch = if self
            .index
            .read()
            .unwrap()
            .get_pool(info.pool_id)
            .is_some()
        {
            expected_epoch.saturating_add(1)
        } else {
            0
        };
        self.apply_save_pool(&PoolEntry {
            op_ms: 0,
            info,
            expected_epoch,
        })
    }

    /// Plan-only helper for tests: compute (updates, changed_pool_ids) the
    /// same way `reconcile_worker_pool_membership` would, but without
    /// proposing through Raft. Tests assert on the planned diff.
    pub fn test_worker_pool_reconcile_updates(
        &self,
        workers: &[NodeInfo],
    ) -> (Vec<PoolInfo>, Vec<u16>) {
        let desired_by_pool = self.compute_desired_workers(workers);
        let index = self.index.read().unwrap();
        let mut updates = Vec::new();
        let mut changed = Vec::new();
        for pool in index.list_pools() {
            let desired = desired_by_pool
                .get(&pool.pool_id)
                .cloned()
                .unwrap_or_default();
            if pool.workers == desired {
                continue;
            }
            let mut updated = pool.clone();
            updated.workers = desired;
            changed.push(updated.pool_id);
            updates.push(updated);
        }
        changed.sort_unstable();
        (updates, changed)
    }
}
