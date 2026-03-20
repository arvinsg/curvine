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
use crate::pd::journal::{self, PdEntry};
use crate::pd::node::NodeManager;
use curvine_common::state::{NodeInfo, NodePayload, NodeType, StorageSpec};
use curvine_common::state::{PlacementPolicy, PoolInfo, PoolStats, StorageType};
use curvine_common::{FsError, FsResult};

/// Compute isolation penalty between two workers along placement rules' location labels.
/// Lower = more isolated.
fn compute_pair_isolation_penalty(
    labels_a: &std::collections::HashMap<String, String>,
    labels_b: &std::collections::HashMap<String, String>,
    rules: &[crate::pd::schedule::placement::PlacementRule],
) -> f64 {
    let mut penalty = 0.0;
    for rule in rules {
        for (d, label_key) in rule.location_labels.iter().enumerate() {
            let val_a = labels_a.get(label_key);
            let val_b = labels_b.get(label_key);
            if val_a.is_some() && val_a == val_b {
                penalty += 1.0 / (d as f64 + 1.0);
                break;
            } else {
                break;
            }
        }
    }
    penalty
}
use orpc::common::LocalTime;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::RwLock;

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

pub struct PoolManager {
    index: Arc<RwLock<PoolIndex>>,
    store: Arc<PoolStore>,
    node_manager: Arc<NodeManager>,
    journal_client: Arc<journal::Client>,
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
        }
    }

    /// Assign worker to pools based on storage_specs (unique storage_type -> pool).
    /// Returns list of pool_ids the worker was added to.
    /// Updates index in-memory and proposes SavePool via Raft for persistence.
    pub fn assign_worker_to_pools(
        &self,
        worker_id: u32,
        storage_specs: &std::collections::HashMap<String, StorageSpec>,
    ) -> FsResult<Vec<u16>> {
        let mut pool_ids = HashSet::new();
        for spec in storage_specs.values() {
            if let Some(pid) = pool_id_for_media(spec.storage_type) {
                pool_ids.insert(pid);
            }
        }
        if pool_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut index = self.index.write().unwrap();
        for &pool_id in &pool_ids {
            index.add_worker_to_pool(pool_id, worker_id);
        }
        let pool_ids_vec: Vec<u16> = pool_ids.into_iter().collect();

        let now = LocalTime::mills();
        for pid in &pool_ids_vec {
            if let Some(pool) = index.get_pool(*pid).cloned() {
                self.journal_client
                    .propose(PdEntry::SavePool(PoolEntry { op_ms: now, info: pool }))?;
            }
        }
        Ok(pool_ids_vec)
    }

    /// Remove worker from all pools (e.g. on worker offline).
    /// Updates index in-memory and proposes SavePool via Raft for persistence.
    pub fn remove_worker_from_pools(&self, worker_id: u32) -> FsResult<()> {
        let mut index = self.index.write().unwrap();
        let pool_ids = { index.remove_worker(worker_id) };
        let Some(pool_ids) = pool_ids else {
            return Ok(());
        };
        let now = LocalTime::mills();
        for pool_id in &pool_ids {
            if let Some(pool) = index.get_pool(*pool_id).cloned() {
                self.journal_client
                    .propose(PdEntry::SavePool(PoolEntry { op_ms: now, info: pool }))?;
            }
        }
        Ok(())
    }

    /// Raft apply callback for SavePool.
    pub fn apply_save_pool(&self, entry: &PoolEntry) -> FsResult<()> {
        self.store.put_pool(&entry.info)?;
        let mut index = self.index.write().unwrap();
        index.insert_pool(entry.info.clone());
        Ok(())
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

    /// Get pool IDs that contain this worker (for schedule/coordinator).
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

    /// Restore from store.
    pub fn restore(&self) -> FsResult<()> {
        let pools = self.store.list_pools()?;
        let mut index = self.index.write().unwrap();
        index.clear();
        for info in pools {
            index.insert_pool(info);
        }
        drop(index);

        if self.store.list_pools()?.is_empty() {
            self.init_default_pools()?;
        }
        Ok(())
    }

    /// Rebuild allocatable_workers from current node states after restore.
    /// Scans all Live workers and marks them allocatable in their pools.
    pub fn rebuild_allocatable(&self) {
        let index = self.index.read().unwrap();
        let all_workers: Vec<(u32, Vec<u16>)> = index
            .list_pools()
            .into_iter()
            .flat_map(|p| p.workers.iter().map(move |&w| (w, p.pool_id)))
            .fold(
                std::collections::HashMap::<u32, Vec<u16>>::new(),
                |mut acc, (w, pid)| {
                    acc.entry(w).or_default().push(pid);
                    acc
                },
            )
            .into_iter()
            .collect();
        drop(index);

        for (worker_id, pool_ids) in all_workers {
            let is_live = self
                .node_manager
                .get_node(worker_id)
                .map(|n| n.state == curvine_common::state::NodeState::Live)
                .unwrap_or(false);
            if is_live {
                let mut index = self.index.write().unwrap();
                for pid in pool_ids {
                    index.add_allocatable(pid, worker_id);
                }
            }
        }
    }

    fn init_default_pools(&self) -> FsResult<()> {
        let default = [
            (POOL_ID_MEM, "mem_pool".to_string(), StorageType::Mem),
            (POOL_ID_SSD, "ssd_pool".to_string(), StorageType::Ssd),
            (POOL_ID_HDD, "hdd_pool".to_string(), StorageType::Hdd),
        ];
        for (pool_id, name, media) in default {
            let info = PoolInfo::new(pool_id, name, media);
            self.store.put_pool(&info)?;
            self.index.write().unwrap().insert_pool(info);
        }
        Ok(())
    }

    /// Select workers for BG replica set with placement-aware multi-level relaxation.
    ///
    /// Level 1 (CONSIDER_ALL): filter by label_constraints + sort by isolation from exclude_workers.
    /// Level 2 (CONSIDER_ISOLATION): relax isolation, keep only constraint filtering.
    /// Level 3 (CONSIDER_BASE): relax all constraints, pick any available worker.
    pub fn select_workers_for_bg(
        &self,
        pool_id: u16,
        replicas: u16,
        _placement: PlacementPolicy,
        exclude_workers: &[u32],
    ) -> FsResult<Vec<u32>> {
        self.select_workers_for_bg_with_rules(pool_id, replicas, &[], exclude_workers)
    }

    /// Select workers for BG replica set with explicit placement rules.
    pub fn select_workers_for_bg_with_rules(
        &self,
        pool_id: u16,
        replicas: u16,
        rules: &[crate::pd::schedule::placement::PlacementRule],
        exclude_workers: &[u32],
    ) -> FsResult<Vec<u32>> {
        let pool = self.get_pool(pool_id)?;
        let exclude: HashSet<u32> = exclude_workers.iter().copied().collect();
        let candidates: Vec<u32> = pool
            .allocatable_workers
            .iter()
            .copied()
            .filter(|w| !exclude.contains(w))
            .collect();
        let n = replicas as usize;
        if candidates.is_empty() || candidates.len() < n {
            return Err(FsError::common(format!(
                "not enough workers in pool {}: need {} have {}",
                pool_id,
                n,
                candidates.len()
            )));
        }

        // If no rules, return simple selection (Level 3 behavior)
        if rules.is_empty() || rules.iter().all(|r| r.label_constraints.is_empty() && r.location_labels.is_empty()) {
            return Ok(candidates.into_iter().take(n).collect());
        }

        let worker_labels = self.get_worker_labels(&candidates);

        // Level 1 (CONSIDER_ALL): constraint filter + isolation sort
        let constrained: Vec<u32> = candidates
            .iter()
            .copied()
            .filter(|&wid| {
                let empty = std::collections::HashMap::new();
                let labels = worker_labels.get(&wid).unwrap_or(&empty);
                rules.iter().all(|rule| {
                    rule.label_constraints.iter().all(|c| c.matches(labels))
                })
            })
            .collect();

        if constrained.len() >= n {
            // Sort by isolation from exclude_workers (pick most isolated first)
            let mut scored: Vec<(u32, f64)> = constrained
                .iter()
                .map(|&wid| {
                    let empty = std::collections::HashMap::new();
                    let labels = worker_labels.get(&wid).unwrap_or(&empty);
                    let isolation_penalty: f64 = exclude_workers.iter().map(|&ew| {
                        let ew_labels_map = self.node_manager.get_node(ew)
                            .map(|n| n.base.labels.clone())
                            .unwrap_or_default();
                        compute_pair_isolation_penalty(labels, &ew_labels_map, rules)
                    }).sum();
                    (wid, isolation_penalty)
                })
                .collect();
            scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            return Ok(scored.into_iter().take(n).map(|(wid, _)| wid).collect());
        }

        // Level 2 (CONSIDER_ISOLATION relaxed): just constraint filter, no isolation sort
        if constrained.len() >= n {
            return Ok(constrained.into_iter().take(n).collect());
        }

        // Level 3 (CONSIDER_BASE): all constraints relaxed
        Ok(candidates.into_iter().take(n).collect())
    }

    /// Check if a worker is allocatable (present in any pool's allocatable_workers).
    pub fn is_worker_available(&self, worker_id: u32) -> bool {
        let index = self.index.read().unwrap();
        index
            .get_pools_by_worker(worker_id)
            .map(|pool_ids| {
                pool_ids.iter().any(|&pid| {
                    index
                        .get_pool(pid)
                        .map(|p| p.allocatable_workers.contains(&worker_id))
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false)
    }

    /// Mark worker as allocatable in all pools it belongs to (memory-only).
    pub fn mark_allocatable(&self, worker_id: u32) {
        let mut index = self.index.write().unwrap();
        let pool_ids: Vec<u16> = index
            .get_pools_by_worker(worker_id)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        for pid in pool_ids {
            index.add_allocatable(pid, worker_id);
        }
    }

    /// Mark worker as unallocatable in all pools it belongs to (memory-only).
    pub fn mark_unallocatable(&self, worker_id: u32) {
        let mut index = self.index.write().unwrap();
        let pool_ids: Vec<u16> = index
            .get_pools_by_worker(worker_id)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        for pid in pool_ids {
            index.remove_allocatable(pid, worker_id);
        }
    }

    pub fn get_worker_az(&self, worker_id: u32) -> Option<String> {
        self.node_manager.get_node(worker_id).and_then(|n| {
            if let NodePayload::Worker(ref p) = n.payload {
                p.az.clone()
            } else {
                None
            }
        })
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
    pub fn get_worker_labels(&self, worker_ids: &[u32]) -> std::collections::HashMap<u32, std::collections::HashMap<String, String>> {
        worker_ids
            .iter()
            .filter_map(|&wid| {
                self.node_manager
                    .get_node(wid)
                    .map(|n| (wid, n.base.labels.clone()))
            })
            .collect()
    }
}
