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
use curvine_common::state::{NodeAddress, NodeInfo, NodePayload, NodeState, NodeType, StorageSpec};
use curvine_common::state::{PoolInfo, PoolStats, StorageType};
use curvine_common::{FsError, FsResult};
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
    pub fn ensure_default_pools(&self) -> FsResult<()> {
        let defaults = [
            (POOL_ID_MEM, "mem_pool", StorageType::Mem),
            (POOL_ID_SSD, "ssd_pool", StorageType::Ssd),
            (POOL_ID_HDD, "hdd_pool", StorageType::Hdd),
        ];
        let index = self.index.read().unwrap();
        for (pool_id, name, media) in defaults {
            if index.get_pool(pool_id).is_some() {
                continue;
            }
            drop(index);
            let info = PoolInfo::new(pool_id, name.to_string(), media);
            let entry = crate::pd::journal::entry::PoolEntry {
                op_ms: orpc::common::LocalTime::mills(),
                info,
            };
            self.journal_client
                .propose(crate::pd::journal::PdEntry::SavePool(entry))?;
            // Re-acquire lock for next iteration
            return self.ensure_default_pools();
        }
        Ok(())
    }

    /// Assign worker to pools based on storage_specs (unique storage_type -> pool).
    /// Returns list of pool_ids the worker was added to.
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

        let now = LocalTime::mills();
        let index = self.index.read().unwrap();
        let pool_ids_vec: Vec<u16> = pool_ids.into_iter().collect();
        for &pid in &pool_ids_vec {
            if let Some(pool) = index.get_pool(pid) {
                let mut updated = pool.clone();
                updated.workers.insert(worker_id);
                self.journal_client.propose(PdEntry::SavePool(PoolEntry {
                    op_ms: now,
                    info: updated,
                }))?;
            }
        }
        Ok(pool_ids_vec)
    }

    /// Remove worker from all pools (e.g. on worker offline).
    pub fn remove_worker_from_pools(&self, worker_id: u32) -> FsResult<()> {
        let index = self.index.read().unwrap();
        let pool_ids = match index.get_pools_by_worker(worker_id) {
            Some(ids) => ids.iter().copied().collect::<Vec<_>>(),
            None => return Ok(()),
        };
        let now = LocalTime::mills();
        for pool_id in pool_ids {
            if let Some(pool) = index.get_pool(pool_id) {
                let mut updated = pool.clone();
                updated.workers.remove(&worker_id);
                self.journal_client.propose(PdEntry::SavePool(PoolEntry {
                    op_ms: now,
                    info: updated,
                }))?;
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
