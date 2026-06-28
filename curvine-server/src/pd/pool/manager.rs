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

use super::PoolIndex;
use crate::pd::node::NodeManager;
use curvine_common::state::{
    is_pool_storage_type, NodeAddress, NodeInfo, NodePayload, NodeState, NodeType, PoolInfo,
    PoolStats, StorageSpec, StorageType, POOL_STORAGE_TYPES,
};
use curvine_common::{FsError, FsResult};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

pub struct PoolManager {
    index: Arc<RwLock<PoolIndex>>,
    node_manager: Arc<NodeManager>,
}

impl PoolManager {
    pub fn new(node_manager: Arc<NodeManager>) -> Self {
        Self {
            index: Arc::new(RwLock::new(PoolIndex::new())),
            node_manager,
        }
    }

    /// Restore pools and rebuild runtime membership from NodeManager.
    pub fn restore(&self) -> FsResult<()> {
        self.reconcile_worker_pool_membership()?;
        self.refresh_pool_stats();
        Ok(())
    }

    pub fn get_pool(&self, media: StorageType) -> FsResult<PoolInfo> {
        let index = self.index.read().unwrap();
        index
            .get_pool(media)
            .cloned()
            .ok_or_else(|| FsError::common(format!("pool {} not found", media)))
    }

    pub fn list_pools(&self) -> Vec<PoolInfo> {
        self.index
            .read()
            .unwrap()
            .list_pools()
            .into_iter()
            .cloned()
            .collect()
    }

    /// List pools that have at least one runtime worker member.
    pub fn list_active_pools(&self) -> Vec<PoolInfo> {
        self.list_pools()
            .into_iter()
            .filter(|p| !p.workers.is_empty())
            .collect()
    }

    pub fn get_workers_in_pool(&self, media: StorageType) -> Vec<u32> {
        let mut workers: Vec<u32> = self
            .index
            .read()
            .unwrap()
            .get_pool(media)
            .map(|p| p.workers.iter().copied().collect())
            .unwrap_or_default();
        workers.sort_unstable();
        workers
    }

    pub fn get_live_workers(&self, media: StorageType) -> Vec<u32> {
        let mut workers: Vec<u32> = self
            .get_workers_in_pool(media)
            .into_iter()
            .filter(|&wid| {
                self.node_manager
                    .get_node(wid)
                    .map(|node| node.state == NodeState::Live)
                    .unwrap_or(false)
            })
            .collect();
        workers.sort_unstable();
        workers
    }

    /// Get runtime pools this worker currently belongs to.
    pub fn get_pools_by_worker(&self, worker_id: u32) -> Vec<StorageType> {
        let mut pools: Vec<StorageType> = self
            .index
            .read()
            .unwrap()
            .get_pools_by_worker(worker_id)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        pools.sort_unstable();
        pools
    }

    /// Assign worker to runtime pools based on storage specs.
    /// Returns the pools whose membership changed.
    pub fn assign_worker_to_pools(
        &self,
        worker_id: u32,
        storage_specs: &HashMap<String, StorageSpec>,
    ) -> FsResult<Vec<StorageType>> {
        let target = Self::pools_from_storage_specs(storage_specs);
        if target.is_empty() {
            log::info!(
                "worker {} has no storage specs that map to a runtime pool",
                worker_id
            );
        }

        let mut changed: Vec<StorageType> = self
            .index
            .write()
            .unwrap()
            .set_worker_pools(worker_id, &target)
            .into_iter()
            .collect();
        changed.sort_unstable();
        if !changed.is_empty() {
            let mut target_pools: Vec<StorageType> = target.into_iter().collect();
            target_pools.sort_unstable();
            log::info!(
                "assigned worker {} to runtime pools {:?} (changed {:?})",
                worker_id,
                target_pools,
                changed
            );
        }
        Ok(changed)
    }

    /// Rebuild runtime pool membership from the current NodeManager state.
    pub fn reconcile_worker_pool_membership(&self) -> FsResult<Vec<StorageType>> {
        let mut index = self.index.write().unwrap();
        let workers = self.node_manager.get_nodes_by_type(NodeType::Worker);

        let mut desired_by_worker: HashMap<u32, HashSet<StorageType>> = HashMap::new();
        for node in &workers {
            let NodePayload::Worker(payload) = &node.payload else {
                continue;
            };
            if !matches!(
                node.state,
                NodeState::Starting | NodeState::Live | NodeState::Lost
            ) {
                continue;
            }
            desired_by_worker.insert(
                node.base.node_id,
                Self::pools_from_storage_specs(&payload.storage_specs),
            );
        }

        index.reset_pools();
        let mut changed = HashSet::new();
        for (worker_id, pools) in desired_by_worker {
            for media in &pools {
                index.add_worker_to_pool(*media, worker_id);
                changed.insert(*media);
            }
        }
        let mut changed_pools: Vec<StorageType> = changed.into_iter().collect();
        changed_pools.sort_unstable();
        Ok(changed_pools)
    }

    /// Remove worker from all runtime pools. This is in-memory only.
    pub fn remove_worker_from_pools(&self, worker_id: u32) -> FsResult<Vec<StorageType>> {
        let mut removed: Vec<StorageType> = self
            .index
            .write()
            .unwrap()
            .remove_worker(worker_id)
            .map(|s| s.into_iter().collect())
            .unwrap_or_default();
        removed.sort_unstable();
        if !removed.is_empty() {
            log::info!(
                "removed worker {} from runtime pools {:?}",
                worker_id,
                removed
            );
        }
        Ok(removed)
    }

    /// Update pool stats (in-memory only). For use by Scheduler to periodically refresh.
    pub fn update_pool_stats(&self, media: StorageType, stats: PoolStats) -> FsResult<()> {
        self.index.write().unwrap().update_pool_stats(media, stats);
        Ok(())
    }

    /// Refresh stats for all pools by aggregating worker storage_stats.
    pub fn refresh_pool_stats(&self) {
        for media in POOL_STORAGE_TYPES {
            let mut stats = PoolStats::default();
            for worker_id in self.get_workers_in_pool(media) {
                let Some(node) = self.node_manager.get_node(worker_id) else {
                    continue;
                };
                let NodePayload::Worker(ref payload) = node.payload else {
                    continue;
                };
                for (sid, spec) in &payload.storage_specs {
                    if spec.storage_type != media {
                        continue;
                    }
                    if let Some(ss) = payload.storage_stats.get(sid) {
                        stats.capacity_bytes += ss.capacity as u64;
                        stats.available_bytes += ss.available as u64;
                        stats.used_bytes += ss.fs_used as u64;
                        stats.block_count += ss.block_num as u64;
                    }
                }
            }
            let _ = self.update_pool_stats(media, stats);
        }
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

    /// Check if a worker is available: it is in at least one runtime pool and node state is Live.
    pub fn is_worker_available(&self, worker_id: u32) -> bool {
        if self.get_pools_by_worker(worker_id).is_empty() {
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

    fn pools_from_storage_specs(
        storage_specs: &HashMap<String, StorageSpec>,
    ) -> HashSet<StorageType> {
        storage_specs
            .values()
            .filter_map(|spec| is_pool_storage_type(spec.storage_type).then_some(spec.storage_type))
            .collect()
    }
}

#[cfg(test)]
impl PoolManager {
    pub fn test_insert_node(&self, node: NodeInfo) {
        self.node_manager.test_insert_node(node);
    }
}
