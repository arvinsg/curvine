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
use curvine_common::state::{NodeAddress, NodeInfo, NodePayload, NodeState, NodeType, StorageSpec};
use curvine_common::state::{PoolInfo, PoolStats, PoolType, StorageType};
use curvine_common::{FsError, FsResult};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::RwLock;

pub struct PoolManager {
    index: Arc<RwLock<PoolIndex>>,
    node_manager: Arc<NodeManager>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolAssignmentResult {
    pub target_pool_types: Vec<PoolType>,
    pub changed_pool_types: Vec<PoolType>,
}

impl PoolAssignmentResult {
    pub fn is_empty(&self) -> bool {
        self.target_pool_types.is_empty()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolReconcileResult {
    pub changed_pool_types: Vec<PoolType>,
}

impl PoolManager {
    pub fn new(node_manager: Arc<NodeManager>) -> Self {
        Self {
            index: Arc::new(RwLock::new(PoolIndex::new())),
            node_manager,
        }
    }

    /// Restore fixed pools and rebuild runtime membership from NodeManager.
    pub fn restore(&self) -> FsResult<()> {
        self.reconcile_worker_pool_membership()?;
        self.refresh_pool_stats();
        Ok(())
    }

    pub fn get_pool_by_media(&self, media: StorageType) -> FsResult<PoolInfo> {
        let index = self.index.read().unwrap();
        index
            .get_pool_by_media(media)
            .cloned()
            .ok_or_else(|| FsError::common(format!("no pool for media {:?}", media)))
    }

    pub fn get_pool(&self, pool_type: PoolType) -> FsResult<PoolInfo> {
        let index = self.index.read().unwrap();
        index
            .get_pool(pool_type)
            .cloned()
            .ok_or_else(|| FsError::common(format!("pool {} not found", pool_type)))
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

    pub fn get_workers_in_pool(&self, pool_type: PoolType) -> Vec<u32> {
        let mut workers: Vec<u32> = self
            .index
            .read()
            .unwrap()
            .get_pool(pool_type)
            .map(|p| p.workers.iter().copied().collect())
            .unwrap_or_default();
        workers.sort_unstable();
        workers
    }

    pub fn get_live_workers(&self, pool_type: PoolType) -> Vec<u32> {
        let mut workers: Vec<u32> = self
            .get_workers_in_pool(pool_type)
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

    /// Get runtime pool types this worker currently belongs to.
    pub fn get_pools_by_worker(&self, worker_id: u32) -> Vec<PoolType> {
        let mut pools: Vec<PoolType> = self
            .index
            .read()
            .unwrap()
            .get_pools_by_worker(worker_id)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        pools.sort_unstable();
        pools
    }

    /// Assign worker to runtime pools based on storage specs. This is in-memory only.
    pub fn assign_worker_to_pools(
        &self,
        worker_id: u32,
        storage_specs: &HashMap<String, StorageSpec>,
    ) -> FsResult<PoolAssignmentResult> {
        let target = Self::pool_types_from_storage_specs(storage_specs);
        let changed = self
            .index
            .write()
            .unwrap()
            .set_worker_pools(worker_id, &target);
        let mut target_pool_types: Vec<PoolType> = target.into_iter().collect();
        let mut changed_pool_types: Vec<PoolType> = changed.into_iter().collect();
        target_pool_types.sort_unstable();
        changed_pool_types.sort_unstable();
        if !changed_pool_types.is_empty() {
            log::info!(
                "assigned worker {} to runtime pools {:?} (changed {:?})",
                worker_id,
                target_pool_types,
                changed_pool_types
            );
        }
        Ok(PoolAssignmentResult {
            target_pool_types,
            changed_pool_types,
        })
    }

    /// Rebuild runtime pool membership from the current NodeManager state.
    ///
    /// This method deliberately reads NodeManager while holding the PoolIndex
    /// write lock, so incremental event updates (assign/remove) cannot be
    /// interleaved between taking a worker snapshot and rebuilding pool
    /// membership. If an offline/decommission event has already updated
    /// NodeManager, reconcile will observe the new state; if the event happens
    /// after reconcile, the event path will apply the remove afterwards.
    pub fn reconcile_worker_pool_membership(&self) -> FsResult<PoolReconcileResult> {
        let mut index = self.index.write().unwrap();
        let workers = self.node_manager.get_nodes_by_type(NodeType::Worker);

        let mut desired_by_worker: HashMap<u32, HashSet<PoolType>> = HashMap::new();
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
                Self::pool_types_from_storage_specs(&payload.storage_specs),
            );
        }

        index.reset_fixed_pools();
        let mut changed = HashSet::new();
        for (worker_id, pools) in desired_by_worker {
            for pool_type in &pools {
                index.add_worker_to_pool(*pool_type, worker_id);
                changed.insert(*pool_type);
            }
        }
        let mut changed_pool_types: Vec<PoolType> = changed.into_iter().collect();
        changed_pool_types.sort_unstable();
        Ok(PoolReconcileResult { changed_pool_types })
    }

    /// Remove worker from all runtime pools. This is in-memory only.
    pub fn remove_worker_from_pools(&self, worker_id: u32) -> FsResult<Vec<PoolType>> {
        let mut removed: Vec<PoolType> = self
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
    pub fn update_pool_stats(&self, pool_type: PoolType, stats: PoolStats) -> FsResult<()> {
        self.index
            .write()
            .unwrap()
            .update_pool_stats(pool_type, stats);
        Ok(())
    }

    /// Refresh stats for all pools by aggregating worker storage_stats.
    pub fn refresh_pool_stats(&self) {
        for pool_type in PoolType::ALL {
            let mut stats = PoolStats::default();
            let media = pool_type.media();
            for worker_id in self.get_workers_in_pool(pool_type) {
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
            let _ = self.update_pool_stats(pool_type, stats);
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

    fn pool_types_from_storage_specs(
        storage_specs: &HashMap<String, StorageSpec>,
    ) -> HashSet<PoolType> {
        storage_specs
            .values()
            .filter_map(|spec| PoolType::from_media(spec.storage_type))
            .collect()
    }
}

#[cfg(test)]
impl PoolManager {
    pub fn test_insert_node(&self, node: NodeInfo) {
        self.node_manager.test_insert_node(node);
    }
}
