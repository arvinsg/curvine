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
use crate::pd::node::NodeManager;
use curvine_common::state::{NodeInfo, NodePayload, NodeType, StorageSpec};
use curvine_common::state::{NodeState, PlacementPolicy, PoolInfo, PoolStats, StorageType};
use curvine_common::{FsError, FsResult};
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
}

impl PoolManager {
    pub fn new(store: Arc<PoolStore>, node_manager: Arc<NodeManager>) -> Self {
        Self {
            index: Arc::new(RwLock::new(PoolIndex::new())),
            store,
            node_manager,
        }
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
        let mut index = self.index.write().unwrap();
        for &pool_id in &pool_ids {
            index.add_worker_to_pool(pool_id, worker_id);
        }
        let pool_ids_vec: Vec<u16> = pool_ids.into_iter().collect();

        for pid in &pool_ids_vec {
            if let Some(pool) = index.get_pool(*pid).cloned() {
                self.store.put_pool(&pool)?;
            }
        }
        Ok(pool_ids_vec)
    }

    /// Remove worker from all pools (e.g. on worker offline).
    pub fn remove_worker_from_pools(&self, worker_id: u32) -> FsResult<()> {
        let mut index = self.index.write().unwrap();
        let pool_ids = { index.remove_worker(worker_id) };
        let Some(pool_ids) = pool_ids else {
            return Ok(());
        };
        for pool_id in &pool_ids {
            if let Some(pool) = index.get_pool(*pool_id).cloned() {
                self.store.put_pool(&pool)?;
            }
        }
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

    /// TODO:Select workers for BG replica set (only from Live workers in pool, excluding given set).
    pub fn select_workers_for_bg(
        &self,
        pool_id: u16,
        replicas: u16,
        _placement: PlacementPolicy,
        exclude_workers: &[u32],
    ) -> FsResult<Vec<u32>> {
        let pool = self.get_pool(pool_id)?;
        let exclude: HashSet<u32> = exclude_workers.iter().copied().collect();
        let candidates: Vec<u32> = pool
            .workers
            .iter()
            .copied()
            .filter(|w| self.is_worker_available(*w))
            .filter(|w| !exclude.contains(w))
            .collect();
        let n = replicas as usize;
        if candidates.len() < n {
            return Err(FsError::common(format!(
                "not enough workers in pool {}: need {} have {}",
                pool_id,
                n,
                candidates.len()
            )));
        }
        Ok(candidates.into_iter().take(n).collect())
    }

    pub fn is_worker_available(&self, worker_id: u32) -> bool {
        self.node_manager
            .get_node(worker_id)
            .map(|n| n.state == NodeState::Live)
            .unwrap_or(false)
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
}
