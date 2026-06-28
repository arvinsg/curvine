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

use curvine_common::state::{PoolInfo, PoolStats, StorageType, POOL_STORAGE_TYPES};
use std::collections::{HashMap, HashSet};

/// In-memory index for fixed pools and runtime worker membership.
///
/// Pool membership is maintained in memory only. It is rebuilt from NodeManager
/// at leader restore/reconcile and updated incrementally by node events.
pub struct PoolIndex {
    pools: HashMap<StorageType, PoolInfo>,
    worker_to_pools: HashMap<u32, HashSet<StorageType>>,
}

impl PoolIndex {
    pub fn new() -> Self {
        let mut idx = Self {
            pools: HashMap::new(),
            worker_to_pools: HashMap::new(),
        };
        idx.reset_pools();
        idx
    }

    pub fn reset_pools(&mut self) {
        self.pools.clear();
        self.worker_to_pools.clear();
        for media in POOL_STORAGE_TYPES {
            self.pools.insert(media, PoolInfo::new(media));
        }
    }

    pub fn get_pool(&self, media: StorageType) -> Option<&PoolInfo> {
        self.pools.get(&media)
    }

    pub fn get_pools_by_worker(&self, worker_id: u32) -> Option<&HashSet<StorageType>> {
        self.worker_to_pools.get(&worker_id)
    }

    pub fn list_pools(&self) -> Vec<&PoolInfo> {
        let mut pools: Vec<_> = self.pools.values().collect();
        pools.sort_by_key(|p| p.media);
        pools
    }

    pub fn add_worker_to_pool(&mut self, media: StorageType, worker_id: u32) -> bool {
        let changed = self
            .worker_to_pools
            .entry(worker_id)
            .or_default()
            .insert(media);
        if changed {
            if let Some(pool) = self.pools.get_mut(&media) {
                pool.workers.insert(worker_id);
            }
        }
        changed
    }

    pub fn set_worker_pools(
        &mut self,
        worker_id: u32,
        target_pools: &HashSet<StorageType>,
    ) -> HashSet<StorageType> {
        let current = self
            .worker_to_pools
            .get(&worker_id)
            .cloned()
            .unwrap_or_default();
        let changed: HashSet<StorageType> = current
            .symmetric_difference(target_pools)
            .copied()
            .collect();
        if changed.is_empty() {
            return changed;
        }

        for media in current
            .difference(target_pools)
            .copied()
            .collect::<Vec<_>>()
        {
            if let Some(pool) = self.pools.get_mut(&media) {
                pool.workers.remove(&worker_id);
            }
        }
        for media in target_pools
            .difference(&current)
            .copied()
            .collect::<Vec<_>>()
        {
            if let Some(pool) = self.pools.get_mut(&media) {
                pool.workers.insert(worker_id);
            }
        }

        if target_pools.is_empty() {
            self.worker_to_pools.remove(&worker_id);
        } else {
            self.worker_to_pools.insert(worker_id, target_pools.clone());
        }
        changed
    }

    pub fn remove_worker(&mut self, worker_id: u32) -> Option<HashSet<StorageType>> {
        let pools = self.worker_to_pools.remove(&worker_id)?;
        for media in &pools {
            if let Some(pool) = self.pools.get_mut(media) {
                pool.workers.remove(&worker_id);
            }
        }
        Some(pools)
    }

    pub fn update_pool_stats(&mut self, media: StorageType, stats: PoolStats) {
        if let Some(pool) = self.pools.get_mut(&media) {
            pool.stats = stats;
        }
    }
}

impl Default for PoolIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pools_exist() {
        let idx = PoolIndex::new();
        assert_eq!(idx.list_pools().len(), 3);
        assert_eq!(
            idx.get_pool(StorageType::Mem).unwrap().media,
            StorageType::Mem
        );
        assert_eq!(
            idx.get_pool(StorageType::Ssd).unwrap().media,
            StorageType::Ssd
        );
        assert_eq!(
            idx.get_pool(StorageType::Hdd).unwrap().media,
            StorageType::Hdd
        );
    }

    #[test]
    fn add_and_remove_worker() {
        let mut idx = PoolIndex::new();
        assert!(idx.add_worker_to_pool(StorageType::Ssd, 100));
        assert!(!idx.add_worker_to_pool(StorageType::Ssd, 100));
        assert!(idx
            .get_pool(StorageType::Ssd)
            .unwrap()
            .workers
            .contains(&100));
        assert_eq!(
            idx.get_pools_by_worker(100),
            Some(&HashSet::from([StorageType::Ssd]))
        );
        let removed = idx.remove_worker(100).unwrap();
        assert_eq!(removed, HashSet::from([StorageType::Ssd]));
        assert!(idx.get_pool(StorageType::Ssd).unwrap().workers.is_empty());
    }

    #[test]
    fn update_pool_stats() {
        let mut idx = PoolIndex::new();
        idx.update_pool_stats(
            StorageType::Ssd,
            PoolStats {
                capacity_bytes: 1,
                available_bytes: 2,
                used_bytes: 3,
                block_count: 4,
            },
        );
        let p = idx.get_pool(StorageType::Ssd).unwrap();
        assert_eq!(p.stats.capacity_bytes, 1);
        assert_eq!(p.stats.block_count, 4);
    }
}
