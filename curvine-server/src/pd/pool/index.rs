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

use curvine_common::state::{PoolInfo, PoolStats, PoolType, StorageType};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// In-memory index for fixed pools and runtime worker membership.
///
/// Pool membership is maintained in memory only. It is rebuilt from NodeManager
/// at leader restore/reconcile and updated incrementally by node events.
pub struct PoolIndex {
    pools: HashMap<PoolType, Arc<PoolInfo>>,
    by_media: HashMap<StorageType, PoolType>,
    worker_to_pools: HashMap<u32, HashSet<PoolType>>,
}

impl PoolIndex {
    pub fn new() -> Self {
        let mut idx = Self {
            pools: HashMap::new(),
            by_media: HashMap::new(),
            worker_to_pools: HashMap::new(),
        };
        idx.reset_fixed_pools();
        idx
    }

    pub fn reset_fixed_pools(&mut self) {
        self.pools.clear();
        self.by_media.clear();
        self.worker_to_pools.clear();
        for pool_type in PoolType::ALL {
            let info = PoolInfo::new(pool_type);
            self.by_media.insert(info.media, pool_type);
            self.pools.insert(pool_type, Arc::new(info));
        }
    }

    pub fn get_pool(&self, pool_type: PoolType) -> Option<&PoolInfo> {
        self.pools.get(&pool_type).map(|arc| arc.as_ref())
    }

    pub fn get_pool_by_media(&self, media: StorageType) -> Option<&PoolInfo> {
        self.by_media
            .get(&media)
            .and_then(|pool_type| self.get_pool(*pool_type))
    }

    pub fn get_pools_by_worker(&self, worker_id: u32) -> Option<&HashSet<PoolType>> {
        self.worker_to_pools.get(&worker_id)
    }

    pub fn list_pools(&self) -> Vec<&PoolInfo> {
        let mut pools: Vec<_> = self.pools.values().map(|arc| arc.as_ref()).collect();
        pools.sort_by_key(|p| p.pool_type);
        pools
    }

    pub fn add_worker_to_pool(&mut self, pool_type: PoolType, worker_id: u32) -> bool {
        let changed = self
            .worker_to_pools
            .entry(worker_id)
            .or_default()
            .insert(pool_type);
        if changed {
            if let Some(arc) = self.pools.get_mut(&pool_type) {
                Arc::make_mut(arc).workers.insert(worker_id);
            }
        }
        changed
    }

    pub fn set_worker_pools(
        &mut self,
        worker_id: u32,
        target_pools: &HashSet<PoolType>,
    ) -> HashSet<PoolType> {
        let current = self
            .worker_to_pools
            .get(&worker_id)
            .cloned()
            .unwrap_or_default();
        let changed: HashSet<PoolType> = current
            .symmetric_difference(target_pools)
            .copied()
            .collect();
        if changed.is_empty() {
            return changed;
        }

        for pool_type in current
            .difference(target_pools)
            .copied()
            .collect::<Vec<_>>()
        {
            if let Some(arc) = self.pools.get_mut(&pool_type) {
                Arc::make_mut(arc).workers.remove(&worker_id);
            }
        }
        for pool_type in target_pools
            .difference(&current)
            .copied()
            .collect::<Vec<_>>()
        {
            if let Some(arc) = self.pools.get_mut(&pool_type) {
                Arc::make_mut(arc).workers.insert(worker_id);
            }
        }

        if target_pools.is_empty() {
            self.worker_to_pools.remove(&worker_id);
        } else {
            self.worker_to_pools.insert(worker_id, target_pools.clone());
        }
        changed
    }

    pub fn remove_worker(&mut self, worker_id: u32) -> Option<HashSet<PoolType>> {
        let pool_types = self.worker_to_pools.remove(&worker_id)?;
        for pool_type in &pool_types {
            if let Some(arc) = self.pools.get_mut(pool_type) {
                Arc::make_mut(arc).workers.remove(&worker_id);
            }
        }
        Some(pool_types)
    }

    pub fn update_pool_stats(&mut self, pool_type: PoolType, stats: PoolStats) {
        if let Some(arc) = self.pools.get_mut(&pool_type) {
            Arc::make_mut(arc).stats = stats;
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
    fn fixed_pools_exist() {
        let idx = PoolIndex::new();
        assert_eq!(idx.list_pools().len(), 3);
        assert_eq!(idx.get_pool(PoolType::Mem).unwrap().media, StorageType::Mem);
        assert_eq!(idx.get_pool(PoolType::Ssd).unwrap().media, StorageType::Ssd);
        assert_eq!(idx.get_pool(PoolType::Hdd).unwrap().media, StorageType::Hdd);
        assert_eq!(
            idx.get_pool_by_media(StorageType::Ssd).unwrap().pool_type,
            PoolType::Ssd
        );
    }

    #[test]
    fn add_and_remove_worker() {
        let mut idx = PoolIndex::new();
        assert!(idx.add_worker_to_pool(PoolType::Ssd, 100));
        assert!(!idx.add_worker_to_pool(PoolType::Ssd, 100));
        assert!(idx.get_pool(PoolType::Ssd).unwrap().workers.contains(&100));
        assert_eq!(
            idx.get_pools_by_worker(100),
            Some(&HashSet::from([PoolType::Ssd]))
        );
        let removed = idx.remove_worker(100).unwrap();
        assert_eq!(removed, HashSet::from([PoolType::Ssd]));
        assert!(idx.get_pool(PoolType::Ssd).unwrap().workers.is_empty());
    }

    #[test]
    fn update_pool_stats() {
        let mut idx = PoolIndex::new();
        idx.update_pool_stats(
            PoolType::Ssd,
            PoolStats {
                capacity_bytes: 1,
                available_bytes: 2,
                used_bytes: 3,
                block_count: 4,
            },
        );
        let p = idx.get_pool(PoolType::Ssd).unwrap();
        assert_eq!(p.stats.capacity_bytes, 1);
        assert_eq!(p.stats.block_count, 4);
    }
}
