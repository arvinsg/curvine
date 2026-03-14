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

use curvine_common::state::{PoolInfo, PoolStats, StorageType};
use std::collections::{HashMap, HashSet};

/// In-memory index for pools and worker->pools mapping.
pub struct PoolIndex {
    pools: HashMap<u16, PoolInfo>,
    by_media: HashMap<StorageType, u16>,
    worker_to_pools: HashMap<u32, HashSet<u16>>,
}

impl PoolIndex {
    pub fn new() -> Self {
        Self {
            pools: HashMap::new(),
            by_media: HashMap::new(),
            worker_to_pools: HashMap::new(),
        }
    }

    /// Clear all index state (for restore).
    pub fn clear(&mut self) {
        self.pools.clear();
        self.by_media.clear();
        self.worker_to_pools.clear();
    }

    /// Insert or replace pool; syncs worker_to_pools from info.workers so restore
    /// can rebuild the full index from store's list_pools() only.
    pub fn insert_pool(&mut self, info: PoolInfo) {
        let pool_id = info.pool_id;
        if let Some(old) = self.pools.get(&pool_id) {
            for w in &old.workers {
                if let Some(s) = self.worker_to_pools.get_mut(w) {
                    s.remove(&pool_id);
                    if s.is_empty() {
                        self.worker_to_pools.remove(w);
                    }
                }
            }
        }
        for &w in &info.workers {
            self.worker_to_pools.entry(w).or_default().insert(pool_id);
        }
        self.by_media.insert(info.media, pool_id);
        self.pools.insert(pool_id, info);
    }

    pub fn get_pool(&self, pool_id: u16) -> Option<&PoolInfo> {
        self.pools.get(&pool_id)
    }

    pub fn get_pool_mut(&mut self, pool_id: u16) -> Option<&mut PoolInfo> {
        self.pools.get_mut(&pool_id)
    }

    pub fn get_pool_by_media(&self, media: StorageType) -> Option<&PoolInfo> {
        self.by_media.get(&media).and_then(|&id| self.pools.get(&id))
    }

    pub fn get_pools_by_worker(&self, worker_id: u32) -> Option<&HashSet<u16>> {
        self.worker_to_pools.get(&worker_id)
    }

    pub fn list_pools(&self) -> Vec<&PoolInfo> {
        self.pools.values().collect()
    }

    /// Add worker to pool; updates pool.workers and worker_to_pools.
    pub fn add_worker_to_pool(&mut self, pool_id: u16, worker_id: u32) {
        if let Some(pool) = self.pools.get_mut(&pool_id) {
            pool.workers.insert(worker_id);
        }
        self.worker_to_pools
            .entry(worker_id)
            .or_default()
            .insert(pool_id);
    }

    /// Remove worker from all pools (used when worker is removed).
    pub fn remove_worker(&mut self, worker_id: u32) -> Option<HashSet<u16>> {
        let pool_ids = self.worker_to_pools.remove(&worker_id)?;
        for pool_id in &pool_ids {
            if let Some(pool) = self.pools.get_mut(pool_id) {
                pool.workers.remove(&worker_id);
            }
        }
        Some(pool_ids)
    }

    pub fn worker_has_storage(&self, worker_id: u32, storage_type: StorageType) -> bool {
        self.worker_to_pools
            .get(&worker_id)
            .and_then(|pool_ids| {
                pool_ids.iter().find_map(|&pid| self.pools.get(&pid)).map(|p| p.media == storage_type)
            })
            .unwrap_or(false)
    }

    pub fn update_pool_stats(&mut self, pool_id: u16, stats: PoolStats) {
        if let Some(pool) = self.pools.get_mut(&pool_id) {
            pool.stats = stats;
        }
    }

    pub fn all_pool_ids(&self) -> Vec<u16> {
        self.pools.keys().copied().collect()
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
    use std::collections::HashSet;

    fn pool_info(pool_id: u16, name: &str, media: StorageType) -> PoolInfo {
        PoolInfo::new(pool_id, name.to_string(), media)
    }

    #[test]
    fn new_is_empty() {
        let idx = PoolIndex::new();
        assert!(idx.list_pools().is_empty());
        assert!(idx.get_pool(1).is_none());
        assert!(idx.get_pool_by_media(StorageType::Ssd).is_none());
    }

    #[test]
    fn insert_pool_and_get() {
        let mut idx = PoolIndex::new();
        let p = pool_info(2, "ssd_pool", StorageType::Ssd);
        idx.insert_pool(p);
        assert_eq!(idx.list_pools().len(), 1);
        let got = idx.get_pool(2).unwrap();
        assert_eq!(got.pool_id, 2);
        assert_eq!(got.name, "ssd_pool");
        assert_eq!(got.media, StorageType::Ssd);
        assert_eq!(idx.get_pool_by_media(StorageType::Ssd).unwrap().pool_id, 2);
    }

    #[test]
    fn add_worker_to_pool() {
        let mut idx = PoolIndex::new();
        idx.insert_pool(pool_info(2, "ssd", StorageType::Ssd));
        idx.add_worker_to_pool(2, 100);
        idx.add_worker_to_pool(2, 101);
        let pool = idx.get_pool(2).unwrap();
        assert!(pool.workers.contains(&100));
        assert!(pool.workers.contains(&101));
        assert_eq!(idx.get_pools_by_worker(100), Some(&HashSet::from([2])));
    }

    #[test]
    fn remove_worker_from_all_pools() {
        let mut idx = PoolIndex::new();
        idx.insert_pool(pool_info(1, "mem", StorageType::Mem));
        idx.insert_pool(pool_info(2, "ssd", StorageType::Ssd));
        idx.add_worker_to_pool(1, 10);
        idx.add_worker_to_pool(2, 10);
        let removed = idx.remove_worker(10).unwrap();
        assert!(removed.contains(&1));
        assert!(removed.contains(&2));
        assert!(idx.get_pool(1).unwrap().workers.is_empty());
        assert!(idx.get_pool(2).unwrap().workers.is_empty());
        assert!(idx.get_pools_by_worker(10).is_none());
    }

    #[test]
    fn remove_worker_returns_none_when_unknown() {
        let mut idx = PoolIndex::new();
        idx.insert_pool(pool_info(2, "ssd", StorageType::Ssd));
        assert!(idx.remove_worker(999).is_none());
    }

    #[test]
    fn worker_has_storage() {
        let mut idx = PoolIndex::new();
        idx.insert_pool(pool_info(2, "ssd", StorageType::Ssd));
        idx.add_worker_to_pool(2, 5);
        assert!(idx.worker_has_storage(5, StorageType::Ssd));
        assert!(!idx.worker_has_storage(5, StorageType::Hdd));
        assert!(!idx.worker_has_storage(99, StorageType::Ssd));
    }

    #[test]
    fn update_pool_stats() {
        use curvine_common::state::PoolStats;
        let mut idx = PoolIndex::new();
        idx.insert_pool(pool_info(2, "ssd", StorageType::Ssd));
        idx.update_pool_stats(2, PoolStats { capacity_bytes: 1000, available_bytes: 500, used_bytes: 500 });
        let p = idx.get_pool(2).unwrap();
        assert_eq!(p.stats.capacity_bytes, 1000);
        assert_eq!(p.stats.available_bytes, 500);
    }

    #[test]
    fn all_pool_ids() {
        let mut idx = PoolIndex::new();
        idx.insert_pool(pool_info(1, "mem", StorageType::Mem));
        idx.insert_pool(pool_info(2, "ssd", StorageType::Ssd));
        let ids = idx.all_pool_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
    }
}
