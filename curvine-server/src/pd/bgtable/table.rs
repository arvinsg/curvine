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

use curvine_common::state::{
    BGKind, BgId, CacheReplicaPolicy, LabelMatch, NamespaceId, StorageType, TableId,
};
use orpc::common::LocalTime;
use serde::{Deserialize, Serialize};

/// Aggregate stats for a BGTable.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BGTableStats {
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub block_count: u64,
    pub last_report_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BGTableBase {
    pub table_id: TableId,
    pub namespace_id: NamespaceId,
    pub storage_type: StorageType,
    pub replica_count: u16,
    pub worker_labels: Vec<LabelMatch>,
    pub epoch: u64,
    pub create_time_ms: u64,
    pub update_time_ms: u64,
    /// Runtime aggregate; not persisted.
    #[serde(skip)]
    pub stats: BGTableStats,
}

impl BGTableBase {
    pub fn update_stats(&mut self, stats: BGTableStats) {
        self.stats = stats;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashBGTable {
    pub base: BGTableBase,
    pub cache_replica_policy: CacheReplicaPolicy,
    pub buckets: Vec<BgId>,
}

impl HashBGTable {
    pub fn bucket_count(&self) -> u32 {
        self.buckets.len() as u32
    }

    pub fn buckets(&self) -> &[BgId] {
        &self.buckets
    }

    pub fn set_buckets(&mut self, buckets: Vec<BgId>) {
        self.buckets = buckets;
        self.base.update_time_ms = LocalTime::mills();
    }

    pub fn cache_replica_policy(&self) -> &CacheReplicaPolicy {
        &self.cache_replica_policy
    }

    pub fn update_stats(&mut self, stats: BGTableStats) {
        self.base.update_stats(stats);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityBGTable {
    pub base: BGTableBase,
    pub capacity_bg_size: u64,
    pub min_active_bgs: u32,
    /// Runtime-only routable Capacity BG index.
    #[serde(skip)]
    pub active_bgs: Vec<BgId>,
}

impl CapacityBGTable {
    pub fn active_bgs(&self) -> &[BgId] {
        &self.active_bgs
    }

    pub fn update_stats(&mut self, stats: BGTableStats) {
        self.base.update_stats(stats);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BGTable {
    Hash(HashBGTable),
    Capacity(CapacityBGTable),
}

impl BGTable {
    pub fn table_id(&self) -> TableId {
        self.base().table_id
    }

    pub fn namespace_id(&self) -> NamespaceId {
        self.base().namespace_id
    }

    pub fn kind(&self) -> BGKind {
        match self {
            BGTable::Hash(_) => BGKind::Hash,
            BGTable::Capacity(_) => BGKind::Capacity,
        }
    }

    pub fn storage_type(&self) -> StorageType {
        self.base().storage_type
    }

    pub fn replica_count(&self) -> u16 {
        self.base().replica_count
    }

    pub fn worker_labels(&self) -> &[LabelMatch] {
        &self.base().worker_labels
    }

    pub fn epoch(&self) -> u64 {
        self.base().epoch
    }

    pub fn stats(&self) -> &BGTableStats {
        &self.base().stats
    }

    pub fn update_stats(&mut self, stats: BGTableStats) {
        match self {
            BGTable::Hash(table) => table.update_stats(stats),
            BGTable::Capacity(table) => table.update_stats(stats),
        }
    }

    pub fn base(&self) -> &BGTableBase {
        match self {
            BGTable::Hash(t) => &t.base,
            BGTable::Capacity(t) => &t.base,
        }
    }

    pub fn hash_table(&self) -> Option<&HashBGTable> {
        match self {
            BGTable::Hash(table) => Some(table),
            BGTable::Capacity(_) => None,
        }
    }

    pub fn capacity_table(&self) -> Option<&CapacityBGTable> {
        match self {
            BGTable::Hash(_) => None,
            BGTable::Capacity(table) => Some(table),
        }
    }

    pub fn new_hash_table_with_config(
        table_id: TableId,
        namespace_id: NamespaceId,
        storage_type: StorageType,
        replica_count: u16,
        buckets: Vec<BgId>,
        worker_labels: Vec<LabelMatch>,
        cache_replica_policy: CacheReplicaPolicy,
    ) -> Self {
        let now = LocalTime::mills();
        BGTable::Hash(HashBGTable {
            base: BGTableBase {
                table_id,
                namespace_id,
                storage_type,
                replica_count,
                worker_labels,
                epoch: 1,
                create_time_ms: now,
                update_time_ms: now,
                stats: BGTableStats::default(),
            },
            cache_replica_policy,
            buckets,
        })
    }

    pub fn new_capacity_table(
        table_id: TableId,
        namespace_id: NamespaceId,
        storage_type: StorageType,
        replica_count: u16,
        capacity_bg_size: u64,
        min_active_bgs: u32,
    ) -> Self {
        let now = LocalTime::mills();
        BGTable::Capacity(CapacityBGTable {
            base: BGTableBase {
                table_id,
                namespace_id,
                storage_type,
                replica_count,
                worker_labels: vec![],
                epoch: 1,
                create_time_ms: now,
                update_time_ms: now,
                stats: BGTableStats::default(),
            },
            capacity_bg_size,
            min_active_bgs,
            active_bgs: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hash_table_exposes_bucket_metadata_without_routing() {
        let table = BGTable::new_hash_table_with_config(
            1,
            0,
            StorageType::Ssd,
            3,
            vec![10, 20, 30, 40],
            vec![],
            CacheReplicaPolicy::default(),
        );
        let hash_table = table.hash_table().expect("hash table");
        assert_eq!(hash_table.bucket_count(), 4);
        assert_eq!(hash_table.buckets(), &[10, 20, 30, 40]);
    }

    #[test]
    fn capacity_table_is_distinct_from_hash_table() {
        let table = BGTable::new_capacity_table(2, 0, StorageType::Ssd, 3, 16 << 30, 4);
        assert!(table.hash_table().is_none());
        assert!(table.capacity_table().is_some());
    }

    #[test]
    fn capacity_table_tracks_active_bgs_runtime_index() {
        let mut table = BGTable::new_capacity_table(7, 0, StorageType::Ssd, 3, 16 << 30, 2);
        if let BGTable::Capacity(t) = &mut table {
            t.active_bgs.push(100);
        }
        assert_eq!(table.capacity_table().unwrap().active_bgs(), &[100]);
    }

    #[test]
    fn capacity_active_bgs_are_runtime_only() {
        let mut table = BGTable::new_capacity_table(7, 0, StorageType::Ssd, 3, 16 << 30, 2);
        if let BGTable::Capacity(t) = &mut table {
            t.active_bgs.push(100);
        }
        assert_eq!(table.capacity_table().unwrap().active_bgs(), &[100]);

        let bytes = curvine_common::utils::SerdeUtils::serialize(&table).unwrap();
        let decoded: BGTable = curvine_common::utils::SerdeUtils::deserialize(&bytes).unwrap();
        assert!(decoded.capacity_table().unwrap().active_bgs().is_empty());
    }

    #[test]
    fn table_id_returns_base_table_id() {
        let table = BGTable::new_hash_table_with_config(
            11,
            0,
            StorageType::Ssd,
            3,
            vec![1],
            vec![],
            CacheReplicaPolicy::default(),
        );
        assert_eq!(table.table_id(), 11);
    }
}
