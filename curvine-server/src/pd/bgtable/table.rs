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
use orpc::common::Utils;
use serde::{Deserialize, Serialize};

/// Aggregate stats for a BGTable (sum of its BGs' stats).
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

    pub fn lookup(&self, key: &[u8]) -> Option<BgId> {
        if self.buckets.is_empty() {
            return None;
        }
        let hash = Utils::murmur3(key);
        let index = hash as usize % self.buckets.len();
        self.buckets.get(index).copied()
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

    pub fn hash_cache_replica_policy(&self) -> Option<&CacheReplicaPolicy> {
        match self {
            BGTable::Hash(table) => Some(&table.cache_replica_policy),
            BGTable::Capacity(_) => None,
        }
    }

    pub fn epoch(&self) -> u64 {
        self.base().epoch
    }

    pub fn set_epoch(&mut self, epoch: u64) {
        self.base_mut().epoch = epoch;
        self.base_mut().update_time_ms = orpc::common::LocalTime::mills();
    }

    pub fn stats(&self) -> &BGTableStats {
        &self.base().stats
    }

    pub fn stats_mut(&mut self) -> &mut BGTableStats {
        &mut self.base_mut().stats
    }

    pub fn base(&self) -> &BGTableBase {
        match self {
            BGTable::Hash(t) => &t.base,
            BGTable::Capacity(t) => &t.base,
        }
    }

    pub fn base_mut(&mut self) -> &mut BGTableBase {
        match self {
            BGTable::Hash(t) => &mut t.base,
            BGTable::Capacity(t) => &mut t.base,
        }
    }

    pub fn hash_bucket_count(&self) -> Option<u32> {
        match self {
            BGTable::Hash(t) => Some(t.bucket_count()),
            BGTable::Capacity(_) => None,
        }
    }

    pub fn hash_buckets(&self) -> Option<&[BgId]> {
        match self {
            BGTable::Hash(t) => Some(&t.buckets),
            BGTable::Capacity(_) => None,
        }
    }

    pub fn capacity_active_bgs(&self) -> Option<&[BgId]> {
        match self {
            BGTable::Hash(_) => None,
            BGTable::Capacity(t) => Some(&t.active_bgs),
        }
    }

    pub fn set_hash_buckets(&mut self, buckets: Vec<BgId>) {
        if let BGTable::Hash(t) = self {
            t.buckets = buckets;
            t.base.update_time_ms = orpc::common::LocalTime::mills();
        }
    }

    pub fn new_hash(
        table_id: TableId,
        namespace_id: NamespaceId,
        storage_type: StorageType,
        replica_count: u16,
        buckets: Vec<BgId>,
    ) -> Self {
        Self::new_hash_with_config(
            table_id,
            namespace_id,
            storage_type,
            replica_count,
            buckets,
            vec![],
            CacheReplicaPolicy::default(),
        )
    }

    pub fn new_hash_with_config(
        table_id: TableId,
        namespace_id: NamespaceId,
        storage_type: StorageType,
        replica_count: u16,
        buckets: Vec<BgId>,
        worker_labels: Vec<LabelMatch>,
        cache_replica_policy: CacheReplicaPolicy,
    ) -> Self {
        let now = orpc::common::LocalTime::mills();
        BGTable::Hash(HashBGTable {
            base: BGTableBase {
                table_id,
                namespace_id,
                storage_type,
                replica_count,
                worker_labels,
                epoch: 0,
                create_time_ms: now,
                update_time_ms: now,
                stats: BGTableStats::default(),
            },
            cache_replica_policy,
            buckets,
        })
    }

    pub fn new_hash_with_epoch(
        table_id: TableId,
        namespace_id: NamespaceId,
        storage_type: StorageType,
        replica_count: u16,
        buckets: Vec<BgId>,
        epoch: u64,
    ) -> Self {
        let mut table =
            Self::new_hash(table_id, namespace_id, storage_type, replica_count, buckets);
        table.set_epoch(epoch);
        table
    }

    pub fn new_capacity(
        table_id: TableId,
        namespace_id: NamespaceId,
        storage_type: StorageType,
        replica_count: u16,
        capacity_bg_size: u64,
        min_active_bgs: u32,
    ) -> Self {
        let now = orpc::common::LocalTime::mills();
        BGTable::Capacity(CapacityBGTable {
            base: BGTableBase {
                table_id,
                namespace_id,
                storage_type,
                replica_count,
                worker_labels: vec![],
                epoch: 0,
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
    use curvine_common::state::{BGPrimary, BGState, BlockGroupInfo};

    fn sample_capacity_bg(bg_id: BgId, state: BGState) -> BlockGroupInfo {
        BlockGroupInfo {
            bg_id,
            table_id: 7,
            kind: BGKind::Capacity,
            bg_epoch: 1,
            replica_set: vec![1, 2, 3],
            isr: vec![1, 2, 3],
            state,
            op_state: Default::default(),
            primary: BGPrimary {
                node_id: 1,
                epoch: 1,
                grant_time_ms: 0,
            },
            stats: Default::default(),
            replicas: Default::default(),
        }
    }

    #[test]
    fn hash_lookup_returns_bg_id_at_bucket_index() {
        let table = BGTable::new_hash(1, 0, StorageType::Ssd, 3, vec![10, 20, 30, 40]);
        let BGTable::Hash(hash_table) = table else {
            panic!("expected hash table")
        };
        let bg_id = hash_table.lookup(b"some_key");
        assert!(matches!(bg_id, Some(10 | 20 | 30 | 40)));
    }

    #[test]
    fn capacity_table_has_no_hash_lookup() {
        let table = BGTable::new_capacity(2, 0, StorageType::Ssd, 3, 16 << 30, 4);
        assert!(matches!(table, BGTable::Capacity(_)));
    }

    #[test]
    fn capacity_table_tracks_routable_active_bgs_only() {
        let mut table = BGTable::new_capacity(7, 0, StorageType::Ssd, 3, 16 << 30, 2);
        let mut active_bg = sample_capacity_bg(100, BGState::Active);
        let sealed_bg = sample_capacity_bg(101, BGState::Sealed);

        if let BGTable::Capacity(t) = &mut table {
            crate::pd::bgtable::CapacityBGTableController::bg_created(t, &active_bg);
            crate::pd::bgtable::CapacityBGTableController::bg_created(t, &sealed_bg);
        }
        assert_eq!(table.capacity_active_bgs().unwrap(), &[100]);

        let old = active_bg.clone();
        active_bg.state = BGState::Sealed;
        if let BGTable::Capacity(t) = &mut table {
            crate::pd::bgtable::CapacityBGTableController::bg_updated(t, &old, &active_bg);
        }
        assert!(table.capacity_active_bgs().unwrap().is_empty());

        active_bg.state = BGState::Active;
        if let BGTable::Capacity(t) = &mut table {
            crate::pd::bgtable::CapacityBGTableController::bg_updated(t, &sealed_bg, &active_bg);
        }
        assert_eq!(table.capacity_active_bgs().unwrap(), &[100]);

        if let BGTable::Capacity(t) = &mut table {
            crate::pd::bgtable::CapacityBGTableController::bg_deleted(t, &active_bg);
        }
        assert!(table.capacity_active_bgs().unwrap().is_empty());
    }

    #[test]
    fn capacity_active_bgs_are_runtime_only() {
        let mut table = BGTable::new_capacity(7, 0, StorageType::Ssd, 3, 16 << 30, 2);
        let active_bg = sample_capacity_bg(100, BGState::Active);
        if let BGTable::Capacity(t) = &mut table {
            crate::pd::bgtable::CapacityBGTableController::bg_created(t, &active_bg);
        }
        assert_eq!(table.capacity_active_bgs().unwrap(), &[100]);

        let bytes = curvine_common::utils::SerdeUtils::serialize(&table).unwrap();
        let decoded: BGTable = curvine_common::utils::SerdeUtils::deserialize(&bytes).unwrap();
        assert!(decoded.capacity_active_bgs().unwrap().is_empty());
    }

    #[test]
    fn table_id_returns_base_table_id() {
        let table = BGTable::new_hash(11, 0, StorageType::Ssd, 3, vec![1]);
        assert_eq!(table.table_id(), 11);
    }
}
