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

use crate::pd::journal::entry::BGUpdateEntry;
use curvine_common::state::{
    BGKind, BGState, BgId, BlockGroupInfo, CacheReplicaPolicy, LabelMatch, NamespaceId,
    StorageType, TableId,
};
use curvine_common::{FsError, FsResult};
use orpc::common::LocalTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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

    pub fn on_bg_created(&mut self, bg: &BlockGroupInfo) {
        if Self::is_writeable(bg) && !self.active_bgs.contains(&bg.bg_id) {
            self.active_bgs.push(bg.bg_id);
        }
    }

    pub fn on_bg_updated(&mut self, old: &BlockGroupInfo, new: &BlockGroupInfo) {
        if Self::is_writeable(old) {
            self.active_bgs.retain(|id| *id != old.bg_id);
        }
        self.on_bg_created(new);
    }

    pub fn on_bg_deleted(&mut self, bg: &BlockGroupInfo) {
        if Self::is_writeable(bg) {
            self.active_bgs.retain(|id| *id != bg.bg_id);
        }
    }

    pub fn rebuild_active_index(&mut self, bgs: &HashMap<BgId, Arc<BlockGroupInfo>>) {
        self.active_bgs.clear();
        for bg in bgs.values() {
            if bg.table_id == self.base.table_id {
                self.on_bg_created(bg);
            }
        }
    }

    pub(crate) fn is_writeable(bg: &BlockGroupInfo) -> bool {
        bg.kind == BGKind::Capacity && bg.state == BGState::Active
    }
}

impl TableEntry for HashBGTable {
    fn table_id(&self) -> TableId {
        self.base.table_id
    }
    fn epoch(&self) -> u64 {
        self.base.epoch
    }
    fn update_stats(&mut self, stats: BGTableStats) {
        self.base.update_stats(stats);
    }
}

impl TableEntry for CapacityBGTable {
    fn table_id(&self) -> TableId {
        self.base.table_id
    }
    fn epoch(&self) -> u64 {
        self.base.epoch
    }
    fn update_stats(&mut self, stats: BGTableStats) {
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

    pub fn on_bg_created(&mut self, bg: &BlockGroupInfo) {
        if let BGTable::Capacity(table) = self {
            table.on_bg_created(bg);
        }
    }

    pub fn on_bg_updated(&mut self, old: &BlockGroupInfo, new: &BlockGroupInfo) {
        if let BGTable::Capacity(table) = self {
            table.on_bg_updated(old, new);
        }
    }

    pub fn on_bg_deleted(&mut self, bg: &BlockGroupInfo) {
        if let BGTable::Capacity(table) = self {
            table.on_bg_deleted(bg);
        }
    }

    pub fn bg_change_bumps_table_epoch(&self, old: &BlockGroupInfo, entry: &BGUpdateEntry) -> bool {
        match self {
            BGTable::Hash(_) => {
                let isr_changed = entry.isr.as_ref().is_some_and(|isr| *isr != old.isr);
                let primary_changed = entry.primary.as_ref().is_some_and(|p| *p != old.primary);
                isr_changed || primary_changed
            }
            BGTable::Capacity(_) => false,
        }
    }

    pub fn rebuild_active_index(&mut self, bgs: &HashMap<BgId, Arc<BlockGroupInfo>>) {
        if let BGTable::Capacity(table) = self {
            table.rebuild_active_index(bgs);
        }
    }

    pub fn bump_epoch(&mut self) {
        let base = match self {
            BGTable::Hash(t) => &mut t.base,
            BGTable::Capacity(t) => &mut t.base,
        };
        base.epoch = base.epoch.saturating_add(1);
        base.update_time_ms = LocalTime::mills();
    }

    pub fn matches_bg(&self, bg: &BlockGroupInfo) -> FsResult<()> {
        if bg.table_id != self.table_id() {
            return Err(FsError::common(format!(
                "bg {} table mismatch: bg.table_id={}, table_id={}",
                bg.bg_id,
                bg.table_id,
                self.table_id()
            )));
        }
        if bg.kind != self.kind() {
            return Err(FsError::common(format!(
                "bg {} kind mismatch: bg.kind={:?}, table.kind={:?}",
                bg.bg_id,
                bg.kind,
                self.kind()
            )));
        }
        Ok(())
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

pub trait TableEntry {
    fn table_id(&self) -> TableId;
    fn epoch(&self) -> u64;
    fn update_stats(&mut self, stats: BGTableStats);
}

pub struct TableRegistry<T> {
    tables: RwLock<HashMap<TableId, Arc<T>>>,
}

impl<T> Default for TableRegistry<T> {
    fn default() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }
}

impl<T: TableEntry + Clone> TableRegistry<T> {
    pub fn get(&self, table_id: TableId) -> Option<Arc<T>> {
        self.tables.read().unwrap().get(&table_id).cloned()
    }

    /// All tables as owned `Arc`s, in unspecified order.
    pub fn values(&self) -> Vec<Arc<T>> {
        self.tables.read().unwrap().values().cloned().collect()
    }

    pub fn snapshot(&self) -> HashMap<TableId, Arc<T>> {
        self.tables.read().unwrap().clone()
    }

    pub fn epochs(&self) -> HashMap<TableId, u64> {
        self.tables
            .read()
            .unwrap()
            .iter()
            .map(|(&id, table)| (id, table.epoch()))
            .collect()
    }

    pub fn replace_all(&self, tables: HashMap<TableId, Arc<T>>) {
        *self.tables.write().unwrap() = tables;
    }

    pub fn put(&self, table: T) {
        self.tables
            .write()
            .unwrap()
            .insert(table.table_id(), Arc::new(table));
    }

    pub fn remove(&self, table_id: TableId) {
        self.tables.write().unwrap().remove(&table_id);
    }

    pub fn update_stats(&self, table_id: TableId, stats: BGTableStats) {
        let mut tables = self.tables.write().unwrap();
        if let Some(table) = tables.get_mut(&table_id) {
            Arc::make_mut(table).update_stats(stats);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hash_table_exposes_buckets() {
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
    fn capacity_distinct_from_hash() {
        let table = BGTable::new_capacity_table(2, 0, StorageType::Ssd, 3, 16 << 30, 4);
        assert!(table.hash_table().is_none());
        assert!(table.capacity_table().is_some());
    }

    #[test]
    fn capacity_tracks_active_bgs() {
        let mut table = BGTable::new_capacity_table(7, 0, StorageType::Ssd, 3, 16 << 30, 2);
        if let BGTable::Capacity(t) = &mut table {
            t.active_bgs.push(100);
        }
        assert_eq!(table.capacity_table().unwrap().active_bgs(), &[100]);
    }

    #[test]
    fn capacity_on_bg_events() {
        use curvine_common::state::{BGPrimary, BGState};

        fn sample_bg(bg_id: BgId, state: BGState) -> BlockGroupInfo {
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

        let mut table = match BGTable::new_capacity_table(7, 0, StorageType::Ssd, 3, 16 << 30, 2) {
            BGTable::Capacity(table) => table,
            BGTable::Hash(_) => panic!("expected capacity table"),
        };
        let mut active_bg = sample_bg(100, BGState::Active);
        let sealed_bg = sample_bg(101, BGState::Sealed);

        table.on_bg_created(&active_bg);
        table.on_bg_created(&sealed_bg); // sealed is not writable -> ignored
        assert_eq!(table.active_bgs(), &[100]);

        let old = active_bg.clone();
        active_bg.state = BGState::Sealed;
        table.on_bg_updated(&old, &active_bg);
        assert!(table.active_bgs().is_empty());

        active_bg.state = BGState::Active;
        table.on_bg_updated(&sealed_bg, &active_bg);
        assert_eq!(table.active_bgs(), &[100]);

        table.on_bg_deleted(&active_bg);
        assert!(table.active_bgs().is_empty());
    }

    #[test]
    fn capacity_active_bgs_not_persisted() {
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
    fn table_id_reads_base() {
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
