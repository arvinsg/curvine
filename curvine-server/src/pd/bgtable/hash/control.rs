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

use crate::pd::bg::BGManager;
use crate::pd::bgtable::hash::HashPlacement;
use crate::pd::bgtable::table::TableRegistry;
use crate::pd::bgtable::{
    BGTable, BGTableControl, BGTableStats, BGTableStore, HashBGTable, PreparedTables,
};
use crate::pd::config::ConfigManager;
use crate::pd::journal::entry::{BGDeleteEntry, BGEntry};
use crate::pd::journal::ApplyOutcome;
use crate::pd::pool::PoolManager;
use curvine_common::state::{BgId, BlockGroupInfo, CacheReplicaPolicy, NamespaceId, TableId};
use curvine_common::{FsError, FsResult};
use std::collections::HashMap;
use std::sync::Arc;

/// Total manager of Hash BGTables: owns the table registry and the hash-domain
/// dependencies (store, BG manager, pool/config) handed to the placement service.
pub struct HashBGTableControl {
    tables: TableRegistry<HashBGTable>,
    store: Arc<BGTableStore>,
    pub(crate) bg_manager: Arc<BGManager>,
    pub(crate) pool_manager: Arc<PoolManager>,
    pub(crate) config_manager: Arc<ConfigManager>,
    pub(crate) location_labels: Vec<String>,
}

impl HashBGTableControl {
    pub fn new(
        store: Arc<BGTableStore>,
        bg_manager: Arc<BGManager>,
        pool_manager: Arc<PoolManager>,
        config_manager: Arc<ConfigManager>,
        location_labels: Vec<String>,
    ) -> Self {
        Self {
            tables: TableRegistry::default(),
            store,
            bg_manager,
            pool_manager,
            config_manager,
            location_labels,
        }
    }

    pub fn placement(&self) -> HashPlacement<'_> {
        HashPlacement::new(self)
    }

    pub fn get_hash_table(&self, table_id: TableId) -> Option<Arc<HashBGTable>> {
        self.tables.get(table_id)
    }

    pub fn put_table(&self, table: HashBGTable) {
        self.tables.put(table);
    }

    pub fn restore_tables(&self, tables: HashMap<TableId, Arc<HashBGTable>>) {
        self.tables.replace_all(tables);
    }
}

impl BGTableControl for HashBGTableControl {
    fn store(&self) -> &BGTableStore {
        &self.store
    }

    fn bg_manager(&self) -> &BGManager {
        &self.bg_manager
    }

    // Hash tables are created whole (buckets fixed at namespace-create) and are
    // never grown/shrunk one BG at a time, so single-BG create/delete is not a
    // supported path. Reject rather than silently mutate the fixed layout.
    fn apply_create_bg(&self, entry: &BGEntry) -> FsResult<ApplyOutcome> {
        Err(FsError::common(format!(
            "hash bg {} single-create is not supported",
            entry.info.bg_id
        )))
    }

    fn apply_delete_bg(&self, entry: &BGDeleteEntry) -> FsResult<ApplyOutcome> {
        Err(FsError::common(format!(
            "hash bg {} single-delete is not supported",
            entry.bg_id
        )))
    }

    fn get_table(&self, table_id: TableId) -> Option<Arc<BGTable>> {
        self.get_hash_table(table_id)
            .map(|table| Arc::new(BGTable::Hash((*table).clone())))
    }

    fn list_tables(&self) -> Vec<Arc<BGTable>> {
        self.tables
            .values()
            .into_iter()
            .map(|table| Arc::new(BGTable::Hash((*table).clone())))
            .collect()
    }

    fn snapshot_tables(&self) -> HashMap<TableId, Arc<BGTable>> {
        self.tables
            .snapshot()
            .into_iter()
            .map(|(id, table)| (id, Arc::new(BGTable::Hash((*table).clone()))))
            .collect()
    }

    fn table_epochs(&self) -> HashMap<TableId, u64> {
        self.tables.epochs()
    }

    fn apply_create_table(&self, table: BGTable) {
        if let BGTable::Hash(table) = table {
            self.tables.put(table);
        }
    }

    fn apply_update_table(&self, table: BGTable) {
        if let BGTable::Hash(table) = table {
            self.tables.put(table);
        }
    }

    fn apply_delete_table(&self, table_id: TableId) {
        self.tables.remove(table_id);
    }

    // Hash tables have fixed buckets: a BG lifecycle event never changes their
    // routing index, so refreshing it is a no-op.
    fn refresh_bg_index(&self, _table: BGTable) {}

    fn update_table_stats(&self, table_id: TableId, stats: BGTableStats) {
        self.tables.update_stats(table_id, stats);
    }

    // Hash tables have fixed buckets: BG lifecycle events do not change their
    // routing index, so this is a no-op.
    fn rebuild_indexes(&self, _table: &mut BGTable, _bgs: &HashMap<BgId, Arc<BlockGroupInfo>>) {}

    /// Rewrite `cache_replica_policy` on every Hash table of a namespace,
    /// bumping each changed table's epoch. Policy affects client behaviour, so
    /// it is a client-visible table update. A table already carrying `policy`
    /// is skipped, so a replay plans no writes.
    fn prepare_update_table(
        &self,
        namespace_id: NamespaceId,
        policy: &CacheReplicaPolicy,
    ) -> FsResult<PreparedTables> {
        let mut tables: Vec<BGTable> = Vec::new();
        let mut ops = Vec::new();
        for table in self.tables.values() {
            if table.base.namespace_id != namespace_id || table.cache_replica_policy == *policy {
                continue;
            }
            let mut updated = (*table).clone();
            updated.cache_replica_policy = policy.clone();
            let mut updated = BGTable::Hash(updated);
            updated.bump_epoch();
            ops.push(self.store.table_put_op(&updated)?);
            tables.push(updated);
        }
        Ok(PreparedTables::table_only(ops, tables))
    }
}
