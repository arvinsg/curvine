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

use crate::pd::bg::{BGManager, PrepareUpdateResult};
use crate::pd::bgtable::hash::HashPlacement;
use crate::pd::bgtable::table::TableRegistry;
use crate::pd::bgtable::{
    BGTable, BGTableControl, BGTableStats, BGTableStore, HashBGTable, PreparedTables,
};
use crate::pd::config::ConfigManager;
use crate::pd::journal::entry::{BGBatchUpdateEntry, BGDeleteEntry, BGEntry, BGUpdateEntry};
use crate::pd::journal::ApplyOutcome;
use crate::pd::pool::PoolManager;
use curvine_common::state::{BgId, BlockGroupInfo, CacheReplicaPolicy, NamespaceId, TableId};
use curvine_common::{FsError, FsResult};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Reject a batch that names the same BG twice (a rebuild plan must not update
/// one BG in two ways within a single atomic apply).
fn validate_unique_update_ids(updates: &[BGUpdateEntry]) -> FsResult<()> {
    let mut ids = HashSet::with_capacity(updates.len());
    for update in updates {
        if !ids.insert(update.bg_id) {
            return Err(FsError::common(format!(
                "duplicate bg {} in batch update",
                update.bg_id
            )));
        }
    }
    Ok(())
}

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
    // never grown/shrunk one BG at a time.
    fn propose_create_bg(&self, info: BlockGroupInfo) -> FsResult<ApplyOutcome> {
        Err(FsError::common(format!(
            "hash bg {} single-create is not supported",
            info.bg_id
        )))
    }

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

    fn on_table_created(&self, table: BGTable) {
        if let BGTable::Hash(table) = table {
            self.tables.put(table);
        }
    }

    fn on_table_updated(&self, table: BGTable) {
        if let BGTable::Hash(table) = table {
            self.tables.put(table);
        }
    }

    fn on_table_removed(&self, table_id: TableId) {
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

    /// Apply a batched BG update (Hash rebuild). Each update's `bump_table_epoch`
    /// was stamped at propose time; here we persist the bumped tables' rows
    /// atomically with every BG op, then refresh in-memory state. A pure
    /// replica_set shuffle refreshes memory without bumping — same rule as the
    /// single-BG path, applied per BG.
    fn apply_batch_update(&self, entry: &BGBatchUpdateEntry) -> FsResult<ApplyOutcome> {
        validate_unique_update_ids(&entry.updates)?;
        let mut planned: HashMap<TableId, BGTable> = self
            .tables
            .snapshot()
            .into_iter()
            .map(|(id, t)| (id, BGTable::Hash((*t).clone())))
            .collect();
        let mut update_plans = Vec::with_capacity(entry.updates.len());
        let mut affected_tables = HashSet::new();
        let mut bump_tables = HashSet::new();

        for update in &entry.updates {
            let plan = match self.bg_manager.prepare_update_bg(update)? {
                PrepareUpdateResult::Applied(plan) => plan,
                PrepareUpdateResult::Outcome(outcome) => return Ok(outcome),
            };
            let table = planned.get_mut(&plan.new_info.table_id).ok_or_else(|| {
                FsError::common(format!("table {} not found", plan.new_info.table_id))
            })?;
            table.matches_bg(&plan.new_info)?;
            if update.bump_table_epoch {
                bump_tables.insert(plan.new_info.table_id);
            }
            table.on_bg_updated(&plan.old_info, &plan.new_info);
            affected_tables.insert(plan.new_info.table_id);
            update_plans.push(plan);
        }

        // Persist only the bumped tables (their row now carries the new epoch),
        // atomically with every BG op in one batch.
        let mut ops = Vec::new();
        for table_id in &bump_tables {
            if let Some(table) = planned.get_mut(table_id) {
                table.bump_epoch();
                ops.push(self.store.table_put_op(table)?);
            }
        }
        ops.extend(update_plans.iter().map(|plan| plan.op.clone()));
        self.store.write_batch(ops)?;

        // Refresh in-memory state for every affected table (bumped or not).
        for table_id in affected_tables {
            if let Some(BGTable::Hash(table)) = planned.remove(&table_id) {
                self.tables.put(table);
            }
        }
        for plan in update_plans {
            self.bg_manager
                .update_bg(&plan.old_info, plan.new_info.clone());
            self.bg_manager
                .cleanup_isr_penalties(&plan.old_info, &plan.new_info);
        }
        Ok(ApplyOutcome::Applied)
    }

    /// Rewrite `cache_replica_policy` on every Hash table of a namespace,
    /// bumping each changed table's epoch. Policy affects client behaviour, so
    /// it is a client-visible table update. A table already carrying `policy`
    /// is skipped, so a replay plans no writes.
    fn prepare_policy_update(
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
