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

mod apply;
mod restore;
mod route;

use crate::pd::bg::BGManager;
use crate::pd::bgtable::{
    BGTable, BGTableControl, BGTableStats, BGTableStore, CapacityBGTable, CapacityBGTableControl,
    HashBGTable, HashBGTableControl, HashPlacement, PreparedTables,
};
use crate::pd::config::ConfigManager;
use crate::pd::journal::entry::BGBatchUpdateEntry;
use crate::pd::journal::ApplyOutcome;
use crate::pd::pool::PoolManager;
use crate::pd::store::KvWrite;
use curvine_common::state::{
    BGKind, BgId, BlockGroupInfo, CacheReplicaPolicy, NamespaceId, TableId, WorkerBGReport,
};
use curvine_common::FsResult;
use orpc::CommonResult;
use std::collections::HashMap;
use std::sync::Arc;

/// Top-level BGTable metadata entry.
///
/// Hash and Capacity BGTable have different management semantics, so typed
/// controllers own their runtime table indexes. The manager remains the single
/// upper-layer entry point and coordinates atomic BG + BGTable journal apply.
pub struct BGTableManager {
    pub(crate) hash: HashBGTableControl,
    pub(crate) capacity: CapacityBGTableControl,
    pub(crate) store: Arc<BGTableStore>,
    pub(crate) bg_manager: Arc<BGManager>,
    pub(crate) pool_manager: Arc<PoolManager>,
}

impl BGTableManager {
    pub fn new(
        store: Arc<BGTableStore>,
        bg_manager: Arc<BGManager>,
        pool_manager: Arc<PoolManager>,
        config_manager: Arc<ConfigManager>,
        location_labels: Vec<String>,
    ) -> Self {
        let hash = HashBGTableControl::new(
            store.clone(),
            bg_manager.clone(),
            pool_manager.clone(),
            config_manager,
            location_labels,
        );
        let capacity = CapacityBGTableControl::new(store.clone(), bg_manager.clone());
        Self {
            hash,
            capacity,
            store,
            bg_manager,
            pool_manager,
        }
    }

    /// The control for a specific BG kind, as the shared `BGTableControl` facade.
    fn control(&self, kind: BGKind) -> &dyn BGTableControl {
        match kind {
            BGKind::Hash => &self.hash,
            BGKind::Capacity => &self.capacity,
        }
    }

    /// Every control, for kind-agnostic aggregation.
    fn controls(&self) -> [&dyn BGTableControl; 2] {
        [&self.hash, &self.capacity]
    }

    pub fn get_table(&self, table_id: TableId) -> Option<Arc<BGTable>> {
        self.controls().iter().find_map(|c| c.get_table(table_id))
    }

    pub fn list_tables(&self) -> Vec<Arc<BGTable>> {
        self.controls()
            .iter()
            .flat_map(|c| c.list_tables())
            .collect()
    }

    pub fn get_table_epochs(&self) -> HashMap<TableId, u64> {
        self.controls()
            .iter()
            .flat_map(|c| c.table_epochs())
            .collect()
    }

    /// Reserve a contiguous BG id range. Forwarded to the BG manager so upper
    /// layers (namespace create) go through BGTable rather than reaching into
    /// the BG manager directly.
    pub fn alloc_bg_ids(&self, count: u64) -> FsResult<BgId> {
        self.bg_manager.alloc_bg_ids(count)
    }

    /// Placement/rebuild service for Hash BGTables. Callers reach the hash
    /// placement algorithms through this single entry rather than a set of
    /// flat forwarding methods on the manager.
    pub fn hash_placement(&self) -> HashPlacement<'_> {
        self.hash.placement()
    }

    /// Capacity control entry, for capacity-specific operations (active-BG
    /// lifecycle in phase-2). Kind-agnostic work goes through `control(kind)`.
    pub fn capacity(&self) -> &CapacityBGTableControl {
        &self.capacity
    }

    /// Read-only / runtime-state access to the underlying BG metadata layer.
    pub fn bg(&self) -> &BGManager {
        &self.bg_manager
    }

    // ---- BG raft-proposing operations (single entry point) -----------------

    pub fn propose_add_replica(
        &self,
        kind: BGKind,
        bg_id: BgId,
        worker_id: u32,
    ) -> FsResult<ApplyOutcome> {
        self.control(kind).propose_update_bg(
            self.bg_manager
                .build_add_replica_entry(kind, bg_id, worker_id),
        )
    }

    pub fn propose_remove_replica(
        &self,
        kind: BGKind,
        bg_id: BgId,
        worker_id: u32,
    ) -> FsResult<ApplyOutcome> {
        self.control(kind).propose_update_bg(
            self.bg_manager
                .build_remove_replica_entry(kind, bg_id, worker_id),
        )
    }

    pub fn propose_transfer_primary(
        &self,
        kind: BGKind,
        bg_id: BgId,
        from_worker: u32,
        to_worker: u32,
    ) -> FsResult<ApplyOutcome> {
        self.control(kind)
            .propose_update_bg(self.bg_manager.build_transfer_primary_entry(
                kind,
                bg_id,
                from_worker,
                to_worker,
            ))
    }

    pub fn propose_seal_bg(&self, kind: BGKind, bg_id: BgId) -> FsResult<ApplyOutcome> {
        self.control(kind)
            .propose_update_bg(self.bg_manager.build_seal_entry(kind, bg_id))
    }

    pub fn propose_delete_bg(&self, kind: BGKind, bg_id: BgId) -> FsResult<ApplyOutcome> {
        self.control(kind)
            .propose_delete_bg(self.bg_manager.build_delete_entry(kind, bg_id))
    }

    pub fn propose_batch_update_bg(&self, entry: BGBatchUpdateEntry) -> FsResult<ApplyOutcome> {
        self.bg_manager.propose_batch_update_bg(entry)
    }

    /// Reconcile a worker's heartbeat BG reports into Hash BG ISR/state.
    /// Runtime-state maintenance owned by the Hash control.
    pub fn reconcile_replicas(&self, reporter: u32, reports: &[WorkerBGReport]) -> FsResult<()> {
        self.hash.reconcile_replicas(reporter, reports)
    }

    /// Plan rewriting `cache_replica_policy` on a namespace's Hash tables.
    pub fn plan_namespace_policy_update(
        &self,
        namespace_id: NamespaceId,
        policy: &CacheReplicaPolicy,
    ) -> FsResult<PreparedTables> {
        self.hash.prepare_policy_update(namespace_id, policy)
    }

    pub fn commit_namespace_policy_update(&self, plan: PreparedTables) {
        self.hash.commit_update_table(plan);
    }

    pub fn table_put_op(&self, table: &BGTable) -> CommonResult<KvWrite> {
        self.store.table_put_op(table)
    }

    pub fn put_table(&self, table: BGTable) {
        match table {
            BGTable::Hash(table) => {
                self.capacity.on_table_removed(table.base.table_id);
                self.hash.put_table(table);
            }
            BGTable::Capacity(table) => {
                self.hash.on_table_removed(table.base.table_id);
                self.capacity.put_table(table);
            }
        }
    }

    pub fn snapshot_tables(&self) -> HashMap<TableId, Arc<BGTable>> {
        self.controls()
            .iter()
            .flat_map(|c| c.snapshot_tables())
            .collect()
    }

    pub(crate) fn update_table_stats(&self, table_id: TableId, stats: BGTableStats) {
        // Each control's update is a no-op when it does not hold the table, so
        // offer the update to every control (consistent with the aggregating
        // reads above; no ownership probe needed).
        for control in self.controls() {
            control.update_table_stats(table_id, stats.clone());
        }
    }

    // ---- table stats: aggregate per-BG stats up to each table --------------

    pub fn get_table_stats(&self, table_id: TableId) -> BGTableStats {
        self.get_table(table_id)
            .map(|table| table.stats().clone())
            .unwrap_or_default()
    }

    pub fn refresh_table_stats(&self) {
        let bgs = self.bg_manager.snapshot_all_bgs();
        self.refresh_stats(&bgs);
    }

    pub fn refresh_stats(&self, bgs: &HashMap<BgId, Arc<BlockGroupInfo>>) {
        for (table_id, stats) in self.aggregate_table_stats(bgs) {
            self.update_table_stats(table_id, stats);
        }
    }

    /// Sum each BG's stats into its owning table's aggregate. Tables with no BGs
    /// keep default (zero) stats.
    fn aggregate_table_stats(
        &self,
        bgs: &HashMap<BgId, Arc<BlockGroupInfo>>,
    ) -> HashMap<TableId, BGTableStats> {
        let mut aggregates: HashMap<TableId, BGTableStats> = self
            .list_tables()
            .into_iter()
            .map(|table| (table.table_id(), BGTableStats::default()))
            .collect();

        for bg in bgs.values() {
            let Some(stats) = aggregates.get_mut(&bg.table_id) else {
                continue;
            };
            stats.used_bytes += bg.stats.used_bytes;
            stats.free_bytes += bg.stats.free_bytes;
            stats.block_count += bg.stats.block_count;
            stats.last_report_ms = stats.last_report_ms.max(bg.stats.last_report_ms);
        }
        aggregates
    }

    pub(crate) fn rebuild_table_indexes(
        &self,
        table: &mut BGTable,
        bgs: &HashMap<BgId, Arc<BlockGroupInfo>>,
    ) {
        self.control(table.kind()).rebuild_indexes(table, bgs);
    }

    pub(crate) fn split_tables(
        tables: impl IntoIterator<Item = BGTable>,
    ) -> (
        HashMap<TableId, Arc<HashBGTable>>,
        HashMap<TableId, Arc<CapacityBGTable>>,
    ) {
        let mut hash_tables = HashMap::new();
        let mut capacity_tables = HashMap::new();
        for table in tables {
            match table {
                BGTable::Hash(table) => {
                    hash_tables.insert(table.base.table_id, Arc::new(table));
                }
                BGTable::Capacity(table) => {
                    capacity_tables.insert(table.base.table_id, Arc::new(table));
                }
            }
        }
        (hash_tables, capacity_tables)
    }

    #[cfg(test)]
    pub fn test_insert_table(&self, table: BGTable) {
        self.put_table(table);
    }

    /// Test-only: plant a pre-existing BG (as if created at namespace-create),
    /// bypassing the production single-BG create path (which rejects Hash). Used
    /// by checker/scheduler fixtures that set up a populated table before
    /// exercising reconcile logic.
    #[cfg(test)]
    pub fn test_seed_bg(&self, info: BlockGroupInfo) -> FsResult<()> {
        use crate::pd::journal::entry::BGEntry;
        let table_id = info.table_id;
        // Refresh the owning table's runtime index (Capacity active_bgs; Hash
        // no-op) so routing sees the seeded BG.
        if let Some(table) = self.get_table(table_id) {
            let mut table = (*table).clone();
            table.on_bg_created(&info);
            self.put_table(table);
        }
        // BG-local apply: store.put + in-memory BG index, no table row / epoch.
        self.bg_manager
            .apply_create_bg(&BGEntry { op_ms: 0, info })?;
        Ok(())
    }

    /// Test-only: build a BGTableManager (and its BG/pool/config/node deps) on
    /// in-memory stores. Returns the manager plus the shared KV store and
    /// journal client so callers can build sibling managers on the same stack.
    #[cfg(test)]
    pub fn new_for_test() -> (
        Arc<Self>,
        Arc<dyn crate::pd::store::KvStore>,
        Arc<crate::pd::journal::Client>,
    ) {
        use crate::pd::bg::{BGManager, BGStore};
        use crate::pd::config::ConfigManager;
        use crate::pd::node::{NodeManager, NodeStore};
        use crate::pd::pool::PoolManager;
        use crate::pd::store::memory_kv_engine::MemoryKvEngine;
        use crate::pd::store::KvStore;
        use curvine_common::conf::JournalConf;
        use curvine_common::raft::RaftClient;

        let store: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
        let journal_conf = JournalConf::default();
        let raft = RaftClient::from_conf(journal_conf.create_runtime(), &journal_conf);
        let jc = Arc::new(crate::pd::journal::Client::new(raft));
        let config_manager = Arc::new(ConfigManager::new(
            Arc::new(MemoryKvEngine::new()),
            jc.clone(),
            HashMap::new(),
        ));
        let node_store = Arc::new(NodeStore::new(Arc::new(MemoryKvEngine::new())));
        let node_manager = Arc::new(NodeManager::new(
            node_store,
            config_manager.clone(),
            jc.clone(),
        ));
        let pool_manager = Arc::new(PoolManager::new(node_manager));
        let bg_manager = Arc::new(BGManager::new(
            Arc::new(BGStore::new(store.clone())),
            jc.clone(),
        ));
        let table_store = Arc::new(super::BGTableStore::new(store.clone()));
        let manager = Arc::new(Self::new(
            table_store,
            bg_manager,
            pool_manager,
            config_manager,
            vec![],
        ));
        (manager, store, jc)
    }
}
