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

use super::{BGStore, BGTable};
use crate::pd::journal::entry::{BGEntry, BGUpdateEntry};
use crate::pd::node::NodeManager;
use crate::pd::pool::PoolManager;
use curvine_common::state::{BGTableSummary, BlockGroupInfo};
use curvine_common::{FsError, FsResult};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

pub struct BGManager {
    tables: RwLock<HashMap<u32, BGTable>>,
    bgs: RwLock<HashMap<u32, BlockGroupInfo>>,
    store: Arc<BGStore>,
    #[allow(dead_code)] // Phase 5+: select_workers_for_bg when creating BGTable
    pool_manager: Arc<PoolManager>,
}

impl BGManager {
    pub fn new(store: Arc<BGStore>, pool_manager: Arc<PoolManager>) -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            bgs: RwLock::new(HashMap::new()),
            store,
            pool_manager,
        }
    }

    pub fn restore(&self) -> FsResult<()> {
        let tables = self.store.list_tables().map_err(FsError::from)?;
        let bgs = self.store.list_all().map_err(FsError::from)?;
        let mut t = self.tables.write().unwrap();
        let mut b = self.bgs.write().unwrap();
        t.clear();
        b.clear();
        for table in tables {
            t.insert(table.table_id, table);
        }
        for bg in bgs {
            b.insert(bg.bg_id, bg);
        }
        Ok(())
    }

    pub fn apply_create_bg(&self, entry: &BGEntry) -> FsResult<()> {
        let info = &entry.info;
        self.store.put(&info)?;
        self.bgs.write().unwrap().insert(info.bg_id, info.clone());
        Ok(())
    }

    pub fn apply_update_bg(&self, entry: &BGUpdateEntry) -> FsResult<()> {
        let mut bgs = self.bgs.write().unwrap();
        let mut info = bgs
            .get(&entry.bg_id)
            .cloned()
            .ok_or_else(|| FsError::common(format!("bg {} not found for update", entry.bg_id)))?;
        if let Some(s) = entry.state {
            info.state = s;
        }
        if let Some(ref rs) = entry.replica_set {
            info.replica_set = rs.clone();
        }
        self.store.put(&info)?;
        bgs.insert(entry.bg_id, info.clone());
        drop(bgs);
        self.bump_table_epoch(info.table_id)?;
        Ok(())
    }

    pub fn apply_delete_bg(&self, bg_id: u32) -> FsResult<()> {
        let table_id = self.bgs.read().unwrap().get(&bg_id).map(|b| b.table_id);
        self.store.delete(bg_id)?;
        self.bgs.write().unwrap().remove(&bg_id);
        if let Some(tid) = table_id {
            let _ = self.bump_table_epoch(tid);
        }
        Ok(())
    }

    fn bump_table_epoch(&self, table_id: u32) -> FsResult<()> {
        let mut tables = self.tables.write().unwrap();
        if let Some(table) = tables.get_mut(&table_id) {
            table.inc_epoch();
            self.store.put_table(table).map_err(FsError::from)?;
        }
        Ok(())
    }

    pub fn get_bg(&self, bg_id: u32) -> Option<BlockGroupInfo> {
        self.bgs.read().unwrap().get(&bg_id).cloned()
    }

    pub fn get_table(&self, table_id: u32) -> Option<BGTable> {
        self.tables.read().unwrap().get(&table_id).cloned()
    }

    /// Lookup bg_id by table_id and key, then return BlockGroupInfo if present.
    pub fn lookup_bg(&self, table_id: u32, key: &[u8]) -> Option<BlockGroupInfo> {
        let tables = self.tables.read().unwrap();
        let table = tables.get(&table_id)?;
        let bg_id = table.lookup(key);
        drop(tables);
        self.bgs.read().unwrap().get(&bg_id).cloned()
    }

    pub fn list_tables(&self) -> Vec<BGTable> {
        self.tables.read().unwrap().values().cloned().collect()
    }

    pub fn list_bgs(&self) -> Vec<BlockGroupInfo> {
        self.bgs.read().unwrap().values().cloned().collect()
    }

    /// Build client-facing summary (buckets as BlockGroupInfoView) for the given table.
    pub fn build_table_summary(
        &self,
        table_id: u32,
        node_manager: &NodeManager,
    ) -> Option<BGTableSummary> {
        let table = self.tables.read().unwrap().get(&table_id).cloned()?;
        let bgs = self.bgs.read().unwrap();
        let buckets: Vec<_> = table
            .buckets
            .iter()
            .filter_map(|&bg_id| bgs.get(&bg_id).cloned())
            .map(|bg| node_manager.block_group_info_to_view(&bg))
            .collect();
        drop(bgs);
        if buckets.len() != table.buckets.len() {
            return None;
        }
        Some(BGTableSummary {
            table_id: table.table_id,
            bucket_count: table.bucket_count,
            epoch: table.epoch,
            buckets,
        })
    }

    /// Rebuild table buckets (reassign BGs to buckets). Epoch is incremented and persisted.
    /// TODO: use pool_manager to recalculate which BG is in which bucket (e.g. after node change).
    #[allow(dead_code)]
    pub fn rebuild_table(&self, _table_id: u32) -> FsResult<()> {
        // TODO: get table, recalculate buckets via pool_manager.select_workers_for_bg etc., update table, inc_epoch, put_table
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::state::{BGLease, BGState};

    fn test_manager() -> BGManager {
        let store: Arc<dyn crate::pd::store::KvStore> =
            Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new());
        let bg_store = Arc::new(BGStore::new(store));
        let pool_store = Arc::new(crate::pd::pool::PoolStore::new(Arc::new(
            crate::pd::store::memory_kv_engine::MemoryKvEngine::new(),
        )));
        let node_store = Arc::new(crate::pd::node::NodeStore::new(Arc::new(
            crate::pd::store::memory_kv_engine::MemoryKvEngine::new(),
        )));
        let journal_conf = curvine_common::conf::JournalConf::default();
        let rt = journal_conf.create_runtime();
        let raft = curvine_common::raft::RaftClient::from_conf(rt, &journal_conf);
        let config_manager = Arc::new(crate::pd::config::ConfigManager::new(
            Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new()),
            raft,
            std::collections::HashMap::new(),
        ));
        let node_manager: Arc<NodeManager> = Arc::new(NodeManager::new(node_store, config_manager));
        let pool_manager = Arc::new(PoolManager::new(pool_store, node_manager));
        BGManager::new(bg_store, pool_manager)
    }

    fn make_bg(bg_id: u32, table_id: u32, replica_set: Vec<u32>) -> BlockGroupInfo {
        BlockGroupInfo {
            bg_id,
            table_id,
            epoch: 1,
            replica_set,
            state: BGState::Assigned,
            lease_owner: BGLease {
                node_id: replica_set.first().copied().unwrap_or(0),
                expire_time_ms: 0,
            },
            stats: Default::default(),
        }
    }

    #[test]
    fn apply_create_bg_and_get() {
        let mgr = test_manager();
        let info = make_bg(1, 10, vec![100, 101, 102]);
        mgr.apply_create_bg(&BGEntry {
            op_ms: 0,
            info: info.clone(),
        })
        .unwrap();
        let got = mgr.get_bg(1).unwrap();
        assert_eq!(got.bg_id, 1);
        assert_eq!(got.replica_set, vec![100, 101, 102]);
    }

    #[test]
    fn apply_update_bg_state() {
        let mgr = test_manager();
        let info = make_bg(2, 10, vec![200, 201]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        mgr.apply_update_bg(&BGUpdateEntry {
            op_ms: 1,
            bg_id: 2,
            state: Some(BGState::Degraded),
            replica_set: None,
        })
        .unwrap();
        let got = mgr.get_bg(2).unwrap();
        assert_eq!(got.state, BGState::Degraded);
    }

    #[test]
    fn apply_update_bg_replica_set() {
        let mgr = test_manager();
        let info = make_bg(3, 10, vec![300]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        mgr.apply_update_bg(&BGUpdateEntry {
            op_ms: 1,
            bg_id: 3,
            state: None,
            replica_set: Some(vec![301, 302]),
        })
        .unwrap();
        let got = mgr.get_bg(3).unwrap();
        assert_eq!(got.replica_set, vec![301, 302]);
    }

    #[test]
    fn apply_delete_bg() {
        let mgr = test_manager();
        let info = make_bg(4, 10, vec![400]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        assert!(mgr.get_bg(4).is_some());
        mgr.apply_delete_bg(4).unwrap();
        assert!(mgr.get_bg(4).is_none());
    }

    #[test]
    fn restore_loads_bgs() {
        let mgr = test_manager();
        let info = make_bg(5, 10, vec![500]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        let bgs_before = mgr.list_bgs();
        assert_eq!(bgs_before.len(), 1);
        mgr.restore().unwrap();
        let bgs_after = mgr.list_bgs();
        assert_eq!(bgs_after.len(), 1);
        assert_eq!(bgs_after[0].bg_id, 5);
    }
}
