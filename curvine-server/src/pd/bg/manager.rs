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

use super::state_machine;
use super::{BGStore, BGTable};
use crate::pd::journal::entry::{BGEntry, BGUpdateEntry};
use crate::pd::node::NodeManager;
use crate::pd::pool::PoolManager;
use curvine_common::state::{
    BlockGroupInfo, BlockGroupInfoView, ReplicaInfo, BGTableSummary,
};
use curvine_common::{FsError, FsResult};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

/// Expand BlockGroupInfo to view (replica_set with address and state). BG module owns this; node module stays agnostic of pool/BG.
fn block_group_info_to_view(bg: &BlockGroupInfo, node_manager: &NodeManager) -> BlockGroupInfoView {
    let replica_set: Vec<ReplicaInfo> = bg
        .replica_set
        .iter()
        .filter_map(|&node_id| {
            node_manager.get_node(node_id).map(|node| ReplicaInfo {
                node_id,
                address: node.base.address.clone(),
                state: node.state,
            })
        })
        .collect();
    BlockGroupInfoView {
        bg_id: bg.bg_id,
        table_id: bg.table_id,
        epoch: bg.epoch,
        replica_set,
        state: bg.state,
        lease_owner: bg.lease_owner.clone(),
    }
}

pub struct BGManager {
    tables: RwLock<HashMap<u32, BGTable>>,
    bgs: RwLock<HashMap<u32, BlockGroupInfo>>,
    store: Arc<BGStore>,
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
        let tables = self.store.list_tables()?;
        let bgs = self.store.list_all()?;
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
            state_machine::validate_transition(info.state, s)?;
            info.state = s;
        }
        if let Some(ref rs) = entry.replica_set {
            info.replica_set = rs.clone();
        }
        if let Some(ref lease) = entry.lease_owner {
            info.lease_owner = lease.clone();
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
            self.store.put_table(table)?;
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

    /// BGs that have this worker in replica_set (for schedule/checkers).
    pub fn get_bgs_on_worker(&self, worker_id: u32) -> Vec<BlockGroupInfo> {
        self.bgs
            .read()
            .unwrap()
            .values()
            .filter(|bg| bg.replica_set.contains(&worker_id))
            .cloned()
            .collect()
    }

    /// BGs in the given state.
    pub fn get_bgs_by_state(&self, state: curvine_common::state::BGState) -> Vec<BlockGroupInfo> {
        self.bgs
            .read()
            .unwrap()
            .values()
            .filter(|bg| bg.state == state)
            .cloned()
            .collect()
    }

    /// BGs whose lease has expired at the given time (for LeaseChecker).
    pub fn get_bgs_with_expired_lease(&self, now_ms: u64) -> Vec<BlockGroupInfo> {
        self.bgs
            .read()
            .unwrap()
            .values()
            .filter(|bg| {
                bg.lease_owner.expire_time_ms > 0 && bg.lease_owner.expire_time_ms < now_ms
            })
            .cloned()
            .collect()
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
            .map(|bg| block_group_info_to_view(bg, node_manager))
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
            last_rebuild_ms: table.last_rebuild_ms,
        })
    }

    /// TODO: Rebuild table buckets: for each bucket, verify the BG's replica_set workers are still alive.
    /// If any BG has insufficient replicas, attempt to select new workers via PoolManager.
    /// Epoch is incremented and persisted after any change.
    pub fn rebuild_table(&self, table_id: u32) -> FsResult<()> {
        let table = {
            let tables = self.tables.read().unwrap();
            tables
                .get(&table_id)
                .cloned()
                .ok_or_else(|| FsError::common(format!("table {} not found", table_id)))?
        };

        // Collect BGs that need replica repair (snapshot under read lock)
        let repair_list: Vec<(u32, Vec<u32>, Vec<u32>)> = {
            let bgs = self.bgs.read().unwrap();
            table
                .buckets
                .iter()
                .filter_map(|&bg_id| {
                    if bg_id == 0 {
                        return None;
                    }
                    let bg = bgs.get(&bg_id)?;
                    let alive: Vec<u32> = bg
                        .replica_set
                        .iter()
                        .filter(|w| self.pool_manager.is_worker_available(**w))
                        .copied()
                        .collect();
                    let desired = table.replica_count() as usize;
                    if alive.len() < desired && !alive.is_empty() {
                        Some((bg_id, alive, bg.replica_set.clone()))
                    } else {
                        None
                    }
                })
                .collect()
        };

        if repair_list.is_empty() {
            return Ok(());
        }

        let mut changed = false;
        for (bg_id, alive, old_replica_set) in &repair_list {
            let needed = table.replica_count() as u16 - alive.len() as u16;
            let new_workers = match self.pool_manager.select_workers_for_bg(
                table.pool_id(),
                needed,
                table.policy.placement,
                old_replica_set,
            ) {
                Ok(w) => w,
                Err(_) => continue,
            };
            if new_workers.is_empty() {
                continue;
            }
            let mut new_replicas = alive.clone();
            new_replicas.extend(new_workers);

            let mut bgs_w = self.bgs.write().unwrap();
            if let Some(bg_mut) = bgs_w.get_mut(bg_id) {
                bg_mut.replica_set = new_replicas;
                bg_mut.epoch += 1;
                let _ = self.store.put(bg_mut);
                changed = true;
            }
        }

        if changed {
            self.bump_table_epoch(table_id)?;
        }
        Ok(())
    }

    /// Rebuild all tables for a pool (e.g. after node join/remove). Calls rebuild_table for each table in the pool.
    pub fn rebuild_tables_for_pool(&self, pool_id: u16) -> FsResult<()> {
        let table_ids: Vec<u32> = self
            .list_tables()
            .into_iter()
            .filter(|t| t.pool_id() == pool_id)
            .map(|t| t.table_id)
            .collect();
        for table_id in table_ids {
            let _ = self.rebuild_table(table_id);
        }
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
            lease_owner: None,
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
            lease_owner: None,
        })
        .unwrap();
        let got = mgr.get_bg(3).unwrap();
        assert_eq!(got.replica_set, vec![301, 302]);
    }

    #[test]
    fn apply_update_bg_lease_owner() {
        let mgr = test_manager();
        let info = make_bg(5, 10, vec![500, 501]);
        mgr.apply_create_bg(&BGEntry { op_ms: 0, info }).unwrap();
        mgr.apply_update_bg(&BGUpdateEntry {
            op_ms: 1,
            bg_id: 5,
            state: None,
            replica_set: None,
            lease_owner: Some(BGLease {
                node_id: 501,
                expire_time_ms: 99_000,
            }),
        })
        .unwrap();
        let got = mgr.get_bg(5).unwrap();
        assert_eq!(got.lease_owner.node_id, 501);
        assert_eq!(got.lease_owner.expire_time_ms, 99_000);
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
