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

use super::manager::PoolManager;
use crate::pd::config::ConfigManager;
use crate::pd::journal;
use crate::pd::node::{NodeManager, NodeStore};
use crate::pd::store::memory_kv_engine::MemoryKvEngine;
use crate::pd::store::KvStore;
use curvine_common::conf::JournalConf;
use curvine_common::raft::RaftClient;
use curvine_common::state::{
    NodeAddress, NodeBase, NodeInfo, NodePayload, NodeState, NodeType, PoolStats, PoolType,
    StorageSpec, StorageType, WorkerNodePayload,
};
use std::collections::HashMap;
use std::sync::{Arc, Barrier};

fn make_journal_client() -> Arc<journal::Client> {
    let journal_conf = JournalConf::default();
    let rt = journal_conf.create_runtime();
    let raft = RaftClient::from_conf(rt, &journal_conf);
    Arc::new(journal::Client::new(raft))
}

fn test_pool_manager() -> PoolManager {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
    let jc = make_journal_client();
    let config_manager = Arc::new(ConfigManager::new(
        store.clone(),
        jc.clone(),
        HashMap::new(),
    ));
    let node_store = Arc::new(NodeStore::new(store.clone()));
    let node_manager = Arc::new(NodeManager::new(node_store, config_manager, jc.clone()));
    PoolManager::new(node_manager)
}

fn worker_node_with_storage(id: u32, state: NodeState, storage_type: StorageType) -> NodeInfo {
    let mut payload = WorkerNodePayload::default();
    payload.storage_specs.insert(
        "s0".to_string(),
        StorageSpec {
            dir_id: 0,
            storage_id: "s0".to_string(),
            failed: false,
            storage_type,
            dir_path: "/tmp/s0".to_string(),
        },
    );
    NodeInfo {
        base: NodeBase {
            node_id: id,
            node_type: NodeType::Worker,
            address: NodeAddress {
                hostname: format!("w-{}", id),
                ip: format!("10.0.0.{}", id),
                rpc_port: 8000 + id as u16,
                web_port: 9000 + id as u16,
            },
            ..Default::default()
        },
        state,
        epoch: 1,
        payload: NodePayload::Worker(payload),
        ..Default::default()
    }
}

#[test]
fn restore_initializes_fixed_pools() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    let pools = mgr.list_pools();
    assert_eq!(pools.len(), 3);
    assert_eq!(mgr.get_pool(PoolType::Mem).unwrap().media, StorageType::Mem);
    assert_eq!(mgr.get_pool(PoolType::Ssd).unwrap().media, StorageType::Ssd);
    assert_eq!(mgr.get_pool(PoolType::Hdd).unwrap().media, StorageType::Hdd);
}

#[test]
fn get_pool_by_media() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    let ssd = mgr.get_pool_by_media(StorageType::Ssd).unwrap();
    assert_eq!(ssd.name, "ssd_pool");
    assert_eq!(ssd.pool_type, PoolType::Ssd);
}

#[test]
fn workers_are_maintained_in_runtime_pool_membership() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    mgr.test_insert_node(worker_node_with_storage(
        100,
        NodeState::Live,
        StorageType::Ssd,
    ));
    mgr.test_insert_node(worker_node_with_storage(
        101,
        NodeState::Live,
        StorageType::Hdd,
    ));
    mgr.test_insert_node(worker_node_with_storage(
        102,
        NodeState::Lost,
        StorageType::Ssd,
    ));
    mgr.reconcile_worker_pool_membership().unwrap();

    assert_eq!(mgr.get_workers_in_pool(PoolType::Ssd), vec![100, 102]);
    assert_eq!(mgr.get_live_workers(PoolType::Ssd), vec![100]);
    assert_eq!(mgr.get_workers_in_pool(PoolType::Hdd), vec![101]);
    assert_eq!(mgr.get_pools_by_worker(100), vec![PoolType::Ssd]);
}

#[test]
fn reconcile_and_offline_event_interleaving_does_not_leave_stale_worker() {
    // Regression test for the race where full reconcile rebuilt membership
    // from an old external worker snapshot after an offline event had already
    // removed the worker from runtime pools.
    for _ in 0..32 {
        let mgr = Arc::new(test_pool_manager());
        mgr.restore().unwrap();

        let node = worker_node_with_storage(100, NodeState::Live, StorageType::Ssd);
        let NodePayload::Worker(ref payload) = node.payload else {
            panic!("expected worker payload");
        };
        let storage_specs = payload.storage_specs.clone();
        mgr.test_insert_node(node);
        mgr.assign_worker_to_pools(100, &storage_specs).unwrap();
        assert_eq!(mgr.get_workers_in_pool(PoolType::Ssd), vec![100]);

        let barrier = Arc::new(Barrier::new(3));

        let reconcile_mgr = mgr.clone();
        let reconcile_barrier = barrier.clone();
        let reconcile = std::thread::spawn(move || {
            reconcile_barrier.wait();
            reconcile_mgr.reconcile_worker_pool_membership().unwrap();
        });

        let offline_mgr = mgr.clone();
        let offline_barrier = barrier.clone();
        let offline = std::thread::spawn(move || {
            offline_barrier.wait();
            let mut node = offline_mgr.get_worker_node(100).unwrap();
            node.state = NodeState::Offline;
            offline_mgr.test_insert_node(node);
            offline_mgr.remove_worker_from_pools(100).unwrap();
        });

        barrier.wait();
        reconcile.join().unwrap();
        offline.join().unwrap();

        assert!(
            mgr.get_workers_in_pool(PoolType::Ssd).is_empty(),
            "offline worker must not be resurrected by concurrent reconcile"
        );
        assert!(mgr.get_pools_by_worker(100).is_empty());
    }
}

#[test]
fn list_active_pools_uses_runtime_membership() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    assert!(mgr.list_active_pools().is_empty());
    let node = worker_node_with_storage(100, NodeState::Live, StorageType::Ssd);
    let NodePayload::Worker(ref payload) = node.payload else {
        panic!("expected worker payload");
    };
    let storage_specs = payload.storage_specs.clone();
    mgr.test_insert_node(node);
    mgr.assign_worker_to_pools(100, &storage_specs).unwrap();
    let active = mgr.list_active_pools();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].pool_type, PoolType::Ssd);
}

#[test]
fn update_pool_stats() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    mgr.update_pool_stats(
        PoolType::Ssd,
        PoolStats {
            capacity_bytes: 2000,
            available_bytes: 1000,
            used_bytes: 1000,
            block_count: 3,
        },
    )
    .unwrap();
    let pool = mgr.get_pool(PoolType::Ssd).unwrap();
    assert_eq!(pool.stats.capacity_bytes, 2000);
    assert_eq!(pool.stats.available_bytes, 1000);
}

#[test]
fn get_worker_node_returns_none_when_not_worker() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    assert!(mgr.get_worker_node(1).is_none());
}
