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
    MetaNodePayload, NodeAddress, NodeBase, NodeInfo, NodePayload, NodeState, NodeType, PoolStats,
    StorageSpec, StorageStats, StorageType, WorkerNodePayload,
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
    let node_store = Arc::new(NodeStore::new(store));
    let node_manager = Arc::new(NodeManager::new(node_store, config_manager, jc));
    PoolManager::new(node_manager)
}

fn worker_node_with_storage(id: u32, state: NodeState, storage_type: StorageType) -> NodeInfo {
    worker_node_with_storages(id, state, &[storage_type])
}

fn worker_node_with_storages(id: u32, state: NodeState, storage_types: &[StorageType]) -> NodeInfo {
    let mut payload = WorkerNodePayload::default();
    for (idx, storage_type) in storage_types.iter().enumerate() {
        let sid = format!("s{}", idx);
        payload
            .storage_specs
            .insert(sid.clone(), storage_spec(&sid, *storage_type));
    }
    worker_node(id, state, payload)
}

fn worker_node_with_stats(
    id: u32,
    state: NodeState,
    storages: &[(&str, StorageType, i64, i64, i64, i64)],
) -> NodeInfo {
    let mut payload = WorkerNodePayload::default();
    for (sid, storage_type, capacity, available, used, block_num) in storages {
        payload
            .storage_specs
            .insert((*sid).to_string(), storage_spec(sid, *storage_type));
        payload.storage_stats.insert(
            (*sid).to_string(),
            StorageStats {
                capacity: *capacity,
                available: *available,
                fs_used: *used,
                block_num: *block_num,
                dir_path: format!("/tmp/{}", sid),
                ..Default::default()
            },
        );
    }
    worker_node(id, state, payload)
}

fn worker_node(id: u32, state: NodeState, payload: WorkerNodePayload) -> NodeInfo {
    let mut labels = HashMap::new();
    labels.insert("zone".to_string(), format!("z{}", id));

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
            labels,
            ..Default::default()
        },
        state,
        epoch: 1,
        payload: NodePayload::Worker(payload),
        ..Default::default()
    }
}

fn storage_spec(storage_id: &str, storage_type: StorageType) -> StorageSpec {
    StorageSpec {
        dir_id: 0,
        storage_id: storage_id.to_string(),
        failed: false,
        storage_type,
        dir_path: format!("/tmp/{}", storage_id),
    }
}

fn worker_storage_specs(node: &NodeInfo) -> HashMap<String, StorageSpec> {
    let NodePayload::Worker(payload) = &node.payload else {
        panic!("expected worker payload");
    };
    payload.storage_specs.clone()
}

#[test]
fn restore_builds_storage_pools() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();

    let cases = [
        (StorageType::Mem, "mem_pool"),
        (StorageType::Ssd, "ssd_pool"),
        (StorageType::Hdd, "hdd_pool"),
    ];
    assert_eq!(mgr.list_pools().len(), cases.len());
    for (media, name) in cases {
        let pool = mgr.get_pool(media).unwrap();
        assert_eq!(pool.media, media);
        assert_eq!(pool.name, name);
        assert!(pool.workers.is_empty());
    }
}

#[test]
fn reconcile_filters_worker_membership() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();

    let cases = [
        (100, NodeState::Starting, true),
        (101, NodeState::Live, true),
        (102, NodeState::Lost, true),
        (103, NodeState::Offline, false),
        (104, NodeState::Decommission, false),
        (105, NodeState::Blacklist, false),
    ];
    for (worker_id, state, _) in cases {
        mgr.test_insert_node(worker_node_with_storage(worker_id, state, StorageType::Ssd));
    }
    mgr.test_insert_node(worker_node_with_storage(
        200,
        NodeState::Live,
        StorageType::Ufs,
    ));

    let changed = mgr.reconcile_worker_pool_membership().unwrap();

    assert_eq!(changed, vec![StorageType::Ssd]);
    assert_eq!(
        mgr.get_workers_in_pool(StorageType::Ssd),
        vec![100, 101, 102]
    );
    assert_eq!(mgr.get_live_workers(StorageType::Ssd), vec![101]);
    for (worker_id, _, included) in cases {
        let expected = if included {
            vec![StorageType::Ssd]
        } else {
            vec![]
        };
        assert_eq!(mgr.get_pools_by_worker(worker_id), expected);
    }
    assert!(mgr.get_pools_by_worker(200).is_empty());
}

#[test]
fn assign_and_remove_worker_membership() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();

    let node = worker_node_with_storages(
        100,
        NodeState::Live,
        &[StorageType::Ssd, StorageType::Hdd, StorageType::Ufs],
    );
    let storage_specs = worker_storage_specs(&node);
    mgr.test_insert_node(node);

    let changed = mgr.assign_worker_to_pools(100, &storage_specs).unwrap();
    assert_eq!(changed, vec![StorageType::Ssd, StorageType::Hdd]);
    assert_eq!(
        mgr.get_pools_by_worker(100),
        vec![StorageType::Ssd, StorageType::Hdd]
    );
    assert!(mgr.is_worker_available(100));

    let changed = mgr.assign_worker_to_pools(100, &storage_specs).unwrap();
    assert!(changed.is_empty());

    let mem_only = HashMap::from([("mem".to_string(), storage_spec("mem", StorageType::Mem))]);
    let changed = mgr.assign_worker_to_pools(100, &mem_only).unwrap();
    assert_eq!(
        changed,
        vec![StorageType::Mem, StorageType::Ssd, StorageType::Hdd]
    );
    assert_eq!(mgr.get_pools_by_worker(100), vec![StorageType::Mem]);

    let removed = mgr.remove_worker_from_pools(100).unwrap();
    assert_eq!(removed, vec![StorageType::Mem]);
    assert!(mgr.get_pools_by_worker(100).is_empty());
    assert!(!mgr.is_worker_available(100));
    assert!(mgr.remove_worker_from_pools(100).unwrap().is_empty());
}

#[test]
fn reconcile_keeps_offline_removed() {
    for _ in 0..32 {
        let mgr = Arc::new(test_pool_manager());
        mgr.restore().unwrap();

        let node = worker_node_with_storage(100, NodeState::Live, StorageType::Ssd);
        let storage_specs = worker_storage_specs(&node);
        mgr.test_insert_node(node);
        mgr.assign_worker_to_pools(100, &storage_specs).unwrap();
        assert_eq!(mgr.get_workers_in_pool(StorageType::Ssd), vec![100]);

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
            mgr.get_workers_in_pool(StorageType::Ssd).is_empty(),
            "offline worker must not be resurrected by concurrent reconcile"
        );
        assert!(mgr.get_pools_by_worker(100).is_empty());
    }
}

#[test]
fn active_pools_and_stats_runtime_membership() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    assert!(mgr.list_active_pools().is_empty());

    let node = worker_node_with_stats(
        100,
        NodeState::Live,
        &[
            ("ssd0", StorageType::Ssd, 1000, 600, 400, 4),
            ("ssd1", StorageType::Ssd, 2000, 1200, 800, 8),
            ("hdd0", StorageType::Hdd, 3000, 2000, 1000, 10),
        ],
    );
    let storage_specs = worker_storage_specs(&node);
    mgr.test_insert_node(node);
    mgr.assign_worker_to_pools(100, &storage_specs).unwrap();
    mgr.refresh_pool_stats();

    let active_media: Vec<_> = mgr
        .list_active_pools()
        .into_iter()
        .map(|p| p.media)
        .collect();
    assert_eq!(active_media, vec![StorageType::Ssd, StorageType::Hdd]);
    assert_eq!(
        mgr.get_worker_storage_stats(100, StorageType::Ssd),
        Some((3000, 1200))
    );

    let ssd = mgr.get_pool(StorageType::Ssd).unwrap();
    assert_eq!(ssd.stats.capacity_bytes, 3000);
    assert_eq!(ssd.stats.available_bytes, 1800);
    assert_eq!(ssd.stats.used_bytes, 1200);
    assert_eq!(ssd.stats.block_count, 12);

    mgr.update_pool_stats(
        StorageType::Ssd,
        PoolStats {
            capacity_bytes: 10,
            available_bytes: 9,
            used_bytes: 1,
            block_count: 2,
        },
    )
    .unwrap();
    assert_eq!(mgr.get_pool(StorageType::Ssd).unwrap().stats.block_count, 2);
}

#[test]
fn worker_lookup_return_node_view() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();

    let node = worker_node_with_storage(100, NodeState::Live, StorageType::Ssd);
    let storage_specs = worker_storage_specs(&node);
    mgr.test_insert_node(node.clone());
    mgr.assign_worker_to_pools(100, &storage_specs).unwrap();

    assert_eq!(mgr.get_worker_node(100).unwrap().base.node_id, 100);
    assert_eq!(
        mgr.get_worker_labels(100).unwrap().get("zone").unwrap(),
        "z100"
    );
    assert_eq!(mgr.get_workers_labels(&[100, 404]).len(), 1);
    assert_eq!(
        mgr.get_worker_address_and_state(100),
        Some((node.base.address, NodeState::Live))
    );

    let meta = NodeInfo {
        base: NodeBase {
            node_id: 200,
            node_type: NodeType::Meta,
            ..Default::default()
        },
        payload: NodePayload::Meta(MetaNodePayload::default()),
        ..Default::default()
    };
    mgr.test_insert_node(meta);
    assert!(mgr.get_worker_node(200).is_none());
    assert!(mgr.get_worker_node(404).is_none());
}
