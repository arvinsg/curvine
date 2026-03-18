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
use super::store::PoolStore;
use crate::pd::config::ConfigManager;
use crate::pd::journal;
use crate::pd::node::NodeManager;
use crate::pd::node::NodeStore;
use crate::pd::store::memory_kv_engine::MemoryKvEngine;
use crate::pd::store::KvStore;
use curvine_common::conf::JournalConf;
use curvine_common::raft::RaftClient;
use curvine_common::state::StorageSpec;
use std::collections::HashMap;
use std::sync::Arc;

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
    let pool_store = Arc::new(PoolStore::new(store));
    PoolManager::new(pool_store, node_manager, jc)
}

#[test]
fn restore_inits_default_pools() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    assert!(mgr.get_pool_by_media(curvine_common::state::StorageType::Mem).is_ok());
    assert!(mgr.get_pool_by_media(curvine_common::state::StorageType::Ssd).is_ok());
    assert!(mgr.get_pool_by_media(curvine_common::state::StorageType::Hdd).is_ok());
    assert!(mgr.list_active_pools().is_empty());
}

#[test]
fn get_pool_by_media_after_restore() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    let ssd = mgr.get_pool_by_media(curvine_common::state::StorageType::Ssd).unwrap();
    assert_eq!(ssd.name, "ssd_pool");
    assert_eq!(ssd.pool_id, super::POOL_ID_SSD);
}

#[test]
fn get_pool_returns_error_for_unknown_id() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    let r = mgr.get_pool(99);
    assert!(r.is_err());
}

#[test]
fn assign_worker_to_pools_with_ssd_spec_adds_to_ssd_pool() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    let mut specs = std::collections::HashMap::new();
    specs.insert(
        "s1".to_string(),
        StorageSpec {
            dir_id: 0,
            storage_id: "s1".to_string(),
            failed: false,
            storage_type: curvine_common::state::StorageType::Ssd,
            dir_path: "/data".to_string(),
        },
    );
    let pool_ids = mgr.assign_worker_to_pools(100, &specs).unwrap();
    assert_eq!(pool_ids, vec![super::POOL_ID_SSD]);
    let pool = mgr.get_pool(super::POOL_ID_SSD).unwrap();
    assert!(pool.workers.contains(&100));
    let active = mgr.list_active_pools();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].pool_id, super::POOL_ID_SSD);
}

#[test]
fn assign_worker_to_pools_empty_specs_returns_empty() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    let specs = std::collections::HashMap::new();
    let pool_ids = mgr.assign_worker_to_pools(200, &specs).unwrap();
    assert!(pool_ids.is_empty());
}

#[test]
fn remove_worker_from_pools() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    let mut specs = std::collections::HashMap::new();
    specs.insert(
        "s1".to_string(),
        StorageSpec {
            dir_id: 0,
            storage_id: "s1".to_string(),
            failed: false,
            storage_type: curvine_common::state::StorageType::Ssd,
            dir_path: "/data".to_string(),
        },
    );
    mgr.assign_worker_to_pools(300, &specs).unwrap();
    let pool = mgr.get_pool(super::POOL_ID_SSD).unwrap();
    assert!(pool.workers.contains(&300));
    mgr.remove_worker_from_pools(300).unwrap();
    let pool = mgr.get_pool(super::POOL_ID_SSD).unwrap();
    assert!(!pool.workers.contains(&300));
}

/// Restore rebuilds worker_to_pools from store (only pool info persisted).
#[test]
fn restore_rebuilds_worker_to_pools_from_store_only() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
    let jc = make_journal_client();
    let config_manager = Arc::new(ConfigManager::new(
        store.clone(),
        jc.clone(),
        HashMap::new(),
    ));
    let node_store = Arc::new(NodeStore::new(store.clone()));
    let node_manager = Arc::new(NodeManager::new(node_store, config_manager, jc.clone()));
    let pool_store1 = Arc::new(PoolStore::new(store.clone()));
    let mgr1 = PoolManager::new(pool_store1, node_manager.clone(), jc.clone());
    mgr1.restore().unwrap();
    let mut specs = std::collections::HashMap::new();
    specs.insert(
        "s1".to_string(),
        StorageSpec {
            dir_id: 0,
            storage_id: "s1".to_string(),
            failed: false,
            storage_type: curvine_common::state::StorageType::Ssd,
            dir_path: "/data".to_string(),
        },
    );
    mgr1.assign_worker_to_pools(100, &specs).unwrap();

    let pool_store2 = Arc::new(PoolStore::new(store));
    let mgr2 = PoolManager::new(pool_store2, node_manager, jc);
    mgr2.restore().unwrap();
    let pool = mgr2.get_pool(super::POOL_ID_SSD).unwrap();
    assert!(pool.workers.contains(&100));
}

#[test]
fn get_worker_node_returns_none_when_not_worker() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    assert!(mgr.get_worker_node(1).is_none());
}

#[test]
fn update_pool_stats() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    mgr.update_pool_stats(
        super::POOL_ID_SSD,
        curvine_common::state::PoolStats {
            capacity_bytes: 2000,
            available_bytes: 1000,
            used_bytes: 1000,
        },
    )
    .unwrap();
    let pool = mgr.get_pool(super::POOL_ID_SSD).unwrap();
    assert_eq!(pool.stats.capacity_bytes, 2000);
    assert_eq!(pool.stats.available_bytes, 1000);
}
