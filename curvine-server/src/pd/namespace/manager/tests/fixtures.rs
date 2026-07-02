use super::super::*;
use crate::pd::bg::{BGManager, BGStore};
use crate::pd::config::ConfigManager;
use crate::pd::journal;
use crate::pd::node::{NodeManager, NodeStore};
use crate::pd::pool::PoolManager;
use crate::pd::store::memory_kv_engine::MemoryKvEngine;
use crate::pd::store::KvStore;
use curvine_common::conf::JournalConf;
use curvine_common::raft::RaftClient;
use curvine_common::state::{
    CacheTierConfig, CreateNamespaceRequest, NodeAddress, NodeBase, NodeInfo, NodePayload,
    NodeState, NodeType, StorageSpec, StorageType, WorkerNodePayload,
};
use std::collections::HashMap;
use std::sync::Arc;

fn make_worker_node(id: u32, pool_type: StorageType) -> NodeInfo {
    let mut payload = WorkerNodePayload::default();
    payload.storage_specs.insert(
        "s0".to_string(),
        StorageSpec {
            dir_id: 0,
            storage_id: "s0".to_string(),
            failed: false,
            storage_type: pool_type,
            dir_path: "/tmp/s0".to_string(),
        },
    );
    NodeInfo {
        base: NodeBase {
            node_id: id,
            node_type: NodeType::Worker,
            address: NodeAddress {
                hostname: format!("worker-{}", id),
                ip: format!("10.0.0.{}", id),
                rpc_port: 8000 + id as u16,
                web_port: 9000 + id as u16,
            },
            ..Default::default()
        },
        state: NodeState::Live,
        epoch: 1,
        payload: NodePayload::Worker(payload),
        ..Default::default()
    }
}

pub fn test_managers() -> (
    Arc<NamespaceManager>,
    Arc<BGManager>,
    Arc<crate::pd::bgtable::BGTableManager>,
) {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
    let journal_conf = JournalConf::default();
    let rt = journal_conf.create_runtime();
    let raft = RaftClient::from_conf(rt, &journal_conf);
    let jc = Arc::new(journal::Client::new(raft));
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
    let pool_manager = Arc::new(PoolManager::new(node_manager.clone()));
    for wid in [100, 101, 102] {
        let node = make_worker_node(wid, StorageType::Ssd);
        let specs = match &node.payload {
            NodePayload::Worker(payload) => payload.storage_specs.clone(),
            _ => unreachable!(),
        };
        node_manager.test_insert_node(node);
        pool_manager.assign_worker_to_pools(wid, &specs).unwrap();
    }

    let bg_store = Arc::new(BGStore::new(store.clone()));
    let bg_manager = Arc::new(BGManager::new(bg_store, jc.clone()));
    let table_store = Arc::new(crate::pd::bgtable::BGTableStore::new(store.clone()));
    let bgtable_manager = Arc::new(crate::pd::bgtable::BGTableManager::new(
        table_store,
        bg_manager.clone(),
        pool_manager,
        config_manager,
        vec![],
    ));
    bgtable_manager.restore().unwrap();
    let ns_manager = Arc::new(NamespaceManager::new(
        store,
        bgtable_manager.clone(),
        jc,
    ));
    ns_manager.restore().unwrap();
    (ns_manager, bg_manager, bgtable_manager)
}

pub fn request(name: &str) -> CreateNamespaceRequest {
    CreateNamespaceRequest::new(
        name,
        CacheTierConfig {
            pools: vec![StorageType::Ssd],
            replica_count: 3,
            bucket_count: 4,
            worker_labels: vec![],
        },
    )
}
