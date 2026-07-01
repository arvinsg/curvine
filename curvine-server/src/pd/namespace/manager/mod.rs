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

use super::NamespaceStore;
use crate::pd::bg::BGManager;
use crate::pd::bgtable::BGTableManager;
use crate::pd::journal::entry::NamespaceCreateEntry;
use crate::pd::journal::{self, ApplyOutcome, PdEntry};
use crate::pd::store::{KvStore, KvWrite};
use curvine_common::state::{
    make_table_id, BgId, CacheAckPolicy, CacheTierConfig, CreateNamespaceRequest, NamespaceId,
    NamespaceInfo, TableId, INVALID_NAMESPACE_ID, MAX_TABLES_PER_NAMESPACE, MAX_NAMESPACE_ID,
};
use curvine_common::utils::SerdeUtils as Serde;
use curvine_common::{FsError, FsResult};
use orpc::common::LocalTime;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

mod create;
mod restore;
#[cfg(test)]
mod tests;
mod validate;

#[derive(Default)]
struct NamespaceIndex {
    by_id: HashMap<NamespaceId, Arc<NamespaceInfo>>,
    by_name: HashMap<String, NamespaceId>,
}

impl NamespaceIndex {
    fn from_namespaces(namespaces: Vec<NamespaceInfo>) -> Self {
        let mut index = Self {
            by_id: HashMap::with_capacity(namespaces.len()),
            by_name: HashMap::with_capacity(namespaces.len()),
        };
        for namespace in namespaces {
            index.insert(namespace);
        }
        index
    }

    fn insert(&mut self, namespace: NamespaceInfo) {
        let id = namespace.id;
        self.by_name.insert(namespace.name.clone(), id);
        self.by_id.insert(id, Arc::new(namespace));
    }

    fn get_by_id(&self, id: NamespaceId) -> Option<Arc<NamespaceInfo>> {
        self.by_id.get(&id).cloned()
    }

    fn get_by_name(&self, name: &str) -> Option<Arc<NamespaceInfo>> {
        let id = self.by_name.get(name).copied()?;
        self.get_by_id(id)
    }

    fn list(&self) -> Vec<Arc<NamespaceInfo>> {
        self.by_id.values().cloned().collect()
    }
}

pub struct NamespaceManager {
    bg_manager: Arc<BGManager>,
    index: RwLock<NamespaceIndex>,
    bgtable_manager: Arc<BGTableManager>,
    journal_client: Arc<journal::Client>,
    store: Arc<NamespaceStore>,
}

impl NamespaceManager {
    pub fn new(
        store: Arc<dyn KvStore>,
        bg_manager: Arc<BGManager>,
        bgtable_manager: Arc<BGTableManager>,
        journal_client: Arc<journal::Client>,
    ) -> Self {
        Self {
            store: Arc::new(NamespaceStore::new(store)),
            bg_manager,
            bgtable_manager,
            journal_client,
            index: RwLock::new(NamespaceIndex::default()),
        }
    }

    pub fn get_namespace(&self, id: NamespaceId) -> Option<Arc<NamespaceInfo>> {
        self.index.read().unwrap().get_by_id(id)
    }

    pub fn get_namespace_by_name(&self, name: &str) -> Option<Arc<NamespaceInfo>> {
        self.index.read().unwrap().get_by_name(name)
    }

    pub fn list_namespaces(&self) -> Vec<Arc<NamespaceInfo>> {
        self.index.read().unwrap().list()
    }

    /// Test-only: build a NamespaceManager with cheap in-memory dependencies.
    /// Lets other modules (e.g. mount) get a real resolver without standing up
    /// the full BGManager / scheduler stack themselves.
    #[cfg(test)]
    pub fn new_for_test() -> Arc<Self> {
        use crate::pd::bg::BGStore;
        use crate::pd::config::ConfigManager;
        use crate::pd::node::{NodeManager, NodeStore};
        use crate::pd::pool::PoolManager;
        use crate::pd::store::memory_kv_engine::MemoryKvEngine;
        use curvine_common::conf::JournalConf;
        use curvine_common::raft::RaftClient;

        let store: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
        let journal_conf = JournalConf::default();
        let raft = RaftClient::from_conf(journal_conf.create_runtime(), &journal_conf);
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
        let pool_manager = Arc::new(PoolManager::new(node_manager));
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
        Arc::new(Self::new(store, bg_manager, bgtable_manager, jc))
    }

    /// Test-only: insert a namespace directly into the in-memory index, bypassing Raft propose.
    #[cfg(test)]
    pub fn test_insert_namespace(&self, info: NamespaceInfo) {
        self.index.write().unwrap().insert(info);
    }
}
