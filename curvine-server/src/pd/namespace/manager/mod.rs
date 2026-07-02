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
use crate::pd::bgtable::{BGTable, BGTableManager, PrepareTablesResult};
use crate::pd::journal::entry::{NamespaceCreateEntry, NamespaceUpdateEntry};
use crate::pd::journal::{self, ApplyOutcome, PdEntry};
use crate::pd::store::{KvStore, KvWrite};
use curvine_common::state::{
    make_table_id, BgId, BlockGroupInfo, CacheAckPolicy, CacheTierConfig, CreateNamespaceRequest,
    NamespaceId, NamespaceInfo, TableId, INVALID_NAMESPACE_ID, MAX_NAMESPACE_ID,
    MAX_TABLES_PER_NAMESPACE,
};
use curvine_common::utils::SerdeUtils as Serde;
use curvine_common::{FsError, FsResult};
use orpc::common::LocalTime;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

mod create;
#[cfg(test)]
mod tests;
mod update;
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
    index: RwLock<NamespaceIndex>,
    bgtable_manager: Arc<BGTableManager>,
    journal_client: Arc<journal::Client>,
    store: Arc<NamespaceStore>,
}

impl NamespaceManager {
    pub fn new(
        store: Arc<dyn KvStore>,
        bgtable_manager: Arc<BGTableManager>,
        journal_client: Arc<journal::Client>,
    ) -> Self {
        Self {
            store: Arc::new(NamespaceStore::new(store)),
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

    pub fn restore(&self) -> FsResult<()> {
        let namespaces = self.store.list_namespaces()?;
        let max_namespace_id = namespaces
            .iter()
            .map(|ns| ns.id)
            .max()
            .unwrap_or(INVALID_NAMESPACE_ID);

        self.heal_next_id(max_namespace_id)?;
        *self.index.write().unwrap() = NamespaceIndex::from_namespaces(namespaces);
        Ok(())
    }

    fn heal_next_id(&self, max_namespace_id: NamespaceId) -> FsResult<()> {
        let floor = max_namespace_id.saturating_add(1).max(1);
        let current = self.store.get_next_namespace_id()?;
        if current < floor {
            log::warn!(
                "namespace restore self-heal: repair next_namespace_id from {} to {}",
                current,
                floor
            );
            self.store.set_next_namespace_id(floor)?;
        }
        Ok(())
    }

    /// Test-only: build a NamespaceManager on cheap in-memory dependencies,
    #[cfg(test)]
    pub fn new_for_test() -> Arc<Self> {
        let (bgtable_manager, store, jc) = BGTableManager::new_for_test();
        Arc::new(Self::new(store, bgtable_manager, jc))
    }

    /// Test-only: insert a namespace directly into the in-memory index, bypassing Raft propose.
    #[cfg(test)]
    pub fn test_insert_namespace(&self, info: NamespaceInfo) {
        self.index.write().unwrap().insert(info);
    }
}
