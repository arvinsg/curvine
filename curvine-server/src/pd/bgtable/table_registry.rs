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

use crate::pd::bgtable::BGTableStats;
use curvine_common::state::TableId;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Minimal view a controller table must expose to be stored in a `TableMap`.
pub trait TableEntry {
    fn table_id(&self) -> TableId;
    fn epoch(&self) -> u64;
    fn update_stats(&mut self, stats: BGTableStats);
}

pub struct TableMap<T> {
    tables: RwLock<HashMap<TableId, Arc<T>>>,
}

impl<T> Default for TableMap<T> {
    fn default() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }
}

impl<T: TableEntry + Clone> TableMap<T> {
    pub fn get(&self, table_id: TableId) -> Option<Arc<T>> {
        self.tables.read().unwrap().get(&table_id).cloned()
    }

    /// All tables as owned `Arc`s, in unspecified order.
    pub fn values(&self) -> Vec<Arc<T>> {
        self.tables.read().unwrap().values().cloned().collect()
    }

    pub fn snapshot(&self) -> HashMap<TableId, Arc<T>> {
        self.tables.read().unwrap().clone()
    }

    pub fn epochs(&self) -> HashMap<TableId, u64> {
        self.tables
            .read()
            .unwrap()
            .iter()
            .map(|(&id, table)| (id, table.epoch()))
            .collect()
    }

    pub fn replace_all(&self, tables: HashMap<TableId, Arc<T>>) {
        *self.tables.write().unwrap() = tables;
    }

    pub fn put(&self, table: T) {
        self.tables
            .write()
            .unwrap()
            .insert(table.table_id(), Arc::new(table));
    }

    pub fn remove(&self, table_id: TableId) {
        self.tables.write().unwrap().remove(&table_id);
    }

    pub fn update_stats(&self, table_id: TableId, stats: BGTableStats) {
        let mut tables = self.tables.write().unwrap();
        if let Some(table) = tables.get_mut(&table_id) {
            Arc::make_mut(table).update_stats(stats);
        }
    }
}
