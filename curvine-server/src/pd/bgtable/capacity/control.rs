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

use crate::pd::bg::BGManager;
use crate::pd::bgtable::capacity::CapacityPlacement;
use crate::pd::bgtable::table::TableRegistry;
use crate::pd::bgtable::{BGTable, BGTableControl, BGTableStats, BGTableStore, CapacityBGTable};
use curvine_common::state::{BgId, BlockGroupInfo, TableId};
use std::collections::HashMap;
use std::sync::Arc;

/// Total manager of Capacity BGTables.
pub struct CapacityBGTableControl {
    tables: TableRegistry<CapacityBGTable>,
    store: Arc<BGTableStore>,
    bg_manager: Arc<BGManager>,
}

impl CapacityBGTableControl {
    pub fn new(store: Arc<BGTableStore>, bg_manager: Arc<BGManager>) -> Self {
        Self {
            tables: TableRegistry::default(),
            store,
            bg_manager,
        }
    }

    pub fn placement(&self) -> CapacityPlacement<'_> {
        CapacityPlacement::new(self)
    }

    pub fn get_capacity_table(&self, table_id: TableId) -> Option<Arc<CapacityBGTable>> {
        self.tables.get(table_id)
    }

    pub fn put_table(&self, table: CapacityBGTable) {
        self.tables.put(table);
    }

    pub fn restore_tables(&self, tables: HashMap<TableId, Arc<CapacityBGTable>>) {
        self.tables.replace_all(tables);
    }
}

impl BGTableControl for CapacityBGTableControl {
    fn store(&self) -> &BGTableStore {
        &self.store
    }

    fn bg_manager(&self) -> &BGManager {
        &self.bg_manager
    }

    fn get_table(&self, table_id: TableId) -> Option<Arc<BGTable>> {
        self.get_capacity_table(table_id)
            .map(|table| Arc::new(BGTable::Capacity((*table).clone())))
    }

    fn list_tables(&self) -> Vec<Arc<BGTable>> {
        self.tables
            .values()
            .into_iter()
            .map(|table| Arc::new(BGTable::Capacity((*table).clone())))
            .collect()
    }

    fn snapshot_tables(&self) -> HashMap<TableId, Arc<BGTable>> {
        self.tables
            .snapshot()
            .into_iter()
            .map(|(id, table)| (id, Arc::new(BGTable::Capacity((*table).clone()))))
            .collect()
    }

    fn table_epochs(&self) -> HashMap<TableId, u64> {
        self.tables.epochs()
    }

    fn on_table_created(&self, table: BGTable) {
        if let BGTable::Capacity(table) = table {
            self.tables.put(table);
        }
    }

    fn on_table_updated(&self, table: BGTable) {
        if let BGTable::Capacity(table) = table {
            self.tables.put(table);
        }
    }

    fn on_table_removed(&self, table_id: TableId) {
        self.tables.remove(table_id);
    }

    fn refresh_bg_index(&self, table: BGTable) {
        if let BGTable::Capacity(table) = table {
            self.tables.put(table);
        }
    }

    fn update_table_stats(&self, table_id: TableId, stats: BGTableStats) {
        self.tables.update_stats(table_id, stats);
    }

    // Capacity tables maintain a derived active-BG index; the table type owns
    // that invariant, so we just drive the rebuild.
    fn rebuild_indexes(&self, table: &mut BGTable, bgs: &HashMap<BgId, Arc<BlockGroupInfo>>) {
        table.rebuild_active_index(bgs);
    }
}
