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

use crate::pd::bgtable::{BGTable, BGTableStats, CapacityBGTable};
use curvine_common::state::{
    BGKind, BGState, BGTableSummary, BgId, BlockGroupInfo, BlockGroupRouteView,
    CapacityBGTableSummary, TableId,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Default)]
pub struct CapacityBGTableController {
    tables: RwLock<HashMap<TableId, Arc<CapacityBGTable>>>,
}

impl CapacityBGTableController {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_capacity_table(&self, table_id: TableId) -> Option<Arc<CapacityBGTable>> {
        self.tables.read().unwrap().get(&table_id).cloned()
    }

    pub fn get_table(&self, table_id: TableId) -> Option<Arc<BGTable>> {
        self.get_capacity_table(table_id)
            .map(|table| Arc::new(BGTable::Capacity((*table).clone())))
    }

    pub fn list_tables(&self) -> Vec<Arc<BGTable>> {
        self.tables
            .read()
            .unwrap()
            .values()
            .map(|table| Arc::new(BGTable::Capacity((**table).clone())))
            .collect()
    }

    pub fn table_epochs(&self) -> HashMap<TableId, u64> {
        self.tables
            .read()
            .unwrap()
            .iter()
            .map(|(&id, table)| (id, table.base.epoch))
            .collect()
    }

    pub fn snapshot_tables(&self) -> HashMap<TableId, Arc<BGTable>> {
        self.tables
            .read()
            .unwrap()
            .iter()
            .map(|(&id, table)| (id, Arc::new(BGTable::Capacity((**table).clone()))))
            .collect()
    }

    // TODO
    pub fn replace_tables_runtime(&self, tables: HashMap<TableId, Arc<CapacityBGTable>>) {
        *self.tables.write().unwrap() = tables;
    }

    // TODO
    pub fn replace_table_runtime(&self, table: CapacityBGTable) {
        self.tables
            .write()
            .unwrap()
            .insert(table.base.table_id, Arc::new(table));
    }

    pub fn remove_table(&self, table_id: TableId) {
        self.tables.write().unwrap().remove(&table_id);
    }

    // TODO
    pub fn bg_created(table: &mut CapacityBGTable, bg: &BlockGroupInfo) {
        if Self::is_routable_capacity_bg(bg) && !table.active_bgs.contains(&bg.bg_id) {
            table.active_bgs.push(bg.bg_id);
        }
    }

    // TODO
    pub fn bg_updated(table: &mut CapacityBGTable, old: &BlockGroupInfo, new: &BlockGroupInfo) {
        if Self::is_routable_capacity_bg(old) {
            table.active_bgs.retain(|id| *id != old.bg_id);
        }
        Self::bg_created(table, new);
    }

    // TODO
    pub fn bg_deleted(table: &mut CapacityBGTable, bg: &BlockGroupInfo) {
        if Self::is_routable_capacity_bg(bg) {
            table.active_bgs.retain(|id| *id != bg.bg_id);
        }
    }

    // TODO
    pub fn build_route_summary(
        table: &CapacityBGTable,
        views: Vec<BlockGroupRouteView>,
    ) -> BGTableSummary {
        BGTableSummary::Capacity(CapacityBGTableSummary {
            table_id: table.base.table_id,
            epoch: table.base.epoch,
            active_bgs: views,
        })
    }

    pub fn update_table_stats(&self, table_id: TableId, stats: BGTableStats) {
        let mut tables = self.tables.write().unwrap();
        if let Some(table) = tables.get_mut(&table_id) {
            Arc::make_mut(table).base.stats = stats;
        }
    }

    pub fn rebuild_indexes(table: &mut CapacityBGTable, bgs: &HashMap<BgId, Arc<BlockGroupInfo>>) {
        table.active_bgs.clear();
        for bg in bgs.values() {
            if bg.table_id == table.base.table_id {
                Self::bg_created(table, bg);
            }
        }
    }

    // TODO
    fn is_routable_capacity_bg(bg: &BlockGroupInfo) -> bool {
        bg.kind == BGKind::Capacity && bg.state == BGState::Active
    }
}
