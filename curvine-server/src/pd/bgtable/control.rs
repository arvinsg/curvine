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

use crate::pd::bgtable::mutator::{
    BGMutator, PrepareChangeResult, PrepareTablesResult, PreparedBGChange,
};
use crate::pd::bgtable::{BGTable, BGTableStats};
use crate::pd::journal::entry::{BGDeleteEntry, BGUpdateEntry};
use crate::pd::journal::ApplyOutcome;
use curvine_common::state::{BGTableSummary, BgId, BlockGroupInfo, BlockGroupRouteView, TableId};
use curvine_common::FsResult;
use std::collections::HashMap;
use std::sync::Arc;

pub trait BGTableControl {
    // ---- primitives each control supplies ----------------------------------

    /// The shared BG-mutation engine.
    fn mutator(&self) -> &BGMutator;

    fn get_table(&self, table_id: TableId) -> Option<Arc<BGTable>>;
    fn list_tables(&self) -> Vec<Arc<BGTable>>;
    fn snapshot_tables(&self) -> HashMap<TableId, Arc<BGTable>>;
    fn table_epochs(&self) -> HashMap<TableId, u64>;

    /// Install a mutated table into this control's kind-typed registry. The
    /// `table` is guaranteed to be of this control's kind.
    fn install_table(&self, table: BGTable);
    fn remove_table(&self, table_id: TableId);
    fn update_table_stats(&self, table_id: TableId, stats: BGTableStats);

    /// Rebuild derived indexes from persisted BGs (restore path).
    fn rebuild_indexes(&self, table: &mut BGTable, bgs: &HashMap<BgId, Arc<BlockGroupInfo>>);

    /// Assemble the client-facing routing summary from precomputed route views.
    fn build_client_summary(
        &self,
        table: &BGTable,
        views: Vec<BlockGroupRouteView>,
    ) -> Option<BGTableSummary>;

    // ---- BG lifecycle operations (shared recipe via BGMutator) -------------

    /// Create one BG in its owning table, committing table + BG atomically.
    fn create_bg(&self, info: &BlockGroupInfo) -> FsResult<ApplyOutcome> {
        match self.mutator().prepare_create(info)? {
            PrepareChangeResult::Ready(change) => self.commit_change(change),
            PrepareChangeResult::Outcome(outcome) => Ok(outcome),
        }
    }

    fn update_bg(&self, entry: &BGUpdateEntry) -> FsResult<ApplyOutcome> {
        match self.mutator().prepare_update(entry)? {
            PrepareChangeResult::Ready(change) => self.commit_change(change),
            PrepareChangeResult::Outcome(outcome) => Ok(outcome),
        }
    }

    fn delete_bg(&self, entry: &BGDeleteEntry) -> FsResult<ApplyOutcome> {
        match self.mutator().prepare_delete(entry)? {
            PrepareChangeResult::Ready(change) => self.commit_change(change),
            PrepareChangeResult::Outcome(outcome) => Ok(outcome),
        }
    }

    /// Load the owning table, let the mutator commit the change, install the
    /// mutated table. The three steps (load / commit / install) are explicit and
    /// closure-free; load and install use this control's kind-typed registry.
    fn commit_change(&self, change: PreparedBGChange) -> FsResult<ApplyOutcome> {
        let table_id = change.table_id();
        let Some(table) = self.get_table(table_id) else {
            return Ok(ApplyOutcome::not_found(format!(
                "table {} not found",
                table_id
            )));
        };
        let updated = self.mutator().commit_change((*table).clone(), change)?;
        self.install_table(updated);
        Ok(ApplyOutcome::Applied)
    }

    /// Prepare the install of one freshly-built table and its BGs (namespace
    /// create path). Rejects if the table already exists. Kind-agnostic: the
    /// manager routes each table here by its kind and merges the plans.
    fn prepare_install(
        &self,
        table: BGTable,
        bgs: &[BlockGroupInfo],
    ) -> FsResult<PrepareTablesResult> {
        if self.get_table(table.table_id()).is_some() {
            return Ok(PrepareTablesResult::Outcome(ApplyOutcome::stale(format!(
                "table {} already exists",
                table.table_id()
            ))));
        }
        self.mutator().prepare_install(table, bgs)
    }
}
