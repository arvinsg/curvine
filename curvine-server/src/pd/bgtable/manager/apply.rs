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

use super::{BGTable, BGTableManager};
use crate::pd::bgtable::control::{PrepareTablesResult, PreparedTables};
use crate::pd::journal::entry::{
    BGBatchUpdateEntry, BGDeleteEntry, BGEntry, BGIdAllocatorEntry, BGUpdateEntry,
};
use crate::pd::journal::ApplyOutcome;
use curvine_common::state::BlockGroupInfo;
use curvine_common::FsResult;

impl BGTableManager {
    // ---- single-BG apply: dispatch to the owning kind's control ------------

    pub fn apply_create_bg(&self, entry: &BGEntry) -> FsResult<ApplyOutcome> {
        self.control(entry.info.kind).apply_create_bg(entry)
    }

    pub fn apply_update_bg(&self, entry: &BGUpdateEntry) -> FsResult<ApplyOutcome> {
        self.control(entry.kind).apply_update_bg(entry)
    }

    pub fn apply_delete_bg(&self, entry: &BGDeleteEntry) -> FsResult<ApplyOutcome> {
        self.control(entry.kind).apply_delete_bg(entry)
    }

    pub fn apply_batch_update_bg(&self, entry: &BGBatchUpdateEntry) -> FsResult<ApplyOutcome> {
        let Some(first) = entry.updates.first() else {
            return Ok(ApplyOutcome::SkippedNoop);
        };
        // A rebuild batch targets one kind's tables; route to that control.
        self.control(first.kind).apply_batch_update(entry)
    }

    /// Apply a BG id-range reservation. Forwarded to the BG layer (id allocation
    /// is pure BG metadata, no table involved) so app_storage still routes every
    /// BG-world entry through `BGTableManager`.
    pub fn apply_allocate_bg_id(&self, entry: &BGIdAllocatorEntry) -> FsResult<ApplyOutcome> {
        self.bg_manager.apply_allocate_bg_id(entry)
    }

    // ---- namespace create: plan a batch of tables + their BGs --------------

    pub fn plan_namespace_bg_create(
        &self,
        tables_to_create: &[BGTable],
        bgs_to_create: &[BlockGroupInfo],
    ) -> FsResult<PrepareTablesResult> {
        let mut merged = PreparedTables::default();
        for table in tables_to_create {
            let table_bgs: Vec<BlockGroupInfo> = bgs_to_create
                .iter()
                .filter(|bg| bg.table_id == table.table_id())
                .cloned()
                .collect();
            match self
                .control(table.kind())
                .prepare_create_table(table.clone(), &table_bgs)?
            {
                PrepareTablesResult::Applied(plan) => merged.merge(plan),
                PrepareTablesResult::Outcome(outcome) => {
                    return Ok(PrepareTablesResult::Outcome(outcome))
                }
            }
        }
        Ok(PrepareTablesResult::Applied(merged))
    }

    /// Install the in-memory BGTable/BG state produced by
    /// `plan_namespace_bg_create` after the namespace batch has been committed.
    pub fn commit_namespace_bg_create(&self, mut plan: PreparedTables) {
        for table in plan.take_tables() {
            self.control(table.kind()).on_table_created(table);
        }
        for bg in plan.take_bgs() {
            self.bg_manager.insert_bg(bg);
        }
    }
}
