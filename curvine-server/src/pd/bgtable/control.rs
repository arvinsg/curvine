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

use crate::pd::bg::PreparedBGUpdate;
use crate::pd::bg::{BGManager, PrepareCreateResult, PrepareDeleteResult, PrepareUpdateResult};
use crate::pd::bgtable::{BGTable, BGTableStats, BGTableStore};
use crate::pd::journal::entry::{BGDeleteEntry, BGEntry, BGUpdateEntry};
use crate::pd::journal::ApplyOutcome;
use crate::pd::store::KvWrite;
use curvine_common::state::{BgId, BlockGroupInfo, CacheReplicaPolicy, NamespaceId, TableId};
use curvine_common::FsResult;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default)]
pub struct PreparedTables {
    pub ops: Vec<KvWrite>,
    pub(crate) tables: Vec<BGTable>,
    pub(crate) bgs: Vec<BlockGroupInfo>,
}

impl PreparedTables {
    pub(crate) fn table_only(ops: Vec<KvWrite>, tables: Vec<BGTable>) -> Self {
        Self {
            ops,
            tables,
            bgs: Vec::new(),
        }
    }

    pub(crate) fn with_bgs(
        ops: Vec<KvWrite>,
        tables: Vec<BGTable>,
        bgs: Vec<BlockGroupInfo>,
    ) -> Self {
        Self { ops, tables, bgs }
    }

    pub(crate) fn merge(&mut self, other: PreparedTables) {
        self.ops.extend(other.ops);
        self.tables.extend(other.tables);
        self.bgs.extend(other.bgs);
    }

    pub fn take_tables(&mut self) -> Vec<BGTable> {
        std::mem::take(&mut self.tables)
    }

    pub fn take_bgs(&mut self) -> Vec<BlockGroupInfo> {
        std::mem::take(&mut self.bgs)
    }
}

pub enum PrepareTablesResult {
    Applied(PreparedTables),
    Outcome(ApplyOutcome),
}

pub trait BGTableControl {
    fn store(&self) -> &BGTableStore;
    fn bg_manager(&self) -> &BGManager;

    fn get_table(&self, table_id: TableId) -> Option<Arc<BGTable>>;
    fn list_tables(&self) -> Vec<Arc<BGTable>>;
    fn snapshot_tables(&self) -> HashMap<TableId, Arc<BGTable>>;
    fn table_epochs(&self) -> HashMap<TableId, u64>;

    fn apply_create_table(&self, table: BGTable);
    fn apply_update_table(&self, table: BGTable);
    fn apply_delete_table(&self, table_id: TableId);
    fn refresh_bg_index(&self, table: BGTable);
    fn update_table_stats(&self, table_id: TableId, stats: BGTableStats);
    fn rebuild_indexes(&self, table: &mut BGTable, bgs: &HashMap<BgId, Arc<BlockGroupInfo>>);

    fn apply_create_bg(&self, entry: &BGEntry) -> FsResult<ApplyOutcome> {
        let plan = match self.bg_manager().prepare_create_bg(&entry.info)? {
            PrepareCreateResult::Applied(p) => p,
            PrepareCreateResult::Outcome(o) => return Ok(o),
        };

        let Some(table) = self.get_table(plan.info.table_id) else {
            return Ok(ApplyOutcome::not_found(format!(
                "table {} not found",
                plan.info.table_id
            )));
        };
        let mut table = (*table).clone();
        table.matches_bg(&plan.info)?;
        table.on_bg_created(&plan.info);
        self.store().write_batch(vec![plan.op.clone()])?;
        self.bg_manager().insert_bg(plan.info);
        self.refresh_bg_index(table);
        Ok(ApplyOutcome::Applied)
    }

    fn apply_update_bg(&self, entry: &BGUpdateEntry) -> FsResult<ApplyOutcome> {
        let plan = match self.bg_manager().prepare_update_bg(entry)? {
            PrepareUpdateResult::Applied(p) => p,
            PrepareUpdateResult::Outcome(o) => return Ok(o),
        };
        self.commit_bg_update(plan, entry.bump_table_epoch)
    }

    fn apply_delete_bg(&self, entry: &BGDeleteEntry) -> FsResult<ApplyOutcome> {
        let plan = match self.bg_manager().prepare_delete_bg(entry)? {
            PrepareDeleteResult::Applied(p) => p,
            PrepareDeleteResult::Outcome(o) => return Ok(o),
        };
        let Some(table) = self.get_table(plan.old_info.table_id) else {
            return Ok(ApplyOutcome::not_found(format!(
                "table {} not found",
                plan.old_info.table_id
            )));
        };
        let mut table = (*table).clone();
        table.on_bg_deleted(&plan.old_info);
        self.store().write_batch(vec![plan.op.clone()])?;
        self.bg_manager().remove_bg(&plan.old_info);
        self.refresh_bg_index(table);
        Ok(ApplyOutcome::Applied)
    }

    /// Commit a prepared single-BG update. When `bump_table_epoch` is set the
    /// table row and BG are persisted atomically in one `write_batch` (the row
    /// carries the new epoch); otherwise only the BG is persisted and the
    /// table's in-memory index is refreshed without an epoch bump.
    fn commit_bg_update(
        &self,
        plan: PreparedBGUpdate,
        bump_table_epoch: bool,
    ) -> FsResult<ApplyOutcome> {
        let Some(table) = self.get_table(plan.new_info.table_id) else {
            return Ok(ApplyOutcome::not_found(format!(
                "table {} not found",
                plan.new_info.table_id
            )));
        };
        let mut table = (*table).clone();
        table.matches_bg(&plan.new_info)?;
        table.on_bg_updated(&plan.old_info, &plan.new_info);

        if bump_table_epoch {
            table.bump_epoch();
            self.store()
                .write_batch(vec![self.store().table_put_op(&table)?, plan.op.clone()])?;
        } else {
            self.store().write_batch(vec![plan.op.clone()])?;
        }
        self.bg_manager()
            .update_bg(&plan.old_info, plan.new_info.clone());
        self.bg_manager()
            .cleanup_isr_penalties(&plan.old_info, &plan.new_info);
        // Epoch bumped → this is a client-visible table change; otherwise it is
        // just a runtime BG-index refresh.
        if bump_table_epoch {
            self.apply_update_table(table);
        } else {
            self.refresh_bg_index(table);
        }
        Ok(ApplyOutcome::Applied)
    }

    // ---- table lifecycle (two-phase: prepare a plan, commit  it) ---

    fn prepare_create_table(
        &self,
        mut table: BGTable,
        bgs: &[BlockGroupInfo],
    ) -> FsResult<PrepareTablesResult> {
        if self.get_table(table.table_id()).is_some() {
            return Ok(PrepareTablesResult::Outcome(ApplyOutcome::stale(format!(
                "table {} already exists",
                table.table_id()
            ))));
        }

        let mut bg_ops = Vec::with_capacity(bgs.len());
        let mut created = Vec::with_capacity(bgs.len());
        for bg in bgs {
            table.matches_bg(bg)?;
            let prepared = match self.bg_manager().prepare_create_bg(bg)? {
                PrepareCreateResult::Applied(p) => p,
                PrepareCreateResult::Outcome(outcome) => {
                    return Ok(PrepareTablesResult::Outcome(outcome))
                }
            };
            table.on_bg_created(&prepared.info);
            bg_ops.push(prepared.op);
            created.push(prepared.info);
        }

        // Table put first, then BG puts (independent keys; order is cosmetic).
        let mut ops = Vec::with_capacity(bg_ops.len() + 1);
        ops.push(self.store().table_put_op(&table)?);
        ops.extend(bg_ops);

        Ok(PrepareTablesResult::Applied(PreparedTables::with_bgs(
            ops,
            vec![table],
            created,
        )))
    }

    /// Plan a table-level update that changes client-visible behaviour, bumping
    /// each affected table's epoch. Currently this is the `cache_replica_policy`
    /// rewrite across a namespace's tables — a Hash concept, so the default is a
    /// no-op plan and `HashBGTableControl` overrides it. Two-phase (method A):
    /// returns the plan; the namespace manager commits it atomically with the
    /// namespace row, then calls `commit_prepared_tables`.
    fn prepare_update_table(
        &self,
        _namespace_id: NamespaceId,
        _policy: &CacheReplicaPolicy,
    ) -> FsResult<PreparedTables> {
        Ok(PreparedTables::default())
    }

    /// Plan deleting a table: a single table-delete op, no BG changes. Defined
    /// for interface completeness / future namespace-drop support; not yet
    /// driven by a caller. Returns a stale outcome if the table is absent so a
    /// replay is a no-op.
    fn prepare_delete_table(&self, table_id: TableId) -> FsResult<PrepareTablesResult> {
        let Some(table) = self.get_table(table_id) else {
            return Ok(PrepareTablesResult::Outcome(ApplyOutcome::stale(format!(
                "table {} already absent",
                table_id
            ))));
        };
        let ops = vec![self.store().table_delete_op(table_id)];
        Ok(PrepareTablesResult::Applied(PreparedTables::table_only(
            ops,
            vec![(*table).clone()],
        )))
    }

    /// Install the in-memory tables from a committed `prepare_update_table`
    /// (each already carries its bumped epoch).
    fn commit_prepared_tables(&self, mut plan: PreparedTables) {
        for table in plan.take_tables() {
            self.apply_update_table(table);
        }
    }
}
