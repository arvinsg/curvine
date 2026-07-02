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

use crate::pd::bg::{
    BGManager, PrepareCreateResult, PrepareDeleteResult, PrepareUpdateResult, PreparedBGCreate,
    PreparedBGDelete, PreparedBGUpdate,
};
use crate::pd::bgtable::{BGTable, BGTableStore};
use crate::pd::journal::entry::{BGDeleteEntry, BGUpdateEntry};
use crate::pd::journal::ApplyOutcome;
use crate::pd::store::KvWrite;
use curvine_common::state::{BlockGroupInfo, TableId};
use curvine_common::FsResult;
use std::sync::Arc;

/// A validated single-BG change, ready to commit against its owning table.
pub enum PreparedBGChange {
    Create(PreparedBGCreate),
    Update(PreparedBGUpdate),
    Delete(PreparedBGDelete),
}

impl PreparedBGChange {
    /// The table that owns the BG being changed.
    pub fn table_id(&self) -> TableId {
        match self {
            PreparedBGChange::Create(p) => p.info.table_id,
            PreparedBGChange::Update(p) => p.new_info.table_id,
            PreparedBGChange::Delete(p) => p.old_info.table_id,
        }
    }
}

/// Result of preparing a single-BG change: either a validated change to commit,
/// or a short-circuit outcome (stale/noop/not-found) with nothing to do.
pub enum PrepareChangeResult {
    Ready(PreparedBGChange),
    Outcome(ApplyOutcome),
}

/// A set of table/BG writes prepared for a namespace-scoped batch.
#[derive(Default)]
pub struct PreparedTables {
    pub ops: Vec<KvWrite>,
    pub(crate) tables: Vec<BGTable>,
    pub(crate) bgs: Vec<BlockGroupInfo>,
}

impl PreparedTables {
    pub(crate) fn merge(&mut self, other: PreparedTables) {
        self.ops.extend(other.ops);
        self.tables.extend(other.tables);
        self.bgs.extend(other.bgs);
    }

    /// Take the mutated tables out for the caller to install into its registry.
    pub fn take_tables(&mut self) -> Vec<BGTable> {
        std::mem::take(&mut self.tables)
    }

    /// Take the created BGs out for the caller to install into the BG index.
    pub fn take_bgs(&mut self) -> Vec<BlockGroupInfo> {
        std::mem::take(&mut self.bgs)
    }
}

/// Result of preparing a namespace batch: either the prepared tables, or a
/// short-circuit outcome before anything is committed.
pub enum PrepareTablesResult {
    Applied(PreparedTables),
    Outcome(ApplyOutcome),
}

pub struct BGMutator {
    store: Arc<BGTableStore>,
    bg_manager: Arc<BGManager>,
}

impl BGMutator {
    pub fn new(store: Arc<BGTableStore>, bg_manager: Arc<BGManager>) -> Self {
        Self { store, bg_manager }
    }

    pub fn prepare_create(&self, info: &BlockGroupInfo) -> FsResult<PrepareChangeResult> {
        Ok(match self.bg_manager.prepare_create_bg(info)? {
            PrepareCreateResult::Applied(p) => {
                PrepareChangeResult::Ready(PreparedBGChange::Create(p))
            }
            PrepareCreateResult::Outcome(o) => PrepareChangeResult::Outcome(o),
        })
    }

    pub fn prepare_update(&self, entry: &BGUpdateEntry) -> FsResult<PrepareChangeResult> {
        Ok(match self.bg_manager.prepare_update_bg(entry)? {
            PrepareUpdateResult::Applied(p) => {
                PrepareChangeResult::Ready(PreparedBGChange::Update(p))
            }
            PrepareUpdateResult::Outcome(o) => PrepareChangeResult::Outcome(o),
        })
    }

    pub fn prepare_delete(&self, entry: &BGDeleteEntry) -> FsResult<PrepareChangeResult> {
        Ok(match self.bg_manager.prepare_delete_bg(entry)? {
            PrepareDeleteResult::Applied(p) => {
                PrepareChangeResult::Ready(PreparedBGChange::Delete(p))
            }
            PrepareDeleteResult::Outcome(o) => PrepareChangeResult::Outcome(o),
        })
    }

    pub fn commit_change(&self, mut table: BGTable, change: PreparedBGChange) -> FsResult<BGTable> {
        let op = match change {
            PreparedBGChange::Create(p) => {
                table.matches_bg(&p.info)?;
                table.on_bg_created(&p.info);
                let op = p.op;
                self.bg_manager.insert_bg(p.info);
                op
            }
            PreparedBGChange::Update(p) => {
                table.matches_bg(&p.new_info)?;
                table.on_bg_updated(&p.old_info, &p.new_info);
                self.bg_manager.update_bg(&p.old_info, p.new_info.clone());
                self.bg_manager
                    .cleanup_isr_penalties(&p.old_info, &p.new_info);
                p.op
            }
            PreparedBGChange::Delete(p) => {
                table.on_bg_deleted(&p.old_info);
                self.bg_manager.remove_bg(&p.old_info);
                p.op
            }
        };
        table.bump_epoch();
        self.store
            .write_batch(vec![self.store.table_put_op(&table)?, op])?;
        Ok(table)
    }

    // TODO:
    // ---- namespace batch: prepare a table + its BGs, install after commit --

    /// Prepare the install of one freshly-built table and its BGs, without
    /// touching the store or in-memory maps. Validates each BG matches the
    /// table, prepares it (producing its KV op), lets the table reflect it, and
    /// returns the ops + objects for the caller to commit + install.
    pub fn prepare_install(
        &self,
        mut table: BGTable,
        bgs: &[BlockGroupInfo],
    ) -> FsResult<PrepareTablesResult> {
        let mut bg_ops = Vec::with_capacity(bgs.len());
        let mut created = Vec::with_capacity(bgs.len());
        for bg in bgs {
            table.matches_bg(bg)?;
            let plan = match self.bg_manager.prepare_create_bg(bg)? {
                PrepareCreateResult::Applied(plan) => plan,
                PrepareCreateResult::Outcome(outcome) => {
                    return Ok(PrepareTablesResult::Outcome(outcome))
                }
            };
            table.on_bg_created(&plan.info);
            bg_ops.push(plan.op);
            created.push(plan.info);
        }

        // Table put first, then BG puts (independent keys; order is cosmetic).
        let mut ops = Vec::with_capacity(bg_ops.len() + 1);
        ops.push(self.store.table_put_op(&table)?);
        ops.extend(bg_ops);

        Ok(PrepareTablesResult::Applied(PreparedTables {
            ops,
            tables: vec![table],
            bgs: created,
        }))
    }

    // TODO
    /// Prepare the persist of already-mutated tables (no BG changes): one
    /// table-put op per table. Used by metadata rewrites like policy propagation.
    pub fn prepare_table_rewrites(&self, tables: Vec<BGTable>) -> FsResult<PreparedTables> {
        let mut ops = Vec::with_capacity(tables.len());
        for table in &tables {
            ops.push(self.store.table_put_op(table)?);
        }
        Ok(PreparedTables {
            ops,
            tables,
            bgs: Vec::new(),
        })
    }
}
