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

use super::BGTableManager;
use curvine_common::state::{BgId, BlockGroupInfo};
use curvine_common::FsResult;
use std::collections::HashMap;
use std::sync::Arc;

impl BGTableManager {
    /// Restore the whole BG world from the KV store, in order: restore the
    /// underlying BG metadata (which already resets per-replica runtime state),
    /// then reload the BGTables and rebuild their in-memory indexes against the
    /// restored BGs. The caller (startup / snapshot-apply) only needs this single
    /// entry; `BGManager` stays encapsulated behind `BGTableManager`.
    pub fn restore(&self) -> FsResult<()> {
        self.bg_manager.restore()?;
        self.restore_tables(&self.bg_manager.snapshot_all_bgs())
    }

    /// Reset per-replica runtime state on the underlying BGs (leader-start).
    pub fn reset_replica_states(&self) {
        self.bg_manager.reset_replica_states();
    }

    /// Load all BGTables from the KV store, rebuild their in-memory indexes
    /// against the restored BGs, and heal the BG id allocator floor.
    ///
    /// Namespace create commits the namespace, its BGTables and their BGs in a
    /// single RocksDB batch, so there are no orphan tables/BGs to reconcile at
    /// startup — restore simply reloads what was durably committed.
    fn restore_tables(&self, bgs: &HashMap<BgId, Arc<BlockGroupInfo>>) -> FsResult<()> {
        let mut tables = self.store.list_tables()?;
        for table in &mut tables {
            self.rebuild_table_indexes(table, bgs);
        }
        let (hash_tables, capacity_tables) = Self::split_tables(tables);
        self.hash.restore_tables(hash_tables);
        self.capacity.restore_tables(capacity_tables);

        self.heal_next_bg_id(bgs)?;
        Ok(())
    }

    /// Ensure the BG id allocator floor never sits below the highest restored
    /// BG id, so a fresh allocation can't collide with an existing BG. This is
    /// a self-heal safety net for the allocator counter.
    fn heal_next_bg_id(&self, bgs: &HashMap<BgId, Arc<BlockGroupInfo>>) -> FsResult<()> {
        let floor = bgs.keys().copied().max().unwrap_or(0).saturating_add(1);
        if let Some((from, to)) = self.bg_manager.ensure_next_id_at_least(floor)? {
            log::warn!("bgtable restore: healed next_bg_id from {} to {}", from, to);
        }
        Ok(())
    }
}
