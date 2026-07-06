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

use crate::pd::bgtable::BGTableManager;
use crate::pd::pool::PoolManager;
use curvine_common::state::{BGKind, BgId, BlockGroupInfo, StorageType, TableId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Per-worker load snapshot for one placement target, constructed by the
/// scheduler / manager layer.
#[derive(Debug, Clone)]
pub struct WorkerLoadSnapshot {
    pub worker_id: u32,

    /// Actual BG count on this worker for the current placement target.
    pub actual_bg: u32,
    /// Actual primary count on this worker for the current placement target.
    pub actual_primary: u32,

    /// In-flight AddReplica count from Operator.
    pub pending_bg_add: u32,
    /// In-flight RemoveReplica count from Operator.
    pub pending_bg_remove: u32,
    /// In-flight primary transfer in count.
    pub pending_primary_in: u32,
    /// In-flight primary transfer out count.
    pub pending_primary_out: u32,

    /// Worker-level total disk capacity (not per-table).
    pub capacity_bytes: u64,
    /// Worker-level total used space (not per-table).
    pub used_bytes: u64,

    /// Worker labels (az, rack...).
    pub labels: HashMap<String, String>,
}

impl WorkerLoadSnapshot {
    pub fn effective_bg(&self) -> i64 {
        self.actual_bg as i64 + self.pending_bg_add as i64 - self.pending_bg_remove as i64
    }

    pub fn effective_primary(&self) -> i64 {
        self.actual_primary as i64 + self.pending_primary_in as i64
            - self.pending_primary_out as i64
    }

    pub fn available_bytes(&self) -> u64 {
        self.capacity_bytes.saturating_sub(self.used_bytes)
    }
}

/// Operator pending influence on workers.
#[derive(Debug, Clone, Default)]
pub struct PendingInfluence {
    /// worker_id → (pending_add, pending_remove)
    pub bg_delta: HashMap<u32, (u32, u32)>,
    /// worker_id → (pending_primary_in, pending_primary_out)
    pub primary_delta: HashMap<u32, (u32, u32)>,
}

/// Common placement context shared by Hash BG and Capacity BG placement.
pub struct PlacementContext<'a> {
    /// Worker load snapshots keyed by worker_id.
    pub workers: &'a HashMap<u32, WorkerLoadSnapshot>,
    /// BG overload tolerance ratio.
    pub tolerant_ratio: f64,
    /// Primary overload tolerance ratio.
    pub primary_tolerant_ratio: f64,
}

impl<'a> PlacementContext<'a> {
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub fn worker_labels(&self) -> HashMap<u32, HashMap<String, String>> {
        self.workers
            .iter()
            .map(|(&wid, snap)| (wid, snap.labels.clone()))
            .collect()
    }

    pub fn worker_ids(&self) -> Vec<u32> {
        self.workers.keys().copied().collect()
    }
}

/// Hash BG placement context. Hash placement is bucket based, so quota and
/// primary balancing are computed from `bucket_count * replica_count`.
pub struct HashPlacementContext<'a> {
    pub common: PlacementContext<'a>,
    pub table_id: TableId,
    pub bucket_count: u32,
    pub replica_count: u16,
}

impl<'a> HashPlacementContext<'a> {
    pub fn total_bg_slots(&self) -> u32 {
        self.bucket_count * self.replica_count as u32
    }

    pub fn workers(&self) -> &'a HashMap<u32, WorkerLoadSnapshot> {
        self.common.workers
    }

    pub fn worker_count(&self) -> usize {
        self.common.worker_count()
    }

    pub fn worker_labels(&self) -> HashMap<u32, HashMap<String, String>> {
        self.common.worker_labels()
    }

    pub fn worker_ids(&self) -> Vec<u32> {
        self.common.worker_ids()
    }

    pub fn tolerant_ratio(&self) -> f64 {
        self.common.tolerant_ratio
    }

    pub fn primary_tolerant_ratio(&self) -> f64 {
        self.common.primary_tolerant_ratio
    }
}

/// Build the per-worker snapshot map for one table over `live_workers`, counting
/// load from `table_bgs` (pass an empty slice for a fresh table, where all
/// counts are zero).
pub fn build_worker_snapshots(
    live_workers: &[u32],
    table_bgs: &[Arc<BlockGroupInfo>],
    pool_manager: &PoolManager,
    influence: &PendingInfluence,
    media: StorageType,
) -> HashMap<u32, WorkerLoadSnapshot> {
    live_workers
        .iter()
        .map(|&wid| {
            (
                wid,
                worker_snapshot(wid, table_bgs, pool_manager, influence, media),
            )
        })
        .collect()
}

/// One worker's load snapshot: actual BG/primary counts from `table_bgs`, plus
/// operator pending influence, labels, and storage stats.
fn worker_snapshot(
    wid: u32,
    table_bgs: &[Arc<BlockGroupInfo>],
    pool_manager: &PoolManager,
    influence: &PendingInfluence,
    media: StorageType,
) -> WorkerLoadSnapshot {
    let (actual_bg, actual_primary) = count_worker_load(table_bgs, wid);
    let (pending_bg_add, pending_bg_remove) =
        influence.bg_delta.get(&wid).copied().unwrap_or((0, 0));
    let (pending_primary_in, pending_primary_out) =
        influence.primary_delta.get(&wid).copied().unwrap_or((0, 0));
    let labels = pool_manager.get_worker_labels(wid).unwrap_or_default();
    let (capacity_bytes, used_bytes) = pool_manager
        .get_worker_storage_stats(wid, media)
        .unwrap_or((0, 0));
    WorkerLoadSnapshot {
        worker_id: wid,
        actual_bg,
        actual_primary,
        pending_bg_add,
        pending_bg_remove,
        pending_primary_in,
        pending_primary_out,
        capacity_bytes,
        used_bytes,
        labels,
    }
}

/// Count how many of `table_bgs` place a replica (and a primary) on `wid`.
fn count_worker_load(table_bgs: &[Arc<BlockGroupInfo>], wid: u32) -> (u32, u32) {
    let bg = table_bgs
        .iter()
        .filter(|b| b.replica_set.contains(&wid))
        .count() as u32;
    let primary = table_bgs
        .iter()
        .filter(|b| b.primary.node_id == wid)
        .count() as u32;
    (bg, primary)
}

/// Scheduler convenience: resolve a Hash table's live workers + existing BGs
/// from the managers, then build the snapshot map with operator influence.
pub fn build_hash_table_snapshot(
    table_id: TableId,
    bgtable_manager: &BGTableManager,
    pool_manager: &PoolManager,
    influence: &PendingInfluence,
    media: StorageType,
) -> HashMap<u32, WorkerLoadSnapshot> {
    let Some(table) = bgtable_manager.get_table(table_id) else {
        return HashMap::new();
    };
    let live_workers = pool_manager.get_live_workers(table.storage_type());
    let bucket_set: HashSet<BgId> = table
        .hash_table()
        .expect("hash table")
        .buckets()
        .iter()
        .copied()
        .collect();
    let table_bgs: Vec<Arc<BlockGroupInfo>> = bgtable_manager
        .bg()
        .list_bgs(BGKind::Hash, None)
        .into_iter()
        .filter(|bg| bucket_set.contains(&bg.bg_id))
        .collect();

    build_worker_snapshots(&live_workers, &table_bgs, pool_manager, influence, media)
}
