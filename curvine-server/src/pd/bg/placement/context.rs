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
use crate::pd::pool::PoolManager;
use curvine_common::state::{BgId, BlockGroupInfo, StorageType, TableId};
use std::collections::{HashMap, HashSet};

/// Per-worker load snapshot for one table, constructed by the scheduler layer.
#[derive(Debug, Clone)]
pub struct WorkerLoadSnapshot {
    pub worker_id: u32,

    /// Actual BG count on this worker for the current table.
    pub actual_bg: u32,
    /// Actual primary count on this worker for the current table.
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

pub struct PlacementContext<'a> {
    /// Worker load snapshots keyed by worker_id.
    pub workers: &'a HashMap<u32, WorkerLoadSnapshot>,
    /// Bucket count of the current table.
    pub bucket_count: u32,
    /// Replica count of the current table.
    pub replica_count: u16,

    /// BG overload tolerance ratio.
    pub tolerant_ratio: f64,
    /// Primary overload tolerance ratio.
    pub primary_tolerant_ratio: f64,
}

impl<'a> PlacementContext<'a> {
    /// Total BG slots for this table.
    pub fn total_bg_slots(&self) -> u32 {
        self.bucket_count * self.replica_count as u32
    }

    /// Number of workers in the snapshot.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Get worker labels map (worker_id -> labels).
    pub fn worker_labels(&self) -> HashMap<u32, HashMap<String, String>> {
        self.workers
            .iter()
            .map(|(&wid, snap)| (wid, snap.labels.clone()))
            .collect()
    }

    /// Get all worker IDs.
    pub fn worker_ids(&self) -> Vec<u32> {
        self.workers.keys().copied().collect()
    }
}

/// Build a per-table worker load snapshot with operator influence.
pub fn build_table_snapshot(
    table_id: TableId,
    bg_manager: &BGManager,
    pool_manager: &PoolManager,
    influence: &PendingInfluence,
    media: StorageType,
) -> HashMap<u32, WorkerLoadSnapshot> {
    let Some(table) = bg_manager.get_table(table_id) else {
        return HashMap::new();
    };
    let live_workers = pool_manager.get_live_workers(table.pool_type());
    let bucket_set: HashSet<BgId> = table.buckets().iter().copied().collect();
    let table_bgs: Vec<_> = bg_manager
        .list_bgs()
        .into_iter()
        .filter(|bg| bucket_set.contains(&bg.bg_id))
        .collect();

    live_workers
        .iter()
        .map(|&wid| {
            (
                wid,
                build_worker_snapshot(wid, &table_bgs, pool_manager, influence, media),
            )
        })
        .collect()
}

fn build_worker_snapshot(
    wid: u32,
    table_bgs: &[std::sync::Arc<BlockGroupInfo>],
    pool_manager: &PoolManager,
    influence: &PendingInfluence,
    media: StorageType,
) -> WorkerLoadSnapshot {
    let actual_bg = table_bgs
        .iter()
        .filter(|bg| bg.replica_set.contains(&wid))
        .count() as u32;

    let actual_primary = table_bgs
        .iter()
        .filter(|bg| bg.primary.as_ref().map(|l| l.node_id) == Some(wid))
        .count() as u32;

    let (pending_bg_add, pending_bg_remove) =
        influence.bg_delta.get(&wid).copied().unwrap_or((0, 0));
    let (pending_primary_in, pending_primary_out) =
        influence.primary_delta.get(&wid).copied().unwrap_or((0, 0));

    let labels = pool_manager.get_worker_labels(wid).unwrap_or_default();
    let (capacity, used) = pool_manager
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
        capacity_bytes: capacity as u64,
        used_bytes: used as u64,
        labels,
    }
}
