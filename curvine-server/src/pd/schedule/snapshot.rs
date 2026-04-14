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

//! Snapshot construction for the scheduler layer.
//!
//! Builds per-table `WorkerLoadSnapshot` maps from BGManager, PoolManager,
//! and OperatorController data. Consumed by placement::BalancePolicy and WorkerSelector.

use crate::pd::bg::placement::context::WorkerLoadSnapshot;
use crate::pd::bg::BGManager;
use crate::pd::pool::PoolManager;
use crate::pd::schedule::operator_controller::OperatorController;
use curvine_common::state::StorageType;
use std::collections::HashMap;

/// Build a per-table worker load snapshot for a specific table.
///
/// `table_id` identifies the table. Per-table BG/lease counts are computed
/// from `bg_manager`. Operator influence is added from `operator_controller`.
pub fn build_table_snapshot(
    table_id: u32,
    bg_manager: &BGManager,
    pool_manager: &PoolManager,
    operator_controller: Option<&OperatorController>,
    media: StorageType,
) -> HashMap<u32, WorkerLoadSnapshot> {
    let pool_id = (table_id >> 16) as u16;
    let live_workers = pool_manager.get_live_workers(pool_id);

    // Get table's BGs for per-table counting.
    let table_bgs = bg_manager
        .get_table(table_id)
        .map(|t| {
            let bgs_lock = bg_manager.list_bgs();
            let bucket_set: std::collections::HashSet<u32> =
                t.buckets.iter().copied().collect();
            bgs_lock
                .into_iter()
                .filter(|bg| bucket_set.contains(&bg.bg_id))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    live_workers
        .iter()
        .map(|&wid| {
            // Per-table BG count for this worker.
            let table_bg_count = table_bgs
                .iter()
                .filter(|bg| bg.replica_set.contains(&wid))
                .count() as u32;

            // Per-table lease count for this worker.
            let table_lease_count = table_bgs
                .iter()
                .filter(|bg| bg.lease_owner.as_ref().map(|l| l.node_id) == Some(wid))
                .count() as u32;

            // Operator influence (if available).
            let (pending_bg_add, pending_bg_remove) = operator_controller
                .map(|oc| oc.get_worker_pending_bg_delta(wid))
                .unwrap_or((0, 0));
            let (pending_lease_in, pending_lease_out) = operator_controller
                .map(|oc| oc.get_worker_pending_lease_delta(wid))
                .unwrap_or((0, 0));

            let labels = pool_manager.get_worker_labels(wid).unwrap_or_default();
            let (capacity, used) = pool_manager
                .get_worker_storage_stats(wid, media)
                .unwrap_or((0, 0));

            (
                wid,
                WorkerLoadSnapshot {
                    worker_id: wid,
                    actual_bg: table_bg_count,
                    actual_lease: table_lease_count,
                    pending_bg_add,
                    pending_bg_remove,
                    pending_lease_in,
                    pending_lease_out,
                    capacity_bytes: capacity as u64,
                    used_bytes: used as u64,
                    labels,
                },
            )
        })
        .collect()
}
