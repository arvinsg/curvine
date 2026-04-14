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

use std::collections::HashMap;

/// Per-worker load snapshot for one table, constructed by the scheduler layer.
#[derive(Debug, Clone)]
pub struct WorkerLoadSnapshot {
    pub worker_id: u32,

    /// Actual BG count on this worker for the current table.
    pub actual_bg: u32,
    /// Actual lease owner count on this worker for the current table.
    pub actual_lease: u32,

    /// In-flight AddReplica count from Operator.
    pub pending_bg_add: u32,
    /// In-flight RemoveReplica count from Operator.
    pub pending_bg_remove: u32,
    /// In-flight lease transfer in count.
    pub pending_lease_in: u32,
    /// In-flight lease transfer out count.
    pub pending_lease_out: u32,

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

    pub fn effective_lease(&self) -> i64 {
        self.actual_lease as i64 + self.pending_lease_in as i64 - self.pending_lease_out as i64
    }

    pub fn available_bytes(&self) -> u64 {
        self.capacity_bytes.saturating_sub(self.used_bytes)
    }
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
    /// Lease overload tolerance ratio.
    pub lease_tolerant_ratio: f64,
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
