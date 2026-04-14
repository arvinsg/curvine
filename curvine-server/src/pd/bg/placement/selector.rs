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

use super::context::PlacementContext;
use super::policy::PolicyState;
use curvine_common::FsResult;
use std::collections::{HashMap, HashSet};

/// Worker candidate with pre-computed scoring inputs.
#[derive(Debug, Clone)]
pub struct WorkerCandidate {
    pub worker_id: u32,
    pub bg_count: u32,
    pub lease_count: u32,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub labels: HashMap<String, String>,
}

/// Worker selection strategy — picks from a pre-filtered legal candidate set.
pub trait WorkerSelector: Send + Sync {
    fn name(&self) -> &str;

    fn init_from_policy(&mut self, ctx: &PlacementContext<'_>, st: &PolicyState);

    fn init(&mut self, candidates: &[WorkerCandidate], bucket_count: u32, replica_count: u16) {
        let workers: HashMap<u32, super::context::WorkerLoadSnapshot> = candidates
            .iter()
            .map(|c| {
                (
                    c.worker_id,
                    super::context::WorkerLoadSnapshot {
                        worker_id: c.worker_id,
                        actual_bg: c.bg_count,
                        actual_lease: c.lease_count,
                        pending_bg_add: 0,
                        pending_bg_remove: 0,
                        pending_lease_in: 0,
                        pending_lease_out: 0,
                        capacity_bytes: c.capacity_bytes,
                        used_bytes: c.used_bytes,
                        labels: c.labels.clone(),
                    },
                )
            })
            .collect();
        let ctx = PlacementContext {
            workers: &workers,
            bucket_count,
            replica_count,
            tolerant_ratio: 1.0,
            lease_tolerant_ratio: 1.0,
        };
        // Use a simple PolicyState with equal quotas
        let worker_ids: Vec<u32> = candidates.iter().map(|c| c.worker_id).collect();
        let total_slots = bucket_count * replica_count as u32;
        let n = worker_ids.len() as u32;
        let (bg_base, bg_rem) = if n > 0 {
            (total_slots / n, total_slots % n)
        } else {
            (0, 0)
        };
        let (lease_base, lease_rem) = if n > 0 {
            (bucket_count / n, bucket_count % n)
        } else {
            (0, 0)
        };
        let st = PolicyState {
            worker_bg_quota: worker_ids
                .iter()
                .enumerate()
                .map(|(i, &w)| (w, bg_base + if (i as u32) < bg_rem { 1 } else { 0 }))
                .collect(),
            worker_lease_quota: worker_ids
                .iter()
                .enumerate()
                .map(|(i, &w)| (w, lease_base + if (i as u32) < lease_rem { 1 } else { 0 }))
                .collect(),
            worker_bg_effective: candidates
                .iter()
                .map(|c| (c.worker_id, c.bg_count as i64))
                .collect(),
            worker_lease_effective: candidates
                .iter()
                .map(|c| (c.worker_id, c.lease_count as i64))
                .collect(),
            bg_load_score: HashMap::new(),
            lease_load_score: HashMap::new(),
        };
        self.init_from_policy(&ctx, &st);
    }

    /// Select `count` workers.
    fn select(
        &mut self,
        st: &PolicyState,
        candidate_ids: &[u32],
        count: usize,
        exclude: &HashSet<u32>,
    ) -> FsResult<Vec<u32>>;

    /// Select a lease owner.
    fn select_lease_owner(&mut self, st: &PolicyState, candidate_ids: &[u32]) -> FsResult<u32>;
}
