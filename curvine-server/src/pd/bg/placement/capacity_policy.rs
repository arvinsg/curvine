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
use super::policy::{
    build_effective_counts, classify_replica, compute_equal_quota, filter_bg_targets,
    filter_lease_targets, should_rebalance_bg, should_rebalance_lease, BalancePolicy, PolicyState,
    ReplicaDecision,
};
use curvine_common::state::BlockGroupInfo;
use curvine_common::FsResult;
use std::collections::{HashMap, HashSet};

/// Capacity-weighted balance policy.
///
/// BG quota: proportional to worker disk capacity (`total_slots × capacity_i / total_capacity`).
/// Lease quota: equal-weight (`bucket_count / num_workers`), same as QuotaBalancePolicy.
pub struct CapacityBalancePolicy;

impl CapacityBalancePolicy {
    pub fn new() -> Self {
        Self
    }
}

impl Default for CapacityBalancePolicy {
    fn default() -> Self {
        Self::new()
    }
}

fn compute_capacity_weighted_quota(
    ctx: &PlacementContext<'_>,
    total_slots: u32,
) -> HashMap<u32, u32> {
    if ctx.workers.is_empty() {
        return HashMap::new();
    }

    let total_capacity: u64 = ctx
        .workers
        .values()
        .map(|snap| snap.capacity_bytes)
        .sum();

    if total_capacity == 0 {
        let worker_ids: Vec<u32> = ctx.worker_ids();
        return compute_equal_quota(&worker_ids, total_slots);
    }

    let t = total_slots as f64;
    let c = total_capacity as f64;

    ctx.workers
        .iter()
        .map(|(&wid, snap)| {
            let quota = if snap.capacity_bytes == 0 {
                0 // Not yet reported, don't allocate.
            } else {
                (t * (snap.capacity_bytes as f64 / c)).round() as u32
            };
            (wid, quota)
        })
        .collect()
}

impl BalancePolicy for CapacityBalancePolicy {
    fn name(&self) -> &str {
        "capacity"
    }

    fn prepare(&self, ctx: &PlacementContext<'_>) -> FsResult<PolicyState> {
        let total_slots = ctx.total_bg_slots();

        // BG quota: capacity-weighted.
        let worker_bg_quota = compute_capacity_weighted_quota(ctx, total_slots);

        // Lease quota: equal-weight (same as QuotaBalancePolicy).
        let worker_ids: Vec<u32> = ctx.worker_ids();
        let worker_lease_quota = compute_equal_quota(&worker_ids, ctx.bucket_count);

        let (worker_bg_effective, worker_lease_effective) = build_effective_counts(ctx);

        Ok(PolicyState {
            worker_bg_quota,
            worker_lease_quota,
            worker_bg_effective,
            worker_lease_effective,
            bg_load_score: HashMap::new(),
            lease_load_score: HashMap::new(),
        })
    }

    fn classify_replica(
        &self,
        ctx: &PlacementContext<'_>,
        st: &PolicyState,
        bg: &BlockGroupInfo,
        pos: usize,
        live_workers: &HashSet<u32>,
    ) -> ReplicaDecision {
        classify_replica(ctx, st, bg, pos, live_workers)
    }

    fn should_rebalance_bg_from(
        &self,
        ctx: &PlacementContext<'_>,
        st: &PolicyState,
        worker_id: u32,
    ) -> bool {
        should_rebalance_bg(ctx, st, worker_id)
    }

    fn should_rebalance_lease_from(
        &self,
        ctx: &PlacementContext<'_>,
        st: &PolicyState,
        worker_id: u32,
    ) -> bool {
        should_rebalance_lease(ctx, st, worker_id)
    }

    fn filter_bg_targets(
        &self,
        ctx: &PlacementContext<'_>,
        st: &PolicyState,
        candidate_ids: &[u32],
        exclude: &HashSet<u32>,
    ) -> Vec<u32> {
        filter_bg_targets(ctx, st, candidate_ids, exclude)
    }

    fn filter_lease_targets(
        &self,
        ctx: &PlacementContext<'_>,
        st: &PolicyState,
        candidate_ids: &[u32],
    ) -> Vec<u32> {
        filter_lease_targets(ctx, st, candidate_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::bg::placement::context::WorkerLoadSnapshot;

    fn make_snapshot(worker_id: u32, actual_bg: u32, capacity_bytes: u64) -> WorkerLoadSnapshot {
        WorkerLoadSnapshot {
            worker_id,
            actual_bg,
            actual_lease: 0,
            pending_bg_add: 0,
            pending_bg_remove: 0,
            pending_lease_in: 0,
            pending_lease_out: 0,
            capacity_bytes,
            used_bytes: 0,
            labels: HashMap::new(),
        }
    }

    #[test]
    fn test_capacity_weighted_quota_heterogeneous() {
        // A=4TB, B=4TB, C=2TB
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 0, 4_000_000));
        workers.insert(2, make_snapshot(2, 0, 4_000_000));
        workers.insert(3, make_snapshot(3, 0, 2_000_000));

        let ctx = PlacementContext {
            workers: &workers,
            bucket_count: 8,
            replica_count: 3,
            tolerant_ratio: 0.1,
            lease_tolerant_ratio: 0.1,
        };

        let policy = CapacityBalancePolicy::new();
        let st = policy.prepare(&ctx).unwrap();

        // A and B (equal capacity) should get equal quota, both > C.
        assert_eq!(st.worker_bg_quota[&1], st.worker_bg_quota[&2]);
        assert!(st.worker_bg_quota[&1] > st.worker_bg_quota[&3]);
    }

    #[test]
    fn test_capacity_weighted_quota_equal_capacity() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 0, 1000));
        workers.insert(2, make_snapshot(2, 0, 1000));
        workers.insert(3, make_snapshot(3, 0, 1000));

        let ctx = PlacementContext {
            workers: &workers,
            bucket_count: 8,
            replica_count: 2,
            tolerant_ratio: 0.1,
            lease_tolerant_ratio: 0.1,
        };

        let policy = CapacityBalancePolicy::new();
        let st = policy.prepare(&ctx).unwrap();

        // Equal capacity → equal quota (± rounding).
        let max = *st.worker_bg_quota.values().max().unwrap();
        let min = *st.worker_bg_quota.values().min().unwrap();
        assert!(max - min <= 1);
    }

    #[test]
    fn test_capacity_zero_worker_gets_zero_quota() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 0, 1000));
        workers.insert(2, make_snapshot(2, 0, 0)); // not yet reported

        let ctx = PlacementContext {
            workers: &workers,
            bucket_count: 4,
            replica_count: 2,
            tolerant_ratio: 0.1,
            lease_tolerant_ratio: 0.1,
        };

        let policy = CapacityBalancePolicy::new();
        let st = policy.prepare(&ctx).unwrap();

        // Worker 1 should get all 8 slots, worker 2 gets 0.
        assert_eq!(st.worker_bg_quota[&1], 8);
        assert_eq!(st.worker_bg_quota[&2], 0);
    }

    #[test]
    fn test_lease_quota_still_equal() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 0, 4000));
        workers.insert(2, make_snapshot(2, 0, 1000));

        let ctx = PlacementContext {
            workers: &workers,
            bucket_count: 8,
            replica_count: 2,
            tolerant_ratio: 0.1,
            lease_tolerant_ratio: 0.1,
        };

        let policy = CapacityBalancePolicy::new();
        let st = policy.prepare(&ctx).unwrap();

        // Lease quota should be equal regardless of capacity.
        assert_eq!(st.worker_lease_quota[&1], 4);
        assert_eq!(st.worker_lease_quota[&2], 4);
    }

    #[test]
    fn test_should_rebalance_with_capacity_quota() {
        // Worker 1: capacity 4TB, quota ~6.4→6, actual 8 → over quota
        // Worker 2: capacity 1TB, quota ~1.6→2, actual 0 → not over quota
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 8, 4000));
        workers.insert(2, make_snapshot(2, 0, 1000));

        let ctx = PlacementContext {
            workers: &workers,
            bucket_count: 4,
            replica_count: 2, // total=8
            tolerant_ratio: 0.1,
            lease_tolerant_ratio: 0.1,
        };

        let policy = CapacityBalancePolicy::new();
        let st = policy.prepare(&ctx).unwrap();

        // Worker 1 quota ≈ 6, tolerant ≈ 1, threshold ≈ 7, actual 8 > 7 → rebalance
        assert!(policy.should_rebalance_bg_from(&ctx, &st, 1));
        assert!(!policy.should_rebalance_bg_from(&ctx, &st, 2));
    }
}
