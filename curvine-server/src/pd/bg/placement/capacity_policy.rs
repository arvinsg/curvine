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
    build_effective_counts, classify_replica, compute_equal_quota, is_bg_overloaded,
    is_lease_overloaded, select_lease_by_hunger, PlacementPolicy, PolicyState, ReplicaDecision,
};
use curvine_common::state::BlockGroupInfo;
use curvine_common::{FsError, FsResult};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

/// Capacity-weighted placement policy.
///
/// BG quota: proportional to worker disk capacity.
/// Lease quota: equal-weight.
/// Selection: weighted-random blending BG hunger + capacity proportion.
pub struct CapacityPolicy {
    bg_weight: f64,
    capacity_weight: f64,
    rng: Mutex<StdRng>,
}

impl CapacityPolicy {
    pub fn new() -> Self {
        Self {
            bg_weight: 0.6,
            capacity_weight: 0.4,
            rng: Mutex::new(StdRng::from_entropy()),
        }
    }

    pub fn with_seed(seed: u64) -> Self {
        Self {
            bg_weight: 0.6,
            capacity_weight: 0.4,
            rng: Mutex::new(StdRng::seed_from_u64(seed)),
        }
    }

    fn compute_weight(&self, wid: u32, st: &PolicyState, ctx: &PlacementContext<'_>) -> f64 {
        let effective = st
            .worker_bg_effective
            .get(&wid)
            .copied()
            .unwrap_or(0)
            .max(0) as u32;
        let quota = st.worker_bg_quota.get(&wid).copied().unwrap_or(0);

        let bg_score = if quota == 0 {
            0.0
        } else {
            (quota as f64 - effective as f64) / quota as f64
        };

        let cap = ctx
            .workers
            .get(&wid)
            .map(|s| s.available_bytes())
            .unwrap_or(0) as f64;
        let total_cap: f64 = ctx
            .workers
            .values()
            .map(|s| s.available_bytes() as f64)
            .sum();
        let cap_score = if total_cap == 0.0 {
            1.0
        } else {
            cap / total_cap
        };

        (self.bg_weight * bg_score + self.capacity_weight * cap_score).max(0.01)
    }
}

impl Default for CapacityPolicy {
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

    let total_capacity: u64 = ctx.workers.values().map(|s| s.capacity_bytes).sum();

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
                0
            } else {
                (t * (snap.capacity_bytes as f64 / c)).round() as u32
            };
            (wid, quota)
        })
        .collect()
}

impl PlacementPolicy for CapacityPolicy {
    fn name(&self) -> &str {
        "capacity"
    }

    fn prepare(&self, ctx: &PlacementContext<'_>) -> FsResult<PolicyState> {
        let total_slots = ctx.total_bg_slots();
        let worker_bg_quota = compute_capacity_weighted_quota(ctx, total_slots);
        let worker_ids: Vec<u32> = ctx.worker_ids();
        let worker_lease_quota = compute_equal_quota(&worker_ids, ctx.bucket_count);
        let (worker_bg_effective, worker_lease_effective) = build_effective_counts(ctx);

        Ok(PolicyState {
            worker_bg_quota,
            worker_lease_quota,
            worker_bg_effective,
            worker_lease_effective,
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

    fn is_bg_overloaded(&self, ctx: &PlacementContext<'_>, st: &PolicyState, wid: u32) -> bool {
        is_bg_overloaded(ctx, st, wid)
    }

    fn is_lease_overloaded(&self, ctx: &PlacementContext<'_>, st: &PolicyState, wid: u32) -> bool {
        is_lease_overloaded(ctx, st, wid)
    }

    fn select_bg_targets(
        &self,
        ctx: &PlacementContext<'_>,
        st: &PolicyState,
        candidates: &[u32],
        count: usize,
        exclude: &HashSet<u32>,
    ) -> FsResult<Vec<u32>> {
        let weighted: Vec<(u32, f64)> = candidates
            .iter()
            .copied()
            .filter(|wid| !exclude.contains(wid))
            .map(|wid| (wid, self.compute_weight(wid, st, ctx)))
            .collect();

        if weighted.is_empty() {
            return Err(FsError::common("no eligible BG target".to_string()));
        }

        let mut rng = self.rng.lock().unwrap();
        let mut total_weight: f64 = weighted.iter().map(|(_, w)| w).sum();
        let mut selected: Vec<u32> = Vec::with_capacity(count);
        let mut selected_set: HashSet<u32> = HashSet::with_capacity(count);

        for _ in 0..count {
            if total_weight <= 0.0 {
                break;
            }
            let r = rng.gen_range(0.0..total_weight);
            let mut cumulative = 0.0;
            let mut picked = None;

            for &(wid, weight) in &weighted {
                if selected_set.contains(&wid) {
                    continue;
                }
                cumulative += weight;
                if cumulative >= r {
                    picked = Some((wid, weight));
                    break;
                }
            }

            match picked {
                Some((wid, weight)) => {
                    selected.push(wid);
                    selected_set.insert(wid);
                    total_weight -= weight;
                }
                None => break,
            }
        }

        if selected.is_empty() {
            return Err(FsError::common("no eligible BG target".to_string()));
        }
        Ok(selected)
    }

    fn select_lease_owner(&self, st: &PolicyState, candidates: &[u32]) -> FsResult<u32> {
        select_lease_by_hunger(st, candidates)
            .ok_or_else(|| FsError::common("no eligible lease owner".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::bg::placement::WorkerLoadSnapshot;

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

        let policy = CapacityPolicy::new();
        let st = policy.prepare(&ctx).unwrap();

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

        let policy = CapacityPolicy::new();
        let st = policy.prepare(&ctx).unwrap();

        let max = *st.worker_bg_quota.values().max().unwrap();
        let min = *st.worker_bg_quota.values().min().unwrap();
        assert!(max - min <= 1);
    }

    #[test]
    fn test_capacity_zero_worker_gets_zero_quota() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 0, 1000));
        workers.insert(2, make_snapshot(2, 0, 0));

        let ctx = PlacementContext {
            workers: &workers,
            bucket_count: 4,
            replica_count: 2,
            tolerant_ratio: 0.1,
            lease_tolerant_ratio: 0.1,
        };

        let policy = CapacityPolicy::new();
        let st = policy.prepare(&ctx).unwrap();

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

        let policy = CapacityPolicy::new();
        let st = policy.prepare(&ctx).unwrap();

        assert_eq!(st.worker_lease_quota[&1], 4);
        assert_eq!(st.worker_lease_quota[&2], 4);
    }

    #[test]
    fn test_should_rebalance_with_capacity_quota() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 8, 4000));
        workers.insert(2, make_snapshot(2, 0, 1000));

        let ctx = PlacementContext {
            workers: &workers,
            bucket_count: 4,
            replica_count: 2,
            tolerant_ratio: 0.1,
            lease_tolerant_ratio: 0.1,
        };

        let policy = CapacityPolicy::new();
        let st = policy.prepare(&ctx).unwrap();

        assert!(policy.is_bg_overloaded(&ctx, &st, 1));
        assert!(!policy.is_bg_overloaded(&ctx, &st, 2));
    }
}
