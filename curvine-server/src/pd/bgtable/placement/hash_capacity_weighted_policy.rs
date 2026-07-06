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

use super::context::HashPlacementContext;
use super::policy::{
    build_hash_effective_counts, compute_equal_quota, HashPlacementPolicy, HashPolicyState,
};
use curvine_common::{FsError, FsResult};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

/// Storage-weighted Hash BG placement policy.
///
/// Hash BG quota is weighted by worker storage capacity.
pub struct HashCapacityWeightedPolicy {
    bg_weight: f64,
    capacity_weight: f64,
    rng: Mutex<StdRng>,
}

impl HashCapacityWeightedPolicy {
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

    fn compute_weight(
        &self,
        wid: u32,
        st: &HashPolicyState,
        ctx: &HashPlacementContext<'_>,
    ) -> f64 {
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
            .workers()
            .get(&wid)
            .map(|s| s.available_bytes())
            .unwrap_or(0) as f64;
        let total_cap: f64 = ctx
            .workers()
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

impl Default for HashCapacityWeightedPolicy {
    fn default() -> Self {
        Self::new()
    }
}

fn compute_capacity_weighted_quota(
    ctx: &HashPlacementContext<'_>,
    total_slots: u32,
) -> HashMap<u32, u32> {
    if ctx.workers().is_empty() {
        return HashMap::new();
    }

    let total_capacity: u64 = ctx.workers().values().map(|s| s.capacity_bytes).sum();
    if total_capacity == 0 {
        return compute_equal_quota(&ctx.worker_ids(), total_slots);
    }

    let mut allocations: Vec<QuotaAllocation> = ctx
        .workers()
        .iter()
        .map(|(&wid, snap)| quota_allocation(wid, snap.capacity_bytes, total_capacity, total_slots))
        .collect();

    let used: u32 = allocations.iter().map(|a| a.floor_quota).sum();
    let remainder = total_slots.saturating_sub(used) as usize;

    allocations.sort_by(|a, b| {
        b.fraction
            .partial_cmp(&a.fraction)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.worker_id.cmp(&b.worker_id))
    });

    let mut quotas = HashMap::with_capacity(allocations.len());
    for (idx, allocation) in allocations.into_iter().enumerate() {
        let extra = if idx < remainder { 1 } else { 0 };
        quotas.insert(allocation.worker_id, allocation.floor_quota + extra);
    }
    quotas
}

struct QuotaAllocation {
    worker_id: u32,
    floor_quota: u32,
    fraction: f64,
}

fn quota_allocation(
    worker_id: u32,
    capacity: u64,
    total_capacity: u64,
    total_slots: u32,
) -> QuotaAllocation {
    if capacity == 0 {
        return QuotaAllocation {
            worker_id,
            floor_quota: 0,
            fraction: 0.0,
        };
    }

    let exact = total_slots as f64 * capacity as f64 / total_capacity as f64;
    let floor_quota = exact.floor() as u32;
    QuotaAllocation {
        worker_id,
        floor_quota,
        fraction: exact - floor_quota as f64,
    }
}

impl HashPlacementPolicy for HashCapacityWeightedPolicy {
    fn name(&self) -> &str {
        "capacity"
    }

    fn prepare_hash(&self, ctx: &HashPlacementContext<'_>) -> FsResult<HashPolicyState> {
        let total_slots = ctx.total_bg_slots();
        let worker_bg_quota = compute_capacity_weighted_quota(ctx, total_slots);
        let worker_ids: Vec<u32> = ctx.worker_ids();
        let worker_primary_quota = compute_equal_quota(&worker_ids, ctx.bucket_count);
        let (worker_bg_effective, worker_primary_effective) = build_hash_effective_counts(ctx);

        Ok(HashPolicyState {
            worker_bg_quota,
            worker_primary_quota,
            worker_bg_effective,
            worker_primary_effective,
        })
    }

    fn select_bg_targets(
        &self,
        ctx: &HashPlacementContext<'_>,
        st: &HashPolicyState,
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
        let mut selected: Vec<u32> = Vec::with_capacity(count);
        let mut selected_set: HashSet<u32> = HashSet::with_capacity(count);

        for _ in 0..count {
            let remaining_weight: f64 = weighted
                .iter()
                .filter(|(wid, _)| !selected_set.contains(wid))
                .map(|(_, weight)| *weight)
                .sum();
            if remaining_weight <= 0.0 {
                break;
            }

            let mut r = rng.gen_range(0.0..remaining_weight);
            let mut picked = None;
            for &(wid, weight) in &weighted {
                if selected_set.contains(&wid) {
                    continue;
                }
                if r <= weight {
                    picked = Some(wid);
                    break;
                }
                r -= weight;
            }

            let Some(wid) = picked else { break };
            selected.push(wid);
            selected_set.insert(wid);
        }

        if selected.is_empty() {
            return Err(FsError::common("no eligible BG target".to_string()));
        }
        Ok(selected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::bgtable::placement::{PlacementContext, WorkerLoadSnapshot};

    fn make_snapshot(worker_id: u32, actual_bg: u32, capacity_bytes: u64) -> WorkerLoadSnapshot {
        WorkerLoadSnapshot {
            worker_id,
            actual_bg,
            actual_primary: 0,
            pending_bg_add: 0,
            pending_bg_remove: 0,
            pending_primary_in: 0,
            pending_primary_out: 0,
            capacity_bytes,
            used_bytes: 0,
            labels: HashMap::new(),
        }
    }

    fn make_ctx(workers: &HashMap<u32, WorkerLoadSnapshot>) -> HashPlacementContext<'_> {
        HashPlacementContext {
            common: PlacementContext {
                workers,
                tolerant_ratio: 0.1,
                primary_tolerant_ratio: 0.1,
            },
            table_id: 1,
            bucket_count: 8,
            replica_count: 2,
        }
    }

    #[test]
    fn capacity_weighted_quota_heterogeneous() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 0, 4_000_000));
        workers.insert(2, make_snapshot(2, 0, 4_000_000));
        workers.insert(3, make_snapshot(3, 0, 2_000_000));

        let ctx = make_ctx(&workers);
        let policy = HashCapacityWeightedPolicy::new();
        let st = policy.prepare_hash(&ctx).unwrap();

        let larger_quota_gap = st.worker_bg_quota[&1].abs_diff(st.worker_bg_quota[&2]);
        assert!(larger_quota_gap <= 1);
        assert!(st.worker_bg_quota[&1] > st.worker_bg_quota[&3]);
        assert!(st.worker_bg_quota[&2] > st.worker_bg_quota[&3]);
        assert_eq!(
            st.worker_bg_quota.values().sum::<u32>(),
            ctx.total_bg_slots()
        );
    }

    #[test]
    fn capacity_weighted_quota_equal_capacity() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 0, 1000));
        workers.insert(2, make_snapshot(2, 0, 1000));
        workers.insert(3, make_snapshot(3, 0, 1000));

        let ctx = make_ctx(&workers);
        let policy = HashCapacityWeightedPolicy::new();
        let st = policy.prepare_hash(&ctx).unwrap();

        let max = *st.worker_bg_quota.values().max().unwrap();
        let min = *st.worker_bg_quota.values().min().unwrap();
        assert!(max - min <= 1);
        assert_eq!(
            st.worker_bg_quota.values().sum::<u32>(),
            ctx.total_bg_slots()
        );
    }

    #[test]
    fn capacity_zero_worker_gets_zero_quota() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 0, 1000));
        workers.insert(2, make_snapshot(2, 0, 0));

        let mut ctx = make_ctx(&workers);
        ctx.bucket_count = 4;
        let policy = HashCapacityWeightedPolicy::new();
        let st = policy.prepare_hash(&ctx).unwrap();

        assert_eq!(st.worker_bg_quota[&1], 8);
        assert_eq!(st.worker_bg_quota[&2], 0);
    }

    #[test]
    fn primary_quota_still_equal() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 0, 4000));
        workers.insert(2, make_snapshot(2, 0, 1000));

        let ctx = make_ctx(&workers);
        let policy = HashCapacityWeightedPolicy::new();
        let st = policy.prepare_hash(&ctx).unwrap();

        assert_eq!(st.worker_primary_quota[&1], 4);
        assert_eq!(st.worker_primary_quota[&2], 4);
    }

    #[test]
    fn should_rebalance_with_capacity_quota() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 8, 4000));
        workers.insert(2, make_snapshot(2, 0, 1000));

        let mut ctx = make_ctx(&workers);
        ctx.bucket_count = 4;
        let policy = HashCapacityWeightedPolicy::new();
        let st = policy.prepare_hash(&ctx).unwrap();

        assert!(policy.is_bg_overloaded(&ctx, &st, 1));
        assert!(!policy.is_bg_overloaded(&ctx, &st, 2));
    }
}
