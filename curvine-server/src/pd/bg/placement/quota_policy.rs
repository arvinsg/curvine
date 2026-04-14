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

/// Default quota-based balance policy.
///
/// BG quota: equal-weight (`total_slots / num_workers`).
/// Lease quota: equal-weight (`bucket_count / num_workers`).
pub struct QuotaBalancePolicy;

impl QuotaBalancePolicy {
    pub fn new() -> Self {
        Self
    }
}

impl Default for QuotaBalancePolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl BalancePolicy for QuotaBalancePolicy {
    fn name(&self) -> &str {
        "quota"
    }

    fn prepare(&self, ctx: &PlacementContext<'_>) -> FsResult<PolicyState> {
        let worker_ids: Vec<u32> = ctx.worker_ids();
        let total_slots = ctx.total_bg_slots();

        let worker_bg_quota = compute_equal_quota(&worker_ids, total_slots);
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
    use crate::pd::bg::placement::policy::ReplicaReplaceReason;
    use curvine_common::state::{BGLease, BGOpState, BGState};

    fn make_snapshot(worker_id: u32, actual_bg: u32, actual_lease: u32) -> WorkerLoadSnapshot {
        WorkerLoadSnapshot {
            worker_id,
            actual_bg,
            actual_lease,
            pending_bg_add: 0,
            pending_bg_remove: 0,
            pending_lease_in: 0,
            pending_lease_out: 0,
            capacity_bytes: 1000,
            used_bytes: 100,
            labels: HashMap::new(),
        }
    }

    fn make_ctx(workers: &HashMap<u32, WorkerLoadSnapshot>) -> PlacementContext<'_> {
        PlacementContext {
            workers,
            bucket_count: 8,
            replica_count: 2,
            tolerant_ratio: 0.1,
            lease_tolerant_ratio: 0.1,
        }
    }

    fn make_bg(bg_id: u32, replica_set: Vec<u32>, lease_owner_id: u32) -> BlockGroupInfo {
        BlockGroupInfo {
            bg_id,
            table_id: 1,
            bg_epoch: 1,
            replica_set,
            state: BGState::Active,
            op_state: BGOpState::Idle,
            lease_owner: Some(BGLease {
                node_id: lease_owner_id,
                epoch: 1,
                grant_time_ms: 0,
            }),
            stats: Default::default(),
        }
    }

    #[test]
    fn test_prepare_equal_quota() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 4, 2));
        workers.insert(2, make_snapshot(2, 4, 2));
        let ctx = make_ctx(&workers);
        let policy = QuotaBalancePolicy::new();
        let st = policy.prepare(&ctx).unwrap();

        assert_eq!(st.worker_bg_quota[&1] + st.worker_bg_quota[&2], 16);
        assert_eq!(st.worker_lease_quota[&1], 4);
    }

    #[test]
    fn test_classify_replica_invalid() {
        let mut workers = HashMap::new();
        workers.insert(2, make_snapshot(2, 4, 2));
        let ctx = make_ctx(&workers);
        let policy = QuotaBalancePolicy::new();
        let st = policy.prepare(&ctx).unwrap();
        let live: HashSet<u32> = vec![2].into_iter().collect();

        let bg = make_bg(100, vec![1, 2], 2);
        assert_eq!(
            policy.classify_replica(&ctx, &st, &bg, 0, &live),
            ReplicaDecision::MustReplace(ReplicaReplaceReason::Invalid)
        );
    }

    #[test]
    fn test_classify_replica_over_quota() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 10, 2));
        workers.insert(2, make_snapshot(2, 2, 2));
        let ctx = make_ctx(&workers);
        let policy = QuotaBalancePolicy::new();
        let st = policy.prepare(&ctx).unwrap();
        let live: HashSet<u32> = vec![1, 2].into_iter().collect();

        let bg = make_bg(100, vec![1, 2], 1);
        assert_eq!(
            policy.classify_replica(&ctx, &st, &bg, 0, &live),
            ReplicaDecision::TryReplace(ReplicaReplaceReason::OverQuota)
        );
    }

    #[test]
    fn test_classify_replica_keep() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 8, 2));
        workers.insert(2, make_snapshot(2, 8, 2));
        let ctx = make_ctx(&workers);
        let policy = QuotaBalancePolicy::new();
        let st = policy.prepare(&ctx).unwrap();
        let live: HashSet<u32> = vec![1, 2].into_iter().collect();

        let bg = make_bg(100, vec![1, 2], 1);
        assert_eq!(
            policy.classify_replica(&ctx, &st, &bg, 0, &live),
            ReplicaDecision::Keep
        );
    }

    #[test]
    fn test_should_rebalance_bg_from() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 10, 2));
        workers.insert(2, make_snapshot(2, 6, 2));
        let ctx = make_ctx(&workers);
        let policy = QuotaBalancePolicy::new();
        let st = policy.prepare(&ctx).unwrap();

        assert!(policy.should_rebalance_bg_from(&ctx, &st, 1));
        assert!(!policy.should_rebalance_bg_from(&ctx, &st, 2));
    }

    #[test]
    fn test_filter_bg_targets() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 10, 0));
        workers.insert(2, make_snapshot(2, 6, 0));
        workers.insert(3, make_snapshot(3, 2, 0));
        let ctx = make_ctx(&workers);
        let policy = QuotaBalancePolicy::new();
        let st = policy.prepare(&ctx).unwrap();

        let targets = policy.filter_bg_targets(&ctx, &st, &[1, 2, 3], &HashSet::new());
        assert!(targets.contains(&3));
        assert!(!targets.contains(&1));
    }

    #[test]
    fn test_record_bg_change_updates_effective() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 10, 0));
        workers.insert(2, make_snapshot(2, 2, 0));
        let ctx = make_ctx(&workers);
        let policy = QuotaBalancePolicy::new();
        let mut st = policy.prepare(&ctx).unwrap();

        st.record_bg_change(Some(1), 2);
        assert_eq!(st.worker_bg_effective[&1], 9);
        assert_eq!(st.worker_bg_effective[&2], 3);
    }
}
