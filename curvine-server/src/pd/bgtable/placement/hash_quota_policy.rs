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
    build_hash_effective_counts, compute_equal_quota, select_by_hunger, HashPlacementPolicy,
    HashPolicyState,
};
use curvine_common::{FsError, FsResult};
use std::collections::HashSet;

/// Default quota-based placement policy.
///
/// BG quota: equal-weight (`total_slots / num_workers`).
/// Primary quota: equal-weight (`bucket_count / num_workers`).
/// Selection: deterministic hunger-based (highest `quota - effective` wins).
pub struct HashQuotaPolicy;

impl HashQuotaPolicy {
    pub fn new() -> Self {
        Self
    }
}

impl Default for HashQuotaPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl HashPlacementPolicy for HashQuotaPolicy {
    fn name(&self) -> &str {
        "quota"
    }

    fn prepare_hash(&self, ctx: &HashPlacementContext<'_>) -> FsResult<HashPolicyState> {
        let worker_ids: Vec<u32> = ctx.worker_ids();
        let total_slots = ctx.total_bg_slots();

        let worker_bg_quota = compute_equal_quota(&worker_ids, total_slots);
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
        _ctx: &HashPlacementContext<'_>,
        st: &HashPolicyState,
        candidates: &[u32],
        count: usize,
        exclude: &HashSet<u32>,
    ) -> FsResult<Vec<u32>> {
        let result = select_by_hunger(st, candidates, count, exclude);
        if result.is_empty() {
            return Err(FsError::common("no eligible BG target".to_string()));
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::bgtable::placement::{
        ReplicaDecision, ReplicaReplaceReason, WorkerLoadSnapshot,
    };
    use curvine_common::state::{BGKind, BGOpState, BGPrimary, BGState, BlockGroupInfo};
    use std::collections::HashMap;

    fn make_snapshot(worker_id: u32, actual_bg: u32, actual_primary: u32) -> WorkerLoadSnapshot {
        WorkerLoadSnapshot {
            worker_id,
            actual_bg,
            actual_primary,
            pending_bg_add: 0,
            pending_bg_remove: 0,
            pending_primary_in: 0,
            pending_primary_out: 0,
            capacity_bytes: 1000,
            used_bytes: 100,
            labels: HashMap::new(),
        }
    }

    fn make_ctx(workers: &HashMap<u32, WorkerLoadSnapshot>) -> HashPlacementContext<'_> {
        HashPlacementContext {
            common: crate::pd::bgtable::placement::PlacementContext {
                workers,
                tolerant_ratio: 0.1,
                primary_tolerant_ratio: 0.1,
            },
            table_id: 1,
            bucket_count: 8,
            replica_count: 2,
        }
    }

    fn make_bg(bg_id: u32, replica_set: Vec<u32>, primary_id: u32) -> BlockGroupInfo {
        BlockGroupInfo {
            bg_id: bg_id.into(),
            table_id: 1,
            kind: BGKind::Hash,
            bg_epoch: 1,
            replica_set: replica_set.clone(),
            isr: replica_set,
            state: BGState::Active,
            op_state: BGOpState::Idle,
            primary: BGPrimary {
                node_id: primary_id,
                epoch: 1,
                grant_time_ms: 0,
            },
            stats: Default::default(),
            replicas: Default::default(),
        }
    }

    #[test]
    fn test_prepare_equal_quota() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 4, 2));
        workers.insert(2, make_snapshot(2, 4, 2));
        let ctx = make_ctx(&workers);
        let policy = HashQuotaPolicy::new();
        let st = policy.prepare_hash(&ctx).unwrap();

        assert_eq!(st.worker_bg_quota[&1] + st.worker_bg_quota[&2], 16);
        assert_eq!(st.worker_primary_quota[&1], 4);
    }

    #[test]
    fn test_classify_replica_invalid() {
        let mut workers = HashMap::new();
        workers.insert(2, make_snapshot(2, 4, 2));
        let ctx = make_ctx(&workers);
        let policy = HashQuotaPolicy::new();
        let st = policy.prepare_hash(&ctx).unwrap();
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
        let policy = HashQuotaPolicy::new();
        let st = policy.prepare_hash(&ctx).unwrap();
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
        let policy = HashQuotaPolicy::new();
        let st = policy.prepare_hash(&ctx).unwrap();
        let live: HashSet<u32> = vec![1, 2].into_iter().collect();

        let bg = make_bg(100, vec![1, 2], 1);
        assert_eq!(
            policy.classify_replica(&ctx, &st, &bg, 0, &live),
            ReplicaDecision::Keep
        );
    }

    #[test]
    fn test_is_bg_overloaded() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 10, 2));
        workers.insert(2, make_snapshot(2, 6, 2));
        let ctx = make_ctx(&workers);
        let policy = HashQuotaPolicy::new();
        let st = policy.prepare_hash(&ctx).unwrap();

        assert!(policy.is_bg_overloaded(&ctx, &st, 1));
        assert!(!policy.is_bg_overloaded(&ctx, &st, 2));
    }

    #[test]
    fn test_select_bg_targets_hungriest() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 10, 0));
        workers.insert(2, make_snapshot(2, 6, 0));
        workers.insert(3, make_snapshot(3, 0, 0));
        let ctx = make_ctx(&workers);
        let policy = HashQuotaPolicy::new();
        let st = policy.prepare_hash(&ctx).unwrap();

        let targets = policy
            .select_bg_targets(&ctx, &st, &[1, 2, 3], 1, &HashSet::new())
            .unwrap();
        assert_eq!(targets, vec![3]);
    }

    #[test]
    fn test_select_primary() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 4, 4));
        workers.insert(2, make_snapshot(2, 4, 0));
        let ctx = make_ctx(&workers);
        let policy = HashQuotaPolicy::new();
        let st = policy.prepare_hash(&ctx).unwrap();

        let owner = policy.select_primary(&st, &[1, 2]).unwrap();
        assert_eq!(owner, 2);
    }

    #[test]
    fn test_record_bg_change_updates_effective() {
        let mut workers = HashMap::new();
        workers.insert(1, make_snapshot(1, 10, 0));
        workers.insert(2, make_snapshot(2, 2, 0));
        let ctx = make_ctx(&workers);
        let policy = HashQuotaPolicy::new();
        let mut st = policy.prepare_hash(&ctx).unwrap();

        st.record_bg_change(Some(1), 2);
        assert_eq!(st.worker_bg_effective[&1], 9);
        assert_eq!(st.worker_bg_effective[&2], 3);
    }
}
