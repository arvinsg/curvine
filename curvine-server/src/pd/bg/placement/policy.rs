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
use curvine_common::state::BlockGroupInfo;
use curvine_common::FsResult;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaReplaceReason {
    Invalid,
    SameNode,
    OverQuota,
}

/// Decision for a single replica position during rebuild.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaDecision {
    Keep,
    MustReplace(ReplicaReplaceReason),
    TryReplace(ReplicaReplaceReason),
}

/// Guardrail for single-BG replacement budget during rebuild.
pub struct RebuildOptions {
    pub max_replace_ratio: f64,
    pub min_keep_replicas: u16,
}

impl Default for RebuildOptions {
    fn default() -> Self {
        Self {
            max_replace_ratio: 0.5,
            min_keep_replicas: 1,
        }
    }
}

impl RebuildOptions {
    pub fn max_replace_budget(&self, replica_count: u16) -> usize {
        let by_ratio = ((replica_count as f64) * self.max_replace_ratio).floor() as usize;
        let by_keep = replica_count.saturating_sub(self.min_keep_replicas) as usize;
        by_ratio.min(by_keep)
    }
}

/// Mutable state produced by `BalancePolicy::prepare()`, updated during build/rebuild.
pub struct PolicyState {
    /// BG quota per worker (equal-weight or capacity-weighted, by policy).
    pub worker_bg_quota: HashMap<u32, u32>,
    /// Lease quota per worker (typically equal-weight).
    pub worker_lease_quota: HashMap<u32, u32>,

    /// Dynamic effective BG count. Initialized from snapshot, updated per assignment.
    pub worker_bg_effective: HashMap<u32, i64>,
    /// Dynamic effective lease count. Same lifecycle.
    pub worker_lease_effective: HashMap<u32, i64>,

    /// Strategy-specific BG load scores.
    pub bg_load_score: HashMap<u32, f64>,
    /// Strategy-specific lease load scores.
    pub lease_load_score: HashMap<u32, f64>,
}

impl PolicyState {
    /// Update effective BG counts after a replica replacement or assignment.
    pub fn record_bg_change(&mut self, old_worker: Option<u32>, new_worker: u32) {
        if let Some(old) = old_worker {
            if let Some(v) = self.worker_bg_effective.get_mut(&old) {
                *v -= 1;
            }
        }
        *self.worker_bg_effective.entry(new_worker).or_insert(0) += 1;
    }

    /// Update effective lease counts after a lease transfer or assignment.
    pub fn record_lease_change(&mut self, old_owner: Option<u32>, new_owner: u32) {
        if let Some(old) = old_owner {
            if let Some(v) = self.worker_lease_effective.get_mut(&old) {
                *v -= 1;
            }
        }
        *self.worker_lease_effective.entry(new_owner).or_insert(0) += 1;
    }
}

/// Unified load judgment for rebuild, BG balance, and lease balance.
///
/// - `prepare`: pre-compute quotas, effective counts, scores from snapshot.
/// - `classify_replica`: rebuild — decide if a replica position should be replaced.
/// - `should_rebalance_*_from`: balance — identify overloaded source workers.
/// - `filter_*_targets`: prune candidate list before selector picks.
pub trait BalancePolicy: Send + Sync {
    fn name(&self) -> &str;

    /// Pre-compute quotas and effective counts from the immutable snapshot.
    fn prepare(&self, ctx: &PlacementContext<'_>) -> FsResult<PolicyState>;

    /// Classify a replica position during rebuild.
    fn classify_replica(
        &self,
        ctx: &PlacementContext<'_>,
        st: &PolicyState,
        bg: &BlockGroupInfo,
        pos: usize,
        live_workers: &HashSet<u32>,
    ) -> ReplicaDecision;

    /// Should this worker be a BG rebalance source? (balance scenario)
    fn should_rebalance_bg_from(
        &self,
        ctx: &PlacementContext<'_>,
        st: &PolicyState,
        worker_id: u32,
    ) -> bool;

    /// Should this worker be a lease rebalance source? (balance scenario)
    fn should_rebalance_lease_from(
        &self,
        ctx: &PlacementContext<'_>,
        st: &PolicyState,
        worker_id: u32,
    ) -> bool;

    /// Filter BG target candidates: remove overloaded/invalid workers.
    fn filter_bg_targets(
        &self,
        ctx: &PlacementContext<'_>,
        st: &PolicyState,
        candidate_ids: &[u32],
        exclude: &HashSet<u32>,
    ) -> Vec<u32>;

    /// Filter lease target candidates.
    fn filter_lease_targets(
        &self,
        ctx: &PlacementContext<'_>,
        st: &PolicyState,
        candidate_ids: &[u32],
    ) -> Vec<u32>;
}

pub fn compute_equal_quota(worker_ids: &[u32], total: u32) -> HashMap<u32, u32> {
    if worker_ids.is_empty() {
        return HashMap::new();
    }
    let n = worker_ids.len() as u32;
    let base = total / n;
    let remainder = total % n;
    worker_ids
        .iter()
        .enumerate()
        .map(|(i, wid)| (*wid, base + if (i as u32) < remainder { 1 } else { 0 }))
        .collect()
}

pub fn tolerant(quota: u32, ratio: f64) -> u32 {
    ((quota as f64) * ratio).max(1.0) as u32
}

pub fn build_effective_counts(
    ctx: &PlacementContext<'_>,
) -> (HashMap<u32, i64>, HashMap<u32, i64>) {
    let bg: HashMap<u32, i64> = ctx
        .workers
        .iter()
        .map(|(&wid, snap)| (wid, snap.effective_bg()))
        .collect();
    let lease: HashMap<u32, i64> = ctx
        .workers
        .iter()
        .map(|(&wid, snap)| (wid, snap.effective_lease()))
        .collect();
    (bg, lease)
}

pub fn classify_replica(
    ctx: &PlacementContext<'_>,
    st: &PolicyState,
    bg: &BlockGroupInfo,
    pos: usize,
    live_workers: &HashSet<u32>,
) -> ReplicaDecision {
    let worker_id = bg.replica_set[pos];

    if !live_workers.contains(&worker_id) {
        return ReplicaDecision::MustReplace(ReplicaReplaceReason::Invalid);
    }

    for (i, &other) in bg.replica_set.iter().enumerate() {
        if i != pos && other == worker_id {
            return ReplicaDecision::TryReplace(ReplicaReplaceReason::SameNode);
        }
    }

    let effective = st.worker_bg_effective.get(&worker_id).copied().unwrap_or(0);
    let quota = st.worker_bg_quota.get(&worker_id).copied().unwrap_or(0);
    let threshold = quota + tolerant(quota, ctx.tolerant_ratio);
    if effective > threshold as i64 {
        return ReplicaDecision::TryReplace(ReplicaReplaceReason::OverQuota);
    }

    ReplicaDecision::Keep
}

pub fn should_rebalance_bg(ctx: &PlacementContext<'_>, st: &PolicyState, worker_id: u32) -> bool {
    let effective = st.worker_bg_effective.get(&worker_id).copied().unwrap_or(0);
    let quota = st.worker_bg_quota.get(&worker_id).copied().unwrap_or(0);
    let threshold = quota + tolerant(quota, ctx.tolerant_ratio);
    effective > threshold as i64
}

pub fn should_rebalance_lease(
    ctx: &PlacementContext<'_>,
    st: &PolicyState,
    worker_id: u32,
) -> bool {
    let effective = st
        .worker_lease_effective
        .get(&worker_id)
        .copied()
        .unwrap_or(0);
    let quota = st.worker_lease_quota.get(&worker_id).copied().unwrap_or(0);
    let threshold = quota + tolerant(quota, ctx.lease_tolerant_ratio);
    effective > threshold as i64
}

pub fn filter_bg_targets(
    ctx: &PlacementContext<'_>,
    st: &PolicyState,
    candidate_ids: &[u32],
    exclude: &HashSet<u32>,
) -> Vec<u32> {
    candidate_ids
        .iter()
        .copied()
        .filter(|wid| !exclude.contains(wid))
        .filter(|wid| {
            let effective = st.worker_bg_effective.get(wid).copied().unwrap_or(0);
            let quota = st.worker_bg_quota.get(wid).copied().unwrap_or(0);
            let threshold = quota + tolerant(quota, ctx.tolerant_ratio);
            effective < threshold as i64
        })
        .collect()
}

pub fn filter_lease_targets(
    ctx: &PlacementContext<'_>,
    st: &PolicyState,
    candidate_ids: &[u32],
) -> Vec<u32> {
    candidate_ids
        .iter()
        .copied()
        .filter(|wid| {
            let effective = st.worker_lease_effective.get(wid).copied().unwrap_or(0);
            let quota = st.worker_lease_quota.get(wid).copied().unwrap_or(0);
            let threshold = quota + tolerant(quota, ctx.lease_tolerant_ratio);
            effective < threshold as i64
        })
        .collect()
}
