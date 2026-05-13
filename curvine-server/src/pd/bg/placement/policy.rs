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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaDecision {
    Keep,
    MustReplace(ReplicaReplaceReason),
    TryReplace(ReplicaReplaceReason),
}

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

pub struct PolicyState {
    pub worker_bg_quota: HashMap<u32, u32>,
    pub worker_lease_quota: HashMap<u32, u32>,
    pub worker_bg_effective: HashMap<u32, i64>,
    pub worker_lease_effective: HashMap<u32, i64>,
}

impl PolicyState {
    pub fn record_bg_change(&mut self, old_worker: Option<u32>, new_worker: u32) {
        if let Some(old) = old_worker {
            if let Some(v) = self.worker_bg_effective.get_mut(&old) {
                *v -= 1;
            }
        }
        *self.worker_bg_effective.entry(new_worker).or_insert(0) += 1;
    }

    pub fn record_lease_change(&mut self, old_owner: Option<u32>, new_owner: u32) {
        if let Some(old) = old_owner {
            if let Some(v) = self.worker_lease_effective.get_mut(&old) {
                *v -= 1;
            }
        }
        *self.worker_lease_effective.entry(new_owner).or_insert(0) += 1;
    }
}

/// Unified placement policy: quota computation + source identification + target selection.
pub trait PlacementPolicy: Send + Sync {
    fn name(&self) -> &str;

    fn prepare(&self, ctx: &PlacementContext<'_>) -> FsResult<PolicyState>;

    fn classify_replica(
        &self,
        ctx: &PlacementContext<'_>,
        st: &PolicyState,
        bg: &BlockGroupInfo,
        pos: usize,
        live_workers: &HashSet<u32>,
    ) -> ReplicaDecision;

    fn is_bg_overloaded(
        &self,
        ctx: &PlacementContext<'_>,
        st: &PolicyState,
        worker_id: u32,
    ) -> bool;

    fn is_lease_overloaded(
        &self,
        ctx: &PlacementContext<'_>,
        st: &PolicyState,
        worker_id: u32,
    ) -> bool;

    /// Select up to `count` BG targets from `candidates`, excluding `exclude`.
    /// Combines filtering (remove overloaded) + ranking (pick best) in one step.
    fn select_bg_targets(
        &self,
        ctx: &PlacementContext<'_>,
        st: &PolicyState,
        candidates: &[u32],
        count: usize,
        exclude: &HashSet<u32>,
    ) -> FsResult<Vec<u32>>;

    /// Select a lease owner from candidates.
    fn select_lease_owner(&self, st: &PolicyState, candidates: &[u32]) -> FsResult<u32>;
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

pub fn is_bg_overloaded(ctx: &PlacementContext<'_>, st: &PolicyState, worker_id: u32) -> bool {
    let effective = st.worker_bg_effective.get(&worker_id).copied().unwrap_or(0);
    let quota = st.worker_bg_quota.get(&worker_id).copied().unwrap_or(0);
    let threshold = quota + tolerant(quota, ctx.tolerant_ratio);
    effective > threshold as i64
}

pub fn is_lease_overloaded(ctx: &PlacementContext<'_>, st: &PolicyState, worker_id: u32) -> bool {
    let effective = st
        .worker_lease_effective
        .get(&worker_id)
        .copied()
        .unwrap_or(0);
    let quota = st.worker_lease_quota.get(&worker_id).copied().unwrap_or(0);
    let threshold = quota + tolerant(quota, ctx.lease_tolerant_ratio);
    effective > threshold as i64
}

/// Hunger-based BG target selection (shared by QuotaPolicy).
pub fn select_by_hunger(
    st: &PolicyState,
    candidates: &[u32],
    count: usize,
    exclude: &HashSet<u32>,
) -> Vec<u32> {
    let mut eligible: Vec<(u32, i64)> = candidates
        .iter()
        .copied()
        .filter(|wid| !exclude.contains(wid))
        .map(|wid| {
            let quota = st.worker_bg_quota.get(&wid).copied().unwrap_or(0) as i64;
            let effective = st.worker_bg_effective.get(&wid).copied().unwrap_or(0);
            (wid, quota - effective)
        })
        .collect();
    eligible.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    eligible
        .into_iter()
        .take(count)
        .map(|(wid, _)| wid)
        .collect()
}

/// Hunger-based lease owner selection (lowest lease effective wins).
pub fn select_lease_by_hunger(st: &PolicyState, candidates: &[u32]) -> Option<u32> {
    candidates.iter().copied().min_by_key(|&wid| {
        let quota = st.worker_lease_quota.get(&wid).copied().unwrap_or(0) as i64;
        let effective = st.worker_lease_effective.get(&wid).copied().unwrap_or(0);
        (-(quota - effective), wid)
    })
}

pub fn is_bg_gap_sufficient(
    st: &PolicyState,
    source_id: u32,
    target_id: u32,
    tolerant_ratio: f64,
) -> bool {
    let src = st.worker_bg_effective.get(&source_id).copied().unwrap_or(0);
    let tgt = st.worker_bg_effective.get(&target_id).copied().unwrap_or(0);
    let src_quota = st.worker_bg_quota.get(&source_id).copied().unwrap_or(0);
    let tgt_quota = st.worker_bg_quota.get(&target_id).copied().unwrap_or(0);
    let avg_quota = (src_quota + tgt_quota) / 2;
    let tol = tolerant(avg_quota, tolerant_ratio) as i64;
    src - tgt > 2 * tol
}

pub fn is_lease_gap_sufficient(
    st: &PolicyState,
    source_id: u32,
    target_id: u32,
    tolerant_ratio: f64,
) -> bool {
    let src = st
        .worker_lease_effective
        .get(&source_id)
        .copied()
        .unwrap_or(0);
    let tgt = st
        .worker_lease_effective
        .get(&target_id)
        .copied()
        .unwrap_or(0);
    let src_quota = st.worker_lease_quota.get(&source_id).copied().unwrap_or(0);
    let tgt_quota = st.worker_lease_quota.get(&target_id).copied().unwrap_or(0);
    let avg_quota = (src_quota + tgt_quota) / 2;
    let tol = tolerant(avg_quota, tolerant_ratio) as i64;
    src - tgt > 2 * tol
}
