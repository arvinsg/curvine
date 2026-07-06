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

pub mod context;
pub mod hash_capacity_weighted_policy;
pub mod hash_plan;
pub mod hash_quota_policy;
pub mod policy;
pub mod rule;
pub mod snapshot;

use crate::pd::config::keys;

pub use context::{HashPlacementContext, PendingInfluence, PlacementContext, WorkerLoadSnapshot};
pub(crate) use hash_plan::select_with_fallback;
pub use hash_plan::{
    build_hash_table, rebuild_hash_table, BuildHashTableResult, RebuildHashTableResult,
};
pub use hash_capacity_weighted_policy::HashCapacityWeightedPolicy;
pub use hash_quota_policy::HashQuotaPolicy;
pub use policy::{
    is_bg_gap_sufficient, is_primary_gap_sufficient, HashPlacementPolicy, HashPolicyState,
    RebuildOptions, ReplicaDecision, ReplicaReplaceReason,
};
pub use rule::{
    best_isolation_candidates, check_isolation_violation, filter_min_isolation, isolation_score,
    worker_passes_constraints, worst_replica, LabelConstraint, LabelOp, Labels, PlacementRule,
};
pub use snapshot::{build_hash_table_snapshot, build_worker_snapshots};

/// Create a Hash BG placement policy by strategy name.
pub fn create_hash_policy(strategy: &str) -> Box<dyn HashPlacementPolicy> {
    match strategy {
        keys::PD_BG_BALANCE_POLICY_CAPACITY => Box::new(HashCapacityWeightedPolicy::new()),
        _ => Box::new(HashQuotaPolicy::new()),
    }
}
