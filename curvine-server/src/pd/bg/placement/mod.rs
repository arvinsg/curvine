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

pub mod capacity_policy;
pub mod context;
pub mod planner;
pub mod policy;
pub mod quota_policy;
pub mod rule;

use crate::pd::config::keys;

pub use capacity_policy::CapacityPolicy;
pub use context::{PlacementContext, WorkerLoadSnapshot};
pub use planner::{build_table, rebuild_table, BuildTableResult, RebuildTableResult};
pub(crate) use planner::select_with_fallback;
pub use policy::{
    PlacementPolicy, PolicyState, RebuildOptions, ReplicaDecision, ReplicaReplaceReason,
    is_bg_gap_sufficient, is_lease_gap_sufficient,
};
pub use quota_policy::QuotaPolicy;
pub use rule::{
    best_isolation_candidates, check_isolation_violation, filter_min_isolation, isolation_score,
    worker_passes_constraints, worst_replica, LabelConstraint, LabelOp, Labels, PlacementRule,
};

/// Create a placement policy by strategy name.
pub fn create_policy(strategy: &str) -> Box<dyn PlacementPolicy> {
    match strategy {
        keys::PD_BG_BALANCE_POLICY_CAPACITY => Box::new(CapacityPolicy::new()),
        _ => Box::new(QuotaPolicy::new()),
    }
}
