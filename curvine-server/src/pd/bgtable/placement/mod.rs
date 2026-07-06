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
pub mod hash_build;
pub mod hash_capacity_weighted_policy;
pub mod hash_policy;
pub mod hash_quota_policy;
pub mod rule;

use crate::pd::config::keys;

pub use context::*;
pub use hash_build::{BuildHashTableResult, HashPlanner, HashTableSpec, RebuildHashTableResult};
pub use hash_capacity_weighted_policy::HashCapacityWeightedPolicy;
pub use hash_policy::*;
pub use hash_quota_policy::HashQuotaPolicy;
pub use rule::*;

/// Create a Hash BG placement policy by strategy name.
pub fn create_hash_policy(strategy: &str) -> Box<dyn HashPlacementPolicy> {
    match strategy {
        keys::PD_BG_BALANCE_POLICY_CAPACITY => Box::new(HashCapacityWeightedPolicy::new()),
        _ => Box::new(HashQuotaPolicy::new()),
    }
}
