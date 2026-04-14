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

pub mod build;
pub mod capacity_policy;
pub mod context;
pub mod normalized_selector;
pub mod policy;
pub mod quota_policy;
pub mod quota_selector;
pub mod random_selector;
pub mod rule;
pub mod selector;

pub use build::{build_table, rebuild_table, BuildTableResult, RebuildTableResult};
pub use context::{PlacementContext, WorkerLoadSnapshot};
pub use capacity_policy::CapacityBalancePolicy;
pub use normalized_selector::NormalizedSelector;
pub use policy::{
    BalancePolicy, PolicyState, RebuildOptions, ReplicaDecision, ReplicaReplaceReason,
};
pub use quota_policy::QuotaBalancePolicy;
pub use quota_selector::QuotaSelector;
pub use random_selector::RandomSelector;
pub use rule::{
    check_violations, filter_by_constraints, filter_by_isolation, find_worst_replica,
    LabelConstraint, LabelOp, PlacementRule, ViolationResult,
};
pub use selector::{WorkerCandidate, WorkerSelector};

use std::collections::{HashMap, HashSet};

/// Legacy worker selection entry point. Creates a temporary PolicyState internally.
/// New code should use build_table/rebuild_table with explicit policy + selector.
pub fn select_workers_for_bg(
    candidates: &[WorkerCandidate],
    count: usize,
    exclude: &HashSet<u32>,
    existing_replicas: &[u32],
    rules: &[PlacementRule],
    selector: &mut dyn WorkerSelector,
    worker_labels: &HashMap<u32, HashMap<String, String>>,
) -> Vec<u32> {
    // Build a temporary PolicyState for the legacy path
    let tmp_st = PolicyState {
        worker_bg_quota: HashMap::new(),
        worker_lease_quota: HashMap::new(),
        worker_bg_effective: candidates
            .iter()
            .map(|c| (c.worker_id, c.bg_count as i64))
            .collect(),
        worker_lease_effective: candidates
            .iter()
            .map(|c| (c.worker_id, c.lease_count as i64))
            .collect(),
        bg_load_score: HashMap::new(),
        lease_load_score: HashMap::new(),
    };

    let mut selected: Vec<u32> = Vec::with_capacity(count);

    for _ in 0..count {
        let eligible_ids: Vec<u32> = candidates
            .iter()
            .filter(|c| !exclude.contains(&c.worker_id) && !selected.contains(&c.worker_id))
            .map(|c| c.worker_id)
            .collect();

        if eligible_ids.is_empty() {
            break;
        }

        let mut all_existing: Vec<u32> = existing_replicas.to_vec();
        all_existing.extend_from_slice(&selected);

        let constrained = filter_by_constraints(&eligible_ids, rules, worker_labels);
        let isolated = if constrained.is_empty() {
            Vec::new()
        } else {
            filter_by_isolation(&constrained, rules, &all_existing, worker_labels)
        };

        let pick_from = if !isolated.is_empty() {
            &isolated
        } else if !constrained.is_empty() {
            &constrained
        } else {
            &eligible_ids
        };

        let pick_ids: Vec<u32> = pick_from.to_vec();

        match selector.select(&tmp_st, &pick_ids, 1, &HashSet::new()) {
            Ok(picks) => {
                if let Some(&wid) = picks.first() {
                    selected.push(wid);
                } else {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    selected
}

/// Create a selector instance by strategy name.
pub fn create_selector(strategy: &str) -> Box<dyn WorkerSelector> {
    match strategy {
        "normalized" => Box::new(NormalizedSelector::default()),
        "random" => Box::new(RandomSelector),
        _ => Box::new(QuotaSelector::new()),
    }
}

/// Create a balance policy instance by strategy name.
pub fn create_policy(strategy: &str) -> Box<dyn BalancePolicy> {
    match strategy {
        "capacity" => Box::new(CapacityBalancePolicy::new()),
        _ => Box::new(QuotaBalancePolicy::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_candidate(
        worker_id: u32,
        bg_count: u32,
        labels: HashMap<String, String>,
    ) -> WorkerCandidate {
        WorkerCandidate {
            worker_id,
            bg_count,
            lease_count: 0,
            capacity_bytes: 1000,
            used_bytes: 100,
            labels,
        }
    }

    fn az_labels(az: &str) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("az".to_string(), az.to_string());
        m
    }

    fn cross_az_rule() -> PlacementRule {
        PlacementRule {
            id: "cross-az".to_string(),
            label_constraints: vec![],
            location_labels: vec!["az".to_string()],
            isolation_level: 0,
        }
    }

    fn worker_labels_map(data: &[(u32, &str)]) -> HashMap<u32, HashMap<String, String>> {
        data.iter().map(|(wid, az)| (*wid, az_labels(az))).collect()
    }

    #[test]
    fn select_with_isolation() {
        let mut selector = NormalizedSelector::default();
        let candidates = vec![
            make_candidate(1, 5, az_labels("az1")),
            make_candidate(2, 5, az_labels("az2")),
            make_candidate(3, 5, az_labels("az3")),
        ];
        let wl = worker_labels_map(&[(1, "az1"), (2, "az2"), (3, "az3")]);
        let rules = vec![cross_az_rule()];

        let result =
            select_workers_for_bg(&candidates, 3, &HashSet::new(), &[], &rules, &mut selector, &wl);
        assert_eq!(result.len(), 3);
        let azs: HashSet<&str> = result
            .iter()
            .filter_map(|wid| wl.get(wid).and_then(|l| l.get("az").map(|s| s.as_str())))
            .collect();
        assert_eq!(azs.len(), 3);
    }

    #[test]
    fn select_with_relaxation() {
        let mut selector = NormalizedSelector::default();
        let candidates = vec![
            make_candidate(1, 0, az_labels("az1")),
            make_candidate(2, 0, az_labels("az1")),
            make_candidate(3, 0, az_labels("az2")),
        ];
        let wl = worker_labels_map(&[(1, "az1"), (2, "az1"), (3, "az2")]);
        let rules = vec![cross_az_rule()];

        let result =
            select_workers_for_bg(&candidates, 3, &HashSet::new(), &[], &rules, &mut selector, &wl);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn select_empty_candidates() {
        let mut selector = NormalizedSelector::default();
        let result = select_workers_for_bg(
            &[],
            3,
            &HashSet::new(),
            &[],
            &[],
            &mut selector,
            &HashMap::new(),
        );
        assert!(result.is_empty());
    }
}
