pub mod build;
pub mod rule;
pub mod selector;

pub use build::{build_table, BuildTableResult};
pub use rule::{
    check_violations, filter_by_constraints, filter_by_isolation, find_worst_replica,
    LabelConstraint, LabelOp, PlacementRule, ViolationResult,
};
pub use selector::{NormalizedSelector, RandomSelector, WorkerCandidate, WorkerSelector};

use std::collections::{HashMap, HashSet};

/// Unified worker selection entry point for all BG placement scenarios.
pub fn select_workers_for_bg(
    candidates: &[WorkerCandidate],
    count: usize,
    exclude: &HashSet<u32>,
    existing_replicas: &[u32],
    rules: &[PlacementRule],
    selector: &dyn WorkerSelector,
    worker_labels: &HashMap<u32, HashMap<String, String>>,
) -> Vec<u32> {
    let mut selected: Vec<u32> = Vec::with_capacity(count);

    for _ in 0..count {
        // Build eligible set: not excluded, not already selected.
        let eligible_ids: Vec<u32> = candidates
            .iter()
            .filter(|c| !exclude.contains(&c.worker_id) && !selected.contains(&c.worker_id))
            .map(|c| c.worker_id)
            .collect();

        if eligible_ids.is_empty() {
            break;
        }

        // Combined existing: original existing_replicas + already selected in this call.
        let mut all_existing: Vec<u32> = existing_replicas.to_vec();
        all_existing.extend_from_slice(&selected);

        // Level 1: constraint + isolation filtering.
        let constrained = filter_by_constraints(&eligible_ids, rules, worker_labels);
        let isolated = if constrained.is_empty() {
            Vec::new()
        } else {
            filter_by_isolation(&constrained, rules, &all_existing, worker_labels)
        };

        // Build candidate slice for selector from the best available filter level.
        let pick_from = if !isolated.is_empty() {
            &isolated
        } else if !constrained.is_empty() {
            // Relaxation level 1: constraint-only (isolation relaxed).
            &constrained
        } else {
            // Relaxation level 2: all eligible (constraints also relaxed).
            &eligible_ids
        };

        let pick_set: HashSet<u32> = pick_from.iter().copied().collect();
        let filtered_candidates: Vec<WorkerCandidate> = candidates
            .iter()
            .filter(|c| pick_set.contains(&c.worker_id))
            .cloned()
            .collect();

        let picks = selector.select(&filtered_candidates, 1, &HashSet::new());
        if let Some(&wid) = picks.first() {
            selected.push(wid);
        } else {
            break;
        }
    }

    selected
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
        let selector = NormalizedSelector::default();
        let candidates = vec![
            make_candidate(1, 5, az_labels("az1")),
            make_candidate(2, 5, az_labels("az2")),
            make_candidate(3, 5, az_labels("az3")),
        ];
        let wl = worker_labels_map(&[(1, "az1"), (2, "az2"), (3, "az3")]);
        let rules = vec![cross_az_rule()];

        let result =
            select_workers_for_bg(&candidates, 3, &HashSet::new(), &[], &rules, &selector, &wl);
        assert_eq!(result.len(), 3);
        // All 3 AZs should be covered.
        let azs: HashSet<&str> = result
            .iter()
            .filter_map(|wid| wl.get(wid).and_then(|l| l.get("az").map(|s| s.as_str())))
            .collect();
        assert_eq!(azs.len(), 3);
    }

    #[test]
    fn select_with_relaxation() {
        // 3 replicas needed but only 2 AZs — must relax isolation.
        let selector = NormalizedSelector::default();
        let candidates = vec![
            make_candidate(1, 0, az_labels("az1")),
            make_candidate(2, 0, az_labels("az1")),
            make_candidate(3, 0, az_labels("az2")),
        ];
        let wl = worker_labels_map(&[(1, "az1"), (2, "az1"), (3, "az2")]);
        let rules = vec![cross_az_rule()];

        let result =
            select_workers_for_bg(&candidates, 3, &HashSet::new(), &[], &rules, &selector, &wl);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn select_respects_exclude() {
        let selector = NormalizedSelector::default();
        let candidates = vec![
            make_candidate(1, 0, HashMap::new()),
            make_candidate(2, 0, HashMap::new()),
            make_candidate(3, 0, HashMap::new()),
        ];
        let mut exclude = HashSet::new();
        exclude.insert(1);

        let result = select_workers_for_bg(
            &candidates,
            2,
            &exclude,
            &[],
            &[],
            &selector,
            &HashMap::new(),
        );
        assert_eq!(result.len(), 2);
        assert!(!result.contains(&1));
    }

    #[test]
    fn select_count_exceeds_available() {
        let selector = NormalizedSelector::default();
        let candidates = vec![
            make_candidate(1, 0, HashMap::new()),
            make_candidate(2, 0, HashMap::new()),
        ];
        let result = select_workers_for_bg(
            &candidates,
            5,
            &HashSet::new(),
            &[],
            &[],
            &selector,
            &HashMap::new(),
        );
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn select_with_existing_replicas() {
        // Existing replica in az1, should prefer az2 workers.
        let selector = NormalizedSelector::default();
        let candidates = vec![
            make_candidate(2, 0, az_labels("az1")),
            make_candidate(3, 0, az_labels("az2")),
        ];
        let wl = worker_labels_map(&[(1, "az1"), (2, "az1"), (3, "az2")]);
        let rules = vec![cross_az_rule()];

        let result = select_workers_for_bg(
            &candidates,
            1,
            &HashSet::new(),
            &[1], // existing replica in az1
            &rules,
            &selector,
            &wl,
        );
        assert_eq!(result, vec![3]); // az2 preferred
    }

    #[test]
    fn select_empty_candidates() {
        let selector = NormalizedSelector::default();
        let result = select_workers_for_bg(
            &[],
            3,
            &HashSet::new(),
            &[],
            &[],
            &selector,
            &HashMap::new(),
        );
        assert!(result.is_empty());
    }

    #[test]
    fn select_constraint_relaxation() {
        // All workers fail constraints -> should fall back to all eligible.
        let selector = NormalizedSelector::default();
        let candidates = vec![
            make_candidate(1, 5, az_labels("az3")),
            make_candidate(2, 10, az_labels("az3")),
        ];
        let wl = worker_labels_map(&[(1, "az3"), (2, "az3")]);
        let rules = vec![PlacementRule {
            id: "test".to_string(),
            label_constraints: vec![LabelConstraint {
                key: "az".to_string(),
                op: rule::LabelOp::In,
                values: vec!["az1".to_string()],
            }],
            location_labels: vec![],
            isolation_level: 0,
        }];

        let result =
            select_workers_for_bg(&candidates, 1, &HashSet::new(), &[], &rules, &selector, &wl);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], 1); // lower bg_count
    }
}
