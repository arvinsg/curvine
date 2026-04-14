use curvine_common::state::PlacementPolicy;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Label constraint operator
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LabelOp {
    In,
    NotIn,
    Exists,
    NotExists,
}

/// Single label constraint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelConstraint {
    pub key: String,
    pub op: LabelOp,
    pub values: Vec<String>,
}

impl LabelConstraint {
    pub fn matches(&self, labels: &HashMap<String, String>) -> bool {
        match self.op {
            LabelOp::In => labels
                .get(&self.key)
                .map(|v| self.values.contains(v))
                .unwrap_or(false),
            LabelOp::NotIn => labels
                .get(&self.key)
                .map(|v| !self.values.contains(v))
                .unwrap_or(true),
            LabelOp::Exists => labels.contains_key(&self.key),
            LabelOp::NotExists => !labels.contains_key(&self.key),
        }
    }
}

/// Placement rule (per-pool granularity)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementRule {
    pub id: String,
    pub label_constraints: Vec<LabelConstraint>,
    pub location_labels: Vec<String>,
    pub isolation_level: usize,
}

impl PlacementRule {
    /// Create a default (empty) rule — no constraints, no isolation.
    pub fn default_rule() -> Self {
        Self {
            id: "default".to_string(),
            label_constraints: vec![],
            location_labels: vec![],
            isolation_level: 0,
        }
    }

    /// Expand a PlacementPolicy into a PlacementRule.
    pub fn from_placement_policy(
        policy: PlacementPolicy,
        default_location_labels: &[String],
    ) -> Self {
        match policy {
            PlacementPolicy::Default => {
                if default_location_labels.is_empty() {
                    Self::default_rule()
                } else {
                    Self {
                        id: "default".to_string(),
                        label_constraints: vec![],
                        location_labels: default_location_labels.to_vec(),
                        isolation_level: 0,
                    }
                }
            }
            PlacementPolicy::CrossAZ => Self {
                id: "cross-az".to_string(),
                label_constraints: vec![],
                location_labels: vec!["az".to_string()],
                isolation_level: 0,
            },
        }
    }

    /// Filter workers by label constraints.
    pub fn filter(&self, worker_labels: &HashMap<u32, HashMap<String, String>>) -> Vec<u32> {
        if self.label_constraints.is_empty() {
            return worker_labels.keys().copied().collect();
        }
        worker_labels
            .iter()
            .filter(|(_, labels)| self.label_constraints.iter().all(|lc| lc.matches(labels)))
            .map(|(wid, _)| *wid)
            .collect()
    }

    /// Filter workers by isolation: returns workers NOT in the same isolation group.
    pub fn filter_isolated(
        &self,
        candidates: &[u32],
        existing_replicas: &[u32],
        worker_labels: &HashMap<u32, HashMap<String, String>>,
    ) -> Vec<u32> {
        let label_key = match self.location_labels.first() {
            Some(k) => k,
            None => return candidates.to_vec(),
        };

        let occupied: HashSet<&str> = existing_replicas
            .iter()
            .filter_map(|wid| {
                worker_labels
                    .get(wid)
                    .and_then(|l| l.get(label_key))
                    .map(|v| v.as_str())
            })
            .collect();

        let filtered: Vec<u32> = candidates
            .iter()
            .copied()
            .filter(|wid| {
                let group = worker_labels
                    .get(wid)
                    .and_then(|l| l.get(label_key))
                    .map(|v| v.as_str());
                match group {
                    Some(g) => !occupied.contains(g),
                    None => true,
                }
            })
            .collect();

        if filtered.is_empty() {
            candidates.to_vec()
        } else {
            filtered
        }
    }
}

/// Result of checking placement violations for a BG's replica set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ViolationResult {
    /// Number of replicas failing label constraints.
    pub constraint_violations: u32,
    /// Number of replica pairs sharing the same isolation group.
    pub isolation_violations: u32,
}

impl ViolationResult {
    pub fn total(&self) -> u32 {
        self.constraint_violations + self.isolation_violations
    }

    pub fn has_violation(&self) -> bool {
        self.total() > 0
    }
}

/// Filter worker IDs by label constraints from placement rules.
pub fn filter_by_constraints(
    worker_ids: &[u32],
    rules: &[PlacementRule],
    worker_labels: &HashMap<u32, HashMap<String, String>>,
) -> Vec<u32> {
    worker_ids
        .iter()
        .copied()
        .filter(|wid| {
            let labels = match worker_labels.get(wid) {
                Some(l) => l,
                None => return false, // no labels means can't satisfy constraints
            };
            rules
                .iter()
                .all(|rule| rule.label_constraints.iter().all(|lc| lc.matches(labels)))
        })
        .collect()
}

/// Filter worker IDs by isolation group preference.
pub fn filter_by_isolation(
    worker_ids: &[u32],
    rules: &[PlacementRule],
    existing_replicas: &[u32],
    worker_labels: &HashMap<u32, HashMap<String, String>>,
) -> Vec<u32> {
    // Find the first rule with non-empty location_labels.
    let location_label = rules
        .iter()
        .find_map(|r| r.location_labels.first())
        .cloned();

    let label_key = match location_label {
        Some(k) => k,
        None => return worker_ids.to_vec(), // no isolation config
    };

    // Collect groups already occupied by existing replicas.
    let occupied_groups: HashSet<&str> = existing_replicas
        .iter()
        .filter_map(|wid| {
            worker_labels
                .get(wid)
                .and_then(|l| l.get(&label_key))
                .map(|v| v.as_str())
        })
        .collect();

    // Filter to workers in unoccupied groups.
    let filtered: Vec<u32> = worker_ids
        .iter()
        .copied()
        .filter(|wid| {
            let group = worker_labels
                .get(wid)
                .and_then(|l| l.get(&label_key))
                .map(|v| v.as_str());
            match group {
                Some(g) => !occupied_groups.contains(g),
                None => true, // no label -> treat as unique group
            }
        })
        .collect();

    if filtered.is_empty() {
        // Relaxation: all groups covered, return all candidates.
        worker_ids.to_vec()
    } else {
        filtered
    }
}

/// Check placement violations for a BG's replica set against rules.
pub fn check_violations(
    replica_set: &[u32],
    rules: &[PlacementRule],
    worker_labels: &HashMap<u32, HashMap<String, String>>,
) -> ViolationResult {
    let mut result = ViolationResult::default();

    for rule in rules {
        // Constraint violations: each replica failing any constraint counts as 1.
        for &wid in replica_set {
            let labels = worker_labels.get(&wid);
            let passes = match labels {
                Some(l) => rule.label_constraints.iter().all(|lc| lc.matches(l)),
                None => rule.label_constraints.is_empty(),
            };
            if !passes {
                result.constraint_violations += 1;
            }
        }

        // Isolation violations: count pairs sharing the same group at location_labels[0].
        if let Some(label_key) = rule.location_labels.first() {
            let groups: Vec<Option<&str>> = replica_set
                .iter()
                .map(|wid| {
                    worker_labels
                        .get(wid)
                        .and_then(|l| l.get(label_key))
                        .map(|v| v.as_str())
                })
                .collect();

            for i in 0..groups.len() {
                for j in (i + 1)..groups.len() {
                    if let (Some(a), Some(b)) = (groups[i], groups[j]) {
                        if a == b {
                            result.isolation_violations += 1;
                        }
                    }
                }
            }
        }
    }

    result
}

/// Find the replica contributing the most violations in the set.
pub fn find_worst_replica(
    replica_set: &[u32],
    rules: &[PlacementRule],
    worker_labels: &HashMap<u32, HashMap<String, String>>,
) -> Option<(u32, f64)> {
    if replica_set.is_empty() {
        return None;
    }

    let location_label = rules
        .iter()
        .find_map(|r| r.location_labels.first())
        .cloned();

    let mut worst: Option<(u32, f64)> = None;

    for &wid in replica_set {
        let mut score = 0.0f64;

        // Constraint violation penalty.
        let labels = worker_labels.get(&wid);
        for rule in rules {
            let passes = match labels {
                Some(l) => rule.label_constraints.iter().all(|lc| lc.matches(l)),
                None => rule.label_constraints.is_empty(),
            };
            if !passes {
                score += 100.0;
            }
        }

        // Isolation overlap penalty.
        if let Some(ref label_key) = location_label {
            let my_group = labels.and_then(|l| l.get(label_key.as_str()));
            for &other in replica_set {
                if other == wid {
                    continue;
                }
                let other_group = worker_labels
                    .get(&other)
                    .and_then(|l| l.get(label_key.as_str()));
                if my_group.is_some() && my_group == other_group {
                    score += 1.0;
                }
            }
        }

        if worst.is_none() || score > worst.unwrap().1 {
            worst = Some((wid, score));
        }
    }

    worst
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn label_op_in_matches() {
        let c = LabelConstraint {
            key: "az".to_string(),
            op: LabelOp::In,
            values: vec!["us-east-1a".to_string(), "us-east-1b".to_string()],
        };
        assert!(c.matches(&labels(&[("az", "us-east-1a")])));
        assert!(!c.matches(&labels(&[("az", "us-west-2a")])));
        assert!(!c.matches(&labels(&[])));
    }

    #[test]
    fn label_op_not_in_matches() {
        let c = LabelConstraint {
            key: "az".to_string(),
            op: LabelOp::NotIn,
            values: vec!["us-east-1a".to_string()],
        };
        assert!(!c.matches(&labels(&[("az", "us-east-1a")])));
        assert!(c.matches(&labels(&[("az", "us-west-2a")])));
        // Missing key passes NotIn
        assert!(c.matches(&labels(&[])));
    }

    #[test]
    fn label_op_exists_matches() {
        let c = LabelConstraint {
            key: "rack".to_string(),
            op: LabelOp::Exists,
            values: vec![],
        };
        assert!(c.matches(&labels(&[("rack", "r1")])));
        assert!(!c.matches(&labels(&[])));
    }

    #[test]
    fn label_op_not_exists_matches() {
        let c = LabelConstraint {
            key: "rack".to_string(),
            op: LabelOp::NotExists,
            values: vec![],
        };
        assert!(!c.matches(&labels(&[("rack", "r1")])));
        assert!(c.matches(&labels(&[])));
    }

    // --- filter_by_constraints tests ---

    fn az_rule_in(azs: &[&str]) -> PlacementRule {
        PlacementRule {
            id: "test".to_string(),
            label_constraints: vec![LabelConstraint {
                key: "az".to_string(),
                op: LabelOp::In,
                values: azs.iter().map(|s| s.to_string()).collect(),
            }],
            location_labels: vec![],
            isolation_level: 0,
        }
    }

    fn cross_az_rule() -> PlacementRule {
        PlacementRule {
            id: "cross-az".to_string(),
            label_constraints: vec![],
            location_labels: vec!["az".to_string()],
            isolation_level: 0,
        }
    }

    fn worker_labels_map(data: &[(u32, &[(&str, &str)])]) -> HashMap<u32, HashMap<String, String>> {
        data.iter()
            .map(|(wid, pairs)| (*wid, labels(pairs)))
            .collect()
    }

    #[test]
    fn filter_by_constraints_basic() {
        let rules = vec![az_rule_in(&["az1", "az2"])];
        let wl = worker_labels_map(&[
            (1, &[("az", "az1")]),
            (2, &[("az", "az2")]),
            (3, &[("az", "az3")]),
        ]);
        let result = filter_by_constraints(&[1, 2, 3], &rules, &wl);
        assert_eq!(result, vec![1, 2]);
    }

    #[test]
    fn filter_by_constraints_no_rules() {
        let wl = worker_labels_map(&[(1, &[("az", "az1")])]);
        let result = filter_by_constraints(&[1], &[], &wl);
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn filter_by_constraints_all_filtered() {
        let rules = vec![az_rule_in(&["az1"])];
        let wl = worker_labels_map(&[(1, &[("az", "az2")]), (2, &[("az", "az3")])]);
        let result = filter_by_constraints(&[1, 2], &rules, &wl);
        assert!(result.is_empty());
    }

    #[test]
    fn filter_by_constraints_missing_labels() {
        let rules = vec![az_rule_in(&["az1"])];
        let wl = HashMap::new(); // no labels for any worker
        let result = filter_by_constraints(&[1, 2], &rules, &wl);
        assert!(result.is_empty());
    }

    // --- filter_by_isolation tests ---

    #[test]
    fn filter_by_isolation_cross_az() {
        let rules = vec![cross_az_rule()];
        let wl = worker_labels_map(&[
            (1, &[("az", "az1")]),
            (2, &[("az", "az1")]),
            (3, &[("az", "az2")]),
            (4, &[("az", "az3")]),
        ]);
        // Existing replica in az1, should filter to az2 and az3.
        let result = filter_by_isolation(&[2, 3, 4], &rules, &[1], &wl);
        assert_eq!(result, vec![3, 4]);
    }

    #[test]
    fn filter_by_isolation_relaxation() {
        let rules = vec![cross_az_rule()];
        let wl = worker_labels_map(&[
            (1, &[("az", "az1")]),
            (2, &[("az", "az1")]),
            (3, &[("az", "az2")]),
        ]);
        // All groups covered by existing replicas -> relaxation returns all.
        let result = filter_by_isolation(&[2, 3], &rules, &[1, 3], &wl);
        assert_eq!(result, vec![2, 3]);
    }

    #[test]
    fn filter_by_isolation_no_location_labels() {
        let rules = vec![PlacementRule::default_rule()];
        let wl = worker_labels_map(&[(1, &[("az", "az1")])]);
        let result = filter_by_isolation(&[1], &rules, &[], &wl);
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn filter_by_isolation_no_existing_replicas() {
        let rules = vec![cross_az_rule()];
        let wl = worker_labels_map(&[(1, &[("az", "az1")]), (2, &[("az", "az2")])]);
        // No existing replicas -> no occupied groups -> all pass.
        let result = filter_by_isolation(&[1, 2], &rules, &[], &wl);
        assert_eq!(result, vec![1, 2]);
    }

    // --- check_violations tests ---

    #[test]
    fn check_violations_no_violations() {
        let rules = vec![PlacementRule {
            id: "test".to_string(),
            label_constraints: vec![LabelConstraint {
                key: "az".to_string(),
                op: LabelOp::In,
                values: vec!["az1".to_string(), "az2".to_string(), "az3".to_string()],
            }],
            location_labels: vec!["az".to_string()],
            isolation_level: 0,
        }];
        let wl = worker_labels_map(&[
            (1, &[("az", "az1")]),
            (2, &[("az", "az2")]),
            (3, &[("az", "az3")]),
        ]);
        let v = check_violations(&[1, 2, 3], &rules, &wl);
        assert_eq!(v.constraint_violations, 0);
        assert_eq!(v.isolation_violations, 0);
        assert!(!v.has_violation());
    }

    #[test]
    fn check_violations_constraint_only() {
        let rules = vec![az_rule_in(&["az1", "az2"])];
        let wl = worker_labels_map(&[
            (1, &[("az", "az1")]),
            (2, &[("az", "az3")]), // violates
        ]);
        let v = check_violations(&[1, 2], &rules, &wl);
        assert_eq!(v.constraint_violations, 1);
        assert_eq!(v.isolation_violations, 0);
    }

    #[test]
    fn check_violations_isolation_only() {
        let rules = vec![cross_az_rule()];
        let wl = worker_labels_map(&[
            (1, &[("az", "az1")]),
            (2, &[("az", "az1")]), // same az as 1
            (3, &[("az", "az2")]),
        ]);
        let v = check_violations(&[1, 2, 3], &rules, &wl);
        assert_eq!(v.constraint_violations, 0);
        assert_eq!(v.isolation_violations, 1); // pair (1,2)
    }

    #[test]
    fn check_violations_mixed() {
        let rules = vec![PlacementRule {
            id: "test".to_string(),
            label_constraints: vec![LabelConstraint {
                key: "az".to_string(),
                op: LabelOp::In,
                values: vec!["az1".to_string(), "az2".to_string()],
            }],
            location_labels: vec!["az".to_string()],
            isolation_level: 0,
        }];
        let wl = worker_labels_map(&[
            (1, &[("az", "az1")]),
            (2, &[("az", "az1")]), // same az
            (3, &[("az", "az3")]), // constraint violation
        ]);
        let v = check_violations(&[1, 2, 3], &rules, &wl);
        assert_eq!(v.constraint_violations, 1); // worker 3
        assert_eq!(v.isolation_violations, 1); // pair (1,2)
        assert!(v.has_violation());
        assert_eq!(v.total(), 2);
    }

    #[test]
    fn check_violations_empty_replica_set() {
        let v = check_violations(&[], &[cross_az_rule()], &HashMap::new());
        assert!(!v.has_violation());
    }

    // --- find_worst_replica tests ---

    #[test]
    fn find_worst_replica_constraint_violation() {
        let rules = vec![az_rule_in(&["az1", "az2"])];
        let wl = worker_labels_map(&[
            (1, &[("az", "az1")]),
            (2, &[("az", "az3")]), // constraint violation
        ]);
        let (worst, score) = find_worst_replica(&[1, 2], &rules, &wl).unwrap();
        assert_eq!(worst, 2);
        assert!(score >= 100.0);
    }

    #[test]
    fn find_worst_replica_isolation_overlap() {
        let rules = vec![cross_az_rule()];
        let wl = worker_labels_map(&[
            (1, &[("az", "az1")]),
            (2, &[("az", "az1")]), // same az as 1
            (3, &[("az", "az2")]),
        ]);
        let (worst, _score) = find_worst_replica(&[1, 2, 3], &rules, &wl).unwrap();
        // Worker 1 or 2 should be worst (both have isolation overlap with each other).
        assert!(worst == 1 || worst == 2);
    }

    #[test]
    fn find_worst_replica_empty() {
        assert!(find_worst_replica(&[], &[cross_az_rule()], &HashMap::new()).is_none());
    }
}
