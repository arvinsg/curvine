use super::rule::PlacementRule;
use std::collections::HashMap;

/// Result of evaluating a BG's placement against rules.
#[derive(Debug, Clone, Default)]
pub struct BGFit {
    /// Number of replicas that violate label constraints.
    pub constraint_violations: u32,
    /// Isolation score: 0.0 = perfect isolation, higher = worse.
    pub isolation_score: f64,
    /// > 0 means missing replicas, < 0 means too many.
    pub missing_replicas: i32,
}

impl BGFit {
    pub fn is_perfect(&self) -> bool {
        self.constraint_violations == 0 && self.isolation_score == 0.0 && self.missing_replicas == 0
    }

    pub fn has_violation(&self) -> bool {
        self.constraint_violations > 0 || self.isolation_score > 0.0
    }
}

/// Evaluate how well a BG's replica set fits the placement rules.
pub fn fit_bg(
    replica_set: &[u32],
    rules: &[PlacementRule],
    worker_labels: &HashMap<u32, HashMap<String, String>>,
) -> BGFit {
    if rules.is_empty() || replica_set.is_empty() {
        return BGFit::default();
    }

    let mut constraint_violations = 0u32;
    let mut isolation_score = 0.0f64;

    for rule in rules {
        // Check label constraints for each replica
        for &worker_id in replica_set {
            let empty = HashMap::new();
            let labels = worker_labels.get(&worker_id).unwrap_or(&empty);
            let all_match = rule.label_constraints.iter().all(|c| c.matches(labels));
            if !all_match {
                constraint_violations += 1;
            }
        }

        // Compute isolation score along location_labels
        if !rule.location_labels.is_empty() && replica_set.len() > 1 {
            for i in 0..replica_set.len() {
                for j in (i + 1)..replica_set.len() {
                    let empty = HashMap::new();
                    let labels_i = worker_labels.get(&replica_set[i]).unwrap_or(&empty);
                    let labels_j = worker_labels.get(&replica_set[j]).unwrap_or(&empty);

                    for (d, label_key) in rule.location_labels.iter().enumerate() {
                        if d < rule.isolation_level {
                            continue;
                        }
                        let val_i = labels_i.get(label_key);
                        let val_j = labels_j.get(label_key);
                        // If both have the same label value at this level, penalize
                        if val_i.is_some() && val_i == val_j {
                            isolation_score += 1.0 / (d as f64 + 1.0);
                            break; // Once we find a shared level, don't check deeper
                        } else {
                            break; // Isolated at this level, no deeper penalty
                        }
                    }
                }
            }
        }
    }

    BGFit {
        constraint_violations,
        isolation_score,
        missing_replicas: 0,
    }
}

/// Compare two fits: returns true if `new` is strictly better than `old`.
pub fn is_better_fit(old: &BGFit, new: &BGFit) -> bool {
    if new.constraint_violations < old.constraint_violations {
        return true;
    }
    if new.constraint_violations > old.constraint_violations {
        return false;
    }
    // Same violations: compare isolation_score
    if new.isolation_score < old.isolation_score {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::schedule::placement::rule::{LabelConstraint, LabelOp, PlacementRule};

    fn make_labels(az: &str, rack: &str) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("az".to_string(), az.to_string());
        m.insert("rack".to_string(), rack.to_string());
        m
    }

    fn default_rule() -> PlacementRule {
        PlacementRule {
            id: "test".to_string(),
            label_constraints: vec![],
            location_labels: vec!["az".to_string(), "rack".to_string()],
            isolation_level: 0,
        }
    }

    #[test]
    fn perfect_isolation() {
        let mut worker_labels = HashMap::new();
        worker_labels.insert(1, make_labels("az1", "r1"));
        worker_labels.insert(2, make_labels("az2", "r2"));
        worker_labels.insert(3, make_labels("az3", "r3"));

        let fit = fit_bg(&[1, 2, 3], &[default_rule()], &worker_labels);
        assert_eq!(fit.constraint_violations, 0);
        assert_eq!(fit.isolation_score, 0.0);
        assert!(fit.is_perfect());
    }

    #[test]
    fn same_az_penalty() {
        let mut worker_labels = HashMap::new();
        worker_labels.insert(1, make_labels("az1", "r1"));
        worker_labels.insert(2, make_labels("az1", "r2")); // same AZ
        worker_labels.insert(3, make_labels("az2", "r3"));

        let fit = fit_bg(&[1, 2, 3], &[default_rule()], &worker_labels);
        assert_eq!(fit.constraint_violations, 0);
        // Pair (1,2) shares az1 at depth 0 → penalty 1.0/(0+1) = 1.0
        assert!(fit.isolation_score > 0.0);
    }

    #[test]
    fn constraint_violation() {
        let rule = PlacementRule {
            id: "test".to_string(),
            label_constraints: vec![LabelConstraint {
                key: "az".to_string(),
                op: LabelOp::In,
                values: vec!["az1".to_string(), "az2".to_string()],
            }],
            location_labels: vec![],
            isolation_level: 0,
        };

        let mut worker_labels = HashMap::new();
        worker_labels.insert(1, make_labels("az1", "r1"));
        worker_labels.insert(2, make_labels("az3", "r2")); // violates In constraint

        let fit = fit_bg(&[1, 2], &[rule], &worker_labels);
        assert_eq!(fit.constraint_violations, 1);
    }

    #[test]
    fn is_better_fit_fewer_violations() {
        let old = BGFit { constraint_violations: 2, isolation_score: 0.0, missing_replicas: 0 };
        let new = BGFit { constraint_violations: 1, isolation_score: 0.0, missing_replicas: 0 };
        assert!(is_better_fit(&old, &new));
        assert!(!is_better_fit(&new, &old));
    }

    #[test]
    fn is_better_fit_lower_isolation_score() {
        let old = BGFit { constraint_violations: 0, isolation_score: 2.0, missing_replicas: 0 };
        let new = BGFit { constraint_violations: 0, isolation_score: 1.0, missing_replicas: 0 };
        assert!(is_better_fit(&old, &new));
    }

    #[test]
    fn is_better_fit_equal_returns_false() {
        let fit = BGFit { constraint_violations: 0, isolation_score: 1.0, missing_replicas: 0 };
        assert!(!is_better_fit(&fit, &fit.clone()));
    }
}
