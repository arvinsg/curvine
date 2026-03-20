use curvine_common::state::PlacementPolicy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    pub fn from_placement_policy(policy: PlacementPolicy, default_location_labels: &[String]) -> Self {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
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
}
