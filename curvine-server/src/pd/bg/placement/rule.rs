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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type Labels = HashMap<u32, HashMap<String, String>>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LabelOp {
    In,
    NotIn,
    Exists,
    NotExists,
}

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

/// Placement rule = hard label constraints + hierarchical topology for isolation scoring.
///
/// `label_constraints` filters workers into a legal set (hard).
/// `location_labels` describes the failure-domain hierarchy (coarse → fine,
/// e.g. `["az", "rack"]`). Used by [`isolation_score`] to quantify replica
/// diversity — higher score = better spread.
///
/// `min_isolation_level` (optional) is a **hard** isolation constraint: replicas
/// must NOT share the same value at this label level. Must be one of the entries
/// in `location_labels`. When set, [`filter_min_isolation`] enforces it as a
/// mandatory pre-filter before the soft score path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementRule {
    pub id: String,
    pub label_constraints: Vec<LabelConstraint>,
    pub location_labels: Vec<String>,
    #[serde(default)]
    pub min_isolation_level: Option<String>,
}

impl PlacementRule {
    pub fn default_rule() -> Self {
        Self {
            id: "default".into(),
            label_constraints: vec![],
            location_labels: vec![],
            min_isolation_level: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.label_constraints.is_empty()
            && self.location_labels.is_empty()
            && self.min_isolation_level.is_none()
    }

    /// Return workers that satisfy all label constraints.
    pub fn filter(&self, workers: &[u32], labels: &Labels) -> Vec<u32> {
        if self.label_constraints.is_empty() {
            return workers.to_vec();
        }
        workers
            .iter()
            .copied()
            .filter(|wid| {
                labels
                    .get(wid)
                    .map(|l| self.label_constraints.iter().all(|lc| lc.matches(l)))
                    .unwrap_or(false)
            })
            .collect()
    }
}

const ISOLATION_BASE: f64 = 100.0;
const ISOLATION_CONSTRAINT_PENALTY: f64 = 100_000.0;

/// Hierarchical isolation score for a replica set.
pub fn isolation_score(replicas: &[u32], location_labels: &[String], labels: &Labels) -> f64 {
    let levels = location_labels.len();
    if levels == 0 || replicas.len() <= 1 {
        return 0.0;
    }
    let mut score = 0.0;
    for i in 0..replicas.len() {
        for j in (i + 1)..replicas.len() {
            if let Some(diff) = first_diff_level(replicas[i], replicas[j], location_labels, labels)
            {
                score += ISOLATION_BASE.powi((levels - diff - 1) as i32);
            }
        }
    }
    score
}

fn first_diff_level(a: u32, b: u32, location_labels: &[String], labels: &Labels) -> Option<usize> {
    let la = labels.get(&a);
    let lb = labels.get(&b);
    for (i, key) in location_labels.iter().enumerate() {
        let va = la.and_then(|l| l.get(key));
        let vb = lb.and_then(|l| l.get(key));
        if va != vb {
            return Some(i);
        }
    }
    None
}

/// Compute the isolation score of `replicas + [candidate]` without allocating
/// a new Vec. Returns the *incremental* score contributed by `candidate`.
pub fn incremental_isolation_score(
    existing: &[u32],
    candidate: u32,
    location_labels: &[String],
    labels: &Labels,
) -> f64 {
    let levels = location_labels.len();
    if levels == 0 {
        return 0.0;
    }
    let mut score = 0.0;
    for &r in existing {
        if let Some(diff) = first_diff_level(r, candidate, location_labels, labels) {
            score += ISOLATION_BASE.powi((levels - diff - 1) as i32);
        }
    }
    score
}

/// Return candidates that yield the highest incremental isolation score.
/// When all candidates score equally (including 0), the full list is returned.
pub fn best_isolation_candidates(
    candidates: &[u32],
    existing: &[u32],
    location_labels: &[String],
    labels: &Labels,
) -> Vec<u32> {
    if location_labels.is_empty() || candidates.is_empty() {
        return candidates.to_vec();
    }
    let scores: Vec<(u32, f64)> = candidates
        .iter()
        .map(|&wid| {
            (
                wid,
                incremental_isolation_score(existing, wid, location_labels, labels),
            )
        })
        .collect();
    let max = scores
        .iter()
        .map(|(_, s)| *s)
        .fold(f64::NEG_INFINITY, f64::max);
    scores
        .into_iter()
        .filter(|(_, s)| (*s - max).abs() < f64::EPSILON)
        .map(|(wid, _)| wid)
        .collect()
}

/// Hard isolation filter: from `candidates`, keep only workers whose label value
/// at `min_level` (and all coarser levels in `location_labels`) does NOT collide
/// with any existing replica.
pub fn filter_min_isolation(
    candidates: &[u32],
    existing_replicas: &[u32],
    min_level: &str,
    location_labels: &[String],
    labels: &Labels,
) -> Vec<u32> {
    let min_idx = match location_labels.iter().position(|l| l == min_level) {
        Some(idx) => idx,
        None => return candidates.to_vec(),
    };

    candidates
        .iter()
        .copied()
        .filter(|&wid| {
            for &rid in existing_replicas {
                let collides = (0..=min_idx).all(|lvl| {
                    let key = &location_labels[lvl];
                    let va = labels.get(&wid).and_then(|l| l.get(key));
                    let vb = labels.get(&rid).and_then(|l| l.get(key));
                    va == vb && va.is_some()
                });
                if collides {
                    return false;
                }
            }
            true
        })
        .collect()
}

/// Per-worker violation score: constraint penalty (high) + isolation penalty (low).
/// Higher = worse. Used by PlacementRuleChecker to find the worst replica.
pub fn worst_replica(replicas: &[u32], rule: &PlacementRule, labels: &Labels) -> Option<u32> {
    if replicas.is_empty() {
        return None;
    }
    let levels = rule.location_labels.len();
    let mut worst: Option<(u32, f64)> = None;

    for &wid in replicas {
        let mut penalty = 0.0f64;

        // Constraint violation: very high penalty.
        let wl = labels.get(&wid);
        let passes = wl
            .map(|l| rule.label_constraints.iter().all(|lc| lc.matches(l)))
            .unwrap_or(rule.label_constraints.is_empty());
        if !passes {
            penalty += ISOLATION_CONSTRAINT_PENALTY;
        }

        // Isolation overlap penalty: for each peer sharing a group at the COARSEST
        // differing level, add a weighted penalty. Same idea as isolation_score
        // but inverted — sharing = bad.
        if levels > 0 {
            for &other in replicas {
                if other == wid {
                    continue;
                }
                if first_diff_level(wid, other, &rule.location_labels, labels).is_none() {
                    penalty += ISOLATION_BASE.powi(levels as i32);
                } else if let Some(diff) =
                    first_diff_level(wid, other, &rule.location_labels, labels)
                {
                    penalty += ISOLATION_BASE.powi(diff as i32);
                }
            }
        }

        if worst.map_or(true, |(_, ws)| penalty > ws) {
            worst = Some((wid, penalty));
        }
    }

    worst.map(|(wid, _)| wid)
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

    fn wl(data: &[(u32, &[(&str, &str)])]) -> Labels {
        data.iter()
            .map(|(wid, pairs)| (*wid, labels(pairs)))
            .collect()
    }

    // ---- LabelConstraint ----

    #[test]
    fn label_op_in() {
        let c = LabelConstraint {
            key: "az".into(),
            op: LabelOp::In,
            values: vec!["a".into(), "b".into()],
        };
        assert!(c.matches(&labels(&[("az", "a")])));
        assert!(!c.matches(&labels(&[("az", "c")])));
        assert!(!c.matches(&labels(&[])));
    }

    #[test]
    fn label_op_not_in() {
        let c = LabelConstraint {
            key: "az".into(),
            op: LabelOp::NotIn,
            values: vec!["a".into()],
        };
        assert!(!c.matches(&labels(&[("az", "a")])));
        assert!(c.matches(&labels(&[("az", "b")])));
        assert!(c.matches(&labels(&[])));
    }

    #[test]
    fn label_op_exists() {
        let c = LabelConstraint {
            key: "rack".into(),
            op: LabelOp::Exists,
            values: vec![],
        };
        assert!(c.matches(&labels(&[("rack", "r1")])));
        assert!(!c.matches(&labels(&[])));
    }

    #[test]
    fn label_op_not_exists() {
        let c = LabelConstraint {
            key: "rack".into(),
            op: LabelOp::NotExists,
            values: vec![],
        };
        assert!(!c.matches(&labels(&[("rack", "r1")])));
        assert!(c.matches(&labels(&[])));
    }

    // ---- PlacementRule::filter ----

    #[test]
    fn filter_basic() {
        let rule = PlacementRule {
            id: "test".into(),
            label_constraints: vec![LabelConstraint {
                key: "az".into(),
                op: LabelOp::In,
                values: vec!["az1".into(), "az2".into()],
            }],
            location_labels: vec![],
            min_isolation_level: None,
        };
        let l = wl(&[
            (1, &[("az", "az1")]),
            (2, &[("az", "az2")]),
            (3, &[("az", "az3")]),
        ]);
        let mut r = rule.filter(&[1, 2, 3], &l);
        r.sort();
        assert_eq!(r, vec![1, 2]);
    }

    #[test]
    fn filter_no_constraints() {
        let rule = PlacementRule::default_rule();
        let l = wl(&[(1, &[("az", "az1")])]);
        assert_eq!(rule.filter(&[1], &l), vec![1]);
    }

    // ---- isolation_score ----

    #[test]
    fn score_empty_labels() {
        let l = wl(&[(1, &[("az", "a")]), (2, &[("az", "b")])]);
        assert_eq!(isolation_score(&[1, 2], &[], &l), 0.0);
    }

    #[test]
    fn score_single_replica() {
        let l = wl(&[(1, &[("az", "a")])]);
        assert_eq!(isolation_score(&[1], &["az".into()], &l), 0.0);
    }

    #[test]
    fn score_cross_az_two_different() {
        let l = wl(&[(1, &[("az", "a")]), (2, &[("az", "b")])]);
        // 1 level: diff at 0 → 100^(1-0-1) = 100^0 = 1
        assert_eq!(isolation_score(&[1, 2], &["az".into()], &l), 1.0);
    }

    #[test]
    fn score_cross_az_two_same() {
        let l = wl(&[(1, &[("az", "a")]), (2, &[("az", "a")])]);
        assert_eq!(isolation_score(&[1, 2], &["az".into()], &l), 0.0);
    }

    #[test]
    fn score_hierarchical() {
        // 2 levels: az, rack
        let loc: Vec<String> = vec!["az".into(), "rack".into()];
        let l = wl(&[
            (1, &[("az", "a"), ("rack", "r1")]),
            (2, &[("az", "a"), ("rack", "r2")]),
            (3, &[("az", "b"), ("rack", "r3")]),
        ]);
        // (1,2): diff at level 1 (rack) → 100^(2-1-1) = 1
        // (1,3): diff at level 0 (az) → 100^(2-0-1) = 100
        // (2,3): diff at level 0 (az) → 100
        assert_eq!(isolation_score(&[1, 2, 3], &loc, &l), 201.0);
    }

    // ---- best_isolation_candidates ----

    #[test]
    fn best_candidates_cross_az() {
        let l = wl(&[
            (1, &[("az", "a")]),
            (2, &[("az", "a")]),
            (3, &[("az", "b")]),
            (4, &[("az", "c")]),
        ]);
        let existing = [1]; // az-a occupied
        let mut best = best_isolation_candidates(&[2, 3, 4], &existing, &["az".into()], &l);
        best.sort();
        // 3 and 4 are in different AZs → max score; 2 is in same AZ → score 0
        assert_eq!(best, vec![3, 4]);
    }

    #[test]
    fn best_candidates_relaxation() {
        let l = wl(&[
            (1, &[("az", "a")]),
            (2, &[("az", "a")]),
            (3, &[("az", "b")]),
        ]);
        // All groups occupied: scores for 2 and 3 are both 0 (nothing new)
        // Wait: existing=[1,3] covers a, b. Candidate 2 adds pair (2,1)=same_az=0 + (2,3)=diff=1. Score=1.
        // And candidate 3: not in candidates list (already in existing). Let me just use [2]:
        let best = best_isolation_candidates(&[2], &[1, 3], &["az".into()], &l);
        assert_eq!(best, vec![2]); // only candidate, natural relaxation
    }

    #[test]
    fn best_candidates_no_location_labels() {
        let l = wl(&[(1, &[("az", "a")]), (2, &[("az", "b")])]);
        let best = best_isolation_candidates(&[1, 2], &[], &[], &l);
        assert_eq!(best, vec![1, 2]);
    }

    // ---- worst_replica ----

    #[test]
    fn worst_constraint_violation() {
        let rule = PlacementRule {
            id: "t".into(),
            label_constraints: vec![LabelConstraint {
                key: "az".into(),
                op: LabelOp::In,
                values: vec!["az1".into(), "az2".into()],
            }],
            location_labels: vec![],
            min_isolation_level: None,
        };
        let l = wl(&[(1, &[("az", "az1")]), (2, &[("az", "az3")])]);
        assert_eq!(worst_replica(&[1, 2], &rule, &l), Some(2));
    }

    #[test]
    fn worst_isolation_overlap() {
        let rule = PlacementRule {
            id: "t".into(),
            label_constraints: vec![],
            location_labels: vec!["az".into()],
            min_isolation_level: None,
        };
        let l = wl(&[
            (1, &[("az", "a")]),
            (2, &[("az", "a")]),
            (3, &[("az", "b")]),
        ]);
        let w = worst_replica(&[1, 2, 3], &rule, &l).unwrap();
        assert!(w == 1 || w == 2);
    }

    #[test]
    fn worst_empty() {
        let rule = PlacementRule::default_rule();
        assert!(worst_replica(&[], &rule, &HashMap::new()).is_none());
    }

    // ---- filter_min_isolation ----

    #[test]
    fn min_isolation_filters_same_az() {
        let l = wl(&[
            (1, &[("az", "a")]),
            (2, &[("az", "a")]),
            (3, &[("az", "b")]),
            (4, &[("az", "c")]),
        ]);
        let loc: Vec<String> = vec!["az".into()];
        let result = filter_min_isolation(&[2, 3, 4], &[1], "az", &loc, &l);
        assert_eq!(result, vec![3, 4]);
    }

    #[test]
    fn min_isolation_returns_empty_when_impossible() {
        let l = wl(&[(1, &[("az", "a")]), (2, &[("az", "a")])]);
        let loc: Vec<String> = vec!["az".into()];
        let result = filter_min_isolation(&[2], &[1], "az", &loc, &l);
        assert!(result.is_empty());
    }

    #[test]
    fn min_isolation_unknown_level_passthrough() {
        let l = wl(&[(1, &[("az", "a")]), (2, &[("az", "a")])]);
        let loc: Vec<String> = vec!["az".into()];
        let result = filter_min_isolation(&[1, 2], &[], "rack", &loc, &l);
        assert_eq!(result, vec![1, 2]);
    }

    #[test]
    fn min_isolation_hierarchical() {
        let l = wl(&[
            (1, &[("az", "a"), ("rack", "r1")]),
            (2, &[("az", "a"), ("rack", "r2")]),
            (3, &[("az", "b"), ("rack", "r1")]),
        ]);
        let loc: Vec<String> = vec!["az".into(), "rack".into()];
        // min_level=rack: must differ at rack (within same az prefix).
        // existing=[1] is (a, r1). Candidate 2 is (a, r2) → az same, rack diff → passes.
        // Candidate 3 is (b, r1) → az differs → passes (not same prefix up to rack level).
        let mut result = filter_min_isolation(&[2, 3], &[1], "rack", &loc, &l);
        result.sort();
        assert_eq!(result, vec![2, 3]);
    }

    #[test]
    fn min_isolation_hierarchical_same_rack() {
        let l = wl(&[
            (1, &[("az", "a"), ("rack", "r1")]),
            (2, &[("az", "a"), ("rack", "r1")]),
            (3, &[("az", "a"), ("rack", "r2")]),
        ]);
        let loc: Vec<String> = vec!["az".into(), "rack".into()];
        // min_level=rack: candidate 2 shares (a, r1) with existing[1] → filtered out.
        let result = filter_min_isolation(&[2, 3], &[1], "rack", &loc, &l);
        assert_eq!(result, vec![3]);
    }
}
