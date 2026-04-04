use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::{HashMap, HashSet};

/// Worker candidate with pre-computed scoring inputs.
#[derive(Debug, Clone)]
pub struct WorkerCandidate {
    pub worker_id: u32,
    /// Number of BGs currently hosted on this worker.
    pub bg_count: u32,
    /// Total capacity (bytes) for the relevant storage type.
    pub capacity_bytes: u64,
    /// Used bytes for the relevant storage type.
    pub used_bytes: u64,
    /// Worker labels (az, rack, etc.).
    pub labels: HashMap<String, String>,
}

/// Extensible worker selection strategy.
pub trait WorkerSelector: Send + Sync {
    /// Strategy name for logging and dynamic switching.
    fn name(&self) -> &str;

    /// Select `count` workers from `candidates`, excluding `exclude`.
    fn select(
        &self,
        candidates: &[WorkerCandidate],
        count: usize,
        exclude: &HashSet<u32>,
    ) -> Vec<u32>;
}

/// Default selector using normalized scoring: BG count + storage usage rate.
pub struct NormalizedSelector {
    pub bg_weight: f64,
    pub capacity_weight: f64,
}

impl Default for NormalizedSelector {
    fn default() -> Self {
        Self {
            bg_weight: 0.6,
            capacity_weight: 0.4,
        }
    }
}

impl WorkerSelector for NormalizedSelector {
    fn name(&self) -> &str {
        "normalized"
    }

    fn select(
        &self,
        candidates: &[WorkerCandidate],
        count: usize,
        exclude: &HashSet<u32>,
    ) -> Vec<u32> {
        let mut selected: Vec<u32> = Vec::with_capacity(count);

        for _ in 0..count {
            let eligible: Vec<&WorkerCandidate> = candidates
                .iter()
                .filter(|c| !exclude.contains(&c.worker_id) && !selected.contains(&c.worker_id))
                .collect();

            if eligible.is_empty() {
                break;
            }

            // max(max_bg, 1) to avoid division by zero on empty clusters.
            let max_bg = eligible
                .iter()
                .map(|c| c.bg_count)
                .max()
                .unwrap_or(1)
                .max(1) as f64;

            let mut best_worker = None;
            let mut best_score = f64::MAX;

            for &c in &eligible {
                let bg_norm = c.bg_count as f64 / max_bg;
                // capacity_bytes=0 (worker not yet reported) treated as full.
                let usage_norm = if c.capacity_bytes == 0 {
                    1.0
                } else {
                    c.used_bytes as f64 / c.capacity_bytes as f64
                };
                let score = self.bg_weight * bg_norm + self.capacity_weight * usage_norm;

                if score < best_score {
                    best_score = score;
                    best_worker = Some(c.worker_id);
                }
            }

            if let Some(wid) = best_worker {
                selected.push(wid);
            } else {
                break;
            }
        }

        selected
    }
}

/// Random selector for testing and baseline comparison.
pub struct RandomSelector;

impl WorkerSelector for RandomSelector {
    fn name(&self) -> &str {
        "random"
    }

    fn select(
        &self,
        candidates: &[WorkerCandidate],
        count: usize,
        exclude: &HashSet<u32>,
    ) -> Vec<u32> {
        let mut eligible: Vec<u32> = candidates
            .iter()
            .filter(|c| !exclude.contains(&c.worker_id))
            .map(|c| c.worker_id)
            .collect();
        eligible.shuffle(&mut thread_rng());
        eligible.truncate(count);
        eligible
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_candidate(
        worker_id: u32,
        bg_count: u32,
        capacity_bytes: u64,
        used_bytes: u64,
        labels: HashMap<String, String>,
    ) -> WorkerCandidate {
        WorkerCandidate {
            worker_id,
            bg_count,
            capacity_bytes,
            used_bytes,
            labels,
        }
    }

    fn az_labels(az: &str) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("az".to_string(), az.to_string());
        m
    }

    #[test]
    fn normalized_name() {
        assert_eq!(NormalizedSelector::default().name(), "normalized");
    }

    #[test]
    fn basic_selection_picks_lowest_score() {
        let selector = NormalizedSelector::default();
        let candidates = vec![
            make_candidate(1, 10, 1000, 500, HashMap::new()),
            make_candidate(2, 5, 1000, 300, HashMap::new()),
            make_candidate(3, 8, 1000, 800, HashMap::new()),
        ];
        let result = selector.select(&candidates, 2, &HashSet::new());
        assert_eq!(result.len(), 2);
        // Worker 2 should be picked first (lowest bg_count and usage)
        assert_eq!(result[0], 2);
    }

    #[test]
    fn exclude_set_respected() {
        let selector = NormalizedSelector::default();
        let candidates = vec![
            make_candidate(1, 0, 1000, 0, HashMap::new()),
            make_candidate(2, 0, 1000, 0, HashMap::new()),
        ];
        let mut exclude = HashSet::new();
        exclude.insert(1);
        let result = selector.select(&candidates, 1, &exclude);
        assert_eq!(result, vec![2]);
    }

    #[test]
    fn max_bg_count_zero_no_panic() {
        // All workers have bg_count=0 — should not panic on division.
        let selector = NormalizedSelector::default();
        let candidates = vec![
            make_candidate(1, 0, 1000, 100, HashMap::new()),
            make_candidate(2, 0, 1000, 200, HashMap::new()),
            make_candidate(3, 0, 1000, 300, HashMap::new()),
        ];
        let result = selector.select(&candidates, 2, &HashSet::new());
        assert_eq!(result.len(), 2);
        // bg_norm=0 for all, so pure capacity sort: worker 1 (lowest usage) first
        assert_eq!(result[0], 1);
        assert_eq!(result[1], 2);
    }

    #[test]
    fn capacity_bytes_zero_treated_as_full() {
        let selector = NormalizedSelector::default();
        let candidates = vec![
            make_candidate(1, 0, 0, 0, HashMap::new()), // no capacity info -> full
            make_candidate(2, 0, 1000, 100, HashMap::new()), // 10% used
        ];
        let result = selector.select(&candidates, 1, &HashSet::new());
        // Worker 2 should be preferred (lower usage_norm)
        assert_eq!(result, vec![2]);
    }

    #[test]
    fn empty_candidates_returns_empty() {
        let selector = NormalizedSelector::default();
        let result = selector.select(&[], 3, &HashSet::new());
        assert!(result.is_empty());
    }

    #[test]
    fn count_exceeds_candidates() {
        let selector = NormalizedSelector::default();
        let candidates = vec![
            make_candidate(1, 0, 1000, 0, HashMap::new()),
            make_candidate(2, 0, 1000, 0, HashMap::new()),
        ];
        let result = selector.select(&candidates, 5, &HashSet::new());
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn custom_weights() {
        // Pure bg_count weight
        let selector = NormalizedSelector {
            bg_weight: 1.0,
            capacity_weight: 0.0,
        };
        let candidates = vec![
            make_candidate(1, 10, 1000, 100, HashMap::new()), // high bg, low usage
            make_candidate(2, 1, 1000, 900, HashMap::new()),  // low bg, high usage
        ];
        let result = selector.select(&candidates, 1, &HashSet::new());
        assert_eq!(result, vec![2]); // lower bg_count wins
    }

    #[test]
    fn random_name() {
        assert_eq!(RandomSelector.name(), "random");
    }

    #[test]
    fn random_excludes_correctly() {
        let selector = RandomSelector;
        let candidates = vec![
            make_candidate(1, 0, 0, 0, HashMap::new()),
            make_candidate(2, 0, 0, 0, HashMap::new()),
            make_candidate(3, 0, 0, 0, HashMap::new()),
        ];
        let mut exclude = HashSet::new();
        exclude.insert(2);
        let result = selector.select(&candidates, 3, &exclude);
        assert!(!result.contains(&2));
        assert!(result.len() <= 2);
    }

    #[test]
    fn random_respects_count() {
        let selector = RandomSelector;
        let candidates = vec![
            make_candidate(1, 0, 0, 0, HashMap::new()),
            make_candidate(2, 0, 0, 0, HashMap::new()),
            make_candidate(3, 0, 0, 0, HashMap::new()),
        ];
        let result = selector.select(&candidates, 1, &HashSet::new());
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn random_empty_candidates() {
        let selector = RandomSelector;
        let result = selector.select(&[], 3, &HashSet::new());
        assert!(result.is_empty());
    }
}
