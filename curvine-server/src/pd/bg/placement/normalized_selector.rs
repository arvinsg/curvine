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

use super::context::PlacementContext;
use super::policy::PolicyState;
use super::selector::{WorkerCandidate, WorkerSelector};
use curvine_common::{FsError, FsResult};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, HashSet};

/// Normalized scoring selector with weighted random selection.
pub struct NormalizedSelector {
    pub bg_weight: f64,
    pub capacity_weight: f64,
    worker_available: HashMap<u32, u64>,
    total_available: u64,
    max_bg: u32,
    min_bg: u32,
    rng: StdRng,
}

impl NormalizedSelector {
    pub fn new(bg_weight: f64, capacity_weight: f64) -> Self {
        Self {
            bg_weight,
            capacity_weight,
            worker_available: HashMap::new(),
            total_available: 0,
            max_bg: 0,
            min_bg: 0,
            rng: StdRng::from_entropy(),
        }
    }

    pub fn with_seed(bg_weight: f64, capacity_weight: f64, seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            ..Self::new(bg_weight, capacity_weight)
        }
    }

    fn compute_weight(&self, worker_id: u32, st: &PolicyState) -> f64 {
        let cur_bg = st
            .worker_bg_effective
            .get(&worker_id)
            .copied()
            .unwrap_or(0)
            .max(0) as u32;

        let bg_score = if self.max_bg == self.min_bg {
            1.0
        } else {
            (self.max_bg as f64 - cur_bg as f64) / (self.max_bg as f64 - self.min_bg as f64)
        };

        let available = *self.worker_available.get(&worker_id).unwrap_or(&0) as f64;
        let cap_score = if self.total_available == 0 {
            1.0
        } else {
            available / self.total_available as f64
        };

        (self.bg_weight * bg_score + self.capacity_weight * cap_score).max(0.01)
    }
}

impl Default for NormalizedSelector {
    fn default() -> Self {
        Self::new(0.6, 0.4)
    }
}

impl WorkerSelector for NormalizedSelector {
    fn name(&self) -> &str {
        "normalized"
    }

    fn init_from_policy(&mut self, ctx: &PlacementContext<'_>, st: &PolicyState) {
        self.worker_available = ctx
            .workers
            .iter()
            .map(|(&wid, snap)| (wid, snap.available_bytes()))
            .collect();
        self.total_available = self.worker_available.values().sum();

        let bgs: Vec<i64> = st.worker_bg_effective.values().copied().collect();
        self.max_bg = bgs.iter().copied().max().unwrap_or(0).max(0) as u32;
        self.min_bg = bgs.iter().copied().min().unwrap_or(0).max(0) as u32;
    }

    fn init(&mut self, candidates: &[WorkerCandidate], _bucket_count: u32, _replica_count: u16) {
        self.max_bg = candidates.iter().map(|c| c.bg_count).max().unwrap_or(0);
        self.min_bg = candidates.iter().map(|c| c.bg_count).min().unwrap_or(0);
        self.worker_available = candidates
            .iter()
            .map(|c| (c.worker_id, c.capacity_bytes.saturating_sub(c.used_bytes)))
            .collect();
        self.total_available = self.worker_available.values().sum();
    }

    fn select(
        &mut self,
        st: &PolicyState,
        candidate_ids: &[u32],
        count: usize,
        exclude: &HashSet<u32>,
    ) -> FsResult<Vec<u32>> {
        if candidate_ids.is_empty() {
            return Err(FsError::common("select: empty candidates".to_string()));
        }

        let mut weighted: Vec<(u32, f64)> = candidate_ids
            .iter()
            .copied()
            .filter(|wid| !exclude.contains(wid))
            .map(|wid| (wid, self.compute_weight(wid, st)))
            .collect();

        weighted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut total_weight: f64 = weighted.iter().map(|(_, w)| w).sum();
        let mut selected: Vec<u32> = Vec::with_capacity(count);
        let mut selected_set: HashSet<u32> = HashSet::with_capacity(count);

        for _ in 0..count {
            if total_weight <= 0.0 {
                break;
            }

            let r = self.rng.gen_range(0.0..total_weight);
            let mut cumulative = 0.0;
            let mut picked = None;

            for &(wid, weight) in &weighted {
                if selected_set.contains(&wid) {
                    continue;
                }
                cumulative += weight;
                if cumulative >= r {
                    picked = Some((wid, weight));
                    break;
                }
            }

            match picked {
                Some((wid, weight)) => {
                    selected.push(wid);
                    selected_set.insert(wid);
                    total_weight -= weight;
                }
                None => break,
            }
        }

        Ok(selected)
    }

    fn select_lease_owner(&mut self, st: &PolicyState, candidate_ids: &[u32]) -> FsResult<u32> {
        if candidate_ids.is_empty() {
            return Err(FsError::common(
                "select_lease_owner: empty candidates".to_string(),
            ));
        }
        match candidate_ids.iter().copied().min_by_key(|&wid| {
            let count = st
                .worker_lease_effective
                .get(&wid)
                .copied()
                .unwrap_or(0)
                .max(0);
            (count, wid as i64)
        }) {
            Some(owner) => Ok(owner),
            None => Err(FsError::common(
                "select_lease_owner: no suitable candidate found".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_st(bg_effective: &[(u32, i64)]) -> PolicyState {
        PolicyState {
            worker_bg_quota: HashMap::new(),
            worker_lease_quota: HashMap::new(),
            worker_bg_effective: bg_effective.iter().copied().collect(),
            worker_lease_effective: bg_effective.iter().map(|&(w, e)| (w, e / 2)).collect(),
            bg_load_score: HashMap::new(),
            lease_load_score: HashMap::new(),
        }
    }

    #[test]
    fn test_name() {
        assert_eq!(NormalizedSelector::default().name(), "normalized");
    }

    #[test]
    fn test_excludes() {
        let mut sel = NormalizedSelector::default();
        sel.worker_available = [(1, 500), (2, 500)].iter().copied().collect();
        sel.total_available = 1000;
        sel.max_bg = 0;
        sel.min_bg = 0;

        let st = make_st(&[(1, 0), (2, 0)]);
        let mut exclude = HashSet::new();
        exclude.insert(1);
        let r = sel.select(&st, &[1, 2], 1, &exclude).unwrap();
        assert_eq!(r, vec![2]);
    }

    #[test]
    fn test_empty_error() {
        let mut sel = NormalizedSelector::default();
        let st = make_st(&[]);
        assert!(sel.select(&st, &[], 1, &HashSet::new()).is_err());
    }

    #[test]
    fn test_lease_owner() {
        let mut sel = NormalizedSelector::default();
        let st = make_st(&[(1, 4), (2, 0)]);
        let owner = sel.select_lease_owner(&st, &[1, 2]).unwrap();
        assert_eq!(owner, 2);
    }
}
