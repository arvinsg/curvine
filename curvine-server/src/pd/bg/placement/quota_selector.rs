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
use super::selector::WorkerSelector;
use curvine_common::{FsError, FsResult};
use std::collections::{HashMap, HashSet};

/// Equal-weight quota selector (default strategy).
///
/// Selects workers with the highest "hunger" (quota - effective count).
/// Reads effective counts from PolicyState at each select call.
pub struct QuotaSelector {
    worker_bg_quota: HashMap<u32, u32>,
    worker_lease_quota: HashMap<u32, u32>,
}

impl QuotaSelector {
    pub fn new() -> Self {
        Self {
            worker_bg_quota: HashMap::new(),
            worker_lease_quota: HashMap::new(),
        }
    }
}

impl Default for QuotaSelector {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerSelector for QuotaSelector {
    fn name(&self) -> &str {
        "quota"
    }

    fn init_from_policy(&mut self, _ctx: &PlacementContext<'_>, st: &PolicyState) {
        self.worker_bg_quota = st.worker_bg_quota.clone();
        self.worker_lease_quota = st.worker_lease_quota.clone();
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
        let mut selected: Vec<u32> = Vec::with_capacity(count);

        for _ in 0..count {
            let mut best: Option<u32> = None;
            let mut best_hunger = i64::MIN;

            for &wid in candidate_ids {
                if exclude.contains(&wid) || selected.contains(&wid) {
                    continue;
                }
                let quota = *self.worker_bg_quota.get(&wid).unwrap_or(&0);
                let effective = st
                    .worker_bg_effective
                    .get(&wid)
                    .copied()
                    .unwrap_or(0)
                    .max(0) as u32;
                let hunger = quota as i64 - effective as i64;
                if hunger > best_hunger
                    || (hunger == best_hunger && best.map_or(true, |b| wid < b))
                {
                    best_hunger = hunger;
                    best = Some(wid);
                }
            }

            match best {
                Some(wid) => selected.push(wid),
                None => break,
            }
        }

        Ok(selected)
    }

    fn select_lease_owner(
        &mut self,
        st: &PolicyState,
        candidate_ids: &[u32],
    ) -> FsResult<u32> {
        if candidate_ids.is_empty() {
            return Err(FsError::common(
                "select_lease_owner: empty candidates".to_string(),
            ));
        }
        match candidate_ids
            .iter()
            .copied()
            .max_by_key(|&wid| {
                let quota = *self.worker_lease_quota.get(&wid).unwrap_or(&0) as i64;
                let effective = st
                    .worker_lease_effective
                    .get(&wid)
                    .copied()
                    .unwrap_or(0)
                    .max(0);
                (quota - effective, std::cmp::Reverse(wid))
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

    fn make_policy_state(bg_quotas: &[(u32, u32)], bg_effective: &[(u32, i64)]) -> PolicyState {
        PolicyState {
            worker_bg_quota: bg_quotas.iter().copied().collect(),
            worker_lease_quota: bg_quotas.iter().map(|&(w, q)| (w, q / 2)).collect(),
            worker_bg_effective: bg_effective.iter().copied().collect(),
            worker_lease_effective: bg_effective.iter().map(|&(w, e)| (w, e / 2)).collect(),
            bg_load_score: HashMap::new(),
            lease_load_score: HashMap::new(),
        }
    }

    #[test]
    fn test_select_hungriest() {
        let mut sel = QuotaSelector::new();
        let st = make_policy_state(&[(1, 4), (2, 4), (3, 4)], &[(1, 0), (2, 0), (3, 0)]);
        sel.worker_bg_quota = st.worker_bg_quota.clone();
        let r = sel.select(&st, &[1, 2, 3], 1, &HashSet::new()).unwrap();
        assert_eq!(r, vec![1]); // all equal, lowest id wins
    }

    #[test]
    fn test_select_favors_more_hungry() {
        let mut sel = QuotaSelector::new();
        let st = make_policy_state(&[(1, 4), (2, 4)], &[(1, 3), (2, 1)]);
        sel.worker_bg_quota = st.worker_bg_quota.clone();
        // Worker 2 hunger = 4-1=3, worker 1 hunger = 4-3=1
        let r = sel.select(&st, &[1, 2], 1, &HashSet::new()).unwrap();
        assert_eq!(r, vec![2]);
    }

    #[test]
    fn test_select_empty_error() {
        let mut sel = QuotaSelector::new();
        let st = make_policy_state(&[], &[]);
        assert!(sel.select(&st, &[], 1, &HashSet::new()).is_err());
    }

    #[test]
    fn test_select_excludes() {
        let mut sel = QuotaSelector::new();
        let st = make_policy_state(&[(1, 4), (2, 4)], &[(1, 0), (2, 0)]);
        sel.worker_bg_quota = st.worker_bg_quota.clone();
        let mut exclude = HashSet::new();
        exclude.insert(1);
        let r = sel.select(&st, &[1, 2], 1, &exclude).unwrap();
        assert_eq!(r, vec![2]);
    }

    #[test]
    fn test_lease_owner() {
        let mut sel = QuotaSelector::new();
        let st = make_policy_state(&[(1, 4), (2, 4)], &[(1, 2), (2, 0)]);
        sel.worker_lease_quota = st.worker_lease_quota.clone();
        let owner = sel.select_lease_owner(&st, &[1, 2]).unwrap();
        assert_eq!(owner, 2); // worker 2 has more lease hunger
    }
}
