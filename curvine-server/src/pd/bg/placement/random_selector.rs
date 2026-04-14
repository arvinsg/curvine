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
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashSet;

/// Random selector for testing and baseline comparison.
pub struct RandomSelector;

impl WorkerSelector for RandomSelector {
    fn name(&self) -> &str {
        "random"
    }

    fn init_from_policy(
        &mut self,
        _ctx: &PlacementContext<'_>,
        _st: &PolicyState,
    ) {
    }

    fn init(
        &mut self,
        _candidates: &[WorkerCandidate],
        _bucket_count: u32,
        _replica_count: u16,
    ) {
    }

    fn select(
        &mut self,
        _st: &PolicyState,
        candidate_ids: &[u32],
        count: usize,
        exclude: &HashSet<u32>,
    ) -> FsResult<Vec<u32>> {
        if candidate_ids.is_empty() {
            return Err(FsError::common("select: empty candidates".to_string()));
        }
        let mut eligible: Vec<u32> = candidate_ids
            .iter()
            .copied()
            .filter(|wid| !exclude.contains(wid))
            .collect();
        eligible.shuffle(&mut thread_rng());
        eligible.truncate(count);
        Ok(eligible)
    }

    fn select_lease_owner(
        &mut self,
        _st: &PolicyState,
        candidate_ids: &[u32],
    ) -> FsResult<u32> {
        if candidate_ids.is_empty() {
            return Err(FsError::common(
                "select_lease_owner: empty candidates".to_string(),
            ));
        }
        let mut ids = candidate_ids.to_vec();
        ids.shuffle(&mut thread_rng());
        Ok(ids[0])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn empty_st() -> PolicyState {
        PolicyState {
            worker_bg_quota: HashMap::new(),
            worker_lease_quota: HashMap::new(),
            worker_bg_effective: HashMap::new(),
            worker_lease_effective: HashMap::new(),
            bg_load_score: HashMap::new(),
            lease_load_score: HashMap::new(),
        }
    }

    #[test]
    fn test_name() {
        assert_eq!(RandomSelector.name(), "random");
    }

    #[test]
    fn test_excludes() {
        let mut sel = RandomSelector;
        let st = empty_st();
        let mut exclude = HashSet::new();
        exclude.insert(2);
        let r = sel.select(&st, &[1, 2, 3], 3, &exclude).unwrap();
        assert!(!r.contains(&2));
        assert!(r.len() <= 2);
    }

    #[test]
    fn test_respects_count() {
        let mut sel = RandomSelector;
        let st = empty_st();
        let r = sel.select(&st, &[1, 2, 3], 1, &HashSet::new()).unwrap();
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn test_empty_error() {
        let mut sel = RandomSelector;
        let st = empty_st();
        assert!(sel.select(&st, &[], 3, &HashSet::new()).is_err());
    }
}
