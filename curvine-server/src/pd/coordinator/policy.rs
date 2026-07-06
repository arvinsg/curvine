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

use curvine_common::state::{BGKind, BGOpState, BGState, BlockGroupInfo};

/// Hash BG correctness checkers may operate on Active or Degraded BGs.
pub(crate) fn is_hash_repair_candidate(bg: &BlockGroupInfo) -> bool {
    is_idle_hash_bg(bg) && matches!(bg.state, BGState::Active | BGState::Degraded)
}

/// Hash BG balancing should only move fully Active BGs.
pub(crate) fn is_hash_balance_candidate(bg: &BlockGroupInfo) -> bool {
    is_idle_hash_bg(bg) && bg.state == BGState::Active
}

/// Hash BG table rebuild is a placement optimization; keep it on Active BGs only.
pub(crate) fn is_hash_table_rebuild_candidate(bg: &BlockGroupInfo) -> bool {
    is_hash_balance_candidate(bg)
}

fn is_idle_hash_bg(bg: &BlockGroupInfo) -> bool {
    bg.kind == BGKind::Hash && bg.op_state == BGOpState::Idle
}
