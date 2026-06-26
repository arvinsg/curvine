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

use curvine_common::raft::RoleState;
use orpc::sync::StateCtl;

/// Predicate for "is this PD node currently the raft leader?".
pub trait LeaderChecker: Send + Sync {
    fn is_leader(&self) -> bool;
}

pub struct RaftLeaderChecker {
    role_ctl: StateCtl,
}

impl RaftLeaderChecker {
    pub fn new(role_ctl: StateCtl) -> Self {
        Self { role_ctl }
    }
}

impl LeaderChecker for RaftLeaderChecker {
    fn is_leader(&self) -> bool {
        let state: RoleState = self.role_ctl.state();
        state == RoleState::Leader
    }
}

/// Test-only checker that always reports leader.
#[cfg(test)]
pub struct AlwaysLeader;

#[cfg(test)]
impl LeaderChecker for AlwaysLeader {
    fn is_leader(&self) -> bool {
        true
    }
}
