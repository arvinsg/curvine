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

/// TaskNode runtime stats reported by heartbeat.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskNodeStats {
    pub running_tasks: u32,
    pub failed_tasks: u64,
}

/// TaskNode persisted payload.
///
/// The first version keeps registration payload empty. Runtime stats are
/// updated from heartbeat and skipped from persistence.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskNodePayload {
    #[serde(skip)]
    pub stats: TaskNodeStats,
}
