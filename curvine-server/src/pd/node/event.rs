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

use curvine_common::state::{NodeState, NodeType};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeEventType {
    Registered,
    HeartbeatResumed,
    Lost,
    Offline,
    DecommissionStarted,
    DecommissionFinished,
}

/// Events emitted by NodeManager for downstream subscribers.
#[derive(Debug, Clone)]
pub struct NodeEvent {
    pub event_type: NodeEventType,
    pub node_id: u32,
    pub node_type: NodeType,
    pub old_state: Option<NodeState>,
    pub new_state: Option<NodeState>,
    pub epoch: u64,
    pub event_time_ms: u64,
}
