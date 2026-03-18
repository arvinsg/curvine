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

/// Events emitted by NodeManager for downstream subscribers.
#[derive(Debug, Clone)]
pub enum NodeEvent {
    Registered {
        node_id: u32,
        node_type: NodeType,
        pool_ids: Vec<u16>,
    },
    StateChanged {
        node_id: u32,
        node_type: NodeType,
        old_state: NodeState,
        new_state: NodeState,
    },
}
