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

use super::NodeHandler;
use curvine_common::state::{HeartbeatPayload, HeartbeatRequest, NodeInfo, NodePayload, NodeType};
use curvine_common::{FsError, FsResult};

pub struct TaskNodeHandler;

impl TaskNodeHandler {
    pub fn new() -> Self {
        Self
    }
}

impl Default for TaskNodeHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeHandler for TaskNodeHandler {
    fn node_type(&self) -> NodeType {
        NodeType::Task
    }

    fn process_heartbeat(&self, node: &mut NodeInfo, req: &HeartbeatRequest) -> FsResult<bool> {
        let HeartbeatPayload::Task(ref t) = req.payload else {
            return Err(FsError::common("expected Task heartbeat payload"));
        };

        node.sys_stats = t.sys_stats.clone();
        if let NodePayload::Task(ref mut p) = node.payload {
            p.stats = t.stats.clone();
        }

        Ok(false)
    }
}
