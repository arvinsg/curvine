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

use curvine_common::state::{HeartbeatRequest, NodeInfo, NodeState, NodeType, RegisterRequest};
use curvine_common::{FsError, FsResult};

/// Handler for type-specific node registration and heartbeat runtime updates.
pub trait NodeHandler: Send + Sync {
    /// Returns the node type this handler supports.
    fn node_type(&self) -> NodeType;

    /// Build initial NodeInfo from a registration request.
    fn build_node_info(&self, req: &RegisterRequest) -> FsResult<NodeInfo> {
        if req.base.node_type != self.node_type() {
            return Err(FsError::common(format!(
                "node_type mismatch: expected {:?}, got {:?}",
                self.node_type(),
                req.base.node_type
            )));
        }

        Ok(NodeInfo {
            base: req.base.clone(),
            epoch: 0,
            state: NodeState::Starting,
            last_heartbeat_ms: 0,
            state_since_ms: orpc::common::LocalTime::mills(),
            last_persist_ms: 0,
            sys_stats: Default::default(),
            payload: req.payload.clone(),
        })
    }

    /// Process heartbeat: update role-specific runtime fields on the in-memory node.
    /// Returns true when persistent payload fields changed and need a Raft update.
    fn process_heartbeat(&self, node: &mut NodeInfo, req: &HeartbeatRequest) -> FsResult<bool>;
}
