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

use curvine_common::state::{
    HeartbeatRequest, HeartbeatResponsePayload, NodeInfo, NodeType, RegisterRequest,
};
use curvine_common::FsResult;

/// Handler for a specific node type (Worker or Meta...).
pub trait HeartbeatHandler: Send + Sync {
    /// Returns the node type this handler supports.
    fn supported_node_type(&self) -> NodeType;

    /// Build initial NodeInfo from a registration request.
    fn build_node_info(&self, req: &RegisterRequest) -> FsResult<NodeInfo>;

    /// Process heartbeat: update role-specific fields on the in-memory node.
    fn process_heartbeat(&self, node: &mut NodeInfo, req: &HeartbeatRequest) -> FsResult<bool>;

    /// Build the role-specific part of the heartbeat response payload.
    fn build_heartbeat_response(
        &self,
        node: &NodeInfo,
        req: &HeartbeatRequest,
    ) -> FsResult<HeartbeatResponsePayload>;
}
