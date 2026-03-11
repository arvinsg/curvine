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

use curvine_common::state::{HeartbeatRequest, HeartbeatResponse, NodeInfo, RegisterRequest};
use curvine_common::FsResult;

/// Handler for a specific node type (Worker or Meta).
pub trait HeartbeatHandler: Send + Sync {
    /// Returns the node type this handler supports.
    fn supported_node_type(&self) -> curvine_common::state::NodeType;

    /// Handles node registration.
    fn handle_register(&self, req: RegisterRequest) -> FsResult<NodeInfo>;

    /// Handles node heartbeat.
    fn handle_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse>;

    /// Validates consistency (epoch, labels, etc.).
    fn validate_consistency(
        &self,
        node: &NodeInfo,
        req: &HeartbeatRequest,
    ) -> FsResult<()>;
}
