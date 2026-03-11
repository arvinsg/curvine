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

use super::HeartbeatHandler;
use curvine_common::state::{
    HeartbeatRequest, HeartbeatResponse, HeartbeatResponsePayload, NodeInfo, NodePayload,
    NodeState, NodeType, RegisterRequest, RegisterRequestPayload, WorkerHeartbeatResponse,
};
use curvine_common::FsResult;

/// Heartbeat handler for Worker nodes.
pub struct WorkerHeartbeatHandler;

impl WorkerHeartbeatHandler {
    pub fn new() -> Self {
        Self
    }
}

impl Default for WorkerHeartbeatHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl HeartbeatHandler for WorkerHeartbeatHandler {
    fn supported_node_type(&self) -> NodeType {
        NodeType::Worker
    }

    // TODO
    fn handle_register(&self, req: RegisterRequest) -> FsResult<NodeInfo> {
        let payload = match &req.payload {
            RegisterRequestPayload::Worker(p) => p.clone(),
            _ => return Err(curvine_common::FsError::common("expected Worker payload")),
        };
        let info = NodeInfo {
            base: req.base.clone(),
            epoch: 0, // NodeManager sets actual epoch
            state: NodeState::Starting,
            last_heartbeat_ms: 0,
            sys_stats: Default::default(),
            payload: NodePayload::Worker(payload),
        };
        Ok(info)
    }

    // TODO
    fn handle_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        // Stub: return empty BG lists; real implementation will use index/config_manager
        Ok(HeartbeatResponse {
            error: None,
            epoch: req.epoch,
            config_version: 0,
            mount_version: 0,
            bg_version: 0,
            payload: HeartbeatResponsePayload::Worker(WorkerHeartbeatResponse::default()),
        })
    }

    fn validate_consistency(&self, node: &NodeInfo, req: &HeartbeatRequest) -> FsResult<()> {
        if node.epoch != req.epoch {
            return Err(curvine_common::FsError::common(format!(
                "epoch mismatch: node {} vs req {}",
                node.epoch, req.epoch
            )));
        }
        Ok(())
    }
}
