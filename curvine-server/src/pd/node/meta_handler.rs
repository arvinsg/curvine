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
    HeartbeatRequest, HeartbeatResponse, HeartbeatResponsePayload, MetaHeartbeatResponse, NodeInfo,
    NodePayload, NodeState, NodeType, RegisterRequest,
};
use curvine_common::FsResult;

pub struct MetaHeartbeatHandler;

impl MetaHeartbeatHandler {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MetaHeartbeatHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl HeartbeatHandler for MetaHeartbeatHandler {
    fn supported_node_type(&self) -> NodeType {
        NodeType::Meta
    }

    // TODO: 1. 没有校验节点的信息，或更新节点的信息 2. 返回的信息也有误
    fn handle_register(&self, req: RegisterRequest) -> FsResult<NodeInfo> {
        let payload = match &req.payload {
            NodePayload::Meta(p) => p.clone(),
            _ => return Err(curvine_common::FsError::common("expected Meta payload")),
        };
        Ok(NodeInfo {
            base: req.base.clone(),
            epoch: 0,
            state: NodeState::Starting,
            last_heartbeat_ms: 0,
            sys_stats: Default::default(),
            payload: NodePayload::Meta(payload),
        })
    }

    // TODO: 1. 没有针对 metanode 特点做针对校验
    fn handle_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        Ok(HeartbeatResponse {
            error: None,
            epoch: req.epoch,
            config_version: 0,
            mount_version: 0,
            bg_version: 0,
            payload: HeartbeatResponsePayload::Meta(MetaHeartbeatResponse {
                path_route_update: None,
                node_group_update: None,
            }),
        })
    }

    // TODO: 1. 必要性待确定
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
