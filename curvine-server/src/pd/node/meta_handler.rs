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
    HeartbeatPayload, HeartbeatRequest, HeartbeatResponsePayload, MetaHeartbeatResponse, NodeInfo,
    NodePayload, NodeState, NodeType, RegisterRequest,
};
use curvine_common::{FsError, FsResult};

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

    fn build_node_info(&self, req: &RegisterRequest) -> FsResult<NodeInfo> {
        let payload = match &req.payload {
            NodePayload::Meta(p) => p.clone(),
            _ => return Err(FsError::common("expected Meta payload")),
        };
        Ok(NodeInfo {
            base: req.base.clone(),
            epoch: 0,
            state: NodeState::Starting,
            last_heartbeat_ms: 0,
            last_persist_ms: 0,
            sys_stats: Default::default(),
            payload: NodePayload::Meta(payload),
        })
    }

    fn process_heartbeat(&self, node: &mut NodeInfo, req: &HeartbeatRequest) -> FsResult<bool> {
        let HeartbeatPayload::Meta(ref m) = req.payload else {
            return Err(FsError::common("expected Meta heartbeat payload"));
        };

        let mut changed = false;

        node.sys_stats = m.sys_stats.clone();

        if let NodePayload::Meta(ref mut p) = node.payload {
            p.stats = m.inodes_stats.clone();
            if p.group_id != m.group_id {
                log::error!(
                    "meta node group id changed, current:{}, new group id:{}",
                    p.group_id,
                    m.group_id
                );
                return Ok(false);
            }

            if p.group_epoch != m.group_epoch {
                p.group_epoch = m.group_epoch;
                p.is_leader = m.is_leader;
                p.rw_policy = m.rw_policy;
                p.peers = m.peers.clone();
                changed = true;
                log::info!(
                    "meta node change group_id:{}, group_epoch:{}",
                    p.group_id,
                    m.group_epoch
                );
            }
        }

        Ok(changed)
    }

    fn build_heartbeat_response(
        &self,
        _node: &NodeInfo,
        _req: &HeartbeatRequest,
    ) -> FsResult<HeartbeatResponsePayload> {
        Ok(HeartbeatResponsePayload::Meta(MetaHeartbeatResponse {
            path_route_update: None,
            node_group_update: None,
        }))
    }
}
