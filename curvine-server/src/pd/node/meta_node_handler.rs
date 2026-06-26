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

pub struct MetaNodeHandler;

impl MetaNodeHandler {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MetaNodeHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeHandler for MetaNodeHandler {
    fn node_type(&self) -> NodeType {
        NodeType::Meta
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
                p.rw_policy = m.rw_policy;
                let mut peers = m.peers.clone();
                // Ensure the current node's peer entry has is_leader set
                let node_id = node.base.node_id;
                if let Some(peer) = peers.iter_mut().find(|p| p.node_id == node_id) {
                    peer.is_leader = Some(m.is_leader);
                }
                p.peers = peers;
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
}
