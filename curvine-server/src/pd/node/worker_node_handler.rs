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

pub struct WorkerNodeHandler;

impl WorkerNodeHandler {
    pub fn new() -> Self {
        Self
    }
}

impl Default for WorkerNodeHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeHandler for WorkerNodeHandler {
    fn node_type(&self) -> NodeType {
        NodeType::Worker
    }

    fn process_heartbeat(&self, node: &mut NodeInfo, req: &HeartbeatRequest) -> FsResult<bool> {
        let HeartbeatPayload::Worker(ref w) = req.payload else {
            return Err(FsError::common("expected Worker heartbeat payload"));
        };

        node.sys_stats = w.sys_stats.clone();

        if let NodePayload::Worker(ref mut p) = node.payload {
            p.storage_stats = w.storage_stats.clone();
            p.bg_epochs = w.bg_epochs.clone();
            p.bg_reports = w.bg_reports.clone();

            for (sid, _stat) in &w.storage_stats {
                if !p.storage_specs.contains_key(sid) {
                    log::warn!(
                        "Worker {} reports unknown storage_id={}, re-register to update specs",
                        node.base.node_id,
                        sid
                    );
                }
            }
        }

        Ok(false)
    }
}
