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
    HeartbeatPayload, HeartbeatRequest, HeartbeatResponsePayload, NodeInfo, NodePayload, NodeState,
    NodeType, RegisterRequest, TaskHeartbeatResponse,
};
use curvine_common::{FsError, FsResult};

pub struct TaskHeartbeatHandler;

impl TaskHeartbeatHandler {
    pub fn new() -> Self {
        Self
    }
}

impl Default for TaskHeartbeatHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl HeartbeatHandler for TaskHeartbeatHandler {
    fn supported_node_type(&self) -> NodeType {
        NodeType::Task
    }

    fn build_node_info(&self, req: &RegisterRequest) -> FsResult<NodeInfo> {
        let payload = match &req.payload {
            NodePayload::Task(p) => p.clone(),
            _ => return Err(FsError::common("expected Task payload")),
        };

        Ok(NodeInfo {
            base: req.base.clone(),
            epoch: 0,
            state: NodeState::Starting,
            last_heartbeat_ms: 0,
            state_since_ms: orpc::common::LocalTime::mills(),
            last_persist_ms: 0,
            sys_stats: Default::default(),
            payload: NodePayload::Task(payload),
        })
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

    fn build_heartbeat_response(
        &self,
        _node: &NodeInfo,
        _req: &HeartbeatRequest,
    ) -> FsResult<HeartbeatResponsePayload> {
        Ok(HeartbeatResponsePayload::Task(
            TaskHeartbeatResponse::default(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::state::{
        HeartbeatPayload, HeartbeatRequest, NodeAddress, NodeBase, NodePayload, NodeType,
        RegisterRequest, SystemStats, TaskHeartbeatPayload, TaskNodePayload, TaskNodeStats,
    };
    use std::collections::HashMap;

    fn base() -> NodeBase {
        NodeBase {
            node_id: 30,
            node_type: NodeType::Task,
            address: NodeAddress {
                hostname: "task-1".to_string(),
                ip: "127.0.0.1".to_string(),
                rpc_port: 9000,
                web_port: 9001,
            },
            labels: HashMap::new(),
            software_version: "test".to_string(),
            startup_time_ms: 1,
        }
    }

    #[test]
    fn build_and_update_task_node() {
        let handler = TaskHeartbeatHandler::new();
        let req = RegisterRequest {
            cluster_id: "curvine".to_string(),
            base: base(),
            payload: NodePayload::Task(TaskNodePayload::default()),
        };
        let mut node = handler.build_node_info(&req).expect("build task node");
        assert_eq!(node.base.node_type, NodeType::Task);
        assert!(matches!(node.payload, NodePayload::Task(_)));

        let hb = HeartbeatRequest {
            cluster_id: "curvine".to_string(),
            node_id: 30,
            node_type: NodeType::Task,
            epoch: 1,
            timestamp_ms: 2,
            address: base().address,
            payload: HeartbeatPayload::Task(TaskHeartbeatPayload {
                sys_stats: SystemStats {
                    cpu_usage: 0.3,
                    memory_usage: 0.4,
                },
                stats: TaskNodeStats {
                    running_tasks: 5,
                    failed_tasks: 7,
                },
            }),
        };

        let critical_changed = handler
            .process_heartbeat(&mut node, &hb)
            .expect("process task heartbeat");
        assert!(!critical_changed);
        assert_eq!(node.sys_stats.cpu_usage, 0.3);
        match node.payload {
            NodePayload::Task(p) => {
                assert_eq!(p.stats.running_tasks, 5);
                assert_eq!(p.stats.failed_tasks, 7);
            }
            _ => panic!("expected task payload"),
        }
    }
}
