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

use super::meta_node_info::MetaNodePayload;
use super::node_state::{NodeBase, NodeState};
use super::worker_node_info::WorkerNodePayload;
use serde::{Deserialize, Serialize};

/// Node payload (type-specific persisted data)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodePayload {
    Worker(WorkerNodePayload),
    Meta(MetaNodePayload),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SystemStats {
    pub cpu_usage: f32,
    pub memory_usage: f32,
}

impl Default for NodePayload {
    fn default() -> Self {
        NodePayload::Worker(WorkerNodePayload::default())
    }
}

/// Node info
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeInfo {
    #[serde(flatten)]
    pub base: NodeBase,
    pub epoch: u64,
    pub state: NodeState,
    pub last_heartbeat_ms: u64,

    #[serde(skip)]
    pub last_persist_ms: u64,

    #[serde(skip)]
    pub sys_stats: SystemStats,

    pub payload: NodePayload,
}

impl NodeInfo {
    pub fn with_last_heartbeat_ms(mut self, ms: u64) -> Self {
        self.last_heartbeat_ms = ms;
        self
    }

    pub fn preserve_memory_fields(&mut self, source: &NodeInfo) {
        self.last_persist_ms = source.last_persist_ms;
        self.sys_stats = source.sys_stats.clone();
        match (&mut self.payload, &source.payload) {
            (NodePayload::Worker(ref mut dst), NodePayload::Worker(ref src)) => {
                dst.storage_stats = src.storage_stats.clone();
                dst.bg_ids = src.bg_ids.clone();
            }
            (NodePayload::Meta(ref mut dst), NodePayload::Meta(ref src)) => {
                dst.stats = src.stats.clone();
            }
            _ => {}
        }
    }

    /// Whether this node should be persisted via Raft.
    /// Returns true if the time since last persist exceeds the given interval.
    pub fn need_persist(&self, now_ms: u64, interval_ms: u64) -> bool {
        if self.last_persist_ms == 0 {
            return true;
        }
        now_ms.saturating_sub(self.last_persist_ms) > interval_ms
    }

    #[inline]
    pub fn node_id(&self) -> u32 {
        self.base.node_id
    }
    #[inline]
    pub fn node_type(&self) -> super::node_state::NodeType {
        self.base.node_type
    }
    #[inline]
    pub fn address(&self) -> &super::node_state::NodeAddress {
        &self.base.address
    }
    #[inline]
    pub fn labels(&self) -> &std::collections::HashMap<String, String> {
        &self.base.labels
    }
    #[inline]
    pub fn software_version(&self) -> &str {
        &self.base.software_version
    }
    #[inline]
    pub fn startup_time_ms(&self) -> u64 {
        self.base.startup_time_ms
    }
}
