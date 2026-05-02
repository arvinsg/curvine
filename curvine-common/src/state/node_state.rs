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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Node type: Worker、Meta...
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum NodeType {
    #[default]
    Worker,
    Meta,
}

impl NodeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeType::Worker => "worker",
            NodeType::Meta => "meta",
        }
    }
}

/// Node state in PD
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum NodeState {
    /// Starting, registered but not yet initialized
    #[default]
    Starting,
    /// Running, heartbeat normal
    Live,
    /// Lost due to heartbeat timeout
    Lost,
    /// Graceful shutdown
    Offline,
    /// Decommissioning, migrating data
    Decommission,
    /// Blacklisted by admin
    Blacklist,
}

impl NodeState {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeState::Starting => "starting",
            NodeState::Live => "live",
            NodeState::Lost => "lost",
            NodeState::Offline => "offline",
            NodeState::Decommission => "decommission",
            NodeState::Blacklist => "blacklist",
        }
    }

    pub const ALL: [NodeState; 6] = [
        NodeState::Starting,
        NodeState::Live,
        NodeState::Lost,
        NodeState::Offline,
        NodeState::Decommission,
        NodeState::Blacklist,
    ];
}

/// Node address (hostname, ip, ports)
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeAddress {
    pub hostname: String,
    pub ip: String,
    pub rpc_port: u16,
    pub web_port: u16,
}

/// Common node fields shared by RegisterRequest and NodeInfo
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeBase {
    pub node_id: u32,
    pub node_type: NodeType,
    pub address: NodeAddress,
    pub labels: HashMap<String, String>,
    pub software_version: String,
    pub startup_time_ms: u64,
}
