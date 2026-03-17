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

use crate::state::node_state::NodeAddress;
use serde::{Deserialize, Serialize};

/// Peer info for MetaNode group (node_id unified as u32)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerInfo {
    pub node_id: u32,
    pub address: NodeAddress,
    pub is_leader: Option<bool>,
}

/// MetaNode group info (group_id + peers; is_leader filled by PD)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeGroupInfo {
    pub group_id: u64,
    pub peers: Vec<PeerInfo>,
}

/// Read-write policy for Meta Raft group
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum RwPolicy {
    /// Only leader read/write
    #[default]
    LeaderOnly,
    /// Leader write, follower readable
    LeaderWriteFollowerRead,
}

/// Meta inodes stats (reported in heartbeat)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InodesStats {
    pub inode_count: u64,
    pub dir_count: u64,
    pub file_count: u64,
    pub total_size: u64,
}

/// Meta node persisted payload
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetaNodePayload {
    pub group_id: u32,
    pub peers: Vec<PeerInfo>,
    pub rw_policy: RwPolicy,
    pub is_leader: bool,
    pub group_epoch: u64,

    #[serde(skip)]
    pub stats: InodesStats,
}
