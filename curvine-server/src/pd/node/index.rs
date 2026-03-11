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

use curvine_common::state::{NodeInfo, NodeState, NodeType};
use std::collections::{HashMap, HashSet};

/// In-memory index for nodes by id, type, and state.
pub struct NodeIndex {
    nodes: HashMap<u32, NodeInfo>,
    by_type: HashMap<NodeType, HashSet<u32>>,
    by_state: HashMap<NodeState, HashSet<u32>>,
}

impl NodeIndex {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            by_type: HashMap::new(),
            by_state: HashMap::new(),
        }
    }

    pub fn insert(&mut self, node: NodeInfo) {
        let node_id = node.base.node_id;
        let nt = node.base.node_type;
        let st = node.state;
        self.by_type
            .entry(nt)
            .or_default()
            .insert(node_id);
        self.by_state
            .entry(st)
            .or_default()
            .insert(node_id);
        self.nodes.insert(node_id, node);
    }

    pub fn remove(&mut self, node_id: u32) -> Option<NodeInfo> {
        let node = self.nodes.remove(&node_id)?;
        self.by_type
            .get_mut(&node.base.node_type)
            .and_then(|s| s.take(&node_id));
        self.by_state
            .get_mut(&node.state)
            .and_then(|s| s.take(&node_id));
        Some(node)
    }

    pub fn get_by_id(&self, node_id: u32) -> Option<&NodeInfo> {
        self.nodes.get(&node_id)
    }

    pub fn get_by_id_mut(&mut self, node_id: u32) -> Option<&mut NodeInfo> {
        self.nodes.get_mut(&node_id)
    }

    pub fn get_by_type(&self, node_type: NodeType) -> Vec<&NodeInfo> {
        self.by_type
            .get(&node_type)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.nodes.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn get_by_state(&self, state: NodeState) -> Vec<&NodeInfo> {
        self.by_state
            .get(&state)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.nodes.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn update_state(&mut self, node_id: u32, new_state: NodeState) -> bool {
        let node = match self.nodes.get_mut(&node_id) {
            Some(n) => n,
            None => return false,
        };
        let old_state = node.state;
        if old_state == new_state {
            return true;
        }
        self.by_state
            .get_mut(&old_state)
            .and_then(|s| s.take(&node_id));
        self.by_state
            .entry(new_state)
            .or_default()
            .insert(node_id);
        node.state = new_state;
        true
    }

    pub fn update_heartbeat(&mut self, node_id: u32, last_heartbeat_ms: u64) -> bool {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.last_heartbeat_ms = last_heartbeat_ms;
            true
        } else {
            false
        }
    }

    pub fn all_node_ids(&self) -> Vec<u32> {
        self.nodes.keys().copied().collect()
    }
}

impl Default for NodeIndex {
    fn default() -> Self {
        Self::new()
    }
}
