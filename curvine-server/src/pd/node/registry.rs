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
use curvine_common::state::NodeType;
use std::collections::HashMap;
use std::sync::Arc;

/// Registry of heartbeat handlers by node type.
pub struct HandlerRegistry {
    handlers: HashMap<NodeType, Arc<dyn HeartbeatHandler>>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    pub fn register(&mut self, handler: Arc<dyn HeartbeatHandler>) {
        let node_type = handler.supported_node_type();
        self.handlers.insert(node_type, handler);
    }

    pub fn get(&self, node_type: NodeType) -> Option<Arc<dyn HeartbeatHandler>> {
        self.handlers.get(&node_type).cloned()
    }

    pub fn supports(&self, node_type: NodeType) -> bool {
        self.handlers.contains_key(&node_type)
    }
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}
