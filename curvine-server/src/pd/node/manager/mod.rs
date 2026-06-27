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

mod apply;
mod decommission;
mod event;
mod heartbeat;
mod liveness;
mod register;
#[cfg(test)]
mod tests;
mod utils;

use super::event::NodeEvent;
use super::index::NodeIndex;
use super::store::NodeStore;
use super::{HandlerRegistry, MetaNodeHandler, NodeHandler, TaskNodeHandler, WorkerNodeHandler};
use crate::pd::config::ConfigManager;
use crate::pd::journal;
use crate::pd::journal::entry::NodePayloadUpdate;
use curvine_common::state::{NodeInfo, NodeState, NodeType};
use curvine_common::{FsError, FsResult};
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

const EVENT_CHANNEL_CAPACITY: usize = 2048;
const MAX_BATCH_UPDATE_NODE_STATE: usize = 256;

pub(super) struct HeartbeatPlan {
    pub(super) now_ms: u64,
    pub(super) snapshot: NodeInfo,
    pub(super) planned: NodeInfo,
    pub(super) old_state: NodeState,
    pub(super) state_changed: bool,
    pub(super) critical_changed: bool,
    pub(super) need_checkpoint: bool,
    pub(super) payload_update: Option<NodePayloadUpdate>,
}

pub struct NodeManager {
    index: Arc<RwLock<NodeIndex>>,
    store: Arc<NodeStore>,
    handler_registry: HandlerRegistry,
    config_manager: Arc<ConfigManager>,
    journal_client: Arc<journal::Client>,
    event_tx: broadcast::Sender<NodeEvent>,
}

impl NodeManager {
    pub fn new(
        store: Arc<NodeStore>,
        config_manager: Arc<ConfigManager>,
        journal_client: Arc<journal::Client>,
    ) -> Self {
        let mut registry = HandlerRegistry::new();
        registry.register(Arc::new(WorkerNodeHandler::new()));
        registry.register(Arc::new(MetaNodeHandler::new()));
        registry.register(Arc::new(TaskNodeHandler::new()));
        let (event_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            index: Arc::new(RwLock::new(NodeIndex::new())),
            store,
            handler_registry: registry,
            config_manager,
            journal_client,
            event_tx,
        }
    }

    /// Restore in-memory index from store (call on startup).
    pub fn restore(&self) -> FsResult<()> {
        let nodes = self.store.list_all()?;
        let mut index = self.index.write().unwrap();
        for mut node in nodes {
            node.last_persist_ms = node.last_heartbeat_ms;
            index.insert(node);
        }
        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<NodeEvent> {
        self.event_tx.subscribe()
    }

    pub fn register_handler(&mut self, handler: Arc<dyn NodeHandler>) {
        self.handler_registry.register(handler);
    }

    fn get_handler(&self, node_type: NodeType) -> FsResult<&(dyn NodeHandler + 'static)> {
        self.handler_registry
            .get(node_type)
            .ok_or_else(|| FsError::common(format!("unsupported node type: {:?}", node_type)))
    }

    pub fn get_node(&self, node_id: u32) -> Option<NodeInfo> {
        let index = self.index.read().unwrap();
        index.get_by_id(node_id).cloned()
    }

    pub fn get_nodes_by_type(&self, node_type: NodeType) -> Vec<NodeInfo> {
        let index = self.index.read().unwrap();
        index.get_by_type(node_type).into_iter().cloned().collect()
    }

    pub fn get_nodes_by_state(&self, state: NodeState) -> Vec<NodeInfo> {
        let index = self.index.read().unwrap();
        index.get_by_state(state).into_iter().cloned().collect()
    }
}
