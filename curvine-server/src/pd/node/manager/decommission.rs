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

use super::NodeManager;
use crate::pd::journal::entry::{DeleteNodeEntry, UpdateNodeStateEntry};
use crate::pd::journal::{ApplyOutcome, PdEntry};
use crate::pd::node::event::{NodeEvent, NodeEventType};
use curvine_common::state::{NodeInfo, NodeState};
use curvine_common::{FsError, FsResult};
use log::{info, warn};
use orpc::common::LocalTime;

impl NodeManager {
    pub fn start_decommission(&self, node_id: u32) -> FsResult<NodeState> {
        let node = self.get_decommission_node(node_id)?;
        if node.state == NodeState::Decommission {
            return Ok(NodeState::Decommission);
        }

        let now = LocalTime::mills();
        let outcome = self.propose_decommission_start(&node, now)?;
        let applied = matches!(&outcome, ApplyOutcome::Applied);
        Self::apply_outcome_to_result(outcome, "start_decommission", node_id)?;
        if applied {
            self.emit_decommission_node_event(&node, NodeEventType::DecommissionStarted, now);
        }
        Ok(NodeState::Decommission)
    }

    fn get_decommission_node(&self, node_id: u32) -> FsResult<NodeInfo> {
        let node = self
            .get_node(node_id)
            .ok_or_else(|| FsError::common(format!("node {} not found", node_id)))?;
        if matches!(node.state, NodeState::Blacklist) {
            return Err(FsError::common(format!(
                "node {} is {:?}, cannot decommission",
                node_id, node.state
            )));
        }
        Ok(node)
    }

    fn propose_decommission_start(&self, node: &NodeInfo, now_ms: u64) -> FsResult<ApplyOutcome> {
        self.journal_client
            .propose(PdEntry::UpdateNodeState(UpdateNodeStateEntry {
                op_ms: now_ms,
                node_id: node.base.node_id,
                expected_epoch: node.epoch,
                expected_state: Some(node.state),
                expected_last_heartbeat_ms: None,
                new_state: NodeState::Decommission,
                state_since_ms: now_ms,
                last_heartbeat_ms: None,
                payload_update: None,
            }))
    }

    pub fn finish_decommission(&self, node_id: u32) -> FsResult<()> {
        let node = self.get_decommissioned_node(node_id)?;
        let now = LocalTime::mills();
        log::info!(
            "Node {} decommission complete, deleting from cluster",
            node_id
        );

        let outcome = self.propose_delete_node(&node, now)?;
        let applied = matches!(&outcome, ApplyOutcome::Applied);
        Self::apply_outcome_to_result(outcome, "delete_node", node_id)?;
        if applied {
            self.emit_decommission_node_event(&node, NodeEventType::DecommissionFinished, now);
        }
        Ok(())
    }

    fn get_decommissioned_node(&self, node_id: u32) -> FsResult<NodeInfo> {
        let node = self
            .get_node(node_id)
            .ok_or_else(|| FsError::common(format!("node {} not found", node_id)))?;
        if node.state == NodeState::Decommission {
            Ok(node)
        } else {
            Err(FsError::common(format!(
                "node {} is {:?}, expected Decommission",
                node_id, node.state
            )))
        }
    }

    fn propose_delete_node(&self, node: &NodeInfo, now_ms: u64) -> FsResult<ApplyOutcome> {
        self.journal_client
            .propose(PdEntry::DeleteNode(DeleteNodeEntry {
                op_ms: now_ms,
                node_id: node.base.node_id,
                expected_epoch: node.epoch,
                expected_state: Some(NodeState::Decommission),
            }))
    }

    fn emit_decommission_node_event(
        &self,
        node: &NodeInfo,
        event_type: NodeEventType,
        now_ms: u64,
    ) {
        match event_type {
            NodeEventType::DecommissionStarted => {
                if self.emit_fenced_state_event(
                    node.base.node_id,
                    node.epoch,
                    Some(node.state),
                    NodeState::Decommission,
                    NodeEventType::DecommissionStarted,
                    now_ms,
                ) {
                    info!(
                        "Node {} decommission started {:?} -> Decommission epoch {}",
                        node.base.node_id, node.state, node.epoch
                    );
                }
            }
            NodeEventType::DecommissionFinished => {
                if self.get_node(node.base.node_id).is_some() {
                    warn!(
                        "DeleteNode propose returned but node still exists node_id={}, epoch={}; skip DecommissionFinished event",
                        node.base.node_id, node.epoch
                    );
                    return;
                }
                self.emit_event(NodeEvent {
                    event_type: NodeEventType::DecommissionFinished,
                    node_id: node.base.node_id,
                    node_type: node.base.node_type,
                    old_state: Some(NodeState::Decommission),
                    new_state: None,
                    epoch: node.epoch,
                    event_time_ms: now_ms,
                });
                info!(
                    "Node {} decommission finished epoch {}",
                    node.base.node_id, node.epoch
                );
            }
            _ => unreachable!("unsupported decommission event type: {:?}", event_type),
        }
    }
}
