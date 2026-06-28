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
use crate::pd::journal::entry::RegisterNodeEntry;
use crate::pd::journal::{ApplyOutcome, PdEntry};
use crate::pd::node::event::{NodeEvent, NodeEventType};
use curvine_common::state::{NodeInfo, NodeState, RegisterRequest};
use curvine_common::{FsError, FsResult};
use log::info;
use orpc::common::LocalTime;

impl NodeManager {
    pub fn register(&self, req: RegisterRequest) -> FsResult<(NodeInfo, u64)> {
        Self::validate_register_request(&req)?;

        let now = LocalTime::mills();
        let new_epoch = self.next_register_epoch(&req)?;
        let node = self.build_register_node(&req, new_epoch, now)?;
        let outcome = self.propose_register_node(&node, now)?;
        let outcome_applied = matches!(outcome, ApplyOutcome::Applied);
        Self::apply_outcome_to_result(outcome, "register_node", node.base.node_id)?;

        if outcome_applied {
            self.emit_register_event(&node, now);
        }
        let applied_node = self
            .get_node(node.base.node_id)
            .unwrap_or_else(|| node.clone());
        Ok((applied_node, new_epoch))
    }

    fn validate_register_request(req: &RegisterRequest) -> FsResult<()> {
        if req.base.node_id == 0 {
            return Err(FsError::common("node_id must not be 0"));
        }
        if !Self::payload_matches_node_type(&req.payload, req.base.node_type) {
            return Err(FsError::common(format!(
                "register payload type mismatch for node_id={}, node_type={:?}",
                req.base.node_id, req.base.node_type
            )));
        }
        Ok(())
    }

    fn next_register_epoch(&self, req: &RegisterRequest) -> FsResult<u64> {
        let index = self.index.read().unwrap();
        let Some(existing) = index.get_by_id(req.base.node_id) else {
            return Ok(1);
        };

        if !Self::can_replace_registered_node(existing, req) {
            return Err(FsError::common(format!(
                "node {} already registered and state {:?}, startup_time_ms current={} requested={}",
                req.base.node_id,
                existing.state,
                existing.base.startup_time_ms,
                req.base.startup_time_ms
            )));
        }
        if existing.base.address != req.base.address {
            info!(
                "Node {} address changed: {:?} -> {:?}, re-registering",
                req.base.node_id, existing.base.address, req.base.address
            );
        }
        Ok(existing.epoch + 1)
    }

    fn can_replace_registered_node(existing: &NodeInfo, req: &RegisterRequest) -> bool {
        match existing.state {
            NodeState::Lost | NodeState::Offline => true,
            NodeState::Starting | NodeState::Live => {
                req.base.startup_time_ms > existing.base.startup_time_ms
            }
            NodeState::Decommission | NodeState::Blacklist => false,
        }
    }

    fn build_register_node(
        &self,
        req: &RegisterRequest,
        epoch: u64,
        now_ms: u64,
    ) -> FsResult<NodeInfo> {
        let handler = self.get_handler(req.base.node_type)?;
        let mut node = handler.build_node_info(req)?;
        node.epoch = epoch;
        node.last_heartbeat_ms = now_ms;
        Ok(node)
    }

    fn propose_register_node(&self, node: &NodeInfo, now_ms: u64) -> FsResult<ApplyOutcome> {
        self.journal_client
            .propose(PdEntry::RegisterNode(RegisterNodeEntry {
                op_ms: now_ms,
                node: node.clone(),
            }))
    }

    fn emit_register_event(&self, node: &NodeInfo, now_ms: u64) {
        info!(
            "Registered node {} type {:?} epoch {} state {:?}",
            node.base.node_id, node.base.node_type, node.epoch, node.state
        );
        self.emit_event(NodeEvent {
            event_type: NodeEventType::Registered,
            node_id: node.base.node_id,
            node_type: node.base.node_type,
            old_state: None,
            new_state: Some(node.state),
            epoch: node.epoch,
            event_time_ms: now_ms,
        });
    }
}
