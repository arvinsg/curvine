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
use crate::pd::node::event::{NodeEvent, NodeEventType};
use crate::pd::pd_server::Pd;
use curvine_common::state::NodeState;
use log::warn;

impl NodeManager {
    pub(super) fn emit_event(&self, event: NodeEvent) {
        Pd::get_metrics()
            .node_event_total
            .with_label_values(&[event.event_type.as_str()])
            .inc();
        let _ = self.event_tx.send(event);
    }

    pub(super) fn emit_fenced_state_event(
        &self,
        node_id: u32,
        expected_epoch: u64,
        old_state: Option<NodeState>,
        new_state: NodeState,
        event_type: NodeEventType,
        event_time_ms: u64,
    ) -> bool {
        let Some(current) = self.get_node(node_id) else {
            warn!(
                "skip {:?} event for missing node_id={}, expected_epoch={}, target_state={:?}",
                event_type, node_id, expected_epoch, new_state
            );
            return false;
        };
        if current.epoch != expected_epoch || current.state != new_state {
            warn!(
                "skip {:?} event due to fencing node_id={}, expected_epoch={}, current_epoch={}, current_state={:?}, target_state={:?}",
                event_type, node_id, expected_epoch, current.epoch, current.state, new_state
            );
            return false;
        }
        self.emit_event(NodeEvent {
            event_type,
            node_id,
            node_type: current.base.node_type,
            old_state,
            new_state: Some(new_state),
            epoch: current.epoch,
            event_time_ms,
        });
        true
    }
}
