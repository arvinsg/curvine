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
use crate::pd::journal::entry::{
    BatchUpdateNodeStatusEntry, MetaNodePayloadPatch, NodePayloadPatch, NodeStatusUpdate,
    RegisterNodeEntry, RemoveNodeEntry, UpdateNodePayloadEntry, UpdateNodeStatusEntry,
};
use crate::pd::journal::ApplyOutcome;
use crate::pd::node::NodeIndex;
use curvine_common::state::{
    MetaNodePayload, NodeInfo, NodePayload, NodeState, NodeType, PeerInfo,
};
use curvine_common::FsResult;
use log::{info, warn};
use std::cmp::Ordering;

impl NodeManager {
    pub fn apply_register_node(&self, entry: &RegisterNodeEntry) -> FsResult<ApplyOutcome> {
        let node = entry.node.clone();
        let node_id = node.base.node_id;
        let epoch = node.epoch;
        let state = node.state;

        let mut index = self.index.write().unwrap();
        if let Some(existing) = index.get_by_id(node_id) {
            if let Err(outcome) = Self::validate_register_node(existing, &node) {
                return Ok(outcome);
            }
        }

        self.persist_node_update(&mut index, node, entry.op_ms)?;
        info!(
            "RegisterNode applied node_id={}, epoch={}, state={:?}",
            node_id, epoch, state
        );
        Ok(ApplyOutcome::Applied)
    }

    pub fn apply_update_node_status(
        &self,
        entry: &UpdateNodeStatusEntry,
    ) -> FsResult<ApplyOutcome> {
        self.apply_node_status_update(entry.op_ms, &entry.update)
    }

    pub fn apply_batch_update_node_status(
        &self,
        entry: &BatchUpdateNodeStatusEntry,
    ) -> FsResult<ApplyOutcome> {
        let mut applied = 0usize;
        let mut noop = 0usize;
        let mut stale = 0usize;
        let mut not_found = 0usize;

        for update in &entry.updates {
            match self.apply_node_status_update(entry.op_ms, update)? {
                ApplyOutcome::Applied => applied += 1,
                ApplyOutcome::SkippedNoop => noop += 1,
                ApplyOutcome::SkippedStale { .. } => stale += 1,
                ApplyOutcome::NotFound { .. } => not_found += 1,
            }
        }
        info!(
            "BatchUpdateNodeStatus applied updates={}, applied={}, noop={}, stale={}, not_found={}",
            entry.updates.len(),
            applied,
            noop,
            stale,
            not_found
        );
        if applied > 0 {
            Ok(ApplyOutcome::Applied)
        } else {
            Ok(ApplyOutcome::SkippedNoop)
        }
    }

    pub fn apply_update_node_payload(
        &self,
        entry: &UpdateNodePayloadEntry,
    ) -> FsResult<ApplyOutcome> {
        let mut index = self.index.write().unwrap();
        let existing = match Self::load_fenced_node_for_apply(
            &index,
            "UpdateNodePayload",
            entry.node_id,
            entry.expected_epoch,
            entry.expected_state,
        ) {
            Ok(node) => node,
            Err(outcome) => return Ok(outcome),
        };

        let mut updated = existing.clone();
        let changed = match Self::apply_payload_patch(
            entry.node_id,
            existing.base.node_type,
            &mut updated.payload,
            &entry.patch,
        ) {
            Ok(changed) => changed,
            Err(outcome) => return Ok(outcome),
        };
        if !changed {
            return Ok(ApplyOutcome::SkippedNoop);
        }

        self.persist_node_update(&mut index, updated, entry.op_ms)?;
        info!(
            "UpdateNodePayload applied node_id={}, epoch={}, state={:?}",
            entry.node_id, existing.epoch, existing.state
        );
        Ok(ApplyOutcome::Applied)
    }

    pub fn apply_remove_node(&self, entry: &RemoveNodeEntry) -> FsResult<ApplyOutcome> {
        let mut index = self.index.write().unwrap();
        let existing = match Self::load_fenced_node_for_apply(
            &index,
            "RemoveNode",
            entry.node_id,
            entry.expected_epoch,
            entry.expected_state,
        ) {
            Ok(node) => node,
            Err(outcome) => return Ok(outcome),
        };

        self.store.delete(entry.node_id)?;
        index.remove(entry.node_id);
        info!(
            "RemoveNode applied node_id={}, epoch={}, old_state={:?}",
            entry.node_id, existing.epoch, existing.state
        );
        Ok(ApplyOutcome::Applied)
    }

    fn apply_node_status_update(
        &self,
        op_ms: u64,
        update: &NodeStatusUpdate,
    ) -> FsResult<ApplyOutcome> {
        let mut index = self.index.write().unwrap();
        let existing = match Self::load_node_for_apply(
            &index,
            "UpdateNodeStatus",
            update.node_id,
            update.expected_epoch,
        ) {
            Ok(node) => node,
            Err(outcome) => return Ok(outcome),
        };

        let mut updated = existing.clone();
        match self.apply_status_patch(&existing, &mut updated, op_ms, update) {
            Ok(false) => return Ok(ApplyOutcome::SkippedNoop),
            Ok(true) => {}
            Err(outcome) => return Ok(outcome),
        }

        self.persist_node_update(&mut index, updated, op_ms)?;
        info!(
            "UpdateNodeStatus applied node_id={}, epoch={}, old_state={:?}, target_state={:?}, heartbeat_ms={:?}",
            update.node_id, update.expected_epoch, existing.state, update.target_state, update.heartbeat_ms
        );
        Ok(ApplyOutcome::Applied)
    }

    fn load_fenced_node_for_apply(
        index: &NodeIndex,
        op: &str,
        node_id: u32,
        expected_epoch: u64,
        expected_state: NodeState,
    ) -> Result<NodeInfo, ApplyOutcome> {
        let node = Self::load_node_for_apply(index, op, node_id, expected_epoch)?;
        Self::validate_node_fence(op, &node, node_id, expected_epoch, expected_state)?;
        Ok(node)
    }

    fn load_node_for_apply(
        index: &NodeIndex,
        op: &str,
        node_id: u32,
        expected_epoch: u64,
    ) -> Result<NodeInfo, ApplyOutcome> {
        let Some(node) = index.get_by_id(node_id).cloned() else {
            warn!(
                "{} for missing node_id={}, expected_epoch={}; skip",
                op, node_id, expected_epoch
            );
            return Err(ApplyOutcome::not_found(format!(
                "node {} not found",
                node_id
            )));
        };
        Ok(node)
    }

    fn persist_node_update(
        &self,
        index: &mut NodeIndex,
        mut node: NodeInfo,
        op_ms: u64,
    ) -> FsResult<()> {
        node.last_persist_ms = op_ms;
        self.store.put(&node)?;
        index.insert(node);
        Ok(())
    }

    fn validate_register_node(existing: &NodeInfo, node: &NodeInfo) -> Result<(), ApplyOutcome> {
        if Self::same_registered_node(existing, node) {
            return Err(ApplyOutcome::SkippedNoop);
        }
        if node.epoch != (existing.epoch + 1) {
            warn!(
                "stale/non-contiguous RegisterNode node_id={}, entry_epoch={}, current_epoch={}, current_state={:?}; skip",
                node.base.node_id, node.epoch, existing.epoch, existing.state
            );
            return Err(ApplyOutcome::stale(format!(
                "non-contiguous epoch: current={}, entry={}",
                existing.epoch, node.epoch
            )));
        }
        Self::validate_register_state(existing, node)
    }

    fn validate_register_state(existing: &NodeInfo, node: &NodeInfo) -> Result<(), ApplyOutcome> {
        let allowed = match existing.state {
            NodeState::Lost | NodeState::Offline => true,
            NodeState::Starting | NodeState::Live => {
                node.base.startup_time_ms > existing.base.startup_time_ms
            }
            NodeState::Decommission | NodeState::Blacklist => false,
        };
        if allowed {
            return Ok(());
        }
        warn!(
            "RegisterNode rejected by current state node_id={}, entry_epoch={}, current_epoch={}, current_state={:?}, current_startup_time_ms={}, entry_startup_time_ms={}; skip",
            node.base.node_id,
            node.epoch,
            existing.epoch,
            existing.state,
            existing.base.startup_time_ms,
            node.base.startup_time_ms
        );
        Err(ApplyOutcome::stale(format!(
            "replacement rejected by current_state={:?}",
            existing.state
        )))
    }

    fn apply_status_patch(
        &self,
        existing: &NodeInfo,
        updated: &mut NodeInfo,
        op_ms: u64,
        update: &NodeStatusUpdate,
    ) -> Result<bool, ApplyOutcome> {
        Self::validate_status_fence(existing, update)?;

        let state_changed = self.apply_target_state(
            existing,
            updated,
            op_ms,
            update.node_id,
            update.target_state,
        )?;
        let heartbeat_changed = Self::apply_heartbeat_timestamp(updated, update.heartbeat_ms);
        Ok(state_changed || heartbeat_changed)
    }

    fn apply_target_state(
        &self,
        existing: &NodeInfo,
        updated: &mut NodeInfo,
        op_ms: u64,
        node_id: u32,
        target_state: Option<NodeState>,
    ) -> Result<bool, ApplyOutcome> {
        let Some(target_state) = target_state else {
            return Ok(false);
        };
        if target_state == existing.state {
            return Ok(false);
        }

        Self::validate_state_transition(node_id, existing, target_state)?;
        if target_state == NodeState::Lost && !self.is_heartbeat_expired(existing, op_ms) {
            return Err(ApplyOutcome::SkippedNoop);
        }

        updated.state = target_state;
        updated.state_since_ms = op_ms;
        Ok(true)
    }

    fn apply_heartbeat_timestamp(updated: &mut NodeInfo, heartbeat_ms: Option<u64>) -> bool {
        let Some(heartbeat_ms) = heartbeat_ms else {
            return false;
        };
        if heartbeat_ms <= updated.last_heartbeat_ms {
            return false;
        }
        updated.last_heartbeat_ms = heartbeat_ms;
        true
    }

    fn validate_status_fence(
        existing: &NodeInfo,
        update: &NodeStatusUpdate,
    ) -> Result<(), ApplyOutcome> {
        Self::validate_epoch_fence(
            "UpdateNodeStatus",
            existing,
            update.node_id,
            update.expected_epoch,
        )?;

        if existing.state == update.expected_state
            || update
                .target_state
                .is_some_and(|target| target == existing.state)
        {
            return Ok(());
        }
        warn!(
            "UpdateNodeStatus rejected node_id={}, expected_state={:?}, current_state={:?}, epoch={}; skip",
            update.node_id, update.expected_state, existing.state, existing.epoch
        );
        Err(ApplyOutcome::stale(format!(
            "state mismatch: current={:?}, expected={:?}",
            existing.state, update.expected_state
        )))
    }

    fn validate_node_fence(
        op: &str,
        existing: &NodeInfo,
        node_id: u32,
        expected_epoch: u64,
        expected_state: NodeState,
    ) -> Result<(), ApplyOutcome> {
        Self::validate_epoch_fence(op, existing, node_id, expected_epoch)?;
        if existing.state == expected_state {
            return Ok(());
        }
        warn!(
            "{} rejected node_id={}, expected_state={:?}, current_state={:?}, epoch={}; skip",
            op, node_id, expected_state, existing.state, existing.epoch
        );
        Err(ApplyOutcome::stale(format!(
            "state mismatch: current={:?}, expected={:?}",
            existing.state, expected_state
        )))
    }

    fn validate_epoch_fence(
        op: &str,
        existing: &NodeInfo,
        node_id: u32,
        expected_epoch: u64,
    ) -> Result<(), ApplyOutcome> {
        if existing.epoch == expected_epoch {
            return Ok(());
        }
        warn!(
            "stale {} epoch node_id={}, expected_epoch={}, current_epoch={}; skip",
            op, node_id, expected_epoch, existing.epoch
        );
        Err(ApplyOutcome::stale(format!(
            "epoch mismatch: current={}, expected={}",
            existing.epoch, expected_epoch
        )))
    }

    fn validate_state_transition(
        node_id: u32,
        existing: &NodeInfo,
        target_state: NodeState,
    ) -> Result<(), ApplyOutcome> {
        if Self::is_valid_state_transition(existing.state, target_state) {
            return Ok(());
        }
        warn!(
            "invalid UpdateNodeStatus transition node_id={}, current_state={:?}, target_state={:?}, epoch={}; skip",
            node_id, existing.state, target_state, existing.epoch
        );
        Err(ApplyOutcome::stale(format!(
            "invalid state transition: {:?}->{:?}",
            existing.state, target_state
        )))
    }

    fn is_heartbeat_expired(&self, existing: &NodeInfo, op_ms: u64) -> bool {
        let expire_before_ms = op_ms.saturating_sub(self.heartbeat_timeout_ms());
        existing.last_heartbeat_ms <= expire_before_ms
    }

    fn apply_payload_patch(
        node_id: u32,
        node_type: NodeType,
        payload: &mut NodePayload,
        patch: &NodePayloadPatch,
    ) -> Result<bool, ApplyOutcome> {
        match (payload, patch) {
            (NodePayload::Meta(payload), NodePayloadPatch::Meta(patch)) => {
                Self::apply_meta_payload_patch(node_id, payload, patch)
            }
            _ => {
                warn!(
                    "UpdateNodePayload payload type mismatch node_id={}, node_type={:?}; skip",
                    node_id, node_type
                );
                Err(ApplyOutcome::stale("payload type mismatch"))
            }
        }
    }

    fn apply_meta_payload_patch(
        node_id: u32,
        payload: &mut MetaNodePayload,
        patch: &MetaNodePayloadPatch,
    ) -> Result<bool, ApplyOutcome> {
        if payload.group_id != patch.group_id {
            warn!(
                "UpdateNodePayload meta group_id mismatch node_id={}, current_group_id={}, patch_group_id={}; skip",
                node_id, payload.group_id, patch.group_id
            );
            return Err(ApplyOutcome::stale(format!(
                "group_id mismatch: current={}, patch={}",
                payload.group_id, patch.group_id
            )));
        }

        match patch.group_epoch.cmp(&payload.group_epoch) {
            Ordering::Less => Ok(false),
            Ordering::Equal if Self::same_meta_payload(payload, patch) => Ok(false),
            Ordering::Equal => {
                warn!(
                    "UpdateNodePayload meta changed without group_epoch bump node_id={}, group_id={}, group_epoch={}; skip",
                    node_id, payload.group_id, payload.group_epoch
                );
                Err(ApplyOutcome::stale(
                    "meta payload changed without group_epoch bump",
                ))
            }
            Ordering::Greater => {
                payload.group_epoch = patch.group_epoch;
                payload.peers = patch.peers.clone();
                payload.rw_policy = patch.rw_policy;
                Ok(true)
            }
        }
    }

    fn same_registered_node(existing: &NodeInfo, node: &NodeInfo) -> bool {
        existing.epoch == node.epoch
            && existing.state == node.state
            && existing.base.node_id == node.base.node_id
            && existing.base.node_type == node.base.node_type
            && existing.base.address == node.base.address
            && existing.base.labels == node.base.labels
            && existing.base.software_version == node.base.software_version
            && existing.base.startup_time_ms == node.base.startup_time_ms
            && Self::same_persistent_payload(&existing.payload, &node.payload)
    }

    fn same_persistent_payload(left: &NodePayload, right: &NodePayload) -> bool {
        match (left, right) {
            (NodePayload::Worker(l), NodePayload::Worker(r)) => {
                l.storage_specs.len() == r.storage_specs.len()
                    && l.storage_specs.iter().all(|(id, left_spec)| {
                        r.storage_specs.get(id).is_some_and(|right_spec| {
                            left_spec.dir_id == right_spec.dir_id
                                && left_spec.storage_id == right_spec.storage_id
                                && left_spec.failed == right_spec.failed
                                && left_spec.storage_type == right_spec.storage_type
                                && left_spec.dir_path == right_spec.dir_path
                        })
                    })
            }
            (NodePayload::Meta(l), NodePayload::Meta(r)) => {
                l.group_id == r.group_id
                    && Self::same_peers(&l.peers, &r.peers)
                    && l.rw_policy == r.rw_policy
                    && l.group_epoch == r.group_epoch
            }
            (NodePayload::Task(_), NodePayload::Task(_)) => true,
            _ => false,
        }
    }

    fn same_meta_payload(payload: &MetaNodePayload, patch: &MetaNodePayloadPatch) -> bool {
        payload.group_id == patch.group_id
            && payload.group_epoch == patch.group_epoch
            && Self::same_peers(&payload.peers, &patch.peers)
            && payload.rw_policy == patch.rw_policy
    }

    fn same_peers(left: &[PeerInfo], right: &[PeerInfo]) -> bool {
        left.len() == right.len()
            && left.iter().zip(right).all(|(a, b)| {
                a.node_id == b.node_id && a.address == b.address && a.is_leader == b.is_leader
            })
    }
}
