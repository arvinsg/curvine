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

use super::state_machine;
use super::{BGStore, BgIdAllocator};
use crate::pd::journal::entry::{
    BGBatchUpdateEntry, BGDeleteEntry, BGEntry, BGIdAllocatorEntry, BGUpdateEntry,
};
use crate::pd::journal::{self, ApplyOutcome, PdEntry};
use crate::pd::store::KvWrite;
use curvine_common::state::{
    BGKind, BGOpState, BGPrimary, BGState, BGStats, BgId, BlockGroupInfo, ReplicaState,
    WorkerBGReport,
};
use curvine_common::{FsError, FsResult};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

mod apply;
pub(crate) use apply::{PrepareCreateResult, PrepareDeleteResult, PrepareUpdateResult};
mod capacity_controller;
mod controller;
mod hash_controller;
mod index;
#[cfg(test)]
mod tests;

use capacity_controller::CapacityBGController;
use controller::BGController;
pub use controller::BGListScope;
use hash_controller::HashBGController;
use index::BGIndex;

pub struct BGManager {
    hash: HashBGController,
    capacity: CapacityBGController,
    store: Arc<BGStore>,
    id_allocator: BgIdAllocator,
    journal_client: Arc<journal::Client>,
}

impl BGManager {
    pub fn new(store: Arc<BGStore>, journal_client: Arc<journal::Client>) -> Self {
        let id_allocator = BgIdAllocator::new(store.clone(), journal_client.clone());
        Self {
            hash: HashBGController::default(),
            capacity: CapacityBGController::default(),
            store,
            id_allocator,
            journal_client,
        }
    }

    pub fn restore(&self) -> FsResult<()> {
        let (hash_bgs, capacity_bgs) = self.restore_bgs_by_kind()?;
        self.id_allocator.restore()?;
        self.hash.restore_bgs(hash_bgs);
        self.capacity.restore_bgs(capacity_bgs);
        Ok(())
    }

    pub fn reset_runtime_route_state_after_snapshot(&self) {
        self.reset_replica_states();
        log::info!("BG runtime replica state reset after snapshot restore");
    }

    pub fn reset_replica_states(&self) {
        self.hash.reset_replica_states();
        self.capacity.reset_replica_states();
    }

    pub fn on_leader_start(&self) -> FsResult<()> {
        self.reset_replica_states();
        log::info!("BG runtime replica state reset on leader start");
        Ok(())
    }

    fn restore_bgs_by_kind(
        &self,
    ) -> FsResult<(
        HashMap<BgId, Arc<BlockGroupInfo>>,
        HashMap<BgId, Arc<BlockGroupInfo>>,
    )> {
        let bgs = self.store.list_all()?;
        let mut hash_bgs = HashMap::new();
        let mut capacity_bgs = HashMap::new();
        for mut bg in bgs {
            match bg.kind {
                BGKind::Hash => {
                    bg.reset_runtime_replicas();
                    hash_bgs.insert(bg.bg_id, Arc::new(bg));
                }
                BGKind::Capacity => {
                    if matches!(bg.state, BGState::Init | BGState::Active) {
                        bg.reset_runtime_replicas();
                    } else {
                        bg.replicas.clear();
                        bg.op_state = BGOpState::Idle;
                    }
                    capacity_bgs.insert(bg.bg_id, Arc::new(bg));
                }
            }
        }
        Ok((hash_bgs, capacity_bgs))
    }

    pub(crate) fn restore_all_bgs(&self, bgs: HashMap<BgId, Arc<BlockGroupInfo>>) {
        let mut hash_bgs = HashMap::new();
        let mut capacity_bgs = HashMap::new();
        for (bg_id, bg) in bgs {
            match bg.kind {
                BGKind::Hash => {
                    hash_bgs.insert(bg_id, bg);
                }
                BGKind::Capacity => {
                    capacity_bgs.insert(bg_id, bg);
                }
            }
        }
        self.hash.restore_bgs(hash_bgs);
        self.capacity.restore_bgs(capacity_bgs);
    }

    pub(crate) fn ensure_next_id_at_least(&self, floor: BgId) -> FsResult<Option<(BgId, BgId)>> {
        self.id_allocator.ensure_next_id_at_least(floor)
    }

    fn controller(&self, kind: BGKind) -> &dyn BGController {
        match kind {
            BGKind::Hash => &self.hash,
            BGKind::Capacity => &self.capacity,
        }
    }

    fn validate_bg_id_absent(&self, bg_id: BgId) -> bool {
        !self.hash.contains_bg(bg_id) && !self.capacity.contains_bg(bg_id)
    }

    pub fn get_bg(&self, kind: BGKind, bg_id: BgId) -> Option<Arc<BlockGroupInfo>> {
        self.controller(kind).get_bg(bg_id)
    }

    pub fn list_bgs(&self, kind: BGKind, scope: BGListScope) -> Vec<Arc<BlockGroupInfo>> {
        self.controller(kind).list_bgs(scope)
    }

    pub fn list_all_bgs(&self) -> Vec<Arc<BlockGroupInfo>> {
        let mut bgs = self.hash.list_bgs(BGListScope::All);
        bgs.extend(self.capacity.list_bgs(BGListScope::All));
        bgs
    }

    pub(crate) fn snapshot_all_bgs(&self) -> HashMap<BgId, Arc<BlockGroupInfo>> {
        let mut bgs = self.hash.snapshot_bgs(BGListScope::All);
        bgs.extend(self.capacity.snapshot_bgs(BGListScope::All));
        bgs
    }

    pub fn worker_primary_counts(&self, kind: BGKind, scope: BGListScope) -> HashMap<u32, u32> {
        self.controller(kind).worker_primary_counts(scope)
    }

    pub fn get_next_bg_id(&self) -> FsResult<BgId> {
        self.store.get_next_bg_id().map_err(Into::into)
    }

    pub fn alloc_bg_ids(&self, count: u64) -> FsResult<BgId> {
        self.id_allocator.alloc(count)
    }

    pub fn set_op_state(&self, kind: BGKind, bg_id: BgId, op_state: BGOpState) {
        self.controller(kind).set_op_state(bg_id, op_state);
    }

    pub fn update_bg_stats(&self, kind: BGKind, bg_stats: &HashMap<BgId, BGStats>) {
        self.controller(kind).update_bg_stats(bg_stats);
    }

    pub fn bgs_on_worker(
        &self,
        kind: BGKind,
        worker_id: u32,
        scope: BGListScope,
    ) -> Vec<Arc<BlockGroupInfo>> {
        self.controller(kind).bgs_on_worker(worker_id, scope)
    }

    pub fn get_bgs_by_state(&self, kind: BGKind, state: BGState) -> Vec<Arc<BlockGroupInfo>> {
        self.controller(kind)
            .list_bgs(BGListScope::All)
            .into_iter()
            .filter(|bg| bg.state == state)
            .collect()
    }

    pub fn get_replica_state(&self, kind: BGKind, bg_id: BgId, worker_id: u32) -> ReplicaState {
        self.controller(kind).get_replica_state(bg_id, worker_id)
    }

    /// Update the PD-observed replica state.
    ///
    /// Normal production flow should prefer `apply_replica_reports`, because
    /// replica state is owned by worker reports. Keep this API for tests and
    /// exceptional control-plane fencing paths.
    pub fn set_replica_state(
        &self,
        kind: BGKind,
        bg_id: BgId,
        worker_id: u32,
        state: ReplicaState,
    ) {
        self.controller(kind)
            .set_replica_state(bg_id, worker_id, state);
    }

    /// Apply detailed replica states from a worker heartbeat.
    pub fn apply_replica_reports(
        &self,
        worker_id: u32,
        reports: &[WorkerBGReport],
    ) -> FsResult<usize> {
        let hash_reports: Vec<WorkerBGReport> = reports
            .iter()
            .filter(|report| report.kind == BGKind::Hash)
            .cloned()
            .collect();
        let capacity_reports: Vec<WorkerBGReport> = reports
            .iter()
            .filter(|report| report.kind == BGKind::Capacity)
            .cloned()
            .collect();
        let changed = self.hash.apply_replica_reports(worker_id, &hash_reports)
            + self
                .capacity
                .apply_replica_reports(worker_id, &capacity_reports);
        if changed > 0 {
            log::info!(
                "Applied replica reports worker_id={}, changed={}",
                worker_id,
                changed
            );
        }
        Ok(changed)
    }

    pub(crate) fn record_isr_failure(&self, kind: BGKind, bg_id: BgId, worker_id: u32) {
        if kind == BGKind::Hash {
            self.hash.record_isr_failure(bg_id, worker_id);
        }
    }

    pub(crate) fn is_isr_rejoin_blocked(&self, kind: BGKind, bg_id: BgId, worker_id: u32) -> bool {
        kind == BGKind::Hash && self.hash.is_isr_rejoin_blocked(bg_id, worker_id)
    }

    pub(crate) fn cleanup_isr_penalties(&self, old: &BlockGroupInfo, new: &BlockGroupInfo) {
        if new.kind == BGKind::Hash {
            self.hash.cleanup_isr_penalties(old, new);
        }
    }

    pub fn serving_replicas(&self, kind: BGKind, bg_id: BgId) -> Vec<u32> {
        self.controller(kind).serving_replicas(bg_id)
    }

    pub fn resident_replicas(&self, kind: BGKind, bg_id: BgId) -> Vec<u32> {
        self.controller(kind).resident_replicas(bg_id)
    }
}
