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

use super::HashBGTableControl;
use crate::pd::bgtable::{BGTable, BGTableControl};
use crate::pd::journal::entry::BGUpdateEntry;
use curvine_common::state::{
    BGKind, BGState, BgId, BlockGroupInfo, NodeState, ReplicaState, WorkerBGReport,
};
use curvine_common::FsResult;
use std::sync::Arc;

impl HashBGTableControl {
    /// Reconcile a worker's heartbeat reports into its Hash BGs. Per-report
    /// failures are logged and skipped so one bad report never aborts the batch.
    pub fn reconcile_replicas(&self, reporter: u32, reports: &[WorkerBGReport]) -> FsResult<()> {
        for report in reports {
            if let Err(e) = self.reconcile_bg(reporter, report) {
                log::warn!(
                    "hash bg reconcile failed reporter={}, bg_id={}, err={}",
                    reporter,
                    report.bg_id,
                    e
                );
            }
        }
        Ok(())
    }

    fn reconcile_bg(&self, reporter: u32, report: &WorkerBGReport) -> FsResult<()> {
        let Some((bg, table)) = self.load_bg_and_table(report.bg_id) else {
            return Ok(());
        };

        let new_isr = self.reconcile_isr(&bg, &table, reporter, report);
        let new_state = derive_bg_state(&bg);
        if new_isr == bg.isr && new_state == bg.state {
            return Ok(());
        }

        let mut entry = BGUpdateEntry {
            op_ms: orpc::common::LocalTime::mills(),
            kind: BGKind::Hash,
            bg_id: bg.bg_id,
            state: (new_state != bg.state).then_some(new_state),
            replica_set: None,
            isr: (new_isr != bg.isr).then_some(new_isr),
            primary: None,
            expected_bg_epoch: bg.bg_epoch,
            bump_table_epoch: false,
        };
        // Table-aware layer: stamp the table-epoch decision before proposing.
        entry.bump_table_epoch = table.bg_change_bumps_table_epoch(&bg, &entry);
        self.bg_manager
            .propose_update_bg(entry)?
            .into_propose_result("reconcile_bg")
    }

    /// Load the BG and its owning table, or `None` if either is gone.
    fn load_bg_and_table(&self, bg_id: BgId) -> Option<(BlockGroupInfo, Arc<BGTable>)> {
        let bg = self.bg_manager.get_bg(BGKind::Hash, bg_id)?;
        let table = self.get_table(bg.table_id)?;
        Some(((*bg).clone(), table))
    }

    /// Compute the new ISR from `report`: the primary may retire lagging
    /// members (down to `min_isr`), and an Active reporter may (re)join.
    fn reconcile_isr(
        &self,
        bg: &BlockGroupInfo,
        table: &BGTable,
        reporter: u32,
        report: &WorkerBGReport,
    ) -> Vec<u32> {
        let mut new_isr = bg.isr.clone();
        if bg.primary.node_id == reporter {
            self.retire_lagging_members(bg, table, report, &mut new_isr);
        }
        if self.can_rejoin_isr(bg, table, reporter, &new_isr) {
            new_isr.push(reporter);
        }
        new_isr
    }

    /// The primary drops reported laggards from ISR, keeping at least `min_isr`
    /// members and never removing the primary itself.
    fn retire_lagging_members(
        &self,
        bg: &BlockGroupInfo,
        table: &BGTable,
        report: &WorkerBGReport,
        new_isr: &mut Vec<u32>,
    ) {
        let mut budget = new_isr
            .len()
            .saturating_sub(self.table_min_isr(table) as usize);
        for &candidate in &report.isr_remove_candidates {
            if budget == 0 {
                break;
            }
            if candidate == bg.primary.node_id {
                continue;
            }
            if let Some(pos) = new_isr.iter().position(|id| *id == candidate) {
                new_isr.remove(pos);
                budget -= 1;
                self.bg_manager
                    .record_isr_failure(BGKind::Hash, bg.bg_id, candidate);
            }
        }
    }

    /// Whether an Active reporter is eligible to (re)join the ISR: it is a
    /// replica-set member, not already in ISR, there is ISR headroom, it is not
    /// rejoin-blocked, and its node is Live.
    fn can_rejoin_isr(
        &self,
        bg: &BlockGroupInfo,
        table: &BGTable,
        reporter: u32,
        new_isr: &[u32],
    ) -> bool {
        bg.replica_set.contains(&reporter)
            && !new_isr.contains(&reporter)
            && new_isr.len() < table.replica_count() as usize
            && !self
                .bg_manager
                .is_isr_rejoin_blocked(BGKind::Hash, bg.bg_id, reporter)
            && self.node_is_live(reporter)
    }

    fn node_is_live(&self, worker_id: u32) -> bool {
        self.pool_manager
            .get_worker_node(worker_id)
            .map(|n| n.state == NodeState::Live)
            .unwrap_or(false)
    }

    fn table_min_isr(&self, table: &BGTable) -> u16 {
        table
            .hash_table()
            .expect("hash table")
            .cache_replica_policy()
            .min_isr
            .max(1)
            .min(table.replica_count())
    }
}

/// Derive a Hash BG's state from its replicas: Active when every replica-set
/// member is Active, else Degraded. A Sealed BG keeps its state.
fn derive_bg_state(bg: &BlockGroupInfo) -> BGState {
    if bg.state == BGState::Sealed {
        return bg.state;
    }
    let all_active = !bg.replica_set.is_empty()
        && bg
            .replica_set
            .iter()
            .all(|wid| bg.replica_state(*wid) == ReplicaState::Active);
    if all_active {
        BGState::Active
    } else {
        BGState::Degraded
    }
}
