use super::placement::HashPlacement;
use crate::pd::bgtable::{BGTable, BGTableControl};
use crate::pd::journal::entry::BGUpdateEntry;
use curvine_common::state::{
    BGKind, BGState, BlockGroupInfo, NodeState, ReplicaState, WorkerBGReport,
};
use curvine_common::FsResult;

impl HashPlacement<'_> {
    pub fn reconcile_replica_reports(
        &self,
        worker_id: u32,
        reports: &[WorkerBGReport],
    ) -> FsResult<()> {
        for report in reports {
            if let Err(e) = self.reconcile_hash_bg_from_report(worker_id, report) {
                log::warn!(
                    "hash bg reconcile from report failed worker_id={}, bg_id={}, err={}",
                    worker_id,
                    report.bg_id,
                    e
                );
            }
        }
        Ok(())
    }

    fn reconcile_hash_bg_from_report(
        &self,
        reporter: u32,
        report: &WorkerBGReport,
    ) -> FsResult<()> {
        let Some(bg_arc) = self.controller().bg_manager.get_bg(BGKind::Hash, report.bg_id) else {
            return Ok(());
        };
        let bg = (*bg_arc).clone();
        if bg.kind != BGKind::Hash {
            return Ok(());
        }
        let Some(table) = self.controller().get_table(bg.table_id) else {
            return Ok(());
        };
        if table.kind() != BGKind::Hash {
            return Ok(());
        }

        let mut new_isr = bg.isr.clone();
        let current_primary = bg.primary.node_id;
        let reporter_is_primary = current_primary == reporter;

        if reporter_is_primary {
            let min_isr = self.table_min_isr(&table);
            let mut removable_budget = new_isr.len().saturating_sub(min_isr as usize);
            for candidate in &report.isr_remove_candidates {
                if removable_budget == 0 {
                    break;
                }
                if *candidate == current_primary {
                    continue;
                }
                if let Some(pos) = new_isr.iter().position(|id| id == candidate) {
                    new_isr.remove(pos);
                    removable_budget -= 1;
                    self.controller()
                        .bg_manager
                        .record_isr_failure(BGKind::Hash, bg.bg_id, *candidate);
                }
            }
        }

        if report.state == ReplicaState::Active
            && bg.replica_set.contains(&reporter)
            && !new_isr.contains(&reporter)
            && new_isr.len() < table.replica_count() as usize
            && !self
                .controller()
                .bg_manager
                .is_isr_rejoin_blocked(BGKind::Hash, bg.bg_id, reporter)
            && self
                .controller()
                .pool_manager
                .get_worker_node(reporter)
                .map(|n| n.state == NodeState::Live)
                .unwrap_or(false)
        {
            new_isr.push(reporter);
        }

        let new_state = self.summarize_hash_bg_state(&bg);
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
        // This layer is table-aware, so it stamps the table-epoch decision
        // before handing the entry to the BG-domain propose.
        entry.bump_table_epoch = table.bg_change_bumps_table_epoch(&bg, &entry);
        self.controller()
            .bg_manager
            .propose_update_bg(entry)?
            .into_propose_result("reconcile_hash_bg_from_report")
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

    fn summarize_hash_bg_state(&self, bg: &BlockGroupInfo) -> BGState {
        if bg.kind != BGKind::Hash || bg.state == BGState::Sealed {
            return bg.state;
        }
        let all_replicas_active = !bg.replica_set.is_empty()
            && bg
                .replica_set
                .iter()
                .all(|wid| bg.replica_state(*wid) == ReplicaState::Active);
        if all_replicas_active {
            BGState::Active
        } else {
            BGState::Degraded
        }
    }
}
