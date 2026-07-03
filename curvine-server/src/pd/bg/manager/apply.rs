use super::*;

pub struct PreparedBGCreate {
    pub info: BlockGroupInfo,
    pub op: KvWrite,
}

pub struct PreparedBGUpdate {
    pub old_info: BlockGroupInfo,
    pub new_info: BlockGroupInfo,
    pub op: KvWrite,
}

pub struct PreparedBGDelete {
    pub old_info: BlockGroupInfo,
}

pub(crate) enum UpdateBuildResult {
    Applied(BlockGroupInfo),
    Outcome(ApplyOutcome),
}

pub enum PrepareCreateResult {
    Applied(PreparedBGCreate),
    Outcome(ApplyOutcome),
}

pub(crate) enum PrepareUpdateResult {
    Applied(PreparedBGUpdate),
    Outcome(ApplyOutcome),
}

pub(crate) enum PrepareDeleteResult {
    Applied(PreparedBGDelete),
    Outcome(ApplyOutcome),
}

/// Result of building a BG-update intent: either a ready-to-propose entry, or a
/// short-circuit outcome (the op is a no-op or the BG is gone) that the caller
/// returns directly without proposing.
pub enum BuiltUpdate {
    Built(BGUpdateEntry),
    ShortCircuit(ApplyOutcome),
}

/// Result of building a BG-delete intent. See `BuiltUpdate`.
pub enum BuiltDelete {
    Built(BGDeleteEntry),
    ShortCircuit(ApplyOutcome),
}

impl BGManager {
    // ---- BG-domain intent builders -----------------------------------------

    pub fn build_add_replica_entry(
        &self,
        kind: BGKind,
        bg_id: BgId,
        worker_id: u32,
    ) -> BuiltUpdate {
        let Some(bg) = self.get_bg(kind, bg_id) else {
            return BuiltUpdate::ShortCircuit(ApplyOutcome::not_found(format!(
                "bg {} not found",
                bg_id
            )));
        };
        if bg.replica_set.contains(&worker_id) {
            return BuiltUpdate::ShortCircuit(ApplyOutcome::SkippedNoop);
        }
        let mut new_rs = bg.replica_set.clone();
        new_rs.push(worker_id);
        BuiltUpdate::Built(BGUpdateEntry {
            op_ms: orpc::common::LocalTime::mills(),
            kind,
            bg_id,
            expected_bg_epoch: bg.bg_epoch,
            state: None,
            replica_set: Some(new_rs),
            isr: Some(bg.isr.clone()),
            primary: None,
            bump_table_epoch: false,
        })
    }

    pub fn build_remove_replica_entry(
        &self,
        kind: BGKind,
        bg_id: BgId,
        worker_id: u32,
    ) -> BuiltUpdate {
        let Some(bg) = self.get_bg(kind, bg_id) else {
            return BuiltUpdate::ShortCircuit(ApplyOutcome::not_found(format!(
                "bg {} not found",
                bg_id
            )));
        };
        let new_rs: Vec<u32> = bg
            .replica_set
            .iter()
            .copied()
            .filter(|&w| w != worker_id)
            .collect();
        let new_isr: Vec<u32> = bg.isr.iter().copied().filter(|&w| w != worker_id).collect();
        BuiltUpdate::Built(BGUpdateEntry {
            op_ms: orpc::common::LocalTime::mills(),
            kind,
            bg_id,
            expected_bg_epoch: bg.bg_epoch,
            state: None,
            replica_set: Some(new_rs),
            isr: Some(new_isr),
            primary: None,
            bump_table_epoch: false,
        })
    }

    pub fn build_transfer_primary_entry(
        &self,
        kind: BGKind,
        bg_id: BgId,
        from_worker: u32,
        to_worker: u32,
    ) -> BuiltUpdate {
        let Some(bg) = self.get_bg(kind, bg_id) else {
            return BuiltUpdate::ShortCircuit(ApplyOutcome::not_found(format!(
                "bg {} not found",
                bg_id
            )));
        };
        if bg.primary.node_id != from_worker {
            return BuiltUpdate::ShortCircuit(ApplyOutcome::SkippedNoop);
        }
        let primary = BGPrimary {
            node_id: to_worker,
            epoch: bg.primary.epoch.saturating_add(1),
            grant_time_ms: orpc::common::LocalTime::mills(),
        };
        BuiltUpdate::Built(BGUpdateEntry {
            op_ms: orpc::common::LocalTime::mills(),
            kind,
            bg_id,
            expected_bg_epoch: bg.bg_epoch,
            state: None,
            replica_set: None,
            isr: None,
            primary: Some(primary),
            bump_table_epoch: false,
        })
    }

    pub fn build_seal_entry(&self, kind: BGKind, bg_id: BgId) -> BuiltUpdate {
        let Some(bg) = self.get_bg(kind, bg_id) else {
            return BuiltUpdate::ShortCircuit(ApplyOutcome::not_found(format!(
                "bg {} not found",
                bg_id
            )));
        };
        if bg.state == BGState::Sealed {
            return BuiltUpdate::ShortCircuit(ApplyOutcome::SkippedNoop);
        }
        BuiltUpdate::Built(BGUpdateEntry {
            op_ms: orpc::common::LocalTime::mills(),
            kind,
            bg_id,
            expected_bg_epoch: bg.bg_epoch,
            state: Some(BGState::Sealed),
            replica_set: None,
            isr: None,
            primary: None,
            bump_table_epoch: false,
        })
    }

    pub fn build_delete_entry(&self, kind: BGKind, bg_id: BgId) -> BuiltDelete {
        let Some(bg) = self.get_bg(kind, bg_id) else {
            return BuiltDelete::ShortCircuit(ApplyOutcome::not_found(format!(
                "bg {} not found",
                bg_id
            )));
        };
        BuiltDelete::Built(BGDeleteEntry {
            op_ms: orpc::common::LocalTime::mills(),
            kind,
            bg_id,
            expected_bg_epoch: bg.bg_epoch,
        })
    }

    // ---- raft propose (submit a built entry) -------------------------------

    pub fn propose_create_bg(&self, entry: BGEntry) -> FsResult<ApplyOutcome> {
        let bg_id = entry.info.bg_id;
        let outcome = self.journal_client.propose(PdEntry::CreateBG(entry))?;
        if let ApplyOutcome::SkippedStale { reason } = &outcome {
            log::warn!("propose_create_bg bg_id={} stale: {}", bg_id, reason);
        }
        Ok(outcome)
    }

    pub fn propose_update_bg(&self, entry: BGUpdateEntry) -> FsResult<ApplyOutcome> {
        let (bg_id, expected) = (entry.bg_id, entry.expected_bg_epoch);
        let outcome = self.journal_client.propose(PdEntry::UpdateBG(entry))?;
        if let ApplyOutcome::SkippedStale { reason } = &outcome {
            log::warn!(
                "propose_update_bg bg_id={} (expected_bg_epoch={}) stale: {}",
                bg_id,
                expected,
                reason
            );
        }
        Ok(outcome)
    }

    pub fn propose_delete_bg(&self, entry: BGDeleteEntry) -> FsResult<ApplyOutcome> {
        let (bg_id, expected) = (entry.bg_id, entry.expected_bg_epoch);
        let outcome = self.journal_client.propose(PdEntry::DeleteBG(entry))?;
        if let ApplyOutcome::SkippedStale { reason } = &outcome {
            log::warn!(
                "propose_delete_bg bg_id={} (expected_bg_epoch={}) stale: {}",
                bg_id,
                expected,
                reason
            );
        }
        Ok(outcome)
    }

    pub fn propose_batch_update_bg(&self, entry: BGBatchUpdateEntry) -> FsResult<ApplyOutcome> {
        let bg_id_for_log = entry.updates.first().map(|u| u.bg_id);
        let outcome = self.journal_client.propose(PdEntry::BatchUpdateBG(entry))?;
        if let ApplyOutcome::SkippedStale { reason } = &outcome {
            log::warn!(
                "propose_batch_update_bg bg_id={:?} returned Stale: {}",
                bg_id_for_log,
                reason
            );
        }
        Ok(outcome)
    }

    pub fn apply_allocate_bg_id(&self, entry: &BGIdAllocatorEntry) -> FsResult<ApplyOutcome> {
        let current = self.store.get_next_bg_id()?;
        if current != entry.expected_next_bg_id {
            return Ok(ApplyOutcome::stale(format!(
                "next_bg_id mismatch: current={}, expected={}",
                current, entry.expected_next_bg_id
            )));
        }
        if entry.next_bg_id < entry.expected_next_bg_id {
            return Err(FsError::common(format!(
                "next_bg_id rollback: expected={}, next={}",
                entry.expected_next_bg_id, entry.next_bg_id
            )));
        }
        if entry.next_bg_id == current {
            return Ok(ApplyOutcome::SkippedNoop);
        }
        self.store.set_next_bg_id(entry.next_bg_id)?;
        Ok(ApplyOutcome::Applied)
    }

    /// BG-only apply path. Production Raft apply for route-visible BG entries is
    /// orchestrated by BGTableManager so table epoch/index updates stay atomic
    /// with BG metadata. This method is kept for BG-local tests and future
    /// BG-only entries.
    pub fn apply_create_bg(&self, entry: &BGEntry) -> FsResult<ApplyOutcome> {
        match self.prepare_create_bg(&entry.info)? {
            PrepareCreateResult::Applied(plan) => {
                self.store.put(&plan.info).map_err(FsError::from)?;
                self.insert_bg(plan.info);
                Ok(ApplyOutcome::Applied)
            }
            PrepareCreateResult::Outcome(outcome) => Ok(outcome),
        }
    }

    pub fn apply_update_bg(&self, entry: &BGUpdateEntry) -> FsResult<ApplyOutcome> {
        match self.prepare_update_bg(entry)? {
            PrepareUpdateResult::Applied(plan) => {
                self.store.put(&plan.new_info).map_err(FsError::from)?;
                self.update_bg(&plan.old_info, plan.new_info.clone());
                self.cleanup_isr_penalties(&plan.old_info, &plan.new_info);
                Ok(ApplyOutcome::Applied)
            }
            PrepareUpdateResult::Outcome(outcome) => Ok(outcome),
        }
    }

    pub fn apply_delete_bg(&self, entry: &BGDeleteEntry) -> FsResult<ApplyOutcome> {
        match self.prepare_delete_bg(entry)? {
            PrepareDeleteResult::Applied(plan) => {
                self.store
                    .delete(plan.old_info.bg_id)
                    .map_err(FsError::from)?;
                self.remove_bg(&plan.old_info);
                Ok(ApplyOutcome::Applied)
            }
            PrepareDeleteResult::Outcome(outcome) => Ok(outcome),
        }
    }

    pub(crate) fn prepare_create_bg(&self, info: &BlockGroupInfo) -> FsResult<PrepareCreateResult> {
        Self::validate_bg_info(info)?;
        if !self.validate_bg_id_absent(info.bg_id) {
            return Ok(PrepareCreateResult::Outcome(ApplyOutcome::SkippedNoop));
        }
        let mut runtime_info = info.clone();
        runtime_info.reset_replicas();
        let op = self.store.bg_put_op(&runtime_info).map_err(FsError::from)?;
        Ok(PrepareCreateResult::Applied(PreparedBGCreate {
            info: runtime_info,
            op,
        }))
    }

    pub(crate) fn prepare_update_bg(&self, entry: &BGUpdateEntry) -> FsResult<PrepareUpdateResult> {
        let existing = match self.get_bg(entry.kind, entry.bg_id) {
            Some(bg) => bg,
            None => {
                return Ok(PrepareUpdateResult::Outcome(ApplyOutcome::not_found(
                    format!("{:?} bg {} not found for update", entry.kind, entry.bg_id),
                )));
            }
        };
        let old_info = (*existing).clone();
        let new_info = match Self::build_updated_bg(&old_info, entry)? {
            UpdateBuildResult::Applied(mut info) => {
                info.sync_replicas_with_replica_set();
                info
            }
            UpdateBuildResult::Outcome(outcome) => {
                return Ok(PrepareUpdateResult::Outcome(outcome))
            }
        };
        Self::validate_bg_info(&new_info)?;
        let op = self.store.bg_put_op(&new_info).map_err(FsError::from)?;
        Ok(PrepareUpdateResult::Applied(PreparedBGUpdate {
            old_info,
            new_info,
            op,
        }))
    }

    pub(crate) fn prepare_delete_bg(&self, entry: &BGDeleteEntry) -> FsResult<PrepareDeleteResult> {
        let Some(existing) = self.get_bg(entry.kind, entry.bg_id) else {
            return Ok(PrepareDeleteResult::Outcome(ApplyOutcome::not_found(
                format!("{:?} bg {} not present", entry.kind, entry.bg_id),
            )));
        };
        if existing.bg_epoch != entry.expected_bg_epoch {
            return Ok(PrepareDeleteResult::Outcome(ApplyOutcome::stale(format!(
                "bg_epoch mismatch: current={}, expected={}",
                existing.bg_epoch, entry.expected_bg_epoch
            ))));
        }
        Ok(PrepareDeleteResult::Applied(PreparedBGDelete {
            old_info: (*existing).clone(),
        }))
    }

    pub(crate) fn build_updated_bg(
        old_info: &BlockGroupInfo,
        entry: &BGUpdateEntry,
    ) -> FsResult<UpdateBuildResult> {
        if old_info.bg_epoch != entry.expected_bg_epoch {
            return Ok(UpdateBuildResult::Outcome(ApplyOutcome::stale(format!(
                "bg_epoch mismatch: current={}, expected={}",
                old_info.bg_epoch, entry.expected_bg_epoch
            ))));
        }
        if let Some(state) = entry.state {
            state_machine::validate_transition(old_info.kind, old_info.state, state)?;
        }
        if let Some(ref new_primary) = entry.primary {
            if new_primary.epoch <= old_info.primary.epoch {
                return Ok(UpdateBuildResult::Outcome(ApplyOutcome::stale(format!(
                    "primary.epoch non-monotonic: current={}, entry={}",
                    old_info.primary.epoch, new_primary.epoch
                ))));
            }
        }

        let mut info = old_info.clone();
        info.bg_epoch = old_info.bg_epoch.saturating_add(1);
        if let Some(state) = entry.state {
            info.state = state;
        }
        if let Some(ref replica_set) = entry.replica_set {
            info.replica_set = replica_set.clone();
        }
        if let Some(ref isr) = entry.isr {
            info.isr = isr.clone();
        }
        if entry.replica_set.is_some() || entry.isr.is_some() {
            let replica_set: HashSet<u32> = info.replica_set.iter().copied().collect();
            info.isr.retain(|worker_id| replica_set.contains(worker_id));
        }
        if let Some(ref primary) = entry.primary {
            info.primary = primary.clone();
        }
        Ok(UpdateBuildResult::Applied(info))
    }

    pub(crate) fn validate_bg_info(info: &BlockGroupInfo) -> FsResult<()> {
        if info.replica_set.is_empty() {
            return Err(FsError::common(format!(
                "bg {} replica_set is empty",
                info.bg_id
            )));
        }
        let replica_set: HashSet<u32> = info.replica_set.iter().copied().collect();
        if replica_set.len() != info.replica_set.len() {
            return Err(FsError::common(format!(
                "bg {} has duplicated replicas",
                info.bg_id
            )));
        }
        if !info.isr.iter().all(|worker| replica_set.contains(worker)) {
            return Err(FsError::common(format!(
                "bg {} has ISR outside replica_set",
                info.bg_id
            )));
        }
        if !replica_set.contains(&info.primary.node_id) {
            return Err(FsError::common(format!(
                "bg {} primary {} is outside replica_set",
                info.bg_id, info.primary.node_id
            )));
        }
        Ok(())
    }

    pub(crate) fn insert_bg(&self, info: BlockGroupInfo) {
        self.controller(info.kind).insert_bg(info);
    }

    pub(crate) fn update_bg(&self, old: &BlockGroupInfo, new: BlockGroupInfo) {
        self.controller(new.kind).update_bg(old, new);
    }

    pub(crate) fn remove_bg(&self, old: &BlockGroupInfo) {
        self.controller(old.kind).remove_bg(old);
    }
}
