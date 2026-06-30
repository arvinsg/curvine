use super::*;

pub(crate) struct BGCreatePlan {
    pub info: BlockGroupInfo,
    pub op: KvWrite,
}

pub(crate) struct BGUpdatePlan {
    pub old_info: BlockGroupInfo,
    pub new_info: BlockGroupInfo,
    pub op: KvWrite,
}

pub(crate) struct BGDeletePlan {
    pub old_info: BlockGroupInfo,
    pub op: KvWrite,
}

pub(crate) enum UpdateBuildResult {
    Applied(BlockGroupInfo),
    Outcome(ApplyOutcome),
}

pub(crate) enum PrepareCreateResult {
    Applied(BGCreatePlan),
    Outcome(ApplyOutcome),
}

pub(crate) enum PrepareUpdateResult {
    Applied(BGUpdatePlan),
    Outcome(ApplyOutcome),
}

pub(crate) enum PrepareDeleteResult {
    Applied(BGDeletePlan),
    Outcome(ApplyOutcome),
}

impl BGManager {
    pub fn propose_remove_replica(
        &self,
        kind: BGKind,
        bg_id: BgId,
        worker_id: u32,
    ) -> FsResult<()> {
        let bg = self
            .get_bg(kind, bg_id)
            .ok_or_else(|| FsError::common(format!("bg {} not found", bg_id)))?;
        let new_rs: Vec<u32> = bg
            .replica_set
            .iter()
            .filter(|&&w| w != worker_id)
            .copied()
            .collect();
        let entry = BGUpdateEntry {
            op_ms: orpc::common::LocalTime::mills(),
            kind,
            bg_id,
            state: None,
            replica_set: Some(new_rs),
            isr: Some(bg.isr.iter().copied().filter(|&w| w != worker_id).collect()),
            primary: None,
            expected_bg_epoch: bg.bg_epoch,
        };
        self.propose_update_bg(entry, "propose_remove_replica")
    }

    pub fn propose_add_replica(&self, kind: BGKind, bg_id: BgId, worker_id: u32) -> FsResult<()> {
        let bg = self
            .get_bg(kind, bg_id)
            .ok_or_else(|| FsError::common(format!("bg {} not found", bg_id)))?;
        if bg.replica_set.contains(&worker_id) {
            return Ok(());
        }
        let mut new_rs = bg.replica_set.clone();
        new_rs.push(worker_id);
        let entry = BGUpdateEntry {
            op_ms: orpc::common::LocalTime::mills(),
            kind,
            bg_id,
            state: None,
            replica_set: Some(new_rs),
            isr: Some(bg.isr.clone()),
            primary: None,
            expected_bg_epoch: bg.bg_epoch,
        };
        self.propose_update_bg(entry, "propose_add_replica")
    }

    pub fn propose_transfer_primary(
        &self,
        kind: BGKind,
        bg_id: BgId,
        from_worker: u32,
        to_worker: u32,
    ) -> FsResult<()> {
        let bg = self
            .get_bg(kind, bg_id)
            .ok_or_else(|| FsError::common(format!("bg {} not found", bg_id)))?;
        let current_owner = bg.primary.node_id;
        if current_owner != from_worker {
            return Ok(());
        }
        let new_epoch = bg.primary.epoch.saturating_add(1);
        let primary = BGPrimary {
            node_id: to_worker,
            epoch: new_epoch,
            grant_time_ms: orpc::common::LocalTime::mills(),
        };
        let entry = BGUpdateEntry {
            op_ms: orpc::common::LocalTime::mills(),
            kind,
            bg_id,
            state: None,
            replica_set: None,
            isr: None,
            primary: Some(primary),
            expected_bg_epoch: bg.bg_epoch,
        };
        self.propose_update_bg(entry, "propose_transfer_primary")
    }

    pub fn propose_seal_bg(&self, kind: BGKind, bg_id: BgId) -> FsResult<()> {
        self.propose_bg_state_transition(kind, bg_id, BGState::Sealed, "propose_seal_bg")
    }

    pub fn propose_mark_deleting_bg(&self, kind: BGKind, bg_id: BgId) -> FsResult<()> {
        self.propose_bg_state_transition(kind, bg_id, BGState::Deleting, "propose_mark_deleting_bg")
    }

    fn propose_bg_state_transition(
        &self,
        kind: BGKind,
        bg_id: BgId,
        target_state: BGState,
        op_name: &str,
    ) -> FsResult<()> {
        let Some(bg) = self.get_bg(kind, bg_id) else {
            return Ok(());
        };
        if bg.state == target_state {
            return Ok(());
        }
        let entry = BGUpdateEntry {
            op_ms: orpc::common::LocalTime::mills(),
            kind,
            bg_id,
            state: Some(target_state),
            replica_set: None,
            isr: None,
            primary: None,
            expected_bg_epoch: bg.bg_epoch,
        };
        self.propose_update_bg(entry, op_name)
    }

    pub fn propose_delete_bg(&self, kind: BGKind, bg_id: BgId) -> FsResult<()> {
        let Some(bg) = self.get_bg(kind, bg_id) else {
            return Ok(());
        };
        let expected_epoch = bg.bg_epoch;
        let entry = BGDeleteEntry {
            op_ms: orpc::common::LocalTime::mills(),
            kind,
            bg_id,
            expected_bg_epoch: expected_epoch,
        };
        let outcome = self.journal_client.propose(PdEntry::DeleteBG(entry))?;
        match outcome {
            ApplyOutcome::Applied | ApplyOutcome::SkippedNoop | ApplyOutcome::NotFound { .. } => {
                Ok(())
            }
            ApplyOutcome::SkippedStale { reason } => {
                log::warn!(
                    "propose_delete_bg bg_id={} returned Stale (expected_bg_epoch={}): {}",
                    bg_id,
                    expected_epoch,
                    reason
                );
                Err(FsError::stale_entry("delete_bg", expected_epoch, reason))
            }
        }
    }

    pub(crate) fn propose_update_bg(&self, entry: BGUpdateEntry, kind: &str) -> FsResult<()> {
        let expected_epoch = entry.expected_bg_epoch;
        let bg_id = entry.bg_id;
        let outcome = self.journal_client.propose(PdEntry::UpdateBG(entry))?;
        match outcome {
            ApplyOutcome::Applied | ApplyOutcome::SkippedNoop => Ok(()),
            ApplyOutcome::SkippedStale { reason } => {
                log::warn!(
                    "{} bg_id={} returned Stale (expected_bg_epoch={}): {}",
                    kind,
                    bg_id,
                    expected_epoch,
                    reason
                );
                Err(FsError::stale_entry("update_bg", expected_epoch, reason))
            }
            ApplyOutcome::NotFound { reason } => Err(FsError::not_found(reason)),
        }
    }

    pub fn propose_batch_update_bg(&self, entry: BGBatchUpdateEntry) -> FsResult<()> {
        let bg_id_for_log = entry.updates.first().map(|u| u.bg_id);
        let outcome = self.journal_client.propose(PdEntry::BatchUpdateBG(entry))?;
        match outcome {
            ApplyOutcome::Applied | ApplyOutcome::SkippedNoop => Ok(()),
            ApplyOutcome::SkippedStale { reason } => Err(FsError::stale_entry(
                "batch_update_bg",
                format!("bg_id={:?}", bg_id_for_log),
                reason,
            )),
            ApplyOutcome::NotFound { reason } => Err(FsError::not_found(reason)),
        }
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
                self.write_batch(vec![plan.op])?;
                self.insert_bg(plan.info);
                Ok(ApplyOutcome::Applied)
            }
            PrepareCreateResult::Outcome(outcome) => Ok(outcome),
        }
    }

    pub fn apply_update_bg(&self, entry: &BGUpdateEntry) -> FsResult<ApplyOutcome> {
        match self.prepare_update_bg(entry)? {
            PrepareUpdateResult::Applied(plan) => {
                self.write_batch(vec![plan.op])?;
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
                self.write_batch(vec![plan.op])?;
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
        if runtime_info.kind == BGKind::Hash
            || matches!(runtime_info.state, BGState::Init | BGState::Active)
        {
            runtime_info.reset_runtime_replicas();
        } else {
            runtime_info.replicas.clear();
            runtime_info.op_state = BGOpState::Idle;
        }
        let op = self.bg_put_op(&runtime_info)?;
        Ok(PrepareCreateResult::Applied(BGCreatePlan {
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
                info.sync_runtime_replicas_with_set();
                info
            }
            UpdateBuildResult::Outcome(outcome) => {
                return Ok(PrepareUpdateResult::Outcome(outcome))
            }
        };
        Self::validate_bg_info(&new_info)?;
        let op = self.bg_put_op(&new_info)?;
        Ok(PrepareUpdateResult::Applied(BGUpdatePlan {
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
        Ok(PrepareDeleteResult::Applied(BGDeletePlan {
            old_info: (*existing).clone(),
            op: self.bg_delete_op(entry.bg_id),
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

    pub(crate) fn bg_put_op(&self, info: &BlockGroupInfo) -> FsResult<KvWrite> {
        self.store.bg_put_op(info).map_err(Into::into)
    }

    pub(crate) fn bg_delete_op(&self, bg_id: BgId) -> KvWrite {
        self.store.bg_delete_op(bg_id)
    }

    pub(crate) fn write_batch(&self, ops: Vec<KvWrite>) -> FsResult<()> {
        self.store.write_batch(ops).map_err(Into::into)
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
