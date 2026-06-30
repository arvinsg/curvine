use super::*;

#[derive(Default)]
pub(crate) struct HashBGController {
    index: BGIndex,
}

impl HashBGController {
    fn active(&self, scope: BGListScope) -> bool {
        matches!(scope, BGListScope::Active | BGListScope::All)
    }

    pub(crate) fn record_isr_failure(&self, bg_id: BgId, worker_id: u32) {
        self.index.record_isr_failure(bg_id, worker_id);
    }

    pub(crate) fn is_isr_rejoin_blocked(&self, bg_id: BgId, worker_id: u32) -> bool {
        self.index.is_isr_rejoin_blocked(bg_id, worker_id)
    }

    pub(crate) fn cleanup_isr_penalties(&self, old: &BlockGroupInfo, new: &BlockGroupInfo) {
        self.index.cleanup_isr_penalties(old, new);
    }
}

impl BGController for HashBGController {
    fn contains_bg(&self, bg_id: BgId) -> bool {
        self.index.contains_bg(bg_id)
    }

    fn get_bg(&self, bg_id: BgId) -> Option<Arc<BlockGroupInfo>> {
        self.index.get_bg(bg_id)
    }

    fn list_bgs(&self, scope: BGListScope) -> Vec<Arc<BlockGroupInfo>> {
        if self.active(scope) {
            self.index.list_bgs()
        } else {
            Vec::new()
        }
    }

    fn snapshot_bgs(&self, scope: BGListScope) -> HashMap<BgId, Arc<BlockGroupInfo>> {
        if self.active(scope) {
            self.index.snapshot_bgs()
        } else {
            HashMap::new()
        }
    }

    fn restore_bgs(&self, bgs: HashMap<BgId, Arc<BlockGroupInfo>>) {
        self.index.restore_bgs(bgs);
    }

    fn insert_bg(&self, mut info: BlockGroupInfo) {
        info.reset_runtime_replicas();
        self.index.insert_bg(info);
    }

    fn update_bg(&self, old: &BlockGroupInfo, mut new: BlockGroupInfo) {
        new.sync_runtime_replicas_with_set();
        self.index.update_bg(old, new);
    }

    fn remove_bg(&self, old: &BlockGroupInfo) {
        self.index.remove_bg(old);
    }

    fn bgs_on_worker(&self, worker_id: u32, scope: BGListScope) -> Vec<Arc<BlockGroupInfo>> {
        if self.active(scope) {
            self.index.bgs_on_worker(worker_id)
        } else {
            Vec::new()
        }
    }

    fn worker_primary_counts(&self, scope: BGListScope) -> HashMap<u32, u32> {
        if self.active(scope) {
            self.index.worker_primary_counts()
        } else {
            HashMap::new()
        }
    }

    fn reset_replica_states(&self) {
        self.index.reset_replica_states();
    }

    fn set_op_state(&self, bg_id: BgId, op_state: BGOpState) {
        self.index.set_op_state(bg_id, op_state);
    }

    fn update_bg_stats(&self, bg_stats: &HashMap<BgId, BGStats>) {
        self.index.update_bg_stats(bg_stats);
    }

    fn get_replica_state(&self, bg_id: BgId, worker_id: u32) -> ReplicaState {
        self.index.get_replica_state(bg_id, worker_id)
    }

    fn set_replica_state(&self, bg_id: BgId, worker_id: u32, state: ReplicaState) {
        self.index.set_replica_state(bg_id, worker_id, state);
    }

    fn apply_replica_reports(&self, worker_id: u32, reports: &[WorkerBGReport]) -> usize {
        self.index.apply_replica_reports(worker_id, reports)
    }

    fn serving_replicas(&self, bg_id: BgId) -> Vec<u32> {
        self.index.serving_replicas(bg_id)
    }

    fn resident_replicas(&self, bg_id: BgId) -> Vec<u32> {
        self.index.resident_replicas(bg_id)
    }
}
