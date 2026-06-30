use super::*;

#[derive(Default)]
pub(crate) struct HashBGController {
    index: BGIndex,
}

impl HashBGController {
    fn filter_state(
        bgs: impl IntoIterator<Item = Arc<BlockGroupInfo>>,
        state: Option<BGState>,
    ) -> Vec<Arc<BlockGroupInfo>> {
        bgs.into_iter()
            .filter(|bg| state.is_none_or(|state| bg.state == state))
            .collect()
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

    fn list_bgs(&self, state: Option<BGState>) -> Vec<Arc<BlockGroupInfo>> {
        Self::filter_state(self.index.list_bgs(), state)
    }

    fn snapshot_bgs(&self, state: Option<BGState>) -> HashMap<BgId, Arc<BlockGroupInfo>> {
        self.list_bgs(state)
            .into_iter()
            .map(|bg| (bg.bg_id, bg))
            .collect()
    }

    fn restore_bgs(&self, bgs: HashMap<BgId, Arc<BlockGroupInfo>>) {
        self.index.restore_bgs(bgs);
    }

    fn insert_bg(&self, mut info: BlockGroupInfo) {
        info.reset_replicas();
        self.index.insert_bg(info);
    }

    fn update_bg(&self, old: &BlockGroupInfo, mut new: BlockGroupInfo) {
        new.sync_replicas_with_replica_set();
        self.index.update_bg(old, new);
    }

    fn remove_bg(&self, old: &BlockGroupInfo) {
        self.index.remove_bg(old);
    }

    fn bgs_on_worker(&self, worker_id: u32, state: Option<BGState>) -> Vec<Arc<BlockGroupInfo>> {
        Self::filter_state(self.index.bgs_on_worker(worker_id), state)
    }

    fn worker_primary_counts(&self, state: Option<BGState>) -> HashMap<u32, u32> {
        let mut counts = HashMap::new();
        for bg in self.list_bgs(state) {
            *counts.entry(bg.primary.node_id).or_default() += 1;
        }
        counts
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
}
