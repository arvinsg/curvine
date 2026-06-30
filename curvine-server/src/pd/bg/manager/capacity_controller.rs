use super::*;

#[derive(Default)]
pub(crate) struct CapacityBGController {
    active: BGIndex,
    sealed: RwLock<HashMap<BgId, Arc<BlockGroupInfo>>>,
    sealed_by_worker: RwLock<HashMap<u32, HashSet<BgId>>>,
}

impl CapacityBGController {
    fn is_active_state(state: BGState) -> bool {
        matches!(state, BGState::Init | BGState::Active)
    }

    fn compact_sealed_runtime(info: &mut BlockGroupInfo) {
        info.replicas.clear();
        info.op_state = BGOpState::Idle;
    }

    fn insert_sealed(&self, mut info: BlockGroupInfo) {
        Self::compact_sealed_runtime(&mut info);
        let bg_id = info.bg_id;
        let replica_set = info.replica_set.clone();
        self.sealed.write().unwrap().insert(bg_id, Arc::new(info));
        let mut by_worker = self.sealed_by_worker.write().unwrap();
        for worker_id in replica_set {
            by_worker.entry(worker_id).or_default().insert(bg_id);
        }
    }

    fn update_sealed(&self, old: &BlockGroupInfo, mut new: BlockGroupInfo) {
        Self::compact_sealed_runtime(&mut new);
        if old.replica_set != new.replica_set {
            let mut by_worker = self.sealed_by_worker.write().unwrap();
            BGIndex::update_worker_to_bgs_for_replica(
                new.bg_id,
                &old.replica_set,
                &new.replica_set,
                &mut by_worker,
            );
        }
        self.sealed
            .write()
            .unwrap()
            .insert(new.bg_id, Arc::new(new));
    }

    fn remove_sealed(&self, old: &BlockGroupInfo) {
        self.sealed.write().unwrap().remove(&old.bg_id);
        let mut by_worker = self.sealed_by_worker.write().unwrap();
        BGIndex::update_worker_to_bgs_for_replica(old.bg_id, &old.replica_set, &[], &mut by_worker);
    }

    fn list_sealed(&self) -> Vec<Arc<BlockGroupInfo>> {
        self.sealed.read().unwrap().values().cloned().collect()
    }

    fn snapshot_sealed(&self) -> HashMap<BgId, Arc<BlockGroupInfo>> {
        self.sealed.read().unwrap().clone()
    }
}

impl BGController for CapacityBGController {
    fn contains_bg(&self, bg_id: BgId) -> bool {
        self.active.contains_bg(bg_id) || self.sealed.read().unwrap().contains_key(&bg_id)
    }

    fn get_bg(&self, bg_id: BgId) -> Option<Arc<BlockGroupInfo>> {
        self.active
            .get_bg(bg_id)
            .or_else(|| self.sealed.read().unwrap().get(&bg_id).cloned())
    }

    fn list_bgs(&self, scope: BGListScope) -> Vec<Arc<BlockGroupInfo>> {
        match scope {
            BGListScope::Active => self.active.list_bgs(),
            BGListScope::Sealed => self.list_sealed(),
            BGListScope::All => {
                let mut bgs = self.active.list_bgs();
                bgs.extend(self.list_sealed());
                bgs
            }
        }
    }

    fn snapshot_bgs(&self, scope: BGListScope) -> HashMap<BgId, Arc<BlockGroupInfo>> {
        match scope {
            BGListScope::Active => self.active.snapshot_bgs(),
            BGListScope::Sealed => self.snapshot_sealed(),
            BGListScope::All => {
                let mut bgs = self.active.snapshot_bgs();
                bgs.extend(self.snapshot_sealed());
                bgs
            }
        }
    }

    fn restore_bgs(&self, bgs: HashMap<BgId, Arc<BlockGroupInfo>>) {
        let mut active = HashMap::new();
        let mut sealed = HashMap::new();
        let mut by_worker: HashMap<u32, HashSet<BgId>> = HashMap::new();
        for (bg_id, bg) in bgs {
            if Self::is_active_state(bg.state) {
                active.insert(bg_id, bg);
                continue;
            }
            let mut sealed_bg = (*bg).clone();
            Self::compact_sealed_runtime(&mut sealed_bg);
            for &worker_id in &sealed_bg.replica_set {
                by_worker.entry(worker_id).or_default().insert(bg_id);
            }
            sealed.insert(bg_id, Arc::new(sealed_bg));
        }
        self.active.restore_bgs(active);
        *self.sealed.write().unwrap() = sealed;
        *self.sealed_by_worker.write().unwrap() = by_worker;
    }

    fn insert_bg(&self, mut info: BlockGroupInfo) {
        if Self::is_active_state(info.state) {
            info.reset_runtime_replicas();
            self.active.insert_bg(info);
        } else {
            self.insert_sealed(info);
        }
    }

    fn update_bg(&self, old: &BlockGroupInfo, mut new: BlockGroupInfo) {
        let old_active = Self::is_active_state(old.state);
        let new_active = Self::is_active_state(new.state);
        match (old_active, new_active) {
            (true, true) => {
                new.sync_runtime_replicas_with_set();
                self.active.update_bg(old, new);
            }
            (true, false) => {
                self.active.remove_bg(old);
                self.insert_sealed(new);
            }
            (false, true) => {
                self.remove_sealed(old);
                new.reset_runtime_replicas();
                self.active.insert_bg(new);
            }
            (false, false) => self.update_sealed(old, new),
        }
    }

    fn remove_bg(&self, old: &BlockGroupInfo) {
        if Self::is_active_state(old.state) {
            self.active.remove_bg(old);
        } else {
            self.remove_sealed(old);
        }
    }

    fn bgs_on_worker(&self, worker_id: u32, scope: BGListScope) -> Vec<Arc<BlockGroupInfo>> {
        match scope {
            BGListScope::Active => self.active.bgs_on_worker(worker_id),
            BGListScope::Sealed => {
                let by_worker = self.sealed_by_worker.read().unwrap();
                let Some(bg_ids) = by_worker.get(&worker_id) else {
                    return Vec::new();
                };
                let sealed = self.sealed.read().unwrap();
                bg_ids
                    .iter()
                    .filter_map(|bg_id| sealed.get(bg_id).cloned())
                    .collect()
            }
            BGListScope::All => {
                let mut bgs = self.active.bgs_on_worker(worker_id);
                bgs.extend(self.bgs_on_worker(worker_id, BGListScope::Sealed));
                bgs
            }
        }
    }

    fn worker_primary_counts(&self, scope: BGListScope) -> HashMap<u32, u32> {
        match scope {
            BGListScope::Active => self.active.worker_primary_counts(),
            BGListScope::Sealed => {
                let mut counts = HashMap::new();
                for bg in self.sealed.read().unwrap().values() {
                    *counts.entry(bg.primary.node_id).or_default() += 1;
                }
                counts
            }
            BGListScope::All => {
                let mut counts = self.active.worker_primary_counts();
                for (worker_id, count) in self.worker_primary_counts(BGListScope::Sealed) {
                    *counts.entry(worker_id).or_default() += count;
                }
                counts
            }
        }
    }

    fn reset_replica_states(&self) {
        self.active.reset_replica_states();
    }

    fn set_op_state(&self, bg_id: BgId, op_state: BGOpState) {
        self.active.set_op_state(bg_id, op_state);
    }

    fn update_bg_stats(&self, bg_stats: &HashMap<BgId, BGStats>) {
        self.active.update_bg_stats(bg_stats);
    }

    fn get_replica_state(&self, bg_id: BgId, worker_id: u32) -> ReplicaState {
        self.active.get_replica_state(bg_id, worker_id)
    }

    fn set_replica_state(&self, bg_id: BgId, worker_id: u32, state: ReplicaState) {
        self.active.set_replica_state(bg_id, worker_id, state);
    }

    fn apply_replica_reports(&self, worker_id: u32, reports: &[WorkerBGReport]) -> usize {
        self.active.apply_replica_reports(worker_id, reports)
    }

    fn record_isr_penalty(&self, bg_id: BgId, worker_id: u32) {
        self.active.record_isr_penalty(bg_id, worker_id);
    }

    fn isr_penalty_active(&self, bg_id: BgId, worker_id: u32) -> bool {
        self.active.isr_penalty_active(bg_id, worker_id)
    }

    fn cleanup_isr_penalties(&self, old: &BlockGroupInfo, new: &BlockGroupInfo) {
        if Self::is_active_state(new.state) {
            self.active.cleanup_isr_penalties(old, new);
        }
    }

    fn serving_replicas(&self, bg_id: BgId) -> Vec<u32> {
        self.active.serving_replicas(bg_id)
    }

    fn resident_replicas(&self, bg_id: BgId) -> Vec<u32> {
        self.active.resident_replicas(bg_id)
    }
}
