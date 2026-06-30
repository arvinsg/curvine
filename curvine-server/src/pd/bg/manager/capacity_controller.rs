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

    fn filter_state(
        bgs: impl IntoIterator<Item = Arc<BlockGroupInfo>>,
        state: Option<BGState>,
    ) -> Vec<Arc<BlockGroupInfo>> {
        bgs.into_iter()
            .filter(|bg| state.is_none_or(|state| bg.state == state))
            .collect()
    }

    fn insert_sealed(&self, info: BlockGroupInfo) {
        let bg_id = info.bg_id;
        let replica_set = info.replica_set.clone();
        self.sealed.write().unwrap().insert(bg_id, Arc::new(info));
        let mut by_worker = self.sealed_by_worker.write().unwrap();
        for worker_id in replica_set {
            by_worker.entry(worker_id).or_default().insert(bg_id);
        }
    }

    fn update_sealed(&self, old: &BlockGroupInfo, new: BlockGroupInfo) {
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

    fn reset_sealed_replica_states(&self) {
        let mut sealed = self.sealed.write().unwrap();
        for bg in sealed.values_mut() {
            Arc::make_mut(bg).reset_replicas();
        }
    }

    fn set_sealed_op_state(&self, bg_id: BgId, op_state: BGOpState) {
        let mut sealed = self.sealed.write().unwrap();
        if let Some(bg) = sealed.get_mut(&bg_id) {
            Arc::make_mut(bg).op_state = op_state;
        }
    }

    fn get_sealed_replica_state(&self, bg_id: BgId, worker_id: u32) -> ReplicaState {
        self.sealed
            .read()
            .unwrap()
            .get(&bg_id)
            .map(|bg| bg.replica_state(worker_id))
            .unwrap_or(ReplicaState::Pending)
    }

    fn set_sealed_replica_state(&self, bg_id: BgId, worker_id: u32, state: ReplicaState) {
        let mut sealed = self.sealed.write().unwrap();
        let Some(bg) = sealed.get_mut(&bg_id) else {
            return;
        };
        Arc::make_mut(bg).set_replica_state(worker_id, state, orpc::common::LocalTime::mills());
    }

    fn apply_sealed_replica_report(&self, worker_id: u32, report: &WorkerBGReport) -> usize {
        let now = orpc::common::LocalTime::mills();
        let mut sealed = self.sealed.write().unwrap();
        let Some(bg) = sealed.get_mut(&report.bg_id) else {
            return 0;
        };
        if !bg.replica_set.contains(&worker_id) {
            log::warn!(
                "worker {} reported sealed capacity bg_id={} but is not in replica_set; skip replica report",
                worker_id,
                report.bg_id
            );
            return 0;
        }
        usize::from(Arc::make_mut(bg).set_replica_state(worker_id, report.state, now))
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

    fn list_bgs(&self, state: Option<BGState>) -> Vec<Arc<BlockGroupInfo>> {
        let mut bgs = self.active.list_bgs();
        bgs.extend(self.list_sealed());
        Self::filter_state(bgs, state)
    }

    fn snapshot_bgs(&self, state: Option<BGState>) -> HashMap<BgId, Arc<BlockGroupInfo>> {
        self.list_bgs(state)
            .into_iter()
            .map(|bg| (bg.bg_id, bg))
            .collect()
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
            sealed_bg.sync_replicas_with_replica_set();
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
            info.reset_replicas();
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
                new.sync_replicas_with_replica_set();
                self.active.update_bg(old, new);
            }
            (true, false) => {
                self.active.remove_bg(old);
                self.insert_sealed(new);
            }
            (false, true) => {
                self.remove_sealed(old);
                new.sync_replicas_with_replica_set();
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

    fn bgs_on_worker(&self, worker_id: u32, state: Option<BGState>) -> Vec<Arc<BlockGroupInfo>> {
        let mut bgs = self.active.bgs_on_worker(worker_id);
        let by_worker = self.sealed_by_worker.read().unwrap();
        if let Some(bg_ids) = by_worker.get(&worker_id) {
            let sealed = self.sealed.read().unwrap();
            bgs.extend(bg_ids.iter().filter_map(|bg_id| sealed.get(bg_id).cloned()));
        }
        Self::filter_state(bgs, state)
    }

    fn worker_primary_counts(&self, state: Option<BGState>) -> HashMap<u32, u32> {
        let mut counts = HashMap::new();
        for bg in self.list_bgs(state) {
            *counts.entry(bg.primary.node_id).or_default() += 1;
        }
        counts
    }

    fn reset_replica_states(&self) {
        self.active.reset_replica_states();
        self.reset_sealed_replica_states();
    }

    fn set_op_state(&self, bg_id: BgId, op_state: BGOpState) {
        self.active.set_op_state(bg_id, op_state);
        self.set_sealed_op_state(bg_id, op_state);
    }

    fn update_bg_stats(&self, bg_stats: &HashMap<BgId, BGStats>) {
        self.active.update_bg_stats(bg_stats);
    }

    fn get_replica_state(&self, bg_id: BgId, worker_id: u32) -> ReplicaState {
        if self.active.contains_bg(bg_id) {
            self.active.get_replica_state(bg_id, worker_id)
        } else {
            self.get_sealed_replica_state(bg_id, worker_id)
        }
    }

    fn set_replica_state(&self, bg_id: BgId, worker_id: u32, state: ReplicaState) {
        if self.active.contains_bg(bg_id) {
            self.active.set_replica_state(bg_id, worker_id, state);
        } else {
            self.set_sealed_replica_state(bg_id, worker_id, state);
        }
    }

    fn apply_replica_reports(&self, worker_id: u32, reports: &[WorkerBGReport]) -> usize {
        let mut changed = 0usize;
        for report in reports {
            if self.active.contains_bg(report.bg_id) {
                changed += self
                    .active
                    .apply_replica_reports(worker_id, std::slice::from_ref(report));
            } else {
                changed += self.apply_sealed_replica_report(worker_id, report);
            }
        }
        changed
    }
}
