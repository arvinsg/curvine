use super::*;

const ISR_PENALTY_BASE_MS: u64 = 30_000;
const ISR_PENALTY_MAX_MS: u64 = 30 * 60 * 1000;

#[derive(Default)]
pub(crate) struct BGIndex {
    bgs: RwLock<HashMap<BgId, Arc<BlockGroupInfo>>>,
    worker_to_bgs: RwLock<HashMap<u32, HashSet<BgId>>>,
}

impl BGIndex {
    pub(crate) fn get_bg(&self, bg_id: BgId) -> Option<Arc<BlockGroupInfo>> {
        self.bgs.read().unwrap().get(&bg_id).cloned()
    }

    pub(crate) fn contains_bg(&self, bg_id: BgId) -> bool {
        self.bgs.read().unwrap().contains_key(&bg_id)
    }

    pub(crate) fn list_bgs(&self) -> Vec<Arc<BlockGroupInfo>> {
        self.bgs.read().unwrap().values().cloned().collect()
    }

    pub(crate) fn restore_bgs(&self, bgs: HashMap<BgId, Arc<BlockGroupInfo>>) {
        *self.bgs.write().unwrap() = bgs;
        self.rebuild_worker_to_bgs();
    }

    pub(crate) fn bgs_on_worker(&self, worker_id: u32) -> Vec<Arc<BlockGroupInfo>> {
        let w2b = self.worker_to_bgs.read().unwrap();
        let Some(bg_ids) = w2b.get(&worker_id) else {
            return Vec::new();
        };
        let bgs = self.bgs.read().unwrap();
        bg_ids
            .iter()
            .filter_map(|bg_id| bgs.get(bg_id).cloned())
            .collect()
    }

    pub(crate) fn insert_bg(&self, info: BlockGroupInfo) {
        let bg_id = info.bg_id;
        let replica_set = info.replica_set.clone();
        self.bgs.write().unwrap().insert(bg_id, Arc::new(info));
        let mut w2b = self.worker_to_bgs.write().unwrap();
        for worker_id in replica_set {
            w2b.entry(worker_id).or_default().insert(bg_id);
        }
    }

    pub(crate) fn update_bg(&self, old: &BlockGroupInfo, new: BlockGroupInfo) {
        if old.replica_set != new.replica_set {
            let mut w2b = self.worker_to_bgs.write().unwrap();
            Self::update_worker_to_bgs_for_replica(
                new.bg_id,
                &old.replica_set,
                &new.replica_set,
                &mut w2b,
            );
        }
        self.bgs.write().unwrap().insert(new.bg_id, Arc::new(new));
    }

    pub(crate) fn remove_bg(&self, old: &BlockGroupInfo) {
        self.bgs.write().unwrap().remove(&old.bg_id);
        let mut w2b = self.worker_to_bgs.write().unwrap();
        Self::update_worker_to_bgs_for_replica(old.bg_id, &old.replica_set, &[], &mut w2b);
    }

    pub(crate) fn reset_replica_states(&self) {
        let mut bgs = self.bgs.write().unwrap();
        for bg in bgs.values_mut() {
            Arc::make_mut(bg).reset_runtime_replicas();
        }
    }

    pub(crate) fn set_op_state(&self, bg_id: BgId, op_state: BGOpState) {
        let mut bgs = self.bgs.write().unwrap();
        if let Some(bg) = bgs.get_mut(&bg_id) {
            Arc::make_mut(bg).op_state = op_state;
        }
    }

    pub(crate) fn update_bg_stats(&self, bg_stats: &HashMap<BgId, BGStats>) {
        let now = orpc::common::LocalTime::mills();
        let mut bgs = self.bgs.write().unwrap();
        for (bg_id, stats) in bg_stats {
            if let Some(bg) = bgs.get_mut(bg_id) {
                let bg_mut = Arc::make_mut(bg);
                bg_mut.stats = stats.clone();
                bg_mut.stats.last_report_ms = now;
            }
        }
    }

    pub(crate) fn get_replica_state(&self, bg_id: BgId, worker_id: u32) -> ReplicaState {
        self.bgs
            .read()
            .unwrap()
            .get(&bg_id)
            .map(|bg| bg.replica_state(worker_id))
            .unwrap_or(ReplicaState::Pending)
    }

    pub(crate) fn set_replica_state(&self, bg_id: BgId, worker_id: u32, state: ReplicaState) {
        let mut bgs = self.bgs.write().unwrap();
        let Some(bg) = bgs.get_mut(&bg_id) else {
            return;
        };
        Arc::make_mut(bg).set_replica_state(worker_id, state, orpc::common::LocalTime::mills());
    }

    pub(crate) fn apply_replica_reports(
        &self,
        worker_id: u32,
        reports: &[WorkerBGReport],
    ) -> usize {
        let now = orpc::common::LocalTime::mills();
        let mut changed = 0usize;
        let mut bgs = self.bgs.write().unwrap();
        for report in reports {
            let Some(bg) = bgs.get_mut(&report.bg_id) else {
                log::warn!(
                    "worker {} reported unknown bg_id={}; skip replica report",
                    worker_id,
                    report.bg_id
                );
                continue;
            };
            if !bg.replica_set.contains(&worker_id) {
                log::warn!(
                    "worker {} reported bg_id={} but is not in replica_set; skip replica report",
                    worker_id,
                    report.bg_id
                );
                continue;
            }
            if Arc::make_mut(bg).set_replica_state(worker_id, report.state, now) {
                changed += 1;
            }
        }
        changed
    }

    pub(crate) fn record_isr_failure(&self, bg_id: BgId, worker_id: u32) {
        let now = orpc::common::LocalTime::mills();
        let mut bgs = self.bgs.write().unwrap();
        let Some(bg) = bgs.get_mut(&bg_id) else {
            return;
        };
        let bg = Arc::make_mut(bg);
        let failures = bg.isr_failure_count(worker_id).saturating_add(1);
        let shift = failures.saturating_sub(1).min(16);
        let delay = ISR_PENALTY_BASE_MS
            .saturating_mul(1u64 << shift)
            .min(ISR_PENALTY_MAX_MS);
        bg.record_isr_failure(worker_id, now, delay);
    }

    pub(crate) fn is_isr_rejoin_blocked(&self, bg_id: BgId, worker_id: u32) -> bool {
        let now = orpc::common::LocalTime::mills();
        self.bgs
            .read()
            .unwrap()
            .get(&bg_id)
            .map(|bg| bg.is_isr_rejoin_blocked(worker_id, now))
            .unwrap_or(false)
    }

    pub(crate) fn cleanup_isr_penalties(&self, old: &BlockGroupInfo, new: &BlockGroupInfo) {
        debug_assert_eq!(old.bg_id, new.bg_id);

        let old_isr: HashSet<u32> = old.isr.iter().copied().collect();
        let new_isr: HashSet<u32> = new.isr.iter().copied().collect();
        let old_replica_set: HashSet<u32> = old.replica_set.iter().copied().collect();
        let new_replica_set: HashSet<u32> = new.replica_set.iter().copied().collect();

        let rejoined_isr = new_isr.difference(&old_isr).copied();
        let removed_replicas = old_replica_set.difference(&new_replica_set).copied();
        let workers: Vec<u32> = rejoined_isr.chain(removed_replicas).collect();
        if workers.is_empty() {
            return;
        }

        let mut bgs = self.bgs.write().unwrap();
        let Some(bg) = bgs.get_mut(&new.bg_id) else {
            return;
        };
        let bg = Arc::make_mut(bg);
        for worker_id in workers {
            bg.clear_isr_penalty(worker_id);
        }
    }

    fn rebuild_worker_to_bgs(&self) {
        let bgs = self.bgs.read().unwrap();
        let mut worker_to_bgs: HashMap<u32, HashSet<BgId>> = HashMap::new();
        for bg in bgs.values() {
            for &worker_id in &bg.replica_set {
                worker_to_bgs.entry(worker_id).or_default().insert(bg.bg_id);
            }
        }
        *self.worker_to_bgs.write().unwrap() = worker_to_bgs;
    }

    pub(crate) fn update_worker_to_bgs_for_replica(
        bg_id: BgId,
        old_replica_set: &[u32],
        new_replica_set: &[u32],
        w2b: &mut HashMap<u32, HashSet<BgId>>,
    ) {
        let old_set: HashSet<u32> = old_replica_set.iter().copied().collect();
        let new_set: HashSet<u32> = new_replica_set.iter().copied().collect();
        for &removed in old_set.difference(&new_set) {
            if let Some(set) = w2b.get_mut(&removed) {
                set.remove(&bg_id);
                if set.is_empty() {
                    w2b.remove(&removed);
                }
            }
        }
        for &added in new_set.difference(&old_set) {
            w2b.entry(added).or_default().insert(bg_id);
        }
    }
}
