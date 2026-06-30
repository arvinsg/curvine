use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BGListScope {
    Active,
    Sealed,
    All,
}

pub(crate) trait BGController: Send + Sync {
    // Basic bg ops.
    fn contains_bg(&self, bg_id: BgId) -> bool;
    fn get_bg(&self, bg_id: BgId) -> Option<Arc<BlockGroupInfo>>;
    fn list_bgs(&self, scope: BGListScope) -> Vec<Arc<BlockGroupInfo>>;
    fn snapshot_bgs(&self, scope: BGListScope) -> HashMap<BgId, Arc<BlockGroupInfo>>;
    fn bgs_on_worker(&self, worker_id: u32, scope: BGListScope) -> Vec<Arc<BlockGroupInfo>>;
    fn worker_primary_counts(&self, scope: BGListScope) -> HashMap<u32, u32>;

    // Runtime index restore.
    fn restore_bgs(&self, bgs: HashMap<BgId, Arc<BlockGroupInfo>>);
    fn insert_bg(&self, info: BlockGroupInfo);
    fn update_bg(&self, old: &BlockGroupInfo, new: BlockGroupInfo);
    fn remove_bg(&self, old: &BlockGroupInfo);

    // Volatile state updates.
    fn reset_replica_states(&self);
    fn set_op_state(&self, bg_id: BgId, op_state: BGOpState);
    fn update_bg_stats(&self, bg_stats: &HashMap<BgId, BGStats>);
    fn get_replica_state(&self, bg_id: BgId, worker_id: u32) -> ReplicaState;
    fn set_replica_state(&self, bg_id: BgId, worker_id: u32, state: ReplicaState);
    fn apply_replica_reports(&self, worker_id: u32, reports: &[WorkerBGReport]) -> usize;

    // Replica visibility helpers.
    fn serving_replicas(&self, bg_id: BgId) -> Vec<u32>;
    fn resident_replicas(&self, bg_id: BgId) -> Vec<u32>;
}
