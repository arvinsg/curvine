use super::super::*;
use crate::pd::journal;
use crate::pd::journal::entry::{BGEntry, BGUpdateEntry};
use crate::pd::journal::ApplyOutcome;
use crate::pd::store::memory_kv_engine::MemoryKvEngine;
use crate::pd::store::KvStore;
use curvine_common::conf::JournalConf;
use curvine_common::raft::RaftClient;
use curvine_common::state::{BGKind, BGPrimary, BGState, BgId, BlockGroupInfo, ReplicaState};
use std::sync::Arc;

impl BGManager {
    pub fn test_reset_replica_statess(&self) {
        self.reset_replica_states();
    }
}

pub(super) fn test_manager() -> BGManager {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
    let bg_store = Arc::new(BGStore::new(store));
    let journal_conf = JournalConf::default();
    let rt = journal_conf.create_runtime();
    let raft = RaftClient::from_conf(rt, &journal_conf);
    let journal_client = Arc::new(journal::Client::new(raft));
    BGManager::new(bg_store, journal_client)
}

pub(super) fn make_bg(bg_id: BgId, table_id: u16, replica_set: Vec<u32>) -> BlockGroupInfo {
    BlockGroupInfo {
        bg_id,
        table_id,
        kind: BGKind::Hash,
        bg_epoch: 1,
        replica_set: replica_set.clone(),
        isr: replica_set.clone(),
        state: BGState::Active,
        op_state: Default::default(),
        primary: BGPrimary {
            node_id: replica_set.first().copied().unwrap_or_default(),
            epoch: 1,
            grant_time_ms: 0,
        },
        stats: Default::default(),
        replicas: Default::default(),
    }
}

pub(super) fn create_bg(mgr: &BGManager, bg: BlockGroupInfo) -> ApplyOutcome {
    mgr.apply_create_bg(&BGEntry { op_ms: 0, info: bg })
        .unwrap()
}

pub(super) fn update_entry(bg_id: BgId, expected: u64) -> BGUpdateEntry {
    BGUpdateEntry {
        op_ms: 1,
        kind: BGKind::Hash,
        bg_id,
        state: None,
        replica_set: None,
        isr: None,
        primary: None,
        expected_bg_epoch: expected,
        bump_table_epoch: false,
    }
}

pub(super) fn summarize_hash_bg_state(bg: &BlockGroupInfo) -> BGState {
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
