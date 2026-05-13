// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::manager::PoolManager;
use super::store::PoolStore;
use crate::pd::config::ConfigManager;
use crate::pd::journal;
use crate::pd::node::NodeManager;
use crate::pd::node::NodeStore;
use crate::pd::store::memory_kv_engine::MemoryKvEngine;
use crate::pd::store::KvStore;
use curvine_common::conf::JournalConf;
use curvine_common::raft::RaftClient;
use std::collections::HashMap;
use std::sync::Arc;

fn make_journal_client() -> Arc<journal::Client> {
    let journal_conf = JournalConf::default();
    let rt = journal_conf.create_runtime();
    let raft = RaftClient::from_conf(rt, &journal_conf);
    Arc::new(journal::Client::new(raft))
}

fn test_pool_manager() -> PoolManager {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
    let jc = make_journal_client();
    let config_manager = Arc::new(ConfigManager::new(
        store.clone(),
        jc.clone(),
        HashMap::new(),
    ));
    let node_store = Arc::new(NodeStore::new(store.clone()));
    let node_manager = Arc::new(NodeManager::new(node_store, config_manager, jc.clone()));
    let pool_store = Arc::new(PoolStore::new(store));
    let mgr = PoolManager::new(pool_store, node_manager, jc);
    // Seed default pools via apply (simulating Raft-committed entries).
    seed_default_pools(&mgr);
    mgr
}

/// Seed default pools via test_install_pool (bypasses Raft, manages epochs).
fn seed_default_pools(mgr: &PoolManager) {
    use curvine_common::state::{PoolInfo, StorageType};
    let defaults = [
        (super::POOL_ID_MEM, "mem_pool", StorageType::Mem),
        (super::POOL_ID_SSD, "ssd_pool", StorageType::Ssd),
        (super::POOL_ID_HDD, "hdd_pool", StorageType::Hdd),
    ];
    for (pool_id, name, media) in defaults {
        let info = PoolInfo::new(pool_id, name.to_string(), media);
        mgr.test_install_pool(info).unwrap();
    }
}

#[test]
fn restore_loads_seeded_pools() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    // Default pools were seeded via apply in test_pool_manager()
    assert!(mgr
        .get_pool_by_media(curvine_common::state::StorageType::Mem)
        .is_ok());
    assert!(mgr
        .get_pool_by_media(curvine_common::state::StorageType::Ssd)
        .is_ok());
    assert!(mgr
        .get_pool_by_media(curvine_common::state::StorageType::Hdd)
        .is_ok());
    assert!(mgr.list_active_pools().is_empty());
}

#[test]
fn get_pool_by_media_after_restore() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    let ssd = mgr
        .get_pool_by_media(curvine_common::state::StorageType::Ssd)
        .unwrap();
    assert_eq!(ssd.name, "ssd_pool");
    assert_eq!(ssd.pool_id, super::POOL_ID_SSD);
}

#[test]
fn get_pool_returns_error_for_unknown_id() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    let r = mgr.get_pool(99);
    assert!(r.is_err());
}

#[test]
fn assign_worker_to_pools_with_ssd_spec_adds_to_ssd_pool() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();

    // Simulate assign: build updated pool and apply via Raft callback
    let pool = mgr.get_pool(super::POOL_ID_SSD).unwrap();
    let mut updated = pool.clone();
    updated.workers.insert(100);
    mgr.test_install_pool(updated).unwrap();

    let pool = mgr.get_pool(super::POOL_ID_SSD).unwrap();
    assert!(pool.workers.contains(&100));
    let active = mgr.list_active_pools();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].pool_id, super::POOL_ID_SSD);
}

#[test]
fn assign_worker_to_pools_empty_specs_returns_empty() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    let specs = std::collections::HashMap::new();
    let pool_ids = mgr.assign_worker_to_pools(200, &specs).unwrap();
    assert!(pool_ids.is_empty());
}

#[test]
fn remove_worker_from_pools() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();

    // Simulate assign via apply
    let pool = mgr.get_pool(super::POOL_ID_SSD).unwrap();
    let mut updated = pool.clone();
    updated.workers.insert(300);
    mgr.test_install_pool(updated).unwrap();

    let pool = mgr.get_pool(super::POOL_ID_SSD).unwrap();
    assert!(pool.workers.contains(&300));

    // Simulate remove via apply
    let mut updated = pool.clone();
    updated.workers.remove(&300);
    mgr.test_install_pool(updated).unwrap();

    let pool = mgr.get_pool(super::POOL_ID_SSD).unwrap();
    assert!(!pool.workers.contains(&300));
}

/// Restore rebuilds worker_to_pools from store (only pool info persisted).
#[test]
fn restore_rebuilds_worker_to_pools_from_store_only() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
    let jc = make_journal_client();
    let config_manager = Arc::new(ConfigManager::new(
        store.clone(),
        jc.clone(),
        HashMap::new(),
    ));
    let node_store = Arc::new(NodeStore::new(store.clone()));
    let node_manager = Arc::new(NodeManager::new(node_store, config_manager, jc.clone()));
    let pool_store1 = Arc::new(PoolStore::new(store.clone()));
    let mgr1 = PoolManager::new(pool_store1, node_manager.clone(), jc.clone());
    seed_default_pools(&mgr1);
    mgr1.restore().unwrap();

    // Simulate assign via apply (persists to store)
    let pool = mgr1.get_pool(super::POOL_ID_SSD).unwrap();
    let mut updated = pool.clone();
    updated.workers.insert(100);
    mgr1.test_install_pool(updated).unwrap();

    // Create fresh manager with same backing store and restore
    let pool_store2 = Arc::new(PoolStore::new(store));
    let mgr2 = PoolManager::new(pool_store2, node_manager, jc);
    mgr2.restore().unwrap();
    let pool = mgr2.get_pool(super::POOL_ID_SSD).unwrap();
    assert!(pool.workers.contains(&100));
}

#[test]
fn get_worker_node_returns_none_when_not_worker() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    assert!(mgr.get_worker_node(1).is_none());
}

#[test]
fn update_pool_stats() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    mgr.update_pool_stats(
        super::POOL_ID_SSD,
        curvine_common::state::PoolStats {
            capacity_bytes: 2000,
            available_bytes: 1000,
            used_bytes: 1000,
            block_count: 0,
        },
    )
    .unwrap();
    let pool = mgr.get_pool(super::POOL_ID_SSD).unwrap();
    assert_eq!(pool.stats.capacity_bytes, 2000);
    assert_eq!(pool.stats.available_bytes, 1000);
}

fn worker_node_with_storage(
    id: u32,
    state: curvine_common::state::NodeState,
    storage_type: curvine_common::state::StorageType,
) -> curvine_common::state::NodeInfo {
    use curvine_common::state::{
        NodeAddress, NodeBase, NodeInfo, NodePayload, NodeType, StorageSpec, WorkerNodePayload,
    };
    let mut payload = WorkerNodePayload::default();
    payload.storage_specs.insert(
        "s0".to_string(),
        StorageSpec {
            dir_id: 0,
            storage_id: "s0".to_string(),
            failed: false,
            storage_type,
            dir_path: "/tmp/s0".to_string(),
        },
    );
    NodeInfo {
        base: NodeBase {
            node_id: id,
            node_type: NodeType::Worker,
            address: NodeAddress {
                hostname: format!("w-{}", id),
                ip: format!("10.0.0.{}", id),
                rpc_port: 8000 + id as u16,
                web_port: 9000 + id as u16,
            },
            ..Default::default()
        },
        state,
        epoch: 1,
        payload: NodePayload::Worker(payload),
        ..Default::default()
    }
}

#[test]
fn reconcile_worker_pool_membership_plan_add_only() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    let worker = worker_node_with_storage(
        100,
        curvine_common::state::NodeState::Live,
        curvine_common::state::StorageType::Ssd,
    );

    let (updates, changed_pool_ids) = mgr.test_worker_pool_reconcile_updates(&[worker]);

    assert_eq!(changed_pool_ids, vec![super::POOL_ID_SSD]);
    let ssd = updates
        .iter()
        .find(|p| p.pool_id == super::POOL_ID_SSD)
        .expect("ssd pool update");
    assert_eq!(ssd.workers, std::collections::HashSet::from([100]));
}

#[test]
fn reconcile_worker_pool_membership_plan_remove_only() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();
    let mut pool = mgr.get_pool(super::POOL_ID_SSD).unwrap();
    pool.workers.insert(100);
    mgr.test_install_pool(pool).unwrap();

    let (updates, changed_pool_ids) = mgr.test_worker_pool_reconcile_updates(&[]);

    assert_eq!(changed_pool_ids, vec![super::POOL_ID_SSD]);
    let ssd = updates
        .iter()
        .find(|p| p.pool_id == super::POOL_ID_SSD)
        .expect("ssd pool update");
    assert!(ssd.workers.is_empty());
}

#[test]
fn reconcile_worker_pool_membership_noop_calls_public_method_without_propose() {
    let mgr = test_pool_manager();
    mgr.restore().unwrap();

    let result = mgr.reconcile_worker_pool_membership(&[]).unwrap();

    assert!(result.changed_pool_ids.is_empty());
}

// =============================================================================
// REGRESSION tests (originally P0.4 baselines, updated for P1.1 CAS).
//
// After P1.1 (expected_epoch CAS in apply_save_pool returning ApplyOutcome),
// SavePool entries built from a stale snapshot are rejected with SkippedStale.
// These tests verify the new contract.
// =============================================================================

/// REGRESSION: full-overwrite SavePool no longer drops concurrent worker
/// updates — the second entry's expected_epoch fails CAS and SkippedStale
/// is returned. Path A's mutation survives.
///
/// Pre-P1.1 (now removed): both writes Applied, last-writer-wins lost W1+W2.
#[test]
fn save_pool_concurrent_writes_second_returns_stale() {
    use crate::pd::journal::ApplyOutcome;
    use curvine_common::state::PoolInfo;

    let mgr = test_pool_manager();

    // Snapshot at epoch=0 read by both paths.
    let snapshot = mgr.get_pool(super::POOL_ID_SSD).unwrap();
    assert_eq!(snapshot.epoch, 0);
    assert!(snapshot.workers.is_empty());

    // Path A: build entry adding {W1, W2} with bumped epoch.
    let mut info_a: PoolInfo = snapshot.clone();
    info_a.workers.insert(1);
    info_a.workers.insert(2);
    info_a.epoch = 1; // proposer bumps from 0 to 1
    let entry_a = crate::pd::journal::entry::PoolEntry {
        op_ms: 1,
        info: info_a,
        expected_epoch: 0, // proposer's snapshot was epoch=0
    };

    // Path B: build entry adding {W3} with bumped epoch (also based on 0).
    let mut info_b: PoolInfo = snapshot.clone();
    info_b.workers.insert(3);
    info_b.epoch = 1;
    let entry_b = crate::pd::journal::entry::PoolEntry {
        op_ms: 2,
        info: info_b,
        expected_epoch: 0,
    };

    // Apply A first → Applied (epoch advances 0 → 1).
    let outcome_a = mgr.apply_save_pool(&entry_a).unwrap();
    assert_eq!(outcome_a, ApplyOutcome::Applied);

    // Apply B → SkippedStale: existing.epoch is now 1, B's expected_epoch is 0.
    let outcome_b = mgr.apply_save_pool(&entry_b).unwrap();
    assert!(
        matches!(outcome_b, ApplyOutcome::SkippedStale { .. }),
        "expected SkippedStale, got {:?}",
        outcome_b
    );

    let final_pool = mgr.get_pool(super::POOL_ID_SSD).unwrap();
    // Path A's mutation survives; W3 from B is rejected.
    assert_eq!(
        final_pool.workers,
        std::collections::HashSet::from([1, 2]),
        "expected {{1,2}} (A applied, B SkippedStale)"
    );
    assert_eq!(final_pool.epoch, 1);
}

/// REGRESSION: non-monotonic info.epoch is rejected (defense-in-depth against
/// malformed entries that pass the expected_epoch check but try to overwrite
/// with the same or older info.epoch).
#[test]
fn save_pool_rejects_non_monotonic_info_epoch() {
    use crate::pd::journal::ApplyOutcome;

    let mgr = test_pool_manager();
    let snapshot = mgr.get_pool(super::POOL_ID_SSD).unwrap();
    assert_eq!(snapshot.epoch, 0);

    // expected_epoch matches but info.epoch == existing.epoch (not +1).
    let mut info = snapshot.clone();
    info.workers.insert(99);
    info.epoch = 0; // BUG: should be 1
    let entry = crate::pd::journal::entry::PoolEntry {
        op_ms: 1,
        info,
        expected_epoch: 0,
    };
    let outcome = mgr.apply_save_pool(&entry).unwrap();
    assert!(
        matches!(outcome, ApplyOutcome::SkippedStale { .. }),
        "expected SkippedStale for non-monotonic, got {:?}",
        outcome
    );

    // State unchanged.
    let pool = mgr.get_pool(super::POOL_ID_SSD).unwrap();
    assert_eq!(pool.workers, std::collections::HashSet::new());
    assert_eq!(pool.epoch, 0);
}
