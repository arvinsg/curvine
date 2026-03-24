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

use super::CoordinatorContext;
use crate::pd::node::{NodeEvent, NodeEventType};
use curvine_common::state::NodeType;
use dashmap::DashMap;
use std::sync::Arc;

/// Reason for BGTable rebuild
#[derive(Clone, Debug)]
pub enum RebuildReason {
    NodeJoined { node_ids: Vec<u32> },
    NodeRemoved { node_ids: Vec<u32> },
    Manual,
}

#[derive(Clone)]
pub struct RebuildTask {
    pub pool_id: u16,
    pub reason: RebuildReason,
    pub scheduled_time_ms: u64,
}

/// Unified BGTable scheduler: handles both initial table creation and
/// event-driven rebuild (with cooldown and reason merging).
pub struct BGTableScheduler {
    ctx: Arc<CoordinatorContext>,
    pending_rebuilds: DashMap<u16, RebuildTask>,
}

impl BGTableScheduler {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        Self {
            ctx,
            pending_rebuilds: DashMap::new(),
        }
    }

    pub fn schedule_rebuild(&self, pool_ids: Vec<u16>, reason: RebuildReason) {
        let auto_enabled = self
            .ctx
            .config_manager
            .get_bool("pd.bg.rebuild.auto_enabled", true);

        if !auto_enabled {
            log::info!(
                "Auto rebuild disabled, skipping rebuild for pools {:?}",
                pool_ids
            );
            return;
        }

        let cooldown = self
            .ctx
            .config_manager
            .get_u64("pd.bg.rebuild.cooldown_ms", 60_000);
        let scheduled_time = orpc::common::LocalTime::mills() + cooldown;

        for pool_id in pool_ids {
            self.pending_rebuilds
                .entry(pool_id)
                .and_modify(|task| {
                    task.reason = merge_reasons(&task.reason, &reason);
                    task.scheduled_time_ms = task.scheduled_time_ms.max(scheduled_time);
                })
                .or_insert(RebuildTask {
                    pool_id,
                    reason: reason.clone(),
                    scheduled_time_ms: scheduled_time,
                });
        }
    }

    pub async fn check_and_rebuild(&self) {
        let now = orpc::common::LocalTime::mills();

        // BGTable initialization: pools with workers but no table
        self.check_table_initialization();

        let ready: Vec<(u16, RebuildTask)> = self
            .pending_rebuilds
            .iter()
            .filter(|e| e.value().scheduled_time_ms <= now)
            .map(|e| (*e.key(), e.value().clone()))
            .collect();

        for (pool_id, task) in ready {
            self.pending_rebuilds.remove(&pool_id);
            if let Err(e) = self.execute_rebuild(pool_id, &task.reason) {
                log::error!("Rebuild BGTable for pool {} failed: {}", pool_id, e);
            }
        }
    }

    /// Check if any active pool lacks a BGTable and create one.
    pub fn check_table_initialization(&self) {
        let active_pools = self.ctx.pool_manager.list_active_pools();
        let bucket_count = self.ctx.bg_manager.bucket_count();
        let replica_counts = self.ctx.bg_manager.replica_counts().to_vec();

        for pool in active_pools {
            for &replica_count in &replica_counts {
                let table_id = ((pool.pool_id as u32) << 16) | (replica_count as u32);
                if self.ctx.bg_manager.get_table(table_id).is_some() {
                    continue;
                }
                let workers: Vec<u32> = pool
                    .workers
                    .iter()
                    .copied()
                    .filter(|w| self.ctx.pool_manager.is_worker_available(*w))
                    .collect();
                if workers.len() < replica_count as usize {
                    continue;
                }
                log::info!(
                    "Initializing BGTable for pool {} with {} buckets, {} replicas, {} workers",
                    pool.pool_id,
                    bucket_count,
                    replica_count,
                    workers.len()
                );
                if let Err(e) = self.ctx.bg_manager.create_table(
                    pool.pool_id,
                    bucket_count,
                    replica_count,
                    &workers,
                ) {
                    log::error!(
                        "Failed to create BGTable for pool {}: {}",
                        pool.pool_id,
                        e
                    );
                }
            }
        }
    }

    fn execute_rebuild(
        &self,
        pool_id: u16,
        reason: &RebuildReason,
    ) -> curvine_common::FsResult<()> {
        log::info!(
            "Rebuilding BGTable for pool {} (reason: {:?})",
            pool_id,
            reason
        );

        self.ctx.bg_manager.rebuild_tables_for_pool(pool_id)?;

        log::info!("BGTable for pool {} rebuild completed", pool_id);
        Ok(())
    }
}

impl super::checker::Scheduler for BGTableScheduler {
    fn name(&self) -> &str {
        "bg-table-scheduler"
    }

    fn on_event(&self, event: &NodeEvent) {
        match event.event_type {
            NodeEventType::Registered if event.node_type == NodeType::Worker => {
                let pool_ids = self.ctx.pool_manager.get_pools_by_worker(event.node_id);
                if !pool_ids.is_empty() {
                    self.schedule_rebuild(
                        pool_ids,
                        RebuildReason::NodeJoined {
                            node_ids: vec![event.node_id],
                        },
                    );
                }
            }
            NodeEventType::Offline if event.node_type == NodeType::Worker => {
                let pool_ids = self.ctx.pool_manager.get_pools_by_worker(event.node_id);
                if !pool_ids.is_empty() {
                    self.schedule_rebuild(
                        pool_ids,
                        RebuildReason::NodeRemoved {
                            node_ids: vec![event.node_id],
                        },
                    );
                }
            }
            NodeEventType::DecommissionFinished if event.node_type == NodeType::Worker => {
                let pool_ids = self.ctx.pool_manager.get_pools_by_worker(event.node_id);
                if !pool_ids.is_empty() {
                    self.schedule_rebuild(
                        pool_ids,
                        RebuildReason::NodeRemoved {
                            node_ids: vec![event.node_id],
                        },
                    );
                }
            }
            _ => {}
        }
    }

    fn tick(&self) {
        self.check_table_initialization();
    }
}

fn merge_reasons(existing: &RebuildReason, new: &RebuildReason) -> RebuildReason {
    match (existing, new) {
        (
            RebuildReason::NodeJoined { node_ids: ids1 },
            RebuildReason::NodeJoined { node_ids: ids2 },
        ) => {
            let mut merged = ids1.clone();
            merged.extend(ids2.iter().copied());
            RebuildReason::NodeJoined { node_ids: merged }
        }
        (
            RebuildReason::NodeRemoved { node_ids: ids1 },
            RebuildReason::NodeRemoved { node_ids: ids2 },
        ) => {
            let mut merged = ids1.clone();
            merged.extend(ids2.iter().copied());
            RebuildReason::NodeRemoved { node_ids: merged }
        }
        _ => new.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== merge_reasons tests ==========

    #[test]
    fn merge_same_type_joined() {
        let a = RebuildReason::NodeJoined {
            node_ids: vec![1, 2],
        };
        let b = RebuildReason::NodeJoined {
            node_ids: vec![3],
        };
        match merge_reasons(&a, &b) {
            RebuildReason::NodeJoined { node_ids } => {
                assert_eq!(node_ids, vec![1, 2, 3]);
            }
            _ => panic!("expected NodeJoined"),
        }
    }

    #[test]
    fn merge_same_type_removed() {
        let a = RebuildReason::NodeRemoved {
            node_ids: vec![10],
        };
        let b = RebuildReason::NodeRemoved {
            node_ids: vec![20, 30],
        };
        match merge_reasons(&a, &b) {
            RebuildReason::NodeRemoved { node_ids } => {
                assert_eq!(node_ids, vec![10, 20, 30]);
            }
            _ => panic!("expected NodeRemoved"),
        }
    }

    #[test]
    fn merge_different_types_uses_new() {
        let a = RebuildReason::NodeJoined {
            node_ids: vec![1],
        };
        let b = RebuildReason::NodeRemoved {
            node_ids: vec![2],
        };
        match merge_reasons(&a, &b) {
            RebuildReason::NodeRemoved { node_ids } => {
                assert_eq!(node_ids, vec![2]);
            }
            _ => panic!("expected NodeRemoved (new reason)"),
        }
    }

    #[test]
    fn merge_manual_overrides() {
        let a = RebuildReason::NodeJoined {
            node_ids: vec![1],
        };
        let b = RebuildReason::Manual;
        assert!(matches!(merge_reasons(&a, &b), RebuildReason::Manual));
    }

    // ========== BGTableScheduler schedule_rebuild tests ==========

    fn test_ctx(
        overrides: std::collections::HashMap<String, String>,
    ) -> Arc<CoordinatorContext> {
        let store: Arc<dyn crate::pd::store::KvStore> =
            Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new());
        let raft = curvine_common::raft::RaftClient::from_conf(
            curvine_common::conf::JournalConf::default().create_runtime(),
            &curvine_common::conf::JournalConf::default(),
        );
        let jc = Arc::new(crate::pd::journal::Client::new(raft));
        let config = Arc::new(crate::pd::config::ConfigManager::new(
            store.clone(),
            jc.clone(),
            overrides,
        ));
        let node_store = Arc::new(crate::pd::node::NodeStore::new(store.clone()));
        let node_mgr = Arc::new(crate::pd::node::NodeManager::new(
            node_store,
            config.clone(),
            jc.clone(),
        ));
        let pool_store = Arc::new(crate::pd::pool::PoolStore::new(store.clone()));
        let pool_mgr = Arc::new(crate::pd::pool::PoolManager::new(
            pool_store,
            node_mgr.clone(),
            jc.clone(),
        ));
        let bg_store = Arc::new(crate::pd::bg::BGStore::new(store));
        let bg_mgr = Arc::new(crate::pd::bg::BGManager::new(bg_store, pool_mgr.clone(), jc.clone(), 1024, vec![3], vec![]));
        Arc::new(CoordinatorContext {
            node_manager: node_mgr,
            pool_manager: pool_mgr,
            bg_manager: bg_mgr,
            config_manager: config,
            journal_client: jc,
            leader_checker: Arc::new(super::super::coordinator::AlwaysLeader),
        })
    }

    #[test]
    fn schedule_rebuild_creates_pending_task() {
        let ctx = test_ctx(std::collections::HashMap::new());
        let sched = BGTableScheduler::new(ctx);

        sched.schedule_rebuild(
            vec![1, 2],
            RebuildReason::NodeJoined {
                node_ids: vec![100],
            },
        );
        assert_eq!(sched.pending_rebuilds.len(), 2);
        assert!(sched.pending_rebuilds.contains_key(&1));
        assert!(sched.pending_rebuilds.contains_key(&2));
    }

    #[test]
    fn duplicate_schedule_merges_reasons() {
        let ctx = test_ctx(std::collections::HashMap::new());
        let sched = BGTableScheduler::new(ctx);

        sched.schedule_rebuild(
            vec![1],
            RebuildReason::NodeJoined {
                node_ids: vec![100],
            },
        );
        sched.schedule_rebuild(
            vec![1],
            RebuildReason::NodeJoined {
                node_ids: vec![200],
            },
        );

        // Should still be 1 pending task with merged reasons
        assert_eq!(sched.pending_rebuilds.len(), 1);
        let task = sched.pending_rebuilds.get(&1).unwrap();
        match &task.reason {
            RebuildReason::NodeJoined { node_ids } => {
                assert_eq!(node_ids, &vec![100, 200]);
            }
            _ => panic!("expected NodeJoined"),
        }
    }

    #[test]
    fn auto_enabled_false_skips_schedule() {
        let overrides: std::collections::HashMap<String, String> = [(
            "pd.bg.rebuild.auto_enabled".to_string(),
            "false".to_string(),
        )]
        .into_iter()
        .collect();
        let ctx = test_ctx(overrides);
        let sched = BGTableScheduler::new(ctx);

        sched.schedule_rebuild(vec![1], RebuildReason::Manual);
        assert_eq!(sched.pending_rebuilds.len(), 0);
    }

    #[test]
    fn cooldown_sets_future_scheduled_time() {
        let ctx = test_ctx(std::collections::HashMap::new());
        let sched = BGTableScheduler::new(ctx);

        let before = orpc::common::LocalTime::mills();
        sched.schedule_rebuild(
            vec![1],
            RebuildReason::NodeJoined {
                node_ids: vec![1],
            },
        );
        let task = sched.pending_rebuilds.get(&1).unwrap();
        // Default cooldown is 60_000ms
        assert!(task.scheduled_time_ms >= before + 60_000);
    }
}
