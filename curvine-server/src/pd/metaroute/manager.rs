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

use crate::pd::journal::{self, ApplyOutcome, PathRouteAddEntry, PathRouteRemoveEntry, PdEntry};
use crate::pd::metaroute::MetaRouteStore;
use crate::pd::node::NodeManager;
use curvine_common::state::*;
use curvine_common::{FsError, FsResult};
use orpc::common::LocalTime;
use std::sync::{Arc, RwLock};

/// Manages MetaNode federation metadata. PD publishes route tables and group membership.
pub struct MetaRouteManager {
    mode: MetaNodeMode,
    federation_route_config: FederationRouteConfig,
    path_route_table: RwLock<PathRouteTable>,

    node_manager: Arc<NodeManager>,
    store: Arc<MetaRouteStore>,
    journal_client: Arc<journal::Client>,
}

impl MetaRouteManager {
    pub fn new(
        mode: MetaNodeMode,
        federation_route_config: FederationRouteConfig,
        node_manager: Arc<NodeManager>,
        store: Arc<MetaRouteStore>,
        journal_client: Arc<journal::Client>,
    ) -> Self {
        Self {
            mode,
            federation_route_config,
            path_route_table: RwLock::new(PathRouteTable::default()),
            node_manager,
            store,
            journal_client,
        }
    }

    pub fn mode(&self) -> MetaNodeMode {
        self.mode
    }

    /// Load path route table and version from store.
    pub fn restore(&self) -> FsResult<()> {
        if self.mode != MetaNodeMode::Federation {
            return Ok(());
        }
        let version = self.store.get_path_route_version()?;
        let routes = self.store.list_path_routes()?;
        let now = LocalTime::mills();
        let mut table = self.path_route_table.write().unwrap();
        table.version = version;
        table.routes = routes;
        table.last_update_ms = now;
        Ok(())
    }

    pub fn apply_add_route(&self, entry: &PathRouteAddEntry) -> FsResult<ApplyOutcome> {
        if self.mode != MetaNodeMode::Federation {
            return Ok(ApplyOutcome::Applied);
        }

        let mut table = self.path_route_table.write().unwrap();
        if let Some(outcome) = Self::validate_table_version(
            "AddPathRoute",
            &entry.route.path,
            table.version,
            entry.expected_table_version,
        ) {
            return Ok(outcome);
        }

        let next_version = table.version + 1;
        self.store.apply_add_route(&entry.route, next_version)?;

        let was_replace = table.routes.iter().any(|r| r.path == entry.route.path);
        if let Some(existing) = table.routes.iter_mut().find(|r| r.path == entry.route.path) {
            *existing = entry.route.clone();
        } else {
            table.routes.push(entry.route.clone());
        }
        table.version = next_version;
        table.last_update_ms = entry.route.update_time_ms;
        log::info!(
            "Apply AddPathRoute path={}, group_id={}, mode={}, table_version={}",
            entry.route.path,
            entry.route.group_id,
            if was_replace { "replace" } else { "insert" },
            table.version
        );
        Ok(ApplyOutcome::Applied)
    }

    pub fn apply_remove_route(&self, entry: &PathRouteRemoveEntry) -> FsResult<ApplyOutcome> {
        if self.mode != MetaNodeMode::Federation {
            return Ok(ApplyOutcome::Applied);
        }

        let mut table = self.path_route_table.write().unwrap();
        if let Some(outcome) = Self::validate_table_version(
            "RemovePathRoute",
            &entry.path,
            table.version,
            entry.expected_table_version,
        ) {
            return Ok(outcome);
        }

        let Some(index) = table.routes.iter().position(|r| r.path == entry.path) else {
            log::warn!(
                "Apply RemovePathRoute skipped: path={} not present in route table",
                entry.path
            );
            return Ok(ApplyOutcome::not_found(format!(
                "path {} not present",
                entry.path
            )));
        };

        let next_version = table.version + 1;
        self.store.apply_remove_route(&entry.path, next_version)?;

        table.routes.remove(index);
        table.version = next_version;
        table.last_update_ms = entry.op_ms;
        log::info!(
            "Apply RemovePathRoute path={}, table_version={}",
            entry.path,
            table.version
        );
        Ok(ApplyOutcome::Applied)
    }

    fn validate_table_version(
        op: &str,
        path: &str,
        current_version: u64,
        expected_version: u64,
    ) -> Option<ApplyOutcome> {
        if expected_version != current_version {
            log::warn!(
                "Apply {} skipped: path={} stale, current_table_version={}, entry_expected_table_version={}",
                op,
                path,
                current_version,
                expected_version
            );
            return Some(ApplyOutcome::stale(format!(
                "table_version mismatch: current={}, expected={}",
                current_version, expected_version
            )));
        }

        None
    }

    // ========== Route management (propose via Raft) ==========

    pub fn add_route(&self, mut route: PathRouteEntry) -> FsResult<()> {
        if self.mode != MetaNodeMode::Federation {
            return Err(FsError::common(
                "add_route only in Federation mode".to_string(),
            ));
        }
        let now = LocalTime::mills();
        if route.create_time_ms == 0 {
            route.create_time_ms = now;
        }
        route.update_time_ms = now;
        let expected_table_version = self.path_route_table.read().unwrap().version;
        let path = route.path.clone();
        let outcome = self
            .journal_client
            .propose(PdEntry::AddPathRoute(PathRouteAddEntry {
                op_ms: now,
                route,
                expected_table_version,
            }))?;
        Self::route_outcome_to_result(outcome, "add_path_route", path)
    }

    pub fn remove_route(&self, path: &str) -> FsResult<()> {
        if self.mode != MetaNodeMode::Federation {
            return Err(FsError::common(
                "remove_route only in Federation mode".to_string(),
            ));
        }
        let expected_table_version = self.path_route_table.read().unwrap().version;
        let outcome =
            self.journal_client
                .propose(PdEntry::RemovePathRoute(PathRouteRemoveEntry {
                    op_ms: LocalTime::mills(),
                    path: path.to_string(),
                    expected_table_version,
                }))?;
        Self::route_outcome_to_result(outcome, "remove_path_route", path.to_string())
    }

    fn route_outcome_to_result(outcome: ApplyOutcome, kind: &str, path: String) -> FsResult<()> {
        match outcome {
            ApplyOutcome::Applied | ApplyOutcome::SkippedNoop => Ok(()),
            ApplyOutcome::SkippedStale { reason } => Err(FsError::stale_entry(kind, path, reason)),
            ApplyOutcome::NotFound { reason } => Err(FsError::not_found(reason)),
        }
    }

    // ========== Read accessors ==========

    pub fn get_path_route_table(&self) -> PathRouteTable {
        self.path_route_table.read().unwrap().clone()
    }

    pub fn get_path_route_update(&self) -> Option<PathRouteUpdate> {
        if self.mode != MetaNodeMode::Federation {
            return None;
        }
        let table = self.path_route_table.read().unwrap();
        if table.version == 0 {
            return None;
        }
        Some(PathRouteUpdate {
            version: table.version,
            routes: table.routes.clone(),
        })
    }

    pub fn get_node_group_update(&self) -> Option<NodeGroupUpdate> {
        let groups = self.get_active_groups();
        if groups.is_empty() {
            return None;
        }
        Some(NodeGroupUpdate {
            version: LocalTime::mills(),
            action: NodeGroupUpdateAction::AddGroup { groups },
        })
    }

    // ========== Client summary (for RPC / cache) ==========

    pub fn build_client_summary(&self) -> FsResult<MetaRouteSummary> {
        match self.mode {
            MetaNodeMode::Proxy => Err(FsError::unsupported("MetaNode mode Proxy")),
            MetaNodeMode::Shard => Err(FsError::unsupported("MetaNode mode Shard")),
            MetaNodeMode::Federation => {
                let groups = self.get_active_groups();
                let path_table = self.path_route_table.read().unwrap().clone();
                let version = path_table.version;
                let meta_groups: std::collections::HashMap<u64, NodeGroupInfo> =
                    groups.iter().map(|g| (g.group_id, g.clone())).collect();
                let mut group_id_order: Vec<u64> = groups.iter().map(|g| g.group_id).collect();
                group_id_order.sort_unstable();
                Ok(MetaRouteSummary {
                    mode: self.mode,
                    version,
                    federation_route_config: self.federation_route_config.clone(),
                    path_table: Some(path_table),
                    meta_groups,
                    group_id_order,
                })
            }
        }
    }

    // ========== MetaNode info queries ==========

    pub fn get_active_groups(&self) -> Vec<NodeGroupInfo> {
        let meta_nodes = self.node_manager.get_nodes_by_type(NodeType::Meta);
        let mut by_group: std::collections::HashMap<u64, Vec<curvine_common::state::PeerInfo>> =
            std::collections::HashMap::new();
        for node in meta_nodes {
            if let NodePayload::Meta(ref p) = node.payload {
                let is_leader = p
                    .peers
                    .iter()
                    .find(|peer| peer.node_id == node.base.node_id)
                    .and_then(|peer| peer.is_leader);
                let peer = curvine_common::state::PeerInfo {
                    node_id: node.base.node_id,
                    address: node.base.address.clone(),
                    is_leader,
                };
                by_group.entry(p.group_id as u64).or_default().push(peer);
            }
        }
        by_group
            .into_iter()
            .map(|(group_id, peers)| NodeGroupInfo { group_id, peers })
            .collect()
    }

    pub fn list_meta_nodes(&self) -> Vec<NodeInfo> {
        self.node_manager.get_nodes_by_type(NodeType::Meta)
    }

    pub fn get_meta_node(&self, node_id: u32) -> Option<NodeInfo> {
        let node = self.node_manager.get_node(node_id)?;
        if node.base.node_type == NodeType::Meta {
            Some(node)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::store::KvStore;

    fn node_manager_with_meta() -> Arc<NodeManager> {
        use curvine_common::state::{
            MetaNodePayload, NodeAddress, NodeBase, NodeInfo, NodePayload, NodeState, NodeType,
        };
        let store: Arc<dyn KvStore> =
            Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new());
        let jc = Arc::new(journal::Client::new(
            curvine_common::raft::RaftClient::from_conf(
                curvine_common::conf::JournalConf::default().create_runtime(),
                &curvine_common::conf::JournalConf::default(),
            ),
        ));
        let config = Arc::new(crate::pd::config::ConfigManager::new(
            store.clone(),
            jc.clone(),
            std::collections::HashMap::new(),
        ));
        let node_store = Arc::new(crate::pd::node::NodeStore::new(store));
        let nm = Arc::new(NodeManager::new(node_store, config, jc));
        for (node_id, group_id) in [(1u32, 1u32), (2u32, 10u32)] {
            nm.test_insert_node(NodeInfo {
                base: NodeBase {
                    node_id,
                    node_type: NodeType::Meta,
                    address: NodeAddress {
                        hostname: format!("meta-{}", node_id),
                        ip: format!("10.0.0.{}", node_id),
                        rpc_port: 8000 + node_id as u16,
                        web_port: 9000 + node_id as u16,
                    },
                    ..Default::default()
                },
                state: NodeState::Live,
                payload: NodePayload::Meta(MetaNodePayload {
                    group_id,
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
        nm
    }

    fn make_journal_client() -> Arc<journal::Client> {
        Arc::new(journal::Client::new(
            curvine_common::raft::RaftClient::from_conf(
                curvine_common::conf::JournalConf::default().create_runtime(),
                &curvine_common::conf::JournalConf::default(),
            ),
        ))
    }

    fn make_store() -> Arc<dyn KvStore> {
        Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new())
    }

    fn make_manager(store: Arc<dyn KvStore>, hash_level: Option<u8>) -> MetaRouteManager {
        MetaRouteManager::new(
            MetaNodeMode::Federation,
            FederationRouteConfig::new(hash_level),
            node_manager_with_meta(),
            Arc::new(MetaRouteStore::new(store)),
            make_journal_client(),
        )
    }

    fn make_proxy_manager(store: Arc<dyn KvStore>) -> MetaRouteManager {
        MetaRouteManager::new(
            MetaNodeMode::Proxy,
            FederationRouteConfig::default(),
            node_manager_with_meta(),
            Arc::new(MetaRouteStore::new(store)),
            make_journal_client(),
        )
    }

    fn route(path: &str, group_id: u64) -> PathRouteEntry {
        PathRouteEntry {
            path: path.to_string(),
            group_id,
            create_time_ms: 0,
            update_time_ms: 0,
        }
    }

    fn add(path: &str, group_id: u64, expected_table_version: u64) -> PathRouteAddEntry {
        PathRouteAddEntry {
            op_ms: 0,
            route: route(path, group_id),
            expected_table_version,
        }
    }

    fn remove(path: &str, expected_table_version: u64) -> PathRouteRemoveEntry {
        PathRouteRemoveEntry {
            op_ms: 0,
            path: path.to_string(),
            expected_table_version,
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum ExpectedOutcome {
        Applied,
        Stale,
        NotFound,
    }

    fn assert_outcome_kind(actual: ApplyOutcome, expected: ExpectedOutcome) {
        match (actual, expected) {
            (ApplyOutcome::Applied, ExpectedOutcome::Applied) => {}
            (ApplyOutcome::SkippedStale { .. }, ExpectedOutcome::Stale) => {}
            (ApplyOutcome::NotFound { .. }, ExpectedOutcome::NotFound) => {}
            (actual, expected) => {
                panic!("unexpected outcome: actual={actual:?}, expected={expected:?}")
            }
        }
    }

    fn assert_routes(table: &PathRouteTable, expected: &[(&str, u64)]) {
        assert_eq!(table.routes.len(), expected.len());
        for (path, group_id) in expected {
            let actual = table
                .routes
                .iter()
                .find(|entry| entry.path == *path)
                .map(|entry| entry.group_id);
            assert_eq!(actual, Some(*group_id), "path={path}");
        }
    }

    #[test]
    fn apply_add_route_cases() {
        struct Case {
            name: &'static str,
            seed: Vec<PathRouteAddEntry>,
            op: PathRouteAddEntry,
            expected_outcome: ExpectedOutcome,
            expected_version: u64,
            expected_routes: Vec<(&'static str, u64)>,
        }

        let cases = vec![
            Case {
                name: "insert first route",
                seed: vec![],
                op: add("/user/a", 10, 0),
                expected_outcome: ExpectedOutcome::Applied,
                expected_version: 1,
                expected_routes: vec![("/user/a", 10)],
            },
            Case {
                name: "replace existing route with fresh version",
                seed: vec![add("/user/a", 1, 0)],
                op: add("/user/a", 10, 1),
                expected_outcome: ExpectedOutcome::Applied,
                expected_version: 2,
                expected_routes: vec![("/user/a", 10)],
            },
            Case {
                name: "reject stale add",
                seed: vec![add("/user/a", 1, 0)],
                op: add("/user/b", 10, 0),
                expected_outcome: ExpectedOutcome::Stale,
                expected_version: 1,
                expected_routes: vec![("/user/a", 1)],
            },
        ];

        for case in cases {
            let mgr = make_manager(make_store(), Some(2));
            for seed in &case.seed {
                assert_eq!(mgr.apply_add_route(seed).unwrap(), ApplyOutcome::Applied);
            }

            let actual = mgr.apply_add_route(&case.op).unwrap();
            assert_outcome_kind(actual, case.expected_outcome);

            let table = mgr.get_path_route_table();
            assert_eq!(table.version, case.expected_version, "{}", case.name);
            assert_routes(&table, &case.expected_routes);
        }
    }

    #[test]
    fn apply_remove_route_cases() {
        struct Case {
            name: &'static str,
            seed: Vec<PathRouteAddEntry>,
            op: PathRouteRemoveEntry,
            expected_outcome: ExpectedOutcome,
            expected_version: u64,
            expected_routes: Vec<(&'static str, u64)>,
        }

        let cases = vec![
            Case {
                name: "remove existing route with fresh version",
                seed: vec![add("/user/a", 10, 0)],
                op: remove("/user/a", 1),
                expected_outcome: ExpectedOutcome::Applied,
                expected_version: 2,
                expected_routes: vec![],
            },
            Case {
                name: "reject stale remove so newer route remains visible",
                seed: vec![add("/user/a", 1, 0), add("/user/a", 10, 1)],
                op: remove("/user/a", 1),
                expected_outcome: ExpectedOutcome::Stale,
                expected_version: 2,
                expected_routes: vec![("/user/a", 10)],
            },
            Case {
                name: "missing route with fresh version returns not found",
                seed: vec![],
                op: remove("/missing", 0),
                expected_outcome: ExpectedOutcome::NotFound,
                expected_version: 0,
                expected_routes: vec![],
            },
        ];

        for case in cases {
            let mgr = make_manager(make_store(), Some(2));
            for seed in &case.seed {
                assert_eq!(mgr.apply_add_route(seed).unwrap(), ApplyOutcome::Applied);
            }

            let actual = mgr.apply_remove_route(&case.op).unwrap();
            assert_outcome_kind(actual, case.expected_outcome);

            let table = mgr.get_path_route_table();
            assert_eq!(table.version, case.expected_version, "{}", case.name);
            assert_routes(&table, &case.expected_routes);
        }
    }

    #[test]
    fn build_client_summary_cases() {
        let mgr = make_manager(make_store(), Some(2));
        assert_eq!(
            mgr.apply_add_route(&add("/user/a", 10, 0)).unwrap(),
            ApplyOutcome::Applied
        );

        let summary = mgr.build_client_summary().unwrap();
        assert_eq!(summary.version, 1);
        assert_eq!(summary.federation_route_config.hash_level, Some(2));
        assert_eq!(summary.group_id_order, vec![1, 10]);
        assert_eq!(summary.meta_groups.len(), 2);
        assert_eq!(summary.route("/user/a/file").unwrap().group_id, 10);
    }

    #[test]
    fn get_path_route_update_returns_empty_full_sync_after_last_route_removed() {
        let mgr = make_manager(make_store(), None);
        assert!(mgr.get_path_route_update().is_none());

        assert_eq!(
            mgr.apply_add_route(&add("/data", 1, 0)).unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(
            mgr.apply_remove_route(&remove("/data", 1)).unwrap(),
            ApplyOutcome::Applied
        );

        let update = mgr.get_path_route_update().expect("empty full-sync update");
        assert_eq!(update.version, 2);
        assert!(update.routes.is_empty());
    }

    #[test]
    fn apply_route_batch_persists_route_and_version_for_restore() {
        let store = make_store();
        let mgr = make_manager(store.clone(), None);
        assert_eq!(
            mgr.apply_add_route(&add("/data", 10, 0)).unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(
            mgr.apply_remove_route(&remove("/data", 1)).unwrap(),
            ApplyOutcome::Applied
        );

        let restored = make_manager(store, None);
        restored.restore().unwrap();
        let table = restored.get_path_route_table();
        assert_eq!(table.version, 2);
        assert!(table.routes.is_empty());
    }

    #[test]
    fn proxy_mode_route_management_is_rejected_or_noop() {
        let mgr = make_proxy_manager(make_store());
        assert!(mgr.build_client_summary().is_err());
        assert!(mgr.add_route(route("/any", 1)).is_err());
        assert!(mgr.remove_route("/any").is_err());
        assert_eq!(
            mgr.apply_add_route(&add("/any", 1, 0)).unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(
            mgr.apply_remove_route(&remove("/any", 0)).unwrap(),
            ApplyOutcome::Applied
        );
    }
}
