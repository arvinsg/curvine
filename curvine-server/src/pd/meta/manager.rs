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

use crate::pd::journal::{self, ApplyOutcome, PdEntry};
use crate::pd::meta::RouteStore;
use crate::pd::node::NodeManager;
use curvine_common::state::*;
use curvine_common::{FsError, FsResult};
use orpc::common::{LocalTime, Utils};
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

/// Manages MetaNode mode, path routing (Federation Static/Hash), and meta node info. Proxy/Shard are not supported.
pub struct MetaManager {
    mode: MetaNodeMode,
    federation_route_mode: Option<FederationRouteMode>,
    hash_level: u8,
    path_route_table: RwLock<PathRouteTable>,
    node_manager: Arc<NodeManager>,
    store: Arc<RouteStore>,
    journal_client: Arc<journal::Client>,
    /// P4.1: serializes the two propose entry points (add_route / remove_route)
    /// so expected_table_version stays fresh between read and propose.
    write_lock: Mutex<()>,
}

impl MetaManager {
    pub fn new(
        mode: MetaNodeMode,
        federation_route_mode: Option<FederationRouteMode>,
        hash_level: u8,
        node_manager: Arc<NodeManager>,
        store: Arc<RouteStore>,
        journal_client: Arc<journal::Client>,
    ) -> Self {
        Self {
            mode,
            federation_route_mode,
            hash_level,
            path_route_table: RwLock::new(PathRouteTable::default()),
            node_manager,
            store,
            journal_client,
            write_lock: Mutex::new(()),
        }
    }

    pub fn mode(&self) -> MetaNodeMode {
        self.mode
    }

    /// Load path route table and version from store (call on PD startup). Only for Federation + Static.
    pub fn restore(&self) -> FsResult<()> {
        if self.mode != MetaNodeMode::Federation {
            return Ok(());
        }
        let Some(FederationRouteMode::Static) = self.federation_route_mode else {
            return Ok(());
        };
        let version = self.store.get_path_route_version()?;
        let routes = self.store.list_path_routes()?;
        let now = LocalTime::mills();
        let mut table = self.path_route_table.write().unwrap();
        table.version = version;
        table.routes = routes;
        table.last_update_ms = now;
        Ok(())
    }

    pub fn apply_add_route(&self, entry: &PathRouteEntry) -> FsResult<ApplyOutcome> {
        if self.mode != MetaNodeMode::Federation
            || self.federation_route_mode != Some(FederationRouteMode::Static)
        {
            return Ok(ApplyOutcome::Applied);
        }
        let mut table = self.path_route_table.write().unwrap();
        // P4.1 + #7: strict CAS — `expected_table_version` must match exactly.
        // First-time install: table.version == 0, proposer also reads 0 → match.
        // Stale legacy entry (expected=0 against table.version=N>0): rejected
        // as Stale, preventing the version-0 bypass that pre-#7 allowed silent
        // overwrite by replayed entries.
        if entry.expected_table_version != table.version {
            log::warn!(
                "Apply AddPathRoute skipped: path={} stale, current_table_version={}, \
                 entry_expected_table_version={}",
                entry.path,
                table.version,
                entry.expected_table_version
            );
            return Ok(ApplyOutcome::stale(format!(
                "table_version mismatch: current={}, expected={}",
                table.version, entry.expected_table_version
            )));
        }
        self.store.put_path_route(entry)?;
        let now = entry.update_time_ms;
        let was_replace = table.routes.iter().any(|r| r.path == entry.path);
        if let Some(existing) = table.routes.iter_mut().find(|r| r.path == entry.path) {
            *existing = entry.clone();
        } else {
            table.routes.push(entry.clone());
        }
        table.version = table.version.saturating_add(1);
        table.last_update_ms = now;
        table.invalidate_cache();
        self.store.put_path_route_version(table.version)?;
        log::info!(
            "Apply AddPathRoute path={}, group_id={}, mode={}, table_version={}",
            entry.path,
            entry.group_id,
            if was_replace { "replace" } else { "insert" },
            table.version
        );
        Ok(ApplyOutcome::Applied)
    }

    pub fn apply_remove_route(&self, path: &str) -> FsResult<ApplyOutcome> {
        if self.mode != MetaNodeMode::Federation
            || self.federation_route_mode != Some(FederationRouteMode::Static)
        {
            return Ok(ApplyOutcome::Applied);
        }
        self.store.delete_path_route(path)?;
        let now = LocalTime::mills();
        let mut table = self.path_route_table.write().unwrap();
        let len_before = table.routes.len();
        table.routes.retain(|r| r.path != path);
        if table.routes.len() < len_before {
            table.version = table.version.saturating_add(1);
            table.last_update_ms = now;
            table.invalidate_cache();
            self.store.put_path_route_version(table.version)?;
            log::info!(
                "Apply RemovePathRoute path={}, table_version={}",
                path,
                table.version
            );
            Ok(ApplyOutcome::Applied)
        } else {
            log::warn!(
                "Apply RemovePathRoute skipped: path={} not present in route table",
                path
            );
            Ok(ApplyOutcome::not_found(format!(
                "path {} not present",
                path
            )))
        }
    }

    pub fn route(&self, path: &str) -> FsResult<u64> {
        match self.mode {
            MetaNodeMode::Proxy => Err(FsError::unsupported("MetaNode mode Proxy")),
            MetaNodeMode::Shard => Err(FsError::unsupported("MetaNode mode Shard")),
            MetaNodeMode::Federation => self.route_federation(path),
        }
    }

    fn route_federation(&self, path: &str) -> FsResult<u64> {
        let groups = self.get_active_groups();
        if groups.is_empty() {
            return Err(FsError::common("no available meta group".to_string()));
        }
        let mode = self
            .federation_route_mode
            .ok_or_else(|| FsError::common("Federation route mode not set".to_string()))?;
        match mode {
            FederationRouteMode::Static => {
                let table = self.path_route_table.read().unwrap();
                let group_id = table.lookup_group_id(path);
                Ok(
                    group_id
                        .unwrap_or_else(|| groups.iter().map(|g| g.group_id).min().unwrap_or(0)),
                )
            }
            FederationRouteMode::Hash => {
                if self.hash_level == 0 {
                    return Err(FsError::common("hash_level must be >= 1"));
                }
                let shard_key = extract_shard_key(path, self.hash_level);
                let hash = Utils::murmur3(shard_key.as_bytes());
                let idx = (hash as usize) % groups.len();
                Ok(groups[idx].group_id)
            }
        }
    }

    // ========== Route management (propose via Raft) ==========

    pub fn add_route(&self, mut entry: PathRouteEntry) -> FsResult<()> {
        if self.mode != MetaNodeMode::Federation
            || self.federation_route_mode != Some(FederationRouteMode::Static)
        {
            return Err(FsError::common(
                "add_route only in Federation Static mode".to_string(),
            ));
        }
        let _g = self.write_lock.lock().unwrap();
        let now = LocalTime::mills();
        if entry.create_time_ms == 0 {
            entry.create_time_ms = now;
        }
        entry.update_time_ms = now;
        // P4.1: snapshot table version inside write_lock so concurrent admin
        // routes don't race past us.
        entry.expected_table_version = self.path_route_table.read().unwrap().version;
        let path = entry.path.clone();
        let outcome = self
            .journal_client
            .propose_as_leader_with_result(PdEntry::AddPathRoute(entry))?;
        match outcome {
            ApplyOutcome::Applied | ApplyOutcome::SkippedNoop => Ok(()),
            ApplyOutcome::SkippedStale { reason } => Err(FsError::stale_entry(
                "add_path_route",
                path,
                reason,
            )),
            ApplyOutcome::NotFound { reason } => Err(FsError::not_found(reason)),
        }
    }

    pub fn remove_route(&self, path: &str) -> FsResult<()> {
        if self.mode != MetaNodeMode::Federation
            || self.federation_route_mode != Some(FederationRouteMode::Static)
        {
            return Err(FsError::common(
                "remove_route only in Federation Static mode".to_string(),
            ));
        }
        let _g = self.write_lock.lock().unwrap();
        let outcome = self
            .journal_client
            .propose_as_leader_with_result(PdEntry::RemovePathRoute(path.to_string()))?;
        match outcome {
            ApplyOutcome::Applied | ApplyOutcome::SkippedNoop => Ok(()),
            ApplyOutcome::SkippedStale { reason } => Err(FsError::stale_entry(
                "remove_path_route",
                path,
                reason,
            )),
            ApplyOutcome::NotFound { reason } => Err(FsError::not_found(reason)),
        }
    }

    // ========== Read accessors ==========

    pub fn get_path_route_table(&self) -> PathRouteTable {
        self.path_route_table.read().unwrap().clone()
    }

    pub fn get_path_route_update(&self) -> Option<PathRouteUpdate> {
        if self.mode != MetaNodeMode::Federation
            || self.federation_route_mode != Some(FederationRouteMode::Static)
        {
            return None;
        }
        let table = self.path_route_table.read().unwrap();
        if table.routes.is_empty() {
            return None;
        }
        Some(PathRouteUpdate {
            version: table.version,
            action: RouteUpdateAction::FullSync {
                routes: table.routes.clone(),
            },
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
                let version = if self.federation_route_mode == Some(FederationRouteMode::Static) {
                    self.path_route_table.read().unwrap().version
                } else {
                    LocalTime::mills()
                };
                let path_table = if self.federation_route_mode == Some(FederationRouteMode::Static)
                {
                    Some(self.path_route_table.read().unwrap().clone())
                } else {
                    None
                };
                let federation_hash_level =
                    if self.federation_route_mode == Some(FederationRouteMode::Hash) {
                        Some(self.hash_level)
                    } else {
                        None
                    };
                let meta_groups: std::collections::HashMap<u64, NodeGroupInfo> =
                    groups.iter().map(|g| (g.group_id, g.clone())).collect();
                let mut group_id_order: Vec<u64> = groups.iter().map(|g| g.group_id).collect();
                group_id_order.sort_unstable();
                Ok(MetaRouteSummary {
                    mode: self.mode,
                    version,
                    federation_route_mode: self.federation_route_mode,
                    path_table,
                    meta_groups,
                    group_id_order,
                    federation_hash_level,
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

    fn node_manager_with_meta() -> Arc<NodeManager> {
        use curvine_common::state::{
            MetaNodePayload, NodeAddress, NodeBase, NodeInfo, NodePayload, NodeState, NodeType,
        };
        let store: Arc<dyn crate::pd::store::KvStore> =
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

    #[test]
    fn extract_shard_key_level2() {
        use curvine_common::state::extract_shard_key;
        assert_eq!(extract_shard_key("/user/a/file", 2), "a");
        assert_eq!(extract_shard_key("/a/b/c", 2), "b");
        assert_eq!(extract_shard_key("/a", 2), "");
    }

    #[test]
    fn static_lookup_and_fallback() {
        let nm = node_manager_with_meta();
        let store: Arc<dyn crate::pd::store::KvStore> =
            Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new());
        let path_store = Arc::new(RouteStore::new(store));
        let jc = make_journal_client();
        let mgr = MetaManager::new(
            MetaNodeMode::Federation,
            Some(FederationRouteMode::Static),
            2,
            nm,
            path_store,
            jc,
        );
        mgr.apply_add_route(&PathRouteEntry {
            path: "/user/a".to_string(),
            group_id: 10,
            create_time_ms: 0,
            update_time_ms: 0,
            expected_table_version: 0,
        })
        .unwrap();
        mgr.apply_add_route(&PathRouteEntry {
            path: "/user".to_string(),
            group_id: 1,
            create_time_ms: 0,
            update_time_ms: 0,
            expected_table_version: 1, // #7 strict CAS: table.version=1 after first add
        })
        .unwrap();
        assert_eq!(mgr.route("/user").unwrap(), 1);
        assert_eq!(mgr.route("/user/a").unwrap(), 10);
        assert_eq!(mgr.route("/user/a/b").unwrap(), 10);
    }

    #[test]
    fn proxy_route_returns_error() {
        let nm = node_manager_with_meta();
        let store: Arc<dyn crate::pd::store::KvStore> =
            Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new());
        let path_store = Arc::new(RouteStore::new(store));
        let jc = make_journal_client();
        let mgr = MetaManager::new(MetaNodeMode::Proxy, None, 2, nm, path_store, jc);
        assert!(mgr.route("/any").is_err());
        assert!(mgr.build_client_summary().is_err());
    }

    /// Regression: trie cache must be invalidated after apply_add_route /
    /// apply_remove_route. Before the fix, lookup populated the trie on the
    /// first call and never picked up subsequent route mutations.
    #[test]
    fn apply_add_then_route_then_add_picks_up_new_route() {
        let nm = node_manager_with_meta();
        let store: Arc<dyn crate::pd::store::KvStore> =
            Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new());
        let path_store = Arc::new(RouteStore::new(store));
        let jc = make_journal_client();
        let mgr = MetaManager::new(
            MetaNodeMode::Federation,
            Some(FederationRouteMode::Static),
            2,
            nm,
            path_store,
            jc,
        );

        // Initial route → first lookup populates trie cache. table.version=0
        // before, will become 1 after this apply.
        mgr.apply_add_route(&PathRouteEntry {
            path: "/data".to_string(),
            group_id: 1,
            create_time_ms: 0,
            update_time_ms: 0,
            expected_table_version: 0,
        })
        .unwrap();
        assert_eq!(mgr.route("/data").unwrap(), 1);

        // Add a new route AFTER the cache is populated. table.version=1 now,
        // so the second apply must specify expected_table_version=1 (#7
        // strict CAS — version-0 bypass removed).
        mgr.apply_add_route(&PathRouteEntry {
            path: "/extra".to_string(),
            group_id: 10,
            create_time_ms: 0,
            update_time_ms: 0,
            expected_table_version: 1,
        })
        .unwrap();
        // Without invalidate_cache, this would return the fallback group (min
        // group_id = 1), masking the newly added route.
        assert_eq!(mgr.route("/extra").unwrap(), 10);

        // Remove a route AFTER the cache is populated; lookup must reflect the
        // removal.
        mgr.apply_remove_route("/data").unwrap();
        // Fallback to min group_id (1 from the test harness's meta groups).
        assert_eq!(mgr.route("/data").unwrap(), 1);
        assert_eq!(mgr.route("/extra").unwrap(), 10);
    }
}
