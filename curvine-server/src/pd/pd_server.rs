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

use crate::pd::bg::{BGManager, BGStore};
use crate::pd::bgtable::BGTableManager;
use crate::pd::bgtable::BGTableStore;
use crate::pd::cluster::ClusterManager;
use crate::pd::config::ConfigManager;
use crate::pd::http_handler::PdHttpHandler;
use crate::pd::journal::{self, PdAppStorage};
use crate::pd::metaroute::{MetaRouteManager, MetaRouteStore};
use crate::pd::mount::MountManager;
use crate::pd::namespace::NamespaceManager;
use crate::pd::node::NodeManager;
use crate::pd::node::NodeStore;
use crate::pd::pd_metrics::PdMetrics;
use crate::pd::pool::PoolManager;
use crate::pd::store::{KvStore, RocksKvEngine, CF_DATA, CF_META};
use curvine_common::conf::PdConf;
use curvine_common::raft::storage::{LogStorage, RocksLogStorage};
use curvine_common::raft::{RaftClient, RaftJournal, RoleMonitor};
use curvine_common::rocksdb::DBEngine;
use curvine_common::state::{FederationRouteConfig, MetaNodeMode};
use curvine_web::server::{WebHandlerService, WebServer};
use log::info;
use once_cell::sync::OnceCell;
use orpc::common::FileUtils;
use orpc::handler::HandlerService;
use orpc::io::net::ConnState;
use orpc::runtime::RpcRuntime;
use orpc::runtime::Runtime;
use orpc::server::{RpcServer, ServerStateListener};
use orpc::CommonResult;
use std::sync::Arc;

use crate::pd::cluster::manager::{LeaderChecker, RaftLeaderChecker};

use crate::pd::rpc_handler::PdRpcHandler;

static PD_METRICS: OnceCell<PdMetrics> = OnceCell::new();

fn parse_metanode_mode(s: &str) -> MetaNodeMode {
    match s.to_lowercase().as_str() {
        "proxy" => MetaNodeMode::Proxy,
        "shard" => MetaNodeMode::Shard,
        _ => MetaNodeMode::Federation,
    }
}

type PdRaftJournal = RaftJournal<RocksLogStorage, PdAppStorage>;

#[derive(Clone)]
struct PdService {
    conf: PdConf,
    config_manager: Arc<ConfigManager>,
    mount_manager: Arc<MountManager>,
    namespace_manager: Arc<NamespaceManager>,
    cluster_manager: Arc<ClusterManager>,
}

impl HandlerService for PdService {
    type Item = PdRpcHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        PdRpcHandler::new(
            self.config_manager.clone(),
            self.mount_manager.clone(),
            self.namespace_manager.clone(),
            self.cluster_manager.clone(),
        )
    }
}

impl WebHandlerService for PdService {
    type Item = PdHttpHandler;

    fn get_handler(&self) -> Self::Item {
        PdHttpHandler::new(
            self.config_manager.clone(),
            self.mount_manager.clone(),
            self.namespace_manager.clone(),
            self.cluster_manager.clone(),
        )
    }
}

pub struct Pd {
    raft_journal: PdRaftJournal,
    service: PdService,
    rpc_server: RpcServer<PdService>,
    web_server: WebServer<PdService>,
}

impl Pd {
    pub fn new(conf: PdConf) -> CommonResult<Self> {
        conf.print();

        let log_store = RocksLogStorage::from_conf(&conf.journal, false);
        let mut db_conf = conf.pd_rocks_conf();
        if log_store.has_snapshot() {
            info!(
                "There is a snapshot currently, the original data directory {} will be \
                deleted and restored based on the snapshot later",
                db_conf.data_dir
            );
            FileUtils::delete_path(&db_conf.data_dir, true)?;
        }

        db_conf = db_conf.add_cf(CF_META).add_cf(CF_DATA);
        let db = DBEngine::new(db_conf, false)?;
        let engine = Arc::new(RocksKvEngine::new(db));
        let store: Arc<dyn KvStore> = engine.clone();

        let snapshot_dir = format!("{}/snapshots", conf.data_dir);
        FileUtils::create_dir(&snapshot_dir, true)?;

        let journal_rt: Arc<Runtime> = conf.journal.create_runtime();
        let raft_client = RaftClient::from_conf(journal_rt.clone(), &conf.journal);

        // Unified journal client for all Raft propose operations.
        let journal_client = Arc::new(journal::Client::new(raft_client));

        let config_manager = Arc::new(ConfigManager::new(
            store.clone(),
            journal_client.clone(),
            conf.dynamic_config.clone(),
        ));
        let node_store = Arc::new(NodeStore::new(store.clone()));
        let node_manager = Arc::new(NodeManager::new(
            node_store,
            config_manager.clone(),
            journal_client.clone(),
        ));
        node_manager.restore()?;

        let pool_manager = Arc::new(PoolManager::new(node_manager.clone()));
        pool_manager.restore()?;

        let bg_store = Arc::new(BGStore::new(store.clone()));
        let bg_manager = Arc::new(BGManager::new(bg_store, journal_client.clone()));

        let table_store = Arc::new(BGTableStore::new(store.clone()));
        let bgtable_manager = Arc::new(BGTableManager::new(
            table_store,
            bg_manager.clone(),
            pool_manager.clone(),
            config_manager.clone(),
            conf.location_labels.clone(),
        ));
        // BGTableManager owns the BG-world restore order (BG metadata, then
        // reset, then table indexes).
        bgtable_manager.restore()?;

        let namespace_manager = Arc::new(NamespaceManager::new(
            store.clone(),
            bgtable_manager.clone(),
            journal_client.clone(),
        ));
        namespace_manager.restore()?;

        let mount_manager = Arc::new(MountManager::new(
            store.clone(),
            journal_client.clone(),
            namespace_manager.clone(),
        ));
        mount_manager.restore()?;

        PD_METRICS.get_or_init(|| {
            PdMetrics::new(
                node_manager.clone(),
                pool_manager.clone(),
                bg_manager.clone(),
                bgtable_manager.clone(),
            )
            .expect("Failed to initialize PD metrics")
        });

        let metanode_mode = parse_metanode_mode(&conf.metanode.mode);
        let federation_route_config =
            FederationRouteConfig::new(conf.metanode.federation_hash_level);
        let route_store = Arc::new(MetaRouteStore::new(store.clone()));
        let metaroute_manager = Arc::new(MetaRouteManager::new(
            metanode_mode,
            federation_route_config,
            node_manager.clone(),
            route_store,
            journal_client.clone(),
        ));
        metaroute_manager.restore()?;

        let app_store = PdAppStorage::new(
            engine,
            snapshot_dir,
            config_manager.clone(),
            mount_manager.clone(),
            node_manager.clone(),
            pool_manager.clone(),
            bgtable_manager.clone(),
            namespace_manager.clone(),
            metaroute_manager.clone(),
        );

        let role_monitor = RoleMonitor::new();
        let role_ctl = role_monitor.read_ctl();
        let leader_checker: Arc<dyn LeaderChecker> = Arc::new(RaftLeaderChecker::new(role_ctl));

        // Wire the leader checker into the journal client so propose
        // paths can fast-fail when this node is no longer the raft leader.
        journal_client.set_leader_checker(leader_checker.clone());

        let rpc_conf = conf.pd_server_conf();
        let rpc_rt: Arc<Runtime> = Arc::new(rpc_conf.create_runtime());
        let coordinator_rt: Arc<Runtime> = Arc::new(Runtime::new(
            "pd-coordinator",
            conf.coordinator_io_threads,
            conf.coordinator_worker_threads,
        ));

        let cluster_manager = Arc::new(ClusterManager::new(
            conf.cluster_id.clone(),
            node_manager,
            pool_manager,
            bgtable_manager,
            config_manager.clone(),
            mount_manager.clone(),
            namespace_manager.clone(),
            metaroute_manager,
            leader_checker,
            coordinator_rt,
        ));

        let raft_journal = PdRaftJournal::new(
            journal_rt,
            log_store,
            app_store,
            conf.journal.clone(),
            role_monitor,
        );

        let service = PdService {
            conf: conf.clone(),
            config_manager,
            mount_manager,
            namespace_manager,
            cluster_manager,
        };
        let rpc_server = RpcServer::with_rt(rpc_rt.clone(), rpc_conf, service.clone());
        let web_server = WebServer::with_rt(rpc_rt, conf.pd_web_conf(), service.clone());

        Ok(Self {
            raft_journal,
            service,
            rpc_server,
            web_server,
        })
    }

    pub async fn start(self) -> CommonResult<ServerStateListener> {
        info!("Starting PD on RPC port {}...", self.service.conf.rpc_port);

        // Step 1: start raft (RaftServer + RaftNode via RaftJournal)
        let _raft_listener = self.raft_journal.run().await?;

        // Step 2: start RPC server
        let mut rpc_status = self.rpc_server.start();
        rpc_status.wait_running().await?;

        // Step 3: start web server
        self.web_server.start();

        // Step 4: start leader lifecycle monitor
        self.service.cluster_manager.start_leader_monitor();

        Ok(rpc_status)
    }

    pub fn config_manager(&self) -> Arc<ConfigManager> {
        self.service.config_manager.clone()
    }

    pub fn get_metrics() -> &'static PdMetrics {
        PD_METRICS.get().expect("PD metrics not initialized")
    }

    pub fn block_on_start(self) {
        let rt = self.rpc_server.clone_rt();
        rt.block_on(async move {
            let mut status = match self.start().await {
                Ok(s) => s,
                Err(e) => {
                    log::error!("PD start failed: {}", e);
                    return;
                }
            };

            if let Err(e) = tokio::signal::ctrl_c().await {
                log::error!("Failed to wait for Ctrl-C: {}", e);
                return;
            }
            info!("Received Ctrl-C, shutting down PD...");
            let _ = status.wait_stop().await;
        });
    }
}

#[cfg(test)]
pub fn init_metrics_for_test() {
    use crate::pd::bg::{BGManager, BGStore};
    use crate::pd::journal::Client;
    use crate::pd::node::{NodeManager, NodeStore};
    use crate::pd::pool::PoolManager;
    use crate::pd::store::{KvStore, MemoryKvEngine};
    use std::collections::HashMap;

    PD_METRICS.get_or_init(|| {
        let store: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
        let raft = curvine_common::raft::RaftClient::from_conf(
            curvine_common::conf::JournalConf::default().create_runtime(),
            &curvine_common::conf::JournalConf::default(),
        );
        let jc = Arc::new(Client::new(raft));
        let config = Arc::new(ConfigManager::new(
            store.clone(),
            jc.clone(),
            HashMap::new(),
        ));
        let node_store = Arc::new(NodeStore::new(store.clone()));
        let node_mgr = Arc::new(NodeManager::new(node_store, config.clone(), jc.clone()));
        let pool_mgr = Arc::new(PoolManager::new(node_mgr.clone()));
        let bg_store = Arc::new(BGStore::new(store.clone()));
        let bg_mgr = Arc::new(BGManager::new(bg_store, jc.clone()));
        let table_store = Arc::new(crate::pd::bgtable::BGTableStore::new(store));
        let bgtable_mgr = Arc::new(crate::pd::bgtable::BGTableManager::new(
            table_store,
            bg_mgr.clone(),
            pool_mgr.clone(),
            config.clone(),
            vec![],
        ));
        PdMetrics::new(node_mgr, pool_mgr, bg_mgr, bgtable_mgr)
            .expect("Failed to init test metrics")
    });
}
