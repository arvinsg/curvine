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

use crate::pd::config::ConfigManager;
use crate::pd::http_handler::PdHttpHandler;
use crate::pd::journal::PdAppStorage;
use crate::pd::mount::MountManager;
use crate::pd::store::{KvStore, RocksKvEngine};
use curvine_common::conf::PdConf;
use curvine_common::raft::storage::{LogStorage, RocksLogStorage};
use curvine_common::raft::{RaftClient, RaftJournal, RoleMonitor};
use curvine_common::rocksdb::DBEngine;
use curvine_web::server::{WebHandlerService, WebServer};
use log::info;
use orpc::common::FileUtils;
use orpc::handler::HandlerService;
use orpc::io::net::ConnState;
use orpc::runtime::RpcRuntime;
use orpc::runtime::Runtime;
use orpc::server::{RpcServer, ServerStateListener};
use orpc::CommonResult;
use std::sync::Arc;

use crate::pd::rpc_handler::PdRpcHandler;

type PdRaftJournal = RaftJournal<RocksLogStorage, PdAppStorage>;

#[derive(Clone)]
struct PdService {
    conf: PdConf,
    config_manager: Arc<ConfigManager>,
    mount_manager: Arc<MountManager>,
}

impl HandlerService for PdService {
    type Item = PdRpcHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        PdRpcHandler::new(self.config_manager.clone(), self.mount_manager.clone())
    }
}

impl WebHandlerService for PdService {
    type Item = PdHttpHandler;

    fn get_handler(&self) -> Self::Item {
        PdHttpHandler::new(self.config_manager.clone(), self.mount_manager.clone())
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

        db_conf = db_conf.add_cf("config").add_cf("mount");
        let db = DBEngine::new(db_conf, false)?;
        let engine = Arc::new(RocksKvEngine::new(db));
        let store: Arc<dyn KvStore> = engine.clone();

        let snapshot_dir = format!("{}/snapshots", conf.data_dir);
        FileUtils::create_dir(&snapshot_dir, true)?;

        let journal_rt: Arc<Runtime> = conf.journal.create_runtime();
        let raft_client = RaftClient::from_conf(journal_rt.clone(), &conf.journal);

        let config_manager = Arc::new(ConfigManager::new(
            store.clone(),
            raft_client.clone(),
            conf.dynamic_config.clone(),
        ));
        let mount_manager = Arc::new(MountManager::new(store, raft_client));
        mount_manager.restore()?;

        let app_store = PdAppStorage::new(
            engine,
            snapshot_dir,
            config_manager.clone(),
            mount_manager.clone(),
        );

        let role_monitor = RoleMonitor::new();
        let raft_journal = PdRaftJournal::new(
            journal_rt,
            log_store,
            app_store,
            conf.journal.clone(),
            role_monitor,
        );

        let rpc_conf = conf.pd_server_conf();
        let rt: Arc<Runtime> = Arc::new(rpc_conf.create_runtime());
        let service = PdService {
            conf: conf.clone(),
            config_manager,
            mount_manager,
        };
        let rpc_server = RpcServer::with_rt(rt.clone(), rpc_conf, service.clone());
        let web_server = WebServer::with_rt(rt.clone(), conf.pd_web_conf(), service.clone());

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

        Ok(rpc_status)
    }

    pub fn config_manager(&self) -> Arc<ConfigManager> {
        self.service.config_manager.clone()
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
