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

use crate::pd::config_handler::ConfigHandler;
use crate::pd::http_handler::HttpConfigHandler;
use crate::pd::storage::PdAppStorage;
use curvine_common::conf::PdConf;
use curvine_common::raft::storage::{LogStorage, RocksLogStorage};
use curvine_common::raft::{RaftClient, RaftNode, RoleMonitor};
use curvine_common::rocksdb::DBEngine;
use curvine_common::FsResult;
use log::info;
use orpc::common::FileUtils;
use orpc::runtime::RpcRuntime;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct PdServer {
    conf: PdConf,
    raft_node: RaftNode<RocksLogStorage, PdAppStorage>,
    config_handler: Arc<ConfigHandler>,
    http_handler: Arc<HttpConfigHandler>,
}

impl PdServer {
    pub fn new(conf: PdConf) -> FsResult<Self> {
        conf.print();

        let rt = conf.journal.create_runtime();

        let log_store = RocksLogStorage::from_conf(&conf.journal, false);

        let db_conf = conf.pd_rocks_conf();
        if log_store.has_snapshot() {
            info!(
                "There is a snapshot currently, the original data directory {} will be \
                deleted and restored based on the snapshot later",
                db_conf.data_dir
            );
            FileUtils::delete_path(&db_conf.data_dir, true)?;
        }

        let db = DBEngine::new(db_conf, false)?;
        let snapshot_dir = format!("{}/snapshots", conf.data_dir);
        FileUtils::create_dir(&snapshot_dir, true)?;

        let app_store = PdAppStorage::new(db, snapshot_dir);
        let config_store = app_store.config_store();

        let role_monitor = RoleMonitor::new();
        let (sender, receiver) = mpsc::channel(1024);

        let raft_client = RaftClient::from_conf(rt.clone(), &conf.journal);

        let config_handler = Arc::new(ConfigHandler::new(config_store, raft_client));
        let http_handler = Arc::new(HttpConfigHandler::new(config_handler.clone()));

        // RaftNode requires a slog::Logger; use Discard to suppress Raft internal logs.
        let logger = slog::Logger::root(slog::Discard, slog::o!());

        let raft_node = rt.block_on(async {
            RaftNode::new_candidate(
                rt.clone(),
                &conf.journal,
                log_store,
                app_store,
                role_monitor,
                receiver,
                sender,
                &logger,
            )
            .await
        })?;

        info!("PD Server initialized successfully with config management");

        Ok(Self {
            conf,
            raft_node,
            config_handler,
            http_handler,
        })
    }

    pub async fn start(&mut self) -> FsResult<()> {
        info!("Starting PD Server on port {}...", self.conf.rpc_port);

        let http_routes = self.http_handler.routes();
        let http_addr = format!("0.0.0.0:{}", self.conf.web_port);

        info!("Starting HTTP server on {}", http_addr);
        let listener = tokio::net::TcpListener::bind(&http_addr).await?;

        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, http_routes).await {
                log::error!("PD HTTP server error: {}", e);
            }
        });

        info!("PD Server started successfully");
        info!("  RPC Port: {}", self.conf.rpc_port);
        info!("  HTTP Port: {}", self.conf.web_port);
        info!("  Config API: http://localhost:{}/api/v1/config", self.conf.web_port);

        Ok(())
    }

    pub fn block_on_start(mut self) {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
        rt.block_on(async move {
            if let Err(e) = self.start().await {
                log::error!("PD Server start failed: {}", e);
                return;
            }
            if let Err(e) = tokio::signal::ctrl_c().await {
                log::error!("Failed to wait for Ctrl-C: {}", e);
                return;
            }
            info!("Received Ctrl-C, shutting down PD Server...");
        });
    }

    pub fn config_handler(&self) -> Arc<ConfigHandler> {
        self.config_handler.clone()
    }
}
