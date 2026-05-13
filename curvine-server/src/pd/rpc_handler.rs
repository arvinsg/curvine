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

use crate::pd::cluster::ClusterManager;
use crate::pd::config::ConfigManager;
use crate::pd::mount::MountManager;
use crate::pd::pd_server::Pd;
use crate::pd::rpc_context::RpcContext;
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::fs::RpcCode;
use curvine_common::proto::*;
use curvine_common::utils::{ProtoUtils, SerdeUtils as Serde};
use curvine_common::FsResult;
use orpc::common::LocalTime;
use orpc::handler::MessageHandler;
use orpc::message::Message;
use std::sync::Arc;

pub struct PdRpcHandler {
    config_manager: Arc<ConfigManager>,
    mount_manager: Arc<MountManager>,
    cluster_manager: Arc<ClusterManager>,
}

impl PdRpcHandler {
    pub fn new(
        config_manager: Arc<ConfigManager>,
        mount_manager: Arc<MountManager>,
        cluster_manager: Arc<ClusterManager>,
    ) -> Self {
        Self {
            config_manager,
            mount_manager,
            cluster_manager,
        }
    }
}

impl MessageHandler for PdRpcHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let ctx = RpcContext::new(msg);
        let operation = ctx.code.as_str();
        let metrics = Pd::get_metrics();
        metrics
            .rpc_request_total
            .with_label_values(&[operation])
            .inc();
        let start = LocalTime::mills();

        let response = match ctx.code {
            RpcCode::GetConfig => {
                let req = ctx.parse_header()?;
                let resp = self.config_manager.get_config(req)?;
                ctx.response(resp)?
            }
            RpcCode::ListConfig => {
                let req = ctx.parse_header()?;
                let resp = self.config_manager.list_config(req)?;
                ctx.response(resp)?
            }
            RpcCode::SetConfig => {
                let req = ctx.parse_header()?;
                let resp = self.config_manager.set_config(req)?;
                ctx.response(resp)?
            }
            RpcCode::Mount => {
                let req: MountRequest = ctx.parse_header()?;
                let mnt_opt = ProtoUtils::mount_options_from_pb(req.mount_options);
                self.mount_manager
                    .mount(None, &req.cv_path, &req.ufs_path, &mnt_opt)?;
                ctx.response(MountResponse::default())?
            }
            RpcCode::UnMount => {
                let req: UnMountRequest = ctx.parse_header()?;
                self.mount_manager.umount(&req.cv_path)?;
                ctx.response(UnMountResponse::default())?
            }
            RpcCode::GetMountTable => {
                let table = self.mount_manager.get_mount_table()?;
                // Wire format expects MountInfoProto; deref Arc and convert.
                let mount_table: Vec<MountInfoProto> = table
                    .into_iter()
                    .map(|arc| ProtoUtils::mount_info_to_pb((*arc).clone()))
                    .collect();
                ctx.response(GetMountTableResponse { mount_table })?
            }
            RpcCode::GetMountInfo => {
                let req: GetMountInfoRequest = ctx.parse_header()?;
                let path = Path::from_str(req.path)?;
                let info = self.mount_manager.get_mount_info(&path)?;
                ctx.response(GetMountInfoResponse {
                    mount_info: info.map(|arc| ProtoUtils::mount_info_to_pb((*arc).clone())),
                })?
            }
            RpcCode::GetMetaRouteSummary => {
                let _req: GetMetaRouteSummaryRequest = ctx.parse_header()?;
                let summary = self.cluster_manager.meta_manager().build_client_summary()?;
                let summary_bytes = Serde::serialize(&summary)?;
                ctx.response(GetMetaRouteSummaryResponse {
                    summary: Some(summary_bytes),
                })?
            }
            RpcCode::Undefined => {
                return Err(FsError::from("PD RPC: undefined config code".to_string()))
            }
            _ => {
                return Err(FsError::from(format!(
                    "Unsupported request type: {:?}",
                    ctx.code
                )));
            }
        };

        let elapsed = LocalTime::mills().saturating_sub(start) as f64;
        metrics
            .rpc_request_duration
            .with_label_values(&[operation])
            .observe(elapsed);

        Ok(response)
    }
}
