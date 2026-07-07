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
use crate::pd::namespace::NamespaceManager;
use crate::pd::pd_server::Pd;
use crate::pd::rpc_context::RpcContext;
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::fs::RpcCode;
use curvine_common::proto::*;
use curvine_common::state::NodeType;
use curvine_common::utils::ProtoUtils;
use curvine_common::FsResult;
use orpc::common::LocalTime;
use orpc::handler::MessageHandler;
use orpc::message::Message;
use std::sync::Arc;

pub struct PdRpcHandler {
    config_manager: Arc<ConfigManager>,
    mount_manager: Arc<MountManager>,
    namespace_manager: Arc<NamespaceManager>,
    cluster_manager: Arc<ClusterManager>,
}

impl PdRpcHandler {
    pub fn new(
        config_manager: Arc<ConfigManager>,
        mount_manager: Arc<MountManager>,
        namespace_manager: Arc<NamespaceManager>,
        cluster_manager: Arc<ClusterManager>,
    ) -> Self {
        Self {
            config_manager,
            mount_manager,
            namespace_manager,
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
                    .mount(&req.cv_path, &req.ufs_path, &mnt_opt)?;
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
                let summary = self
                    .cluster_manager
                    .metaroute_manager()
                    .build_client_summary()?;
                ctx.response(GetMetaRouteSummaryResponse {
                    summary: ProtoUtils::meta_route_summary_to_pb(&summary),
                })?
            }
            RpcCode::GetSimpleClusterView => {
                let _req: GetSimpleClusterViewRequest = ctx.parse_header()?;
                let view = self.cluster_manager.build_simple_cluster_view()?;
                ctx.response(GetSimpleClusterViewResponse {
                    view: ProtoUtils::simple_cluster_view_to_pb(&view),
                })?
            }
            RpcCode::GetBGTableSummary => {
                let req: GetBgTableSummaryRequest = ctx.parse_header()?;
                let table_id = u16::try_from(req.table_id).map_err(|_| {
                    FsError::common(format!("table_id out of range: {}", req.table_id))
                })?;
                let summary = self
                    .cluster_manager
                    .bgtable_manager()
                    .build_table_summary(table_id)
                    .map(|summary| ProtoUtils::bg_table_summary_to_pb(&summary));
                ctx.response(GetBgTableSummaryResponse { summary })?
            }
            RpcCode::CreateNamespace => {
                let req_pb: CreateNamespaceRequestProto = ctx.parse_header()?;
                let req = ProtoUtils::create_namespace_request_from_pb(req_pb)?;
                let name = req.name.clone();
                self.namespace_manager.create_namespace(req)?;
                let namespace = self
                    .namespace_manager
                    .get_namespace_by_name(&name)
                    .ok_or_else(|| FsError::not_found(format!("namespace {} not found", name)))?;
                ctx.response(CreateNamespaceResponseProto {
                    namespace: ProtoUtils::namespace_info_to_pb(&namespace),
                })?
            }
            RpcCode::GetNamespace => {
                let req: GetNamespaceRequestProto = ctx.parse_header()?;
                let namespace = if let Some(id) = req.id {
                    self.namespace_manager
                        .get_namespace(id as curvine_common::state::NamespaceId)
                } else if let Some(name) = req.name {
                    self.namespace_manager.get_namespace_by_name(&name)
                } else {
                    None
                };
                ctx.response(GetNamespaceResponseProto {
                    namespace: namespace
                        .as_ref()
                        .map(|ns| ProtoUtils::namespace_info_to_pb(ns)),
                })?
            }
            RpcCode::ListNamespaces => {
                let _req: ListNamespacesRequestProto = ctx.parse_header()?;
                let mut namespaces = self.namespace_manager.list_namespaces();
                namespaces.sort_by_key(|ns| ns.id);
                ctx.response(ListNamespacesResponseProto {
                    namespaces: namespaces
                        .iter()
                        .map(|ns| ProtoUtils::namespace_info_to_pb(ns))
                        .collect(),
                })?
            }
            RpcCode::NodeRegister => {
                self.cluster_manager.ensure_leader()?;
                let req_pb: NodeRegisterRequest = ctx.parse_header()?;
                let req = ProtoUtils::register_request_from_pb(req_pb)?;
                let resp = match req.base.node_type {
                    NodeType::Worker => self.cluster_manager.handle_worker_register(req)?,
                    NodeType::Meta => self.cluster_manager.handle_meta_register(req)?,
                    NodeType::Task => self.cluster_manager.handle_task_register(req)?,
                };
                ctx.response(ProtoUtils::register_response_to_pb(&resp))?
            }
            RpcCode::NodeHeartbeat => {
                self.cluster_manager.ensure_leader()?;
                let req_pb: NodeHeartbeatRequest = ctx.parse_header()?;
                let req = ProtoUtils::heartbeat_request_from_pb(req_pb)?;
                let resp = match req.node_type {
                    NodeType::Worker => self.cluster_manager.handle_worker_heartbeat(req)?,
                    NodeType::Meta => self.cluster_manager.handle_meta_heartbeat(req)?,
                    NodeType::Task => self.cluster_manager.handle_task_heartbeat(req)?,
                };
                ctx.response(ProtoUtils::heartbeat_response_to_pb(&resp))?
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
