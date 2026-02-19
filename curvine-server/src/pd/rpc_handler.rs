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
use crate::pd::rpc_context::RpcContext;
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::FsResult;
use orpc::handler::MessageHandler;
use orpc::message::Message;
use orpc::runtime::{RpcRuntime, Runtime};
use std::sync::Arc;

pub struct PdHandler {
    config_manager: Arc<ConfigManager>,
    rt: Arc<Runtime>,
}

impl PdHandler {
    pub fn new(config_manager: Arc<ConfigManager>, rt: Arc<Runtime>) -> Self {
        Self { config_manager, rt }
    }
}

impl MessageHandler for PdHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let mut ctx = RpcContext::new(msg);

        let response = match ctx.code {
            RpcCode::GetConfig => {
                let req = ctx.parse_header().map_err(|e| {
                    FsError::from(format!("Failed to parse GetConfigRequest: {}", e))
                })?;
                let resp = self.config_manager.get_config(req)?;
                ctx.response(resp)?
            }
            RpcCode::ListConfig => {
                let req = ctx.parse_header().map_err(|e| {
                    FsError::from(format!("Failed to parse ListConfigRequest: {}", e))
                })?;
                let resp = self.config_manager.list_config(req)?;
                ctx.response(resp)?
            }
            RpcCode::SetConfig => {
                let req = ctx.parse_header().map_err(|e| {
                    FsError::from(format!("Failed to parse SetConfigRequest: {}", e))
                })?;
                let manager = self.config_manager.clone();
                let resp = self
                    .rt
                    .block_on(async move { manager.set_config(req).await })?;
                ctx.response(resp)?
            }
            RpcCode::DeleteConfig => {
                let req = ctx.parse_header().map_err(|e| {
                    FsError::from(format!("Failed to parse DeleteConfigRequest: {}", e))
                })?;
                let manager = self.config_manager.clone();
                let resp = self
                    .rt
                    .block_on(async move { manager.delete_config(req).await })?;
                ctx.response(resp)?
            }
            RpcCode::Undefined => {
                return Err(FsError::from("PD RPC: undefined config code".to_string()))
            }
            _ => {
                return Err(FsError::from(format!(
                    "Unsupported request type: {:?}",
                    ctx.code
                )))
            }
        };

        Ok(response)
    }
}
