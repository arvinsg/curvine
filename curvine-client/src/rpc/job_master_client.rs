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

use std::time::Duration;
use curvine_common::fs::{Path, RpcCode};
use curvine_common::proto::{
    CancelLoadRequest, CancelLoadResponse, GetLoadStatusRequest, GetLoadStatusResponse,
    LoadJobRequest, LoadJobResponse,
};
use curvine_common::state::LoadJobOptions;
use orpc::CommonResult;
use prost::Message as PMessage;
use curvine_common::FsResult;
use curvine_common::utils::{ProtoUtils, RpcUtils};
use orpc::client::RpcClient;

/// Job master client
#[derive(Clone)]
pub struct JobMasterClient {
    client: RpcClient,
    timeout: Duration,
}

impl JobMasterClient {
    pub fn new(client: RpcClient, timeout: Duration) -> Self {
        Self { client, timeout}
    }


    pub async fn rpc<T, R>(&self, code: RpcCode, header: T) -> FsResult<R>
        where
            T: PMessage + Default,
            R: PMessage + Default,
    {
        RpcUtils::proto_rpc(&self.client, self.timeout, code, header).await
    }

    // Submit loading task
    pub async fn submit_load(
        &self,
        path: &Path,
        opts: LoadJobOptions,
    ) -> FsResult<LoadJobResponse> {
        // Create a request
        let req = LoadJobRequest {
            path: path.encode_uri(),
            job_options: ProtoUtils::job_options_to_pb(opts)
        };
        let rep: LoadJobResponse = self.rpc(RpcCode::SubmitLoadJob, req).await?;
        Ok(rep)
    }

    /// Get loading task status according to the path
    pub async fn get_load_status(&self, job_id: &str) -> CommonResult<GetLoadStatusResponse> {
        // Create a request
        let req = GetLoadStatusRequest {
            job_id: job_id.to_string(),
            verbose: Option::from(false),
        };

        // Send a request
        let rep: GetLoadStatusResponse = self.rpc(RpcCode::GetLoadStatus, req).await?;

        Ok(rep)
    }

    /// Cancel the loading task
    pub async fn cancel_load(&self, job_id: &str) -> CommonResult<CancelLoadResponse> {
        // Create a request
        let req = CancelLoadRequest {
            job_id: job_id.to_string(),
        };

        let rep: CancelLoadResponse = self.rpc(RpcCode::CancelLoadJob, req).await?;

        Ok(rep)
    }
}
