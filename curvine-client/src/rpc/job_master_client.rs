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

use std::sync::Arc;
use std::time::Duration;
use curvine_common::fs::{Path, RpcCode};
use curvine_common::proto::{CancelLoadRequest, CancelLoadResponse, GetLoadStatusRequest, GetLoadStatusResponse, LoadJobRequest, LoadJobResponse, LoadMetrics, LoadTaskReportRequest, LoadTaskReportResponse};
use curvine_common::state::LoadJobOptions;
use orpc::CommonResult;
use prost::Message as PMessage;
use curvine_common::FsResult;
use curvine_common::utils::{ProtoUtils, RpcUtils};
use crate::file::FsClient;

/// Job master client
#[derive(Clone)]
pub struct JobMasterClient {
    client: Arc<FsClient>,
}

impl JobMasterClient {
    pub fn new(client: Arc<FsClient>) -> Self {
        Self { client}
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
        let rep: LoadJobResponse = self.client.rpc(RpcCode::SubmitLoadJob, req).await?;
        Ok(rep)
    }

    /// Get loading task status according to the path
    pub async fn get_load_status(&self, job_id: &str) -> CommonResult<GetLoadStatusResponse> {
        let req = GetLoadStatusRequest {
            job_id: job_id.to_string(),
            verbose: Option::from(false),
        };

        let rep: GetLoadStatusResponse = self.client.rpc(RpcCode::GetLoadStatus, req).await?;

        Ok(rep)
    }

    /// Cancel the loading task
    pub async fn cancel_load(&self, job_id: &str) -> CommonResult<CancelLoadResponse> {
        let req = CancelLoadRequest {
            job_id: job_id.to_string(),
        };

        let rep: CancelLoadResponse = self.client.rpc(RpcCode::CancelLoadJob, req).await?;

        Ok(rep)
    }

    pub async fn report_task_status(&self, request: LoadTaskReportRequest) -> FsResult<()> {
        let _: LoadTaskReportResponse = self.client.rpc(RpcCode::ReportLoadTask, request).await?;
        Ok(())
    }
}
