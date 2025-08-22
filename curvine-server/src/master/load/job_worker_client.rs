use std::time::Duration;
use curvine_common::fs::RpcCode;
use curvine_common::FsResult;
use curvine_common::proto::{CancelLoadRequest, CancelLoadResponse, LoadTaskRequest, LoadTaskResponse};
use orpc::client::RpcClient;
use crate::master::{LoadJob, TaskDetail};
use prost::Message as PMessage;
use curvine_common::state::MountInfo;
use curvine_common::utils::RpcUtils;

#[derive(Clone)]
pub struct JobWorkerClient {
    client: RpcClient,
    timeout: Duration
}

impl JobWorkerClient {
    pub fn new(client: RpcClient, timeout: Duration) -> Self {
        Self { client, timeout }
    }

    pub async fn rpc<T, R>(&self, code: RpcCode, header: T) -> FsResult<R>
        where
            T: PMessage + Default,
            R: PMessage + Default,
    {
        RpcUtils::proto_rpc(&self.client, self.timeout, code, header).await
    }

    pub async fn submit_load_task(
        &self,
        worker_id: u32,
        job: &LoadJob,
        source_path: String,
        target_path: String,
        mnt: &MountInfo,
    ) -> FsResult<TaskDetail> {
        let request = LoadTaskRequest {
            job_id: job.job_id.to_string(),
            source_path: source_path.to_string(),
            target_path: target_path.to_string(),
            replicas: job.replicas,
            block_size: job.block_size,
            storage_type: job.storage_type.into(),
            ttl_ms: job.ttl_ms,
            ttl_action: job.ttl_action.into(),
            ufs_conf: mnt.properties.clone(),
        };

        let response: LoadTaskResponse = self.rpc(RpcCode::SubmitLoadTask, request).await?;
        let task_detail = TaskDetail::new(
            response.task_id,
            source_path,
            target_path,
            worker_id,
        );

        Ok(task_detail)
    }

    pub async fn cancel_job(&self, job_id: &str) -> FsResult<CancelLoadResponse> {
        let request = CancelLoadRequest {
            job_id: job_id.to_string(),
        };

        let response: CancelLoadResponse = self.rpc(RpcCode::CancelLoadJob, request).await?;
        Ok(response)
    }
}