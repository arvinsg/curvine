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

use crate::worker::load::LoadTask;
use curvine_client::file::{CurvineFileSystem, FsClient, FsWriter};
use curvine_client::unified::{UfsFileSystem, UnifiedReader};
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use log::info;
use std::sync::{Arc, Mutex};
use curvine_client::rpc::JobMasterClient;
use curvine_common::FsResult;
use curvine_common::proto::{LoadMetrics, LoadTaskReportRequest};
use curvine_common::state::CreateFileOptsBuilder;
use orpc::common::{LocalTime, TimeSpent};
use orpc::err_box;
use orpc::sync::FastDashMap;

pub struct TaskRunner {
    task: LoadTask,
    tasks: Arc<FastDashMap<String, LoadTask>>,
    fs: CurvineFileSystem,
    master_client: JobMasterClient,
    progress_interval_ms: u64,
    task_timeout_ms: u64,
}

impl TaskRunner {
    pub fn new(
        task: LoadTask,
        tasks: Arc<FastDashMap<String, LoadTask>>,
        fs: CurvineFileSystem,
        master_client: JobMasterClient,
        progress_interval_ms: u64,
        task_timeout_ms: u64,
    ) -> Self {
        Self {
            task,
            tasks,
            fs,
            master_client,
            progress_interval_ms,
            task_timeout_ms,
        }
    }

    pub async fn run(&self) -> FsResult<()> {
        let (mut reader, mut writer) = self.create_stream().await?;

        // copy data
        let mut last_progress_time = LocalTime::mills();
        let mut read_cost = 0;
        let mut total_cost = 0;

        loop {
            let mut spend = TimeSpent::new();
            let chunk = reader.read_chunk0().await?;
            read_cost += spend.used_ms();

            if chunk.is_empty() {
                break;
            }

            self.writer.write_chunk(chunk).await?;
            total_cost += spend.used_ms();

            if LocalTime::mills() > last_progress_time + self.progress_interval_ms {
                last_progress_time = LocalTime::mills();
                self.update_progress(writer.pos(), reader.len()).await;
            }

            if total_cost > self.task_timeout_ms {
                return err_box!("Task {} for job {} exceed timeout {} ms",
                    task.task_id,
                    task.job_id,
                    self.task_timeout_ms,
                )
            }
        }

        reader.complete().await?;
        writer.complete().await?;
        self.update_progress(writer.pos(), reader.len()).await;

        info!("task {} for job {} completed, copy bytes {}, read cost {} ms, task cost {} ms",
            task.task_id,
            task.job_id,
            writer.pos(),
            read_cost,
            total_cost,
        );

        Ok(())
    }

    async fn create_stream(&self) -> FsResult<(UnifiedReader, FsWriter)> {
        let task = &self.task;

        // create ufs reader
        let source_path = Path::from_str(&task.source_path)?;
        let ufs = UfsFileSystem::new(&source_path, task.ufs_conf.clone())?;
        let reader = ufs.open(&source_path).await?;

        // create cv writer
        let target_path = Path::from_str(&task.target_path)?;
        let opts = CreateFileOptsBuilder::new()
            .overwrite(true)
            .replicas(task.replicas)
            .block_size(task.block_size)
            .storage_type(task.storage_type)
            .ttl_ms(task.ttl_ms)
            .ttl_action(task.ttl_action)
            .build();
        let writer = self.fs.create_with_opts(&target_path, opts).await?;

        Ok((reader, writer))
    }

    pub async fn update_progress(&self, loaded_size: i64, total_size: i64) {
        if let Err(e) = self.update_progress0(loaded_size, total_size).await {
            log::warn!("update progress failed, err: {:?}", e);
        }
    }

    pub async fn update_progress0(&self, loaded_size: i64, total_size: i64) -> FsResult<()> {
        if let Some(mut task) = self.tasks.get_mut(&self.task.task_id) {
            task.update_progress(loaded_size as u64, total_size as u64);
        }

        let task = &self.task;
        let metrics = LoadMetrics {
            job_id: task.job_id.clone(),
            task_id: task.task_id.clone(),
            path: task.source_path.clone(),
            target_path: task.target_path.clone(),
            total_size: if task.total_size > 0 {
                Some(task.total_size as i64)
            } else {
                None
            },
            loaded_size: Some(task.loaded_size as i64),
            create_time: Some(task.create_time as i64),
            update_time: Some(task.update_time as i64),
            expire_time: None,
        };

        let report_request = LoadTaskReportRequest {
            job_id: task.job_id.clone(),
            state: task.state as i32,
            worker_id,
            metrics: Some(metrics),
            message: if task.message.is_empty() {
                None
            } else {
                Some(task.message.clone())
            },
        };

        self.master_client.report_task_status(report_request).await
    }
}