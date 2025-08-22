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

use crate::master::fs::MasterFilesystem;
use crate::master::{JobWorkerClient, LoadJob, LoadManagerConfig, MountManager, TaskDetail};
use chrono::Utc;
use core::time::Duration;
use std::collections::linked_list::LinkedList;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::proto::{LoadState, LoadTaskReportRequest};
use curvine_common::state::{FileStatus, LoadJobOptions, MountInfo, WorkerAddress};
use log::{debug, error, info, warn};
use orpc::client::ClientFactory;
use orpc::common::ByteUnit;
use orpc::io::net::InetAddr;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::{CommonResult, err_box};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use curvine_client::unified::UfsFileSystem;
use curvine_common::FsResult;
use orpc::sync::FastDashMap;
use crate::master::fs::policy::ChooseContext;

/// Load the Task Manager
#[derive(Clone)]
pub struct LoadManager {
    jobs: Arc<FastDashMap<String, LoadJob>>,
    master_fs: MasterFilesystem,
    running: Arc<Mutex<bool>>,
    client_factory: Arc<ClientFactory>,
    config: LoadManagerConfig,
    mount_manager: Arc<MountManager>,
    rt: Arc<Runtime>,
}

impl LoadManager {
    pub fn from_cluster_conf(
        master_fs: MasterFilesystem,
        mount_manager: Arc<MountManager>,
        rt: Arc<Runtime>,
        conf: &ClusterConf,
    ) -> Self {
        let client_factory = Arc::new(ClientFactory::with_rt(
            conf.client.client_rpc_conf(),
            rt.clone(),
        ));

        Self {
            jobs: Arc::new(FastDashMap::default()),
            master_fs,
            running: Arc::new(Mutex::new(false)),
            client_factory,
            config: LoadManagerConfig::from_cluster_conf(conf),
            mount_manager,
            rt,
        }
    }

    /// Start the load manager
    pub fn start(&self) {
        let mut running = self.running.lock().unwrap();
        if *running {
            info!("LoadManager is already running");
            return;
        }
        *running = true;

        let cleanup_interval = Duration::from_secs(self.config.cleanup_interval_seconds);
        let jobs = self.jobs.clone();
        self.rt.spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);
            loop {
                interval.tick().await;
                Self::cleanup_expired_jobs(jobs.clone());
            }
        });
        info!("LoadManager started");
    }

    async fn get_worker_client(&self, worker: &WorkerAddress) -> FsResult<JobWorkerClient> {
        let worker_addr = InetAddr::new(worker.ip_addr.clone(), worker.rpc_port as u16);

        let client = self.client_factory.get(&worker_addr).await?;
        let timeout = Duration::from_millis(self.client_factory.conf().rpc_timeout_ms);
        let client = JobWorkerClient::new(client, timeout);
        Ok(client)
    }

    pub fn choose_worker(&self, block_size: i64) -> FsResult<WorkerAddress> {
        let ctx = ChooseContext::with_num(1, block_size, vec![]);
        let worker_mgr = self.master_fs.worker_manager.read();
        let workers = worker_mgr.choose_worker(ctx)?;
        if let Some(worker) = workers.first() {
            Ok(worker.clone())
        } else {
            err_box!("No available worker found")
        }
    }

    fn check_job_exists(
        &self,
        job_id: &str,
        source_status: &FileStatus,
        target_path: &Path
    ) -> bool {
        if source_status.is_dir {
            if let Some(job) = self.jobs.get(job_id) {
                job.state == LoadState::Pending || job.state == LoadState::Loading
            } else {
                false
            }
        } else {
            // 文件一般是自动加载，并且是并行执行的，校验ufs_mtime，防止分发大量重复任务。
            let cv_status = self.master_fs.file_status(target_path.path())?;
            if cv_status.storage_policy.ufs_mtime == 0 {
                false
            }  else {
                cv_status.storage_policy.ufs_mtime == source_status.mtime
            }
        }
    }


    /// Handle cancellation of tasks
    pub fn cancel_job(&self, job_id: String) -> CommonResult<bool> {
        // Get task information
        let mut cancel_result = true;
        let assigned_workers = {
            if let Some(mut job) = self.jobs.get_mut(&job_id) {
                // Check whether it can be canceled
                if job.state == LoadState::Completed
                    || job.state == LoadState::Failed
                    || job.state == LoadState::Canceled
                {
                    return err_box!("Cannot cancel job in state {}", job.state)
                }

                // Update status is Cancel
                job.update_state(LoadState::Canceled, "Canceling job");

                // Get the assigned Worker
                job.assigned_workers.clone()
            } else {
                return err_box!("Job {} not found", job_id)
            }
        };

        // Send a cancel request to all assigned Workers
        for worker in assigned_workers {
            let res = self.rt.block_on(async{
                let client = self.get_worker_client(&worker).await?;
                let res = client.cancel_job(&job_id).await?;
                Ok(res)
            });
            match res {
                Ok(res) => {
                    if !res.success {
                        cancel_result = false;
                        error!("Failed to send load task to worker{}: {:?}", worker, res.message);
                        self.update_job_state(
                            &job_id,
                            LoadState::Failed,
                            format!("Failed to send load task to worker{}: {:?}", worker, res.message)
                        );
                    }
                }

                Err(e) => {
                    cancel_result = false;
                    error!("Failed to send cancel load request to worker{}: {}", worker, e);
                    self.update_job_state(
                        &job_id,
                        LoadState::Failed,
                        format!("Failed to send cancel load request to worker {}: {}", worker, e)
                    );
                }
            }
        }

        Ok(cancel_result)
    }

    fn update_job_state(&self, job_id: &str, state: LoadState, message: impl Into<String>) {
        if let Some(mut job) = self.jobs.get_mut(job_id) {
            job.update_state(state, message);
        }
    }

    /// Handle the task status reported by Worker
    pub fn handle_task_report(&self, report: LoadTaskReportRequest) -> CommonResult<()> {
        let job_id = report.job_id.clone();
        let mut task_id = String::new();
        let mut path = String::new();
        let mut target_path = String::new();
        let mut total_size: u64 = 0;
        let mut loaded_size: u64 = 0;
        let message = report.message.unwrap_or_default();
        let state_name = LoadState::from_i32(report.state).unwrap();

        if let Some(metrics) = report.metrics {
            task_id = metrics.task_id;
            path = metrics.path;
            target_path = metrics.target_path;
            total_size = metrics.total_size.unwrap_or(0) as u64;
            loaded_size = metrics.loaded_size.unwrap_or(0) as u64;
        }

        info!(
            "Received task report for job {}, task {}: state={:?}, loaded={}/{}, message={}.",
            job_id,
            task_id,
            state_name.as_str_name(),
            loaded_size,
            total_size,
            message
        );

        // Update task status
        if let Some(mut job) = self.jobs.get_mut(&job_id) {
            // Update the status of subtasks, as well as progress information + job progress
            job.update_sub_task(
                &task_id,
                LoadState::from_i32(report.state).unwrap(),
                Some(loaded_size),
                Some(total_size),
                Some(message),
            );
            Ok(())
        } else {
            warn!("Received status update for unknown job: {}", job_id);
            err_box!("Job {} not found", job_id)
        }
    }

    pub fn submit_job(
        &self,
        path: &str,
        opts: LoadJobOptions,
    ) -> FsResult<(String, String)> {
        let source_path = Path::from_str(path)?;
        if source_path.is_cv() {
            return err_box!("No need to load cv path")
        }

        // check mount
        let mnt = if let Some(v) = self.mount_manager.get_mount_info(&source_path)? {
            v
        } else {
            return err_box!("Not found mount info for path: {}", source_path)
        };

        let target_path = mnt.get_cv_path(&source_path)?;
        let job_id = format!("job-{}", source_path);

        let ufs = UfsFileSystem::with_mount(&mnt)?;
        let source_status = self.rt.block_on(ufs.get_status(&source_path))?;

        info!("Submitting load job for path: {}", path);

        // check job status
        if self.check_job_exists(&job_id, &source_status, &target_path) {
            return Ok((job_id.clone(), target_path.clone_uri()))
        }

        // create job
        let mut job = LoadJob::new(
            job_id.clone(),
            source_path.clone_uri(),
            target_path.clone_path(),
            &opts,
            &mnt,
            &self.config
        );

        // 设置job已经提交。
        self.jobs.insert(job_id.clone(), job.clone());

        // 创建task。
        let tasks = match self.rt.block_on(self.create_all_tasks(job.clone(), source_status, &ufs, &mnt)) {
            Err(e) => {
                job.update_state(LoadState::Failed, format!("Failed to create tasks: {}", e));
                return Err(e);
            }

            Ok(v) => v
        };

        // 更新job状态
        let mut total_files = 0;
        let mut total_size  = 0;
        for task in tasks {
            debug!("all task: {:?}", task);
            total_files += 1;
            total_size += task.total_size.unwrap_or(0);
            job.add_sub_task(task);
        }
        job.update_state(
            LoadState::Loading,
            format!("job {} create succes, files: {}, total size: {}",
                    job_id, total_files, ByteUnit::byte_to_string(total_size)));
        self.jobs.insert(job_id.clone(), job);

        Ok((job_id, target_path.clone_path()))
    }

    async fn create_all_tasks(
        &self,
        mut job: LoadJob,
        source_status: FileStatus,
        ufs: &UfsFileSystem,
        mnt: &MountInfo,
    ) -> FsResult<LinkedList<TaskDetail>>{
        job.update_state(LoadState::Pending, "Assigning workers");
        let block_size = job.block_size;

        let mut tasks = LinkedList::new();
        let mut stack = LinkedList::new();
        stack.push_back(source_status);
        while let Some(status) = stack.pop_front() {
            if status.is_dir {
                let dir_path = Path::from_str(status.path)?;
                let childs = ufs.list_status(&dir_path).await?;
                for child in childs {
                    stack.push_back(child);
                }
            } else {
                let worker = self.choose_worker(block_size)?;

                job.update_state(LoadState::Loading, format!("Assigned to worker {}", worker));
                job.assign_worker(worker.clone());

                let source_path = Path::from_str(status.path)?;
                let target_path = mnt.get_cv_path(&source_path)?;

                let client = self.get_worker_client(&worker).await?;
                let mut task = client.submit_load_task(
                    worker.worker_id,
                    &job,
                    source_path.clone_uri(),
                    target_path.clone_path(),
                ).await?;
                task.total_size = Some(status.len as u64);

                info!("Added sub-task {} for job {}", task.task_id, job.job_id);
                tasks.push_back(task);
            }
        }

        Ok(tasks)
    }

    pub fn get_load_job_status(&self, job_id: String) -> Option<LoadJob> {
        match self.jobs.get(&job_id) {
            Some(job_ref) => {
                // DashMap returns Ref, and cloned value is required
                Some(job_ref.clone())
            }
            None => {
                Ok(None)
            }
        }
    }

    fn cleanup_expired_jobs(jobs: Arc<FastDashMap<String, LoadJob>>) {
        let now = Utc::now();
        let mut jobs_to_remove = Vec::new();

        // Collect tasks that need to be removed first
        for entry in jobs.iter() {
            let job = entry.value();
            if let Some(expire_time) = job.expire_time {
                if now > expire_time {
                    jobs_to_remove.push(job.job_id.clone());
                }
            }
        }

        for job_id in jobs_to_remove {
            if let Some(_) = jobs.remove(&job_id) {
                info!("Removing expired job: {}", job_id);
            }
        }
    }
}

/// Load manager error
#[derive(Debug, Error)]
pub enum LoadManagerError {
    #[error("The task does not exist: {0}")]
    JobNotFound(String),
    #[error("The task already exists: {0}")]
    JobAlreadyExists(String),
    #[error("There are no available worker nodes")]
    NoAvailableWorker,
    #[error("RPC error: {0}")]
    RpcError(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}
