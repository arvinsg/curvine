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

#![allow(unused)]

use std::collections::HashMap;
use chrono::{DateTime, Utc};
use curvine_common::proto::{LoadState, LoadTaskRequest};
use orpc::io::IOError;
use thiserror::Error;
use tokio::sync::mpsc;
use uuid::Uuid;

/// A load task is a task that is loaded from an external storage to a local computer
#[derive(Debug, Clone)]
pub struct LoadTask {
    /// Task ID (subtask ID)
    pub task_id: String,
    /// The job ID to which the task belongs
    pub job_id: String,
    /// Source path
    pub path: String,
    /// Destination path
    pub target_path: String,
    /// Task status
    pub state: LoadState,
    /// Status messages, such as error messages
    pub message: String,
    /// Total Size (Bytes)
    pub total_size: u64,
    /// Loaded Size (Bytes)
    pub loaded_size: u64,
    /// Creation time
    pub create_time: DateTime<Utc>,
    /// Updated time
    pub update_time: DateTime<Utc>,

    pub replicas: i32,
    pub block_size: i64,
    pub storage_type: StorageType,
    pub ttl_ms: i64,
    pub ttl_action: TtlAction,

    pub ufs_conf: HashMap<String, String>,
}

impl LoadTask {
    /// Create a new load task
    pub fn new(req: LoadTaskRequest) -> Self {
        let now = Utc::now();
        Self {
            task_id: format!("task-{}", req.source_path),
            job_id: req.job_id,
            path: req.source_path,
            target_path: req.target_path,
            state: LoadState::Pending,
            message: String::new(),
            total_size: 0,
            loaded_size: 0,
            create_time: now,
            update_time: now,
            replicas: req.replicas,
            block_size: req.block_size,
            storage_type: req.storage_type.into(),
            ttl_ms: req.ttl_ms,
            ttl_action: req.ttl_action.into(),
            ufs_conf: req.ufs_conf,
        }
    }

    /// Update the task status
    pub fn update_state(&mut self, state: LoadState, message: Option<String>) {
        self.state = state;
        if let Some(msg) = message {
            self.message = msg;
        }
        self.update_time = Utc::now();
    }

    /// Set the total size
    pub fn set_total_size(&mut self, size: u64) {
        self.total_size = size;
        self.update_time = Utc::now();
    }

    /// Update progress
    pub fn update_progress(&mut self, loaded_size: u64, message: Option<String>) {
        self.loaded_size = loaded_size;
        self.update_time = Utc::now();
        if self.total_size == self.loaded_size {
            self.update_state(LoadState::Completed, message);
        }
    }
}

/// Worker loading processor error
#[derive(Debug, Error)]
pub enum WorkerLoadError {
    #[error("The task does not exist: {0}")]
    TaskNotFound(String),
    #[error("The task already exists: {0}")]
    TaskAlreadyExists(String),
    #[error("Loading error: {0}")]
    LoadError(String),
    #[error("IO error: {0}")]
    IoError(#[from] IOError),
}

/// The type of task action
#[derive(Clone)]
pub(crate) enum TaskOperation {
    /// Submit a new task
    Submit(LoadTask),
    /// Cancel the task
    Cancel(String),
    /// Get the task status
    GetStatus(String, mpsc::Sender<Option<LoadTask>>),
    /// Get all the tasks
    GetAll(mpsc::Sender<Vec<LoadTask>>),
}

use curvine_common::conf::ClusterConf;
use curvine_common::state::{StorageType, TtlAction};

/// Task execution configuration
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TaskExecutionConfig {
    /// Progress Report Interval (ms)
    pub(crate) status_report_interval_ms: u64,
    /// Default block size (bytes)
    pub(crate) default_chunk_size: usize,
    /// Number of buffers in the default buffer（Number of）
    pub(crate) default_buffer_size: usize,
    /// The maximum number of concurrent tasks
    pub(crate) max_concurrent_tasks: u8,
    /// Task timeout period (seconds)
    pub(crate) task_timeout_seconds: i64,
}

impl Default for TaskExecutionConfig {
    fn default() -> Self {
        Self {
            status_report_interval_ms: 1000,
            default_chunk_size: 1024 * 1024,
            default_buffer_size: 16,
            max_concurrent_tasks: 5,
            task_timeout_seconds: 3600,
        }
    }
}
/// Task execution configuration
impl TaskExecutionConfig {
    /// Create a task execution configuration from the cluster configuration
    pub fn from_cluster_conf(conf: &ClusterConf) -> Self {
        let load_conf = &conf.worker.load;
        Self {
            status_report_interval_ms: load_conf.task_status_report_interval_ms,
            default_chunk_size: load_conf.task_read_chunk_size_bytes as usize,
            default_buffer_size: load_conf.task_transfer_buffer_count,
            max_concurrent_tasks: load_conf.task_transfer_max_concurrent_tasks,
            task_timeout_seconds: load_conf.task_timeout_seconds,
        }
    }

    fn new(
        progress_report_interval_ms: u64,
        default_chunk_size: usize,
        max_concurrent_tasks: u8,
        task_timeout_seconds: i64,
    ) -> Self {
        Self {
            status_report_interval_ms: progress_report_interval_ms,
            default_chunk_size,
            default_buffer_size: 16,
            max_concurrent_tasks,
            task_timeout_seconds,
        }
    }

    /// Obtain task timeout period (seconds)
    pub fn task_timeout_seconds(&self) -> i8 {
        self.task_timeout_seconds as i8
    }

    /// Obtain the maximum number of concurrent tasks
    pub fn max_concurrent_tasks(&self) -> u8 {
        self.max_concurrent_tasks
    }

    /// Get Progress Report Interval (ms)
    pub fn progress_report_interval_ms(&self) -> u64 {
        self.status_report_interval_ms
    }

    /// Get the default block size (bytes)
    pub fn default_chunk_size(&self) -> usize {
        self.default_chunk_size
    }
}
