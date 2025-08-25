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

//! Worker side data loading module
//! Responsible for loading data from external storage systems (such as S3) to local storage

pub mod load_task;
mod task_processor;
mod ufs_connector;
mod worker_load_handler;

pub use load_task::LoadTask;
pub use ufs_connector::{CurvineFsWriter, UfsConnector};
pub use worker_load_handler::FileLoadService;

mod task_runner;

mod task_manager;
pub use self::task_manager::TaskState;