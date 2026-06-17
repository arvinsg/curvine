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

use num_enum::{FromPrimitive, IntoPrimitive};
use std::fmt;

#[repr(i8)]
#[derive(Debug, IntoPrimitive, FromPrimitive, PartialEq, Eq, Hash, Copy, Clone)]
pub enum RpcCode {
    #[num_enum(default)]
    Undefined = 0,
    Heartbeat = 1,

    // filesystem API
    Mkdir = 2,
    Delete = 3,
    CreateFile = 4,
    OpenFile = 5,
    AppendFile = 6,
    FileStatus = 7,
    ListStatus = 8,
    Exists = 9,
    Rename = 10,
    AddBlock = 11,
    CompleteFile = 12,
    GetBlockLocations = 13,
    GetMasterInfo = 14,
    SetAttr = 15,
    Symlink = 16,
    Link = 17,
    ResizeFile = 18,
    AssignWorker = 19,
    GetLock = 20,
    SetLock = 21,
    ListLock = 22,
    CreateFilesBatch = 23,
    AddBlocksBatch = 24,
    CompleteFilesBatch = 25,

    // manager interface.
    Mount = 30,
    UnMount = 31,
    UpdateMount = 32,
    GetMountTable = 33,
    GetMountInfo = 34,

    SubmitJob = 35,
    GetJobStatus = 36,
    CancelJob = 37,
    ReportTask = 38,
    SubmitTask = 39,
    WorkerHeartbeat = 40,
    WorkerBlockReport = 41,

    SubmitBlockReplicationJob = 42,
    ReportBlockReplicationResult = 43,

    MetricsReport = 60,

    // block interface.
    WriteBlock = 80,
    ReadBlock = 81,
    WriteBlocksBatch = 82,
    WriteCommitsBatch = 83,

    // pd config interface.
    GetConfig = 101,
    ListConfig = 102,
    SetConfig = 103,

    // pd metanode route.
    GetMetaRouteSummary = 104,

    // pd node register / heartbeat.
    NodeRegister = 105,
    NodeHeartbeat = 106,
}

impl RpcCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            RpcCode::Undefined => "Undefined",
            RpcCode::Heartbeat => "Heartbeat",
            RpcCode::Mkdir => "Mkdir",
            RpcCode::Delete => "Delete",
            RpcCode::CreateFile => "CreateFile",
            RpcCode::OpenFile => "OpenFile",
            RpcCode::AppendFile => "AppendFile",
            RpcCode::FileStatus => "FileStatus",
            RpcCode::ListStatus => "ListStatus",
            RpcCode::Exists => "Exists",
            RpcCode::Rename => "Rename",
            RpcCode::AddBlock => "AddBlock",
            RpcCode::CompleteFile => "CompleteFile",
            RpcCode::GetBlockLocations => "GetBlockLocations",
            RpcCode::GetMasterInfo => "GetMasterInfo",
            RpcCode::SetAttr => "SetAttr",
            RpcCode::Symlink => "Symlink",
            RpcCode::Link => "Link",
            RpcCode::ResizeFile => "ResizeFile",
            RpcCode::AssignWorker => "AssignWorker",
            RpcCode::GetLock => "GetLock",
            RpcCode::SetLock => "SetLock",
            RpcCode::ListLock => "ListLock",
            RpcCode::CreateFilesBatch => "CreateFilesBatch",
            RpcCode::AddBlocksBatch => "AddBlocksBatch",
            RpcCode::CompleteFilesBatch => "CompleteFilesBatch",
            RpcCode::Mount => "Mount",
            RpcCode::UnMount => "UnMount",
            RpcCode::UpdateMount => "UpdateMount",
            RpcCode::GetMountTable => "GetMountTable",
            RpcCode::GetMountInfo => "GetMountInfo",
            RpcCode::SubmitJob => "SubmitJob",
            RpcCode::GetJobStatus => "GetJobStatus",
            RpcCode::CancelJob => "CancelJob",
            RpcCode::ReportTask => "ReportTask",
            RpcCode::SubmitTask => "SubmitTask",
            RpcCode::WorkerHeartbeat => "WorkerHeartbeat",
            RpcCode::WorkerBlockReport => "WorkerBlockReport",
            RpcCode::SubmitBlockReplicationJob => "SubmitBlockReplicationJob",
            RpcCode::ReportBlockReplicationResult => "ReportBlockReplicationResult",
            RpcCode::MetricsReport => "MetricsReport",
            RpcCode::WriteBlock => "WriteBlock",
            RpcCode::ReadBlock => "ReadBlock",
            RpcCode::WriteBlocksBatch => "WriteBlocksBatch",
            RpcCode::WriteCommitsBatch => "WriteCommitsBatch",
            RpcCode::GetConfig => "GetConfig",
            RpcCode::ListConfig => "ListConfig",
            RpcCode::SetConfig => "SetConfig",
            RpcCode::GetMetaRouteSummary => "GetMetaRouteSummary",
            RpcCode::NodeRegister => "NodeRegister",
            RpcCode::NodeHeartbeat => "NodeHeartbeat",
        }
    }

    pub fn is_high_frequency(&self) -> bool {
        matches!(
            self,
            RpcCode::NodeHeartbeat | RpcCode::WorkerHeartbeat | RpcCode::WorkerBlockReport
        )
    }
}

impl fmt::Display for RpcCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
