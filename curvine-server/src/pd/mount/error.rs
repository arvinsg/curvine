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

use axum::http::StatusCode;
use curvine_common::error::FsError;
use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub enum MountError {
    #[error("cv path and ufs path are required")]
    CvUfsRequired,

    #[error("path query parameter is required")]
    PathQueryRequired,

    #[error("{0}")]
    InvalidArgument(String),

    #[error("{0}")]
    InvalidPath(String),

    /// Mount path (cv_path or ufs_path) already exists in mount table
    #[error("{0}")]
    MountPathExists(String),

    /// Mount path conflict (e.g. prefix conflict with existing mount)
    #[error("{0}")]
    MountPathConflict(String),

    #[error("{0}")]
    NotFound(String),

    #[error("{0}")]
    StaleEntry(String),

    #[error("{0}")]
    InternalError(String),
}

impl MountError {
    pub fn code(&self) -> &'static str {
        match self {
            MountError::CvUfsRequired => "CV_UFS_REQUIRED",
            MountError::PathQueryRequired => "PATH_REQUIRED",
            MountError::InvalidArgument(_) => "INVALID_ARGUMENT",
            MountError::InvalidPath(_) => "INVALID_PATH",
            MountError::MountPathExists(_) => "MOUNT_PATH_EXISTS",
            MountError::MountPathConflict(_) => "MOUNT_PATH_CONFLICT",
            MountError::NotFound(_) => "NOT_FOUND",
            MountError::StaleEntry(_) => "STALE_ENTRY",
            MountError::InternalError(_) => "INTERNAL_ERROR",
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            MountError::CvUfsRequired
            | MountError::PathQueryRequired
            | MountError::InvalidArgument(_)
            | MountError::InvalidPath(_) => StatusCode::BAD_REQUEST,
            MountError::MountPathExists(_)
            | MountError::MountPathConflict(_)
            | MountError::StaleEntry(_) => StatusCode::CONFLICT,
            MountError::NotFound(_) => StatusCode::NOT_FOUND,
            MountError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    // Map FsError from mount manager to MountError for HTTP response.
    pub fn from_fs_error(e: FsError) -> Self {
        let msg = e.to_string();
        match &e {
            FsError::InvalidArgument(_) => MountError::InvalidArgument(msg),
            FsError::InvalidPath(_) => MountError::InvalidPath(msg),
            FsError::MountPathExists(_) => MountError::MountPathExists(msg),
            FsError::MountPathConflict(_) => MountError::MountPathConflict(msg),
            FsError::NotFound(_) => MountError::NotFound(msg),
            FsError::StaleEntry(_) => MountError::StaleEntry(msg),
            _ => MountError::InternalError(msg),
        }
    }

    pub fn cv_ufs_required() -> Self {
        MountError::CvUfsRequired
    }

    pub fn namespace_required() -> Self {
        MountError::InvalidArgument("namespace must be specified".to_string())
    }

    pub fn path_query_required() -> Self {
        MountError::PathQueryRequired
    }

    pub fn invalid_path(e: impl ToString) -> Self {
        MountError::InvalidPath(e.to_string())
    }

    pub fn internal_error(e: impl ToString) -> Self {
        MountError::InternalError(e.to_string())
    }
}
