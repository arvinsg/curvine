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
pub enum NamespaceError {
    #[error("{0}")]
    InvalidArgument(String),

    #[error("{0}")]
    AlreadyExists(String),

    #[error("{0}")]
    NotFound(String),

    #[error("{0}")]
    StaleEntry(String),

    #[error("{0}")]
    InternalError(String),
}

impl NamespaceError {
    pub fn code(&self) -> &'static str {
        match self {
            NamespaceError::InvalidArgument(_) => "INVALID_ARGUMENT",
            NamespaceError::AlreadyExists(_) => "NAMESPACE_EXISTS",
            NamespaceError::NotFound(_) => "NOT_FOUND",
            NamespaceError::StaleEntry(_) => "STALE_ENTRY",
            NamespaceError::InternalError(_) => "INTERNAL_ERROR",
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            NamespaceError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
            NamespaceError::AlreadyExists(_) | NamespaceError::StaleEntry(_) => {
                StatusCode::CONFLICT
            }
            NamespaceError::NotFound(_) => StatusCode::NOT_FOUND,
            NamespaceError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Map an `FsError` from the namespace manager to an HTTP-facing error.
    pub fn from_fs_error(e: FsError) -> Self {
        let msg = e.to_string();
        match &e {
            FsError::InvalidArgument(_) | FsError::Common(_) => {
                NamespaceError::InvalidArgument(msg)
            }
            FsError::AlreadyExists(_) => NamespaceError::AlreadyExists(msg),
            FsError::NotFound(_) => NamespaceError::NotFound(msg),
            FsError::StaleEntry(_) => NamespaceError::StaleEntry(msg),
            _ => NamespaceError::InternalError(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fs_error_maps_to_code_and_status() {
        let cases = [
            (
                FsError::invalid_argument("bad name"),
                "INVALID_ARGUMENT",
                StatusCode::BAD_REQUEST,
            ),
            (
                FsError::common("some internal detail"),
                "INVALID_ARGUMENT",
                StatusCode::BAD_REQUEST,
            ),
            (
                FsError::already_exists("ns exists"),
                "NAMESPACE_EXISTS",
                StatusCode::CONFLICT,
            ),
            (
                FsError::not_found("ns missing"),
                "NOT_FOUND",
                StatusCode::NOT_FOUND,
            ),
            (
                FsError::stale_entry("create_namespace", 1, "conflict"),
                "STALE_ENTRY",
                StatusCode::CONFLICT,
            ),
        ];
        for (fs_err, code, status) in cases {
            let err = NamespaceError::from_fs_error(fs_err);
            assert_eq!(err.code(), code);
            assert_eq!(err.status_code(), status);
        }
    }
}
