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
use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub enum ConfigError {
    #[error("Missing value: use JSON body or query param value")]
    MissingValue,

    #[error("Missing key: use query param key")]
    MissingKey,

    #[error("{0}")]
    InternalError(String),
}

impl ConfigError {
    pub fn code(&self) -> &'static str {
        match self {
            ConfigError::MissingValue => "MISSING_VALUE",
            ConfigError::MissingKey => "MISSING_KEY",
            ConfigError::InternalError(_) => "INTERNAL_ERROR",
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            ConfigError::MissingValue | ConfigError::MissingKey => StatusCode::BAD_REQUEST,
            ConfigError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn missing_value() -> Self {
        ConfigError::MissingValue
    }

    pub fn missing_key() -> Self {
        ConfigError::MissingKey
    }

    pub fn internal_error(e: impl ToString) -> Self {
        ConfigError::InternalError(e.to_string())
    }
}

pub fn unknown_key_error(key: &str) -> String {
    format!(
        "unknown config key: {} (only keys registered in dynamic_config can be set)",
        key
    )
}
