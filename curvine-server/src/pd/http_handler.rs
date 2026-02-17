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

use crate::pd::config::http_handler::*;
use crate::pd::config::ConfigManager;
use crate::pd::mount::http_handler::*;
use crate::pd::mount::MountManager;
use axum::routing::{delete, get, post, put};
use axum::{Extension, Router};
use curvine_web::router::RouterHandler;
use std::sync::Arc;

#[derive(Clone)]
pub struct PdHttpHandler {
    pub(crate) config_manager: Arc<ConfigManager>,
    pub(crate) mount_manager: Arc<MountManager>,
}

impl PdHttpHandler {
    pub fn new(config_manager: Arc<ConfigManager>, mount_manager: Arc<MountManager>) -> Self {
        Self {
            config_manager,
            mount_manager,
        }
    }
}

impl RouterHandler for PdHttpHandler {
    fn router(&self) -> Router {
        let instance = Arc::new(self.clone());
        Router::new()
            .route("/api/v1/config/set", put(set_config_by_query_handler))
            .route("/api/v1/config/:key", get(get_config_handler))
            .route("/api/v1/config/:key", put(set_config_handler))
            .route("/api/v1/config/:key", delete(delete_config_handler))
            .route("/api/v1/config", get(list_configs_handler))
            .route("/api/v1/mount", get(list_mounts_handler))
            .route("/api/v1/mount", post(create_mount_handler))
            .route("/api/v1/mount", delete(delete_mount_handler))
            .route("/api/v1/mount/path", get(get_mount_by_path_handler))
            .layer(Extension(instance))
    }
}
