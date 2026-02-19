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

use crate::pd::config::http_handler::{
    delete_config_handler, get_config_handler, list_configs_handler, set_config_by_query_handler,
    set_config_handler,
};
use crate::pd::config::ConfigManager;
use axum::routing::{delete, get, put};
use axum::Router;
use curvine_web::router::RouterHandler;
use std::sync::Arc;

#[derive(Clone)]
pub struct PdRouterHandler {
    pub(crate) config_manager: Arc<ConfigManager>,
}

impl PdRouterHandler {
    pub fn new(config_manager: Arc<ConfigManager>) -> Self {
        Self { config_manager }
    }
}

impl RouterHandler for PdRouterHandler {
    fn router(&self) -> Router {
        Router::new()
            .route("/api/v1/config/set", put(set_config_by_query_handler))
            .route("/api/v1/config/:key", get(get_config_handler))
            .route("/api/v1/config/:key", put(set_config_handler))
            .route("/api/v1/config/:key", delete(delete_config_handler))
            .route("/api/v1/configs", get(list_configs_handler))
            .with_state(self.config_manager.clone())
    }
}
