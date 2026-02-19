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

use crate::conf::JournalConf;
use crate::rocksdb::DBConf;
use crate::version;
use log::info;
use orpc::common::{DurationUnit, Utils};
use orpc::io::net::InetAddr;
use orpc::server::ServerConf;
use orpc::{err_box, try_err, CommonResult};
use serde::{Deserialize, Serialize};
use std::fs::read_to_string;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PdConf {
    pub cluster_id: String,
    pub hostname: String,
    pub rpc_port: u16,
    pub web_port: u16,
    pub io_threads: usize,
    pub worker_threads: usize,
    pub io_timeout: String,
    pub io_close_idle: bool,

    pub data_dir: String,

    #[serde(default)]
    pub journal: JournalConf,
}

impl Default for PdConf {
    fn default() -> Self {
        Self {
            cluster_id: PdConf::DEFAULT_CLUSTER_ID.to_string(),
            hostname: PdConf::DEFAULT_HOSTNAME.to_string(),
            rpc_port: PdConf::DEFAULT_RPC_PORT,
            web_port: PdConf::DEFAULT_WEB_PORT,
            io_threads: 32,
            worker_threads: Utils::worker_threads(32),
            io_timeout: "10m".to_string(),
            io_close_idle: true,

            data_dir: default_data_dir(),
            journal: JournalConf::default(),
        }
    }
}

impl PdConf {
    pub const DEFAULT_HOSTNAME: &'static str = "localhost";
    pub const DEFAULT_CLUSTER_ID: &'static str = "curvine";
    pub const DEFAULT_RPC_PORT: u16 = 2379;
    pub const DEFAULT_WEB_PORT: u16 = 2380;

    pub fn from<T: AsRef<str>>(path: T) -> CommonResult<Self> {
        let path = path.as_ref();
        let s = try_err!(read_to_string(path));
        let conf = try_err!(toml::from_str::<Self>(&s));
        Ok(conf)
    }

    /// Validate required fields so startup fails fast with a clear error.
    pub fn check(&self) -> CommonResult<()> {
        if self.data_dir.is_empty() {
            return err_box!("PD config: data_dir must not be empty");
        }
        if self.journal.journal_dir.is_empty() {
            return err_box!("PD config: journal.journal_dir must not be empty");
        }
        if self.journal.journal_addrs.is_empty() {
            return err_box!(
                "PD config: journal.journal_addrs must not be empty (at least one Raft peer)"
            );
        }
        Ok(())
    }

    pub fn pd_server_conf(&self) -> ServerConf {
        let mut conf = ServerConf::with_hostname(&self.hostname, self.rpc_port);
        conf.name = format!("{}-pd", self.cluster_id);
        conf.io_threads = self.io_threads;
        conf.worker_threads = self.worker_threads;
        conf.close_idle = self.io_close_idle;
        conf.timeout_ms = self.io_timeout_ms();

        conf
    }

    pub fn pd_web_conf(&self) -> ServerConf {
        let mut conf = ServerConf::with_hostname(&self.hostname, self.web_port);
        conf.name = format!("{}-pd", self.cluster_id);
        conf.io_threads = self.io_threads;
        conf.worker_threads = self.worker_threads;
        conf
    }

    pub fn pd_rocks_conf(&self) -> DBConf {
        DBConf::new(&self.data_dir)
    }

    pub fn io_timeout_ms(&self) -> u64 {
        let dur = DurationUnit::from_str(&self.io_timeout).unwrap();
        dur.as_millis()
    }

    pub fn local_addr(&self) -> InetAddr {
        self.journal.local_addr()
    }

    pub fn to_pretty_toml(&self) -> CommonResult<String> {
        Ok(toml::to_string_pretty(self)?)
    }

    pub fn print(&self) {
        let conf = self.to_pretty_toml().unwrap();
        info!("git version: {}", version::GIT_VERSION);
        info!("cluster conf start: \n{}\n", conf);
    }
}

fn default_data_dir() -> String {
    "/data/pd/data".to_string()
}
