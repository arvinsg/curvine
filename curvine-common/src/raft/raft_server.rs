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
use crate::raft::snapshot::SnapshotDownloadHandler;
use crate::raft::{RaftCode, RaftError, RaftResult};
use log::warn;
use mini_moka::sync::{Cache, CacheBuilder};
use orpc::client::dispatch::Envelope;
use orpc::common::DurationUnit;
use orpc::handler::{HandlerService, MessageHandler};
use orpc::io::net::ConnState;
use orpc::message::{Builder, Message, ResponseStatus};
use orpc::runtime::Runtime;
use orpc::server::{RpcServer, ServerConf, ServerStateListener};
use orpc::sys::DataSlice;
use orpc::{err_box, try_option, CommonResult};
use prost::bytes::BytesMut;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};

/// Cached response for a single propose request.
///
/// We cache the full response semantics, not just `ProposeResponse.apply_result`.
/// A failed propose (for example `NotLeader`) has no `apply_result`; caching only
/// bytes would turn a retry into a fake success with empty `apply_result`, which
/// PD decodes as `ApplyOutcome::Applied`.
#[derive(Clone)]
struct CachedProposeResponse {
    response_status: ResponseStatus,
    header: Option<Vec<u8>>,
    data: Vec<u8>,
}

impl CachedProposeResponse {
    fn from_message(rep: &Message) -> Self {
        Self {
            response_status: rep.response_status(),
            header: rep.header_bytes().map(|h| h.to_vec()),
            data: rep.data.as_slice().to_vec(),
        }
    }

    fn to_message(&self, req: &Message) -> Message {
        let mut builder = Builder::new()
            .code(req.code())
            .request(req.request_status())
            .response(self.response_status)
            .req_id(req.req_id())
            .seq_id(req.seq_id());
        if let Some(header) = &self.header {
            builder = builder.header(BytesMut::from(header.as_slice()));
        }
        if !self.data.is_empty() {
            builder = builder.data(DataSlice::Buffer(BytesMut::from(self.data.as_slice())));
        }
        builder.build()
    }

    fn is_success(&self) -> bool {
        self.response_status == ResponseStatus::Success
    }
}

/// Slot for a single propose response. `None` while the first request is still
/// in flight; `Some(response)` after the raft node returns. The async mutex is
/// held by the first request between propose and cache-write so concurrent
/// retries with the same `req_id` block on `lock().await` until the first
/// request publishes its exact response semantics.
type RetrySlot = Arc<AsyncMutex<Option<CachedProposeResponse>>>;

/// Maximum time a retry will wait on the in-flight slot. If the first request
/// never completes (e.g. raft node hang, dropped sender), the retry returns an
/// error rather than blocking forever.
const RETRY_SLOT_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

pub struct RaftServer {
    server: RpcServer<RaftService>,
}

impl RaftServer {
    pub fn with_rt(rt: Arc<Runtime>, conf: &JournalConf) -> Self {
        let mut server_conf = ServerConf::with_hostname(conf.hostname.to_string(), conf.rpc_port);
        server_conf.name = "curvine-journal".to_string();
        server_conf.enable_splice = false;
        server_conf.enable_send_file = false;
        server_conf.pipe_pool_init_cap = 0;
        server_conf.pipe_pool_max_cap = 0;

        let service = RaftService::new(conf);

        let server = RpcServer::with_rt(rt, server_conf, service);
        Self { server }
    }

    pub fn start(self) -> ServerStateListener {
        RpcServer::run_server(self.server)
    }

    pub fn take_receiver(&mut self) -> CommonResult<mpsc::Receiver<Envelope>> {
        self.server.service_mut().take_receiver()
    }

    pub fn new_sender(&self) -> mpsc::Sender<Envelope> {
        self.server.service().sender.clone()
    }
}

pub struct RaftService {
    sender: mpsc::Sender<Envelope>,
    receiver: Option<mpsc::Receiver<Envelope>>,
    /// Retry dedup cache for Propose RPC. Each `req_id` maps to a `RetrySlot`
    /// which is filled with the original response once the first request
    /// finishes.
    ///
    /// Pre-#8: `Cache<i64, ()>` — retries returned `msg.success()` with empty
    /// payload, masking the real ApplyOutcome (Stale/NotFound decoded as
    /// Applied).
    ///
    /// Pre-#1-redo: `Cache<i64, Vec<u8>>` — retries after apply got the right
    /// outcome but in-flight retries (cache miss while first req is pending)
    /// proposed a SECOND raft entry whose CAS produced a different outcome.
    ///
    /// Post-#1-redo: `Cache<i64, RetrySlot>` — first request acquires the
    /// slot's async mutex before propose and holds it until the raft node
    /// responds; concurrent retries `lock().await` the same slot, waking up
    /// after the first request publishes its response. All retries see the SAME
    /// success/error semantics.
    retry_cache: Arc<Cache<i64, RetrySlot>>,
    /// Serializes the get-or-create dance for `retry_cache`. `mini_moka` does
    /// not expose an atomic get-or-insert; without this lock, two concurrent
    /// requests with the same `req_id` could both miss, both create separate
    /// slots, and dedup would be lost (regressing to the pre-#1-redo behavior).
    /// Held only briefly during the slot lookup, never across propose/apply.
    init_lock: Arc<StdMutex<()>>,
}

impl RaftService {
    pub fn new(conf: &JournalConf) -> Self {
        let ttl = DurationUnit::from_str(&conf.raft_retry_cache_ttl).unwrap();
        let cache = CacheBuilder::new(conf.raft_retry_cache_size)
            .time_to_live(Duration::from_millis(ttl.as_millis()))
            .build();

        let (sender, receiver) = mpsc::channel(conf.message_size);
        Self {
            sender,
            receiver: Some(receiver),
            retry_cache: Arc::new(cache),
            init_lock: Arc::new(StdMutex::new(())),
        }
    }

    pub fn take_receiver(&mut self) -> CommonResult<mpsc::Receiver<Envelope>> {
        let rx = self.receiver.take();
        Ok(try_option!(rx))
    }
}

impl HandlerService for RaftService {
    type Item = RaftHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        RaftHandler {
            sender: self.sender.clone(),
            download_handler: SnapshotDownloadHandler::new(1024 * 1024),
            retry_cache: self.retry_cache.clone(),
            init_lock: self.init_lock.clone(),
        }
    }
}

pub struct RaftHandler {
    sender: mpsc::Sender<Envelope>,
    download_handler: SnapshotDownloadHandler,
    retry_cache: Arc<Cache<i64, RetrySlot>>,
    init_lock: Arc<StdMutex<()>>,
}

impl RaftHandler {
    /// Atomic get-or-create for the per-`req_id` `RetrySlot`. The init_lock
    /// serializes the get/create gap (mini_moka has no atomic compute API);
    /// the lock is held only across the cache lookup, never across propose.
    fn slot_for(&self, req_id: i64) -> RetrySlot {
        if let Some(s) = self.retry_cache.get(&req_id) {
            return s;
        }
        let _g = self.init_lock.lock().unwrap();
        // Re-check under init_lock in case another task created it between
        // our miss above and acquiring the lock.
        if let Some(s) = self.retry_cache.get(&req_id) {
            return s;
        }
        let slot: RetrySlot = Arc::new(AsyncMutex::new(None));
        self.retry_cache.insert(req_id, slot.clone());
        slot
    }
}

impl MessageHandler for RaftHandler {
    type Error = RaftError;

    fn is_sync(&self, msg: &Message) -> bool {
        let code = RaftCode::from(msg.code());
        matches!(code, RaftCode::SnapshotDownload)
    }

    fn handle(&mut self, msg: &Message) -> RaftResult<Message> {
        let code = RaftCode::from(msg.code());
        match code {
            RaftCode::SnapshotDownload => self.download_handler.handle(msg),

            _ => err_box!("Unsupported request type: {:?}", code),
        }
    }

    async fn async_handle(&mut self, msg: Message) -> Result<Message, Self::Error> {
        // Non-Propose: no caching, just forward to raft node.
        if RaftCode::from(msg.code()) != RaftCode::Propose {
            let (tx, rx) = oneshot::channel();
            self.sender.send(Envelope::new(msg, tx)).await?;
            return Ok(rx.await??);
        }

        let req_id = msg.req_id();
        let slot = self.slot_for(req_id);

        // Acquire the slot's mutex.
        // - First request for this req_id: acquires immediately, slot is None.
        // - In-flight retry (first req still proposing/applying): blocks here
        //   until the first request publishes its response, then sees Some(...).
        // - Completed retry (first req already wrote response): acquires
        //   immediately, sees Some(...), returns cached response.
        //
        // RETRY_SLOT_WAIT_TIMEOUT bounds the wait so a hung first request
        // (e.g., raft node stuck) cannot block retries forever.
        let mut guard = match tokio::time::timeout(RETRY_SLOT_WAIT_TIMEOUT, slot.lock()).await {
            Ok(g) => g,
            Err(_) => {
                warn!(
                    "Retry propose req_id={} timed out waiting for in-flight slot ({}s)",
                    req_id,
                    RETRY_SLOT_WAIT_TIMEOUT.as_secs()
                );
                return Err(RaftError::other(
                    format!(
                        "retry propose req_id={} blocked: in-flight first request did not finish within {}s",
                        req_id,
                        RETRY_SLOT_WAIT_TIMEOUT.as_secs()
                    )
                    .into(),
                ));
            }
        };

        if let Some(cached) = guard.as_ref() {
            warn!(
                "Retry propose req_id={}, returning cached response status={:?}, header_bytes={}, data_bytes={}",
                req_id,
                cached.response_status,
                cached.header.as_ref().map(|h| h.len()).unwrap_or(0),
                cached.data.len()
            );
            return Ok(cached.to_message(&msg));
        }

        // We are the first owner of this slot. Hold the mutex across propose
        // + apply so concurrent retries block on lock().await above.
        let (tx, rx) = oneshot::channel();
        self.sender.send(Envelope::new(msg, tx)).await?;
        let rep = rx.await??;

        // Publish the full response semantics to the slot before releasing the
        // mutex. This preserves error responses such as NotLeader; retries must
        // not convert them into success with an empty apply_result.
        let cached = CachedProposeResponse::from_message(&rep);
        let cache_success = cached.is_success();
        *guard = Some(cached);
        // guard drops here, releasing the mutex; queued retries wake up,
        // observe Some(response), and return the cached response.
        drop(guard);

        // Keep successful apply results for the configured TTL so late retries
        // get the same ApplyOutcome. Do not keep error responses for the full
        // TTL: a transient NotLeader/timeout should not poison this node if it
        // becomes leader shortly afterwards. In-flight waiters already hold the
        // slot Arc and will still observe the cached error after invalidation.
        if !cache_success {
            self.retry_cache.invalidate(&req_id);
        }

        Ok(rep)
    }
}
