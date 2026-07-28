use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use spider::{net, payload, scheduler, trace};
use tokio::sync::RwLockReadGuard;

use super::{
    client,
    error::Error,
    state::{Action, Operation, Runtime},
};

const DEFAULT_MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const TRACE_CACHE_CAPACITY: usize = 128;
const TRACE_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

pub struct Api {
    pub(super) client: client::Client,
    lease: scheduler::Lease,
    pub(super) runtime: Arc<Runtime>,
}

impl Api {
    pub fn new(base_url: impl AsRef<str>, token: impl Into<String>) -> Result<Self, Error> {
        let mut base_url = url::Url::parse(base_url.as_ref())?;
        if !matches!(base_url.scheme(), "http" | "https") || !base_url.has_host() {
            return Err(Error::Config(
                "base_url must be an absolute http or https URL".to_string(),
            ));
        }
        if base_url.query().is_some() || base_url.fragment().is_some() {
            return Err(Error::Config(
                "base_url must not contain a query or fragment".to_string(),
            ));
        }
        if !base_url.username().is_empty() || base_url.password().is_some() {
            return Err(Error::Config(
                "base_url must not contain embedded credentials".to_string(),
            ));
        }
        if !base_url.path().ends_with('/') {
            let path = format!("{}/", base_url.path());
            base_url.set_path(&path);
        }

        let token = token.into();
        if token.trim().is_empty() {
            return Err(Error::Config("token must not be empty".to_string()));
        }
        if !header_value(&format!("Bearer {token}")) {
            return Err(Error::Config(
                "token must form a valid HTTP Authorization header".to_string(),
            ));
        }

        let lease = scheduler::Lease::default();
        let client = client::Client::new(
            base_url,
            token,
            "default".to_string(),
            DEFAULT_MAX_RESPONSE_BYTES,
        )?;
        Ok(Self {
            client,
            lease,
            runtime: Arc::new(Runtime::new(
                lease.interval(),
                TRACE_CACHE_CAPACITY,
                TRACE_CACHE_MAX_BYTES,
            )),
        })
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Result<Self, Error> {
        self.require_configurable()?;
        let namespace = namespace.into();
        if namespace.trim().is_empty()
            || namespace.len() > 128
            || namespace
                .chars()
                .any(|value| value.is_control() || value.is_whitespace())
            || !header_value(&namespace)
        {
            return Err(Error::Config(
                "namespace must be a valid HTTP header value of at most 128 bytes without whitespace or control characters"
                    .to_string(),
            ));
        }
        self.client = self.client.with_namespace(namespace);
        Ok(self)
    }

    pub fn with_lease(mut self, lease: scheduler::Lease) -> Result<Self, Error> {
        self.require_configurable()?;
        self.lease = lease;
        self.client = self.client.with_retry_deadline(lease.interval());
        Ok(self)
    }

    pub fn with_max_response_bytes(mut self, max_response_bytes: usize) -> Result<Self, Error> {
        self.require_configurable()?;
        if max_response_bytes == 0 {
            return Err(Error::Config(
                "max_response_bytes must be positive".to_string(),
            ));
        }
        self.client = self.client.with_max_response_bytes(max_response_bytes);
        Ok(self)
    }

    fn require_configurable(&self) -> Result<(), Error> {
        if self.runtime.opened.load(Ordering::Acquire) {
            Err(Error::Config(
                "API Scheduler cannot be reconfigured while it is open".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    pub(super) fn require_open(&self) -> Result<u64, scheduler::Error> {
        if !self.runtime.opened.load(Ordering::Acquire) {
            return Err(scheduler::Error::Message(
                "API Scheduler is not open".to_string(),
            ));
        }
        let epoch = self.runtime.epoch.load(Ordering::Acquire);
        if self.runtime.is_open(epoch) {
            Ok(epoch)
        } else {
            Err(scheduler::Error::Message(
                "API Scheduler is closing".to_string(),
            ))
        }
    }

    pub(super) fn require_epoch(&self, epoch: u64) -> Result<(), scheduler::Error> {
        if self.runtime.is_open(epoch) {
            Ok(())
        } else {
            Err(scheduler::Error::Message(
                "API Scheduler is closing".to_string(),
            ))
        }
    }

    async fn enter(&self) -> Result<RwLockReadGuard<'_, ()>, scheduler::Error> {
        let activity = self.runtime.activity.read().await;
        self.require_open()?;
        Ok(activity)
    }

    pub(super) fn invocation_key() -> String {
        uuid::Uuid::now_v7().to_string()
    }

    pub(super) async fn operation_key(
        &self,
        action: Action,
    ) -> Result<(Operation, String), scheduler::Error> {
        let operation = Operation::new(action);
        let key = self
            .runtime
            .operations
            .lock()
            .await
            .key(operation.clone())?;
        Ok((operation, key))
    }

    pub(super) async fn resolve<T>(
        &self,
        operation: Operation,
        result: Result<T, scheduler::Error>,
    ) -> Result<T, scheduler::Error> {
        if result
            .as_ref()
            .err()
            .is_none_or(|error| !error.is_transient())
        {
            self.runtime.operations.lock().await.remove(&operation);
        }
        result
    }
}

fn header_value(value: &str) -> bool {
    reqwest::header::HeaderValue::from_str(value).is_ok_and(|value| value.to_str().is_ok())
}

impl scheduler::Scheduler for Api {
    fn lease(&self) -> Option<scheduler::Lease> {
        Some(self.lease)
    }

    async fn open(&self) -> Result<(), scheduler::Error> {
        let _lifecycle = self.runtime.lifecycle.lock().await;
        let _activity = self.runtime.activity.write().await;
        if self.runtime.opened.load(Ordering::Acquire) {
            return Ok(());
        }

        let policy = self
            .client
            .get::<super::wire::Policy>("v1/worker/policy")
            .await?;
        let remote = scheduler::Lease::new(
            Duration::from_millis(policy.lease_timeout_ms),
            Duration::from_millis(policy.lease_interval_ms),
        )?;
        if remote != self.lease {
            return Err(scheduler::Error::Message(format!(
                "Master lease policy does not match API Scheduler: expected {}/{} ms, received {}/{} ms",
                self.lease.timeout().as_millis(),
                self.lease.interval().as_millis(),
                remote.timeout().as_millis(),
                remote.interval().as_millis()
            )));
        }
        if policy.heartbeat_interval_ms == 0
            || policy.heartbeat_interval_ms >= policy.lease_timeout_ms
        {
            return Err(scheduler::Error::Message(
                "Master heartbeat interval must be positive and shorter than the lease timeout"
                    .to_string(),
            ));
        }
        let max_response_bytes = usize::try_from(policy.max_response_bytes).map_err(|_| {
            scheduler::Error::Message(
                "Master response limit exceeds this platform's supported size".to_string(),
            )
        })?;
        if max_response_bytes == 0 || max_response_bytes > self.client.max_response_bytes() {
            return Err(scheduler::Error::Message(format!(
                "Master response limit {max_response_bytes} exceeds API Scheduler capacity {}",
                self.client.max_response_bytes()
            )));
        }
        self.client.set_max_request_bytes(max_response_bytes);

        *self.runtime.heartbeat_interval.write().await =
            Duration::from_millis(policy.heartbeat_interval_ms);
        self.runtime.epoch.fetch_add(1, Ordering::AcqRel);
        self.runtime.opened.store(true, Ordering::Release);
        Ok(())
    }

    async fn close(&self) -> Result<(), scheduler::Error> {
        let _lifecycle = self.runtime.lifecycle.lock().await;
        let _activity = self.runtime.activity.write().await;
        self.runtime.opened.store(false, Ordering::Release);
        self.runtime.epoch.fetch_add(1, Ordering::AcqRel);

        let workers = self
            .runtime
            .workers
            .lock()
            .await
            .drain()
            .map(|(_, worker)| worker)
            .collect::<Vec<_>>();
        for worker in &workers {
            worker.task.abort();
        }
        for worker in workers {
            let _ = worker.task.await;
        }
        self.runtime.operations.lock().await.clear();
        self.runtime.traces.lock().await.clear();
        Ok(())
    }

    async fn push(&self, payload: payload::Payload) -> Result<(), scheduler::Error> {
        let _activity = self.enter().await?;
        self.enqueue(payload).await
    }

    async fn trace(&self, trace_id: &str) -> Result<Option<trace::Snapshot>, scheduler::Error> {
        let _activity = self.enter().await?;
        self.load_trace(trace_id).await
    }

    async fn next_requests(
        &self,
        limit: usize,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<Vec<net::Request>, scheduler::Error> {
        let _activity = self.enter().await?;
        self.claim(limit, worker_id, modes).await
    }

    async fn has_pending_requests(
        &self,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<bool, scheduler::Error> {
        let _activity = self.enter().await?;
        self.pending(worker_id, modes).await
    }

    async fn ack(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        let _activity = self.enter().await?;
        self.acknowledge(payload).await
    }

    async fn release(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        let _activity = self.enter().await?;
        self.return_to_queue(payload).await
    }

    async fn refresh_lease(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        let _activity = self.enter().await?;
        self.refresh(payload).await
    }

    async fn success(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        let _activity = self.enter().await?;
        self.succeed(payload).await
    }

    async fn failure(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        let _activity = self.enter().await?;
        self.fail(payload).await
    }
}

impl scheduler::Init for Api {
    fn initializes_run(&self) -> bool {
        false
    }

    async fn init(
        &self,
        trace_id: String,
        snapshot: trace::Snapshot,
        requests: Vec<net::Request>,
    ) -> Result<(), scheduler::Error> {
        let _activity = self.enter().await?;
        self.initialize(trace_id, snapshot, requests).await
    }
}
