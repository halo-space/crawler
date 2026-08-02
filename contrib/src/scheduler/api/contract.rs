use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use spider::{net, payload, scheduler, trace};
use tokio::sync::RwLockReadGuard;

use super::{client, error::Error, state::Runtime, worker};

const TRACE_CACHE_CAPACITY: usize = 128;
const TRACE_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

pub struct Api {
    pub(super) client: client::Client,
    lease: scheduler::Lease,
    pub(super) worker: worker::Config,
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
        let client = client::Client::new(base_url, token, "default".to_string())?;
        Ok(Self {
            client,
            lease,
            worker: worker::Config::default(),
            runtime: Arc::new(Runtime::new(TRACE_CACHE_CAPACITY, TRACE_CACHE_MAX_BYTES)),
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

    pub fn with_worker_id(mut self, worker_id: impl Into<String>) -> Result<Self, Error> {
        self.require_configurable()?;
        self.worker.set_id(worker_value("worker_id", worker_id)?);
        Ok(self)
    }

    pub fn with_worker_host(mut self, host: impl Into<String>) -> Result<Self, Error> {
        self.require_configurable()?;
        self.worker.set_host(worker_value("worker_host", host)?);
        Ok(self)
    }

    pub fn with_worker_version(mut self, version: impl Into<String>) -> Result<Self, Error> {
        self.require_configurable()?;
        self.worker
            .set_version(worker_value("worker_version", version)?);
        Ok(self)
    }

    pub fn with_modes(mut self, modes: impl IntoIterator<Item = net::Mode>) -> Result<Self, Error> {
        self.require_configurable()?;
        let modes = modes.into_iter().collect::<Vec<_>>();
        if modes.is_empty() {
            return Err(Error::Config("worker modes must not be empty".to_string()));
        }
        self.worker.set_modes(modes);
        Ok(self)
    }

    fn require_configurable(&self) -> Result<(), Error> {
        if !self.runtime.is_configurable() {
            Err(Error::Config(
                "API Scheduler cannot be reconfigured during an active Worker lifecycle"
                    .to_string(),
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

    async fn enter(&self) -> Result<RwLockReadGuard<'_, ()>, scheduler::Error> {
        let activity = self.runtime.activity.read().await;
        self.require_open()?;
        Ok(activity)
    }
}

fn header_value(value: &str) -> bool {
    reqwest::header::HeaderValue::from_str(value).is_ok_and(|value| value.to_str().is_ok())
}

fn worker_value(name: &str, value: impl Into<String>) -> Result<String, Error> {
    let value = value.into();
    if value.trim().is_empty() {
        Err(Error::Config(format!("{name} must not be empty")))
    } else {
        Ok(value)
    }
}

impl scheduler::Scheduler for Api {
    fn lease(&self) -> Option<scheduler::Lease> {
        Some(self.lease)
    }

    async fn open(&self, concurrency: usize) -> Result<(), scheduler::Error> {
        let _lifecycle = self.runtime.lifecycle.lock().await;
        let _activity = self.runtime.activity.write().await;
        if self.runtime.opened.load(Ordering::Acquire) {
            let active = self.runtime.concurrency().ok_or_else(|| {
                scheduler::Error::Message(
                    "API Scheduler Worker lifecycle state is missing".to_string(),
                )
            })?;
            return if active == concurrency {
                Ok(())
            } else {
                Err(scheduler::Error::Message(format!(
                    "API Scheduler is already open with concurrency {active}; received {concurrency}"
                )))
            };
        }
        self.worker.validate(concurrency)?;
        self.runtime
            .check_concurrency(concurrency)
            .map_err(scheduler::Error::Message)?;

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
        if policy.heartbeat_interval_ms == 0 {
            return Err(scheduler::Error::Message(
                "Master heartbeat interval must be positive".to_string(),
            ));
        }
        let heartbeat_interval = Duration::from_millis(policy.heartbeat_interval_ms);
        let key = self
            .runtime
            .open_key(concurrency)
            .map_err(scheduler::Error::Message)?;
        let token = worker::register(&self.client, &self.worker, concurrency, &key).await?;
        self.runtime.confirm_registration();
        let epoch = self
            .runtime
            .epoch
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        self.runtime.set_token(token.clone());
        self.runtime.can_claim.store(true, Ordering::Release);
        self.runtime.opened.store(true, Ordering::Release);
        self.runtime.set_heartbeat(worker::start_heartbeat(
            self.client.clone(),
            self.runtime.clone(),
            epoch,
            self.worker.id()?.to_string(),
            token,
            heartbeat_interval,
        ));
        Ok(())
    }

    async fn close(&self) -> Result<(), scheduler::Error> {
        let _lifecycle = self.runtime.lifecycle.lock().await;
        let _activity = self.runtime.activity.write().await;
        self.runtime.opened.store(false, Ordering::Release);
        self.runtime.epoch.fetch_add(1, Ordering::AcqRel);
        self.runtime.can_claim.store(false, Ordering::Release);

        self.runtime.stop_heartbeat().await;
        self.runtime.clear_stopped_heartbeat();
        self.runtime.can_claim.store(false, Ordering::Release);
        if let Some(token) = self.runtime.token() {
            let worker_id = self.worker.id()?;
            let result = worker::offline(&self.client, worker_id, token).await;
            self.runtime.take_token();
            if let Err(error) = result {
                tracing::warn!(
                    worker_id = %worker_id,
                    error = %error,
                    "API Scheduler failed to mark Worker offline"
                );
            }
        }
        self.runtime.clear_registration();
        self.runtime.clear_operations();
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

    async fn next_requests(&self, limit: usize) -> Result<Vec<net::Request>, scheduler::Error> {
        let _activity = self.enter().await?;
        if !self.runtime.can_claim.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }
        self.claim(limit).await
    }

    async fn has_pending_requests(&self) -> Result<bool, scheduler::Error> {
        let _activity = self.enter().await?;
        self.pending().await
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

impl Drop for Api {
    fn drop(&mut self) {
        // Destructors only tear down local runtime state; close owns the remote offline update.
        self.runtime.abandon();
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
