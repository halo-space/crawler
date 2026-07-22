use spider::{net, payload, scheduler, trace};
use tokio::sync::Mutex;

use super::error::{message, redis as redis_error, status};
use super::key::Keys;
use super::script::Scripts;

/// A Redis 7 standalone Scheduler.
///
/// The Scheduler owns no Worker-local files. Its queue, leases, Trace Snapshots, completions,
/// statistics, and Item output are scoped by its namespace in Redis.
pub struct Redis {
    pub(super) client: redis::Client,
    pub(super) connection: Mutex<Option<redis::aio::ConnectionManager>>,
    pub(super) keys: Keys,
    pub(super) lease: scheduler::Lease,
    pub(super) scripts: Scripts,
}

impl Redis {
    /// Creates a Redis Scheduler with the default `crawler` namespace.
    ///
    /// The URL is parsed here, while [`scheduler::Scheduler::open`] establishes the connection.
    pub fn new(url: impl Into<String>) -> Result<Self, scheduler::Error> {
        let client = redis::Client::open(url.into()).map_err(message)?;
        Ok(Self {
            client,
            connection: Mutex::new(None),
            keys: Keys::new("crawler")?,
            lease: scheduler::Lease::default(),
            scripts: Scripts::new(),
        })
    }

    /// Selects the namespace used for all Redis keys owned by this Scheduler.
    pub fn with_namespace(
        mut self,
        namespace: impl Into<String>,
    ) -> Result<Self, scheduler::Error> {
        self.keys = Keys::new(namespace)?;
        Ok(self)
    }

    /// Replaces the default lease policy.
    pub fn with_lease(mut self, lease: scheduler::Lease) -> Self {
        self.lease = lease;
        self
    }

    /// Returns the namespace selected for this Scheduler.
    pub fn namespace(&self) -> &str {
        self.keys.namespace()
    }

    pub(super) async fn connection(
        &self,
    ) -> Result<redis::aio::ConnectionManager, scheduler::Error> {
        self.connection
            .lock()
            .await
            .as_ref()
            .cloned()
            .ok_or_else(|| scheduler::Error::Message("Redis Scheduler is not open".to_string()))
    }

    async fn connect(&self) -> Result<redis::aio::ConnectionManager, scheduler::Error> {
        let mut connection = redis::aio::ConnectionManager::new(self.client.clone())
            .await
            .map_err(redis_error)?;
        let _: String = redis::cmd("PING")
            .query_async(&mut connection)
            .await
            .map_err(redis_error)?;
        self.scripts
            .load(&mut connection)
            .await
            .map_err(redis_error)?;
        Ok(connection)
    }

    pub(super) fn encode<T: serde::Serialize>(value: &T) -> Result<String, scheduler::Error> {
        serde_json::to_string(value).map_err(message)
    }

    pub(super) fn result(value: String, id: &str) -> Result<(), scheduler::Error> {
        if value == "OK" {
            Ok(())
        } else {
            Err(status(&value, id))
        }
    }
}

impl scheduler::Scheduler for Redis {
    fn lease(&self) -> Option<scheduler::Lease> {
        Some(self.lease)
    }

    async fn open(&self) -> Result<(), scheduler::Error> {
        if let Some(mut connection) = self.connection.lock().await.clone() {
            let _: String = redis::cmd("PING")
                .query_async(&mut connection)
                .await
                .map_err(redis_error)?;
            self.scripts
                .load(&mut connection)
                .await
                .map_err(redis_error)?;
            return Ok(());
        }

        let connection = self.connect().await?;
        let mut active = self.connection.lock().await;
        if active.is_none() {
            *active = Some(connection);
        }
        Ok(())
    }

    async fn close(&self) -> Result<(), scheduler::Error> {
        self.connection.lock().await.take();
        Ok(())
    }

    async fn push(&self, payload: payload::Payload) -> Result<(), scheduler::Error> {
        self.enqueue(payload).await
    }

    async fn push_items(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.write_items(payload).await
    }

    async fn trace(&self, trace_id: &str) -> Result<Option<trace::Snapshot>, scheduler::Error> {
        self.load_trace(trace_id).await
    }

    async fn next_requests(
        &self,
        limit: usize,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<Vec<net::Request>, scheduler::Error> {
        self.claim(limit, worker_id, modes).await
    }

    async fn has_pending_requests(
        &self,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<bool, scheduler::Error> {
        self.pending(worker_id, modes).await
    }

    async fn ack(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.acknowledge(payload).await
    }

    async fn release(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.return_to_queue(payload).await
    }

    async fn refresh_lease(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.refresh(payload).await
    }

    async fn success(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.succeed(payload).await
    }

    async fn failure(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.fail(payload).await
    }
}

impl scheduler::Init for Redis {
    fn initializes_run(&self) -> bool {
        false
    }

    async fn init(
        &self,
        trace_id: String,
        snapshot: trace::Snapshot,
        requests: Vec<net::Request>,
    ) -> Result<(), scheduler::Error> {
        self.initialize(trace_id, snapshot, requests).await
    }
}
