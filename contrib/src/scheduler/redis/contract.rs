use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use spider::{net, payload, scheduler, trace};
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};

use super::error::{message, redis as redis_error, status};
use super::key::Keys;
use super::request::Cursor;
use super::script::Scripts;
use super::worker::Worker;

/// A Redis 7 standalone Scheduler.
///
/// The Scheduler owns no Worker-local files. Its queues, mode-scoped processing ownership, Trace
/// Snapshots, completions, and statistics are scoped by its namespace in Redis.
pub struct Redis {
    pub(super) client: redis::Client,
    pub(super) connection: Mutex<Option<redis::aio::ConnectionManager>>,
    pub(super) keys: Keys,
    pub(super) lease: scheduler::Lease,
    pub(super) scripts: Scripts,
    pub(super) claim_cursor: Mutex<Cursor>,
    pub(super) worker: Worker,
    opened: AtomicBool,
    lifecycle: Mutex<()>,
    activity: RwLock<()>,
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
            claim_cursor: Mutex::new(Cursor::default()),
            worker: Worker::new(),
            opened: AtomicBool::new(false),
            lifecycle: Mutex::new(()),
            activity: RwLock::new(()),
        })
    }

    /// Selects the namespace used for all Redis keys owned by this Scheduler.
    pub fn with_namespace(
        mut self,
        namespace: impl Into<String>,
    ) -> Result<Self, scheduler::Error> {
        self.require_configurable()?;
        self.keys = Keys::new(namespace)?;
        Ok(self)
    }

    /// Replaces the default lease policy.
    pub fn with_lease(mut self, lease: scheduler::Lease) -> Result<Self, scheduler::Error> {
        self.require_configurable()?;
        self.lease = lease;
        Ok(self)
    }

    /// Sets the stable identity registered and used by this Redis Scheduler.
    pub fn with_worker_id(
        mut self,
        worker_id: impl Into<String>,
    ) -> Result<Self, scheduler::Error> {
        self.require_configurable()?;
        self.worker.set_id(worker_id)?;
        Ok(self)
    }

    /// Sets the host metadata stored for this Worker.
    pub fn with_worker_host(mut self, host: impl Into<String>) -> Result<Self, scheduler::Error> {
        self.require_configurable()?;
        self.worker.set_host(host)?;
        Ok(self)
    }

    /// Sets the application version stored for this Worker.
    pub fn with_worker_version(
        mut self,
        version: impl Into<String>,
    ) -> Result<Self, scheduler::Error> {
        self.require_configurable()?;
        self.worker.set_version(version)?;
        Ok(self)
    }

    /// Replaces the default HTTP download capability advertised by this Worker.
    pub fn with_modes(
        mut self,
        modes: impl IntoIterator<Item = net::Mode>,
    ) -> Result<Self, scheduler::Error> {
        self.require_configurable()?;
        self.worker.set_modes(modes)?;
        Ok(self)
    }

    /// Replaces the default 10-second heartbeat and 30-second offline timeout.
    pub fn with_heartbeat(
        mut self,
        interval: Duration,
        timeout: Duration,
    ) -> Result<Self, scheduler::Error> {
        self.require_configurable()?;
        self.worker.set_heartbeat(interval, timeout)?;
        Ok(self)
    }

    fn require_configurable(&self) -> Result<(), scheduler::Error> {
        if self.opened.load(Ordering::Acquire) || !self.worker.is_configurable() {
            Err(scheduler::Error::Message(
                "Redis Scheduler cannot be reconfigured during an active Worker lifecycle"
                    .to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn require_open(&self) -> Result<(), scheduler::Error> {
        if self.opened.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(scheduler::Error::Message(
                "Redis Scheduler is not open".to_string(),
            ))
        }
    }

    async fn enter(&self) -> Result<RwLockReadGuard<'_, ()>, scheduler::Error> {
        let activity = self.activity.read().await;
        self.require_open()?;
        Ok(activity)
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

    async fn open(&self, concurrency: usize) -> Result<(), scheduler::Error> {
        let _lifecycle = self.lifecycle.lock().await;
        let _activity = self.activity.write().await;
        // Do not let close finish while this open is still establishing its connection.
        let mut active = self.connection.lock().await;
        if let Some(connection) = active.as_mut() {
            let active_concurrency = self.worker.concurrency().ok_or_else(|| {
                scheduler::Error::Message(
                    "Redis Scheduler Worker lifecycle state is missing".to_string(),
                )
            })?;
            if active_concurrency != concurrency {
                return Err(scheduler::Error::Message(format!(
                    "Redis Scheduler is already open with concurrency {active_concurrency}; received {concurrency}"
                )));
            }
            let _: String = redis::cmd("PING")
                .query_async(connection)
                .await
                .map_err(redis_error)?;
            self.scripts.load(connection).await.map_err(redis_error)?;
            self.opened.store(true, Ordering::Release);
            return Ok(());
        }

        self.worker.validate(concurrency)?;
        if let Some(active_concurrency) = self.worker.concurrency()
            && active_concurrency != concurrency
        {
            return Err(scheduler::Error::Message(format!(
                "Redis Scheduler Worker lifecycle is frozen with concurrency {active_concurrency}; received {concurrency}"
            )));
        }
        let mut connection = self.connect().await?;
        self.worker
            .start(&mut connection, &self.keys, &self.scripts, concurrency)
            .await?;
        *active = Some(connection);
        self.opened.store(true, Ordering::Release);
        Ok(())
    }

    async fn close(&self) -> Result<(), scheduler::Error> {
        let _lifecycle = self.lifecycle.lock().await;
        let _activity = self.activity.write().await;
        let mut active = self.connection.lock().await;
        let mut cursor = self.claim_cursor.lock().await;
        self.worker.stop_heartbeat().await;
        if let Some(connection) = active.as_mut()
            && let Err(error) = self
                .worker
                .offline(connection, &self.keys, &self.scripts)
                .await
        {
            tracing::warn!(error = %error, "Redis Worker offline update failed");
        }
        self.worker.reset();
        active.take();
        *cursor = Cursor::default();
        self.opened.store(false, Ordering::Release);
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
        if !self.worker.can_claim() {
            return Ok(Vec::new());
        }
        self.claim(limit, self.worker.id()?, self.worker.modes())
            .await
    }

    async fn has_pending_requests(&self) -> Result<bool, scheduler::Error> {
        let _activity = self.enter().await?;
        self.pending(self.worker.id()?, self.worker.modes()).await
    }

    async fn ack(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        let _activity = self.enter().await?;
        self.acknowledge(payload, self.worker.id()?).await
    }

    async fn release(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        let _activity = self.enter().await?;
        self.return_to_queue(payload, self.worker.id()?).await
    }

    async fn refresh_lease(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        let _activity = self.enter().await?;
        self.refresh(payload, self.worker.id()?).await
    }

    async fn success(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        let _activity = self.enter().await?;
        self.succeed(payload, self.worker.id()?).await
    }

    async fn failure(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        let _activity = self.enter().await?;
        self.fail(payload, self.worker.id()?).await
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
        let _activity = self.enter().await?;
        self.initialize(trace_id, snapshot, requests).await
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Read as _, Write as _};
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use spider::scheduler::Init as _;
    use spider::{Scheduler as _, net, payload, scheduler, trace};
    use tokio::sync::oneshot;

    use super::Redis;

    #[test]
    fn configuration_is_frozen_while_the_scheduler_is_open() {
        assert!(opened().with_namespace("other").is_err());
        assert!(opened().with_lease(scheduler::Lease::default()).is_err());
        assert!(opened().with_worker_id("other").is_err());
        assert!(opened().with_worker_host("other-host").is_err());
        assert!(opened().with_worker_version("other-version").is_err());
        assert!(opened().with_modes([net::Mode::Browser]).is_err());
        assert!(
            opened()
                .with_heartbeat(Duration::from_secs(1), Duration::from_secs(2))
                .is_err()
        );
    }

    #[tokio::test]
    async fn failed_open_does_not_freeze_configuration() {
        let scheduler = Redis::new("redis://127.0.0.1:1").unwrap();
        let error = scheduler.open(16).await.unwrap_err();
        assert!(error.to_string().contains("worker_id"), "{error}");
        assert!(!scheduler.opened.load(Ordering::Acquire));
        scheduler.with_namespace("other").unwrap();
    }

    #[tokio::test]
    async fn closed_operations_return_not_open() {
        let scheduler = configured("redis://127.0.0.1:6379");
        not_open(scheduler.push(payload::Payload::new()).await.unwrap_err());
        not_open(scheduler.trace("trace").await.unwrap_err());
        not_open(scheduler.next_requests(1).await.unwrap_err());
        not_open(scheduler.has_pending_requests().await.unwrap_err());
        not_open(scheduler.ack(&payload::Payload::new()).await.unwrap_err());
        not_open(
            scheduler
                .release(&payload::Payload::new())
                .await
                .unwrap_err(),
        );
        not_open(
            scheduler
                .refresh_lease(&payload::Payload::new())
                .await
                .unwrap_err(),
        );
        not_open(
            scheduler
                .success(&payload::Payload::new())
                .await
                .unwrap_err(),
        );
        not_open(
            scheduler
                .failure(&payload::Payload::new())
                .await
                .unwrap_err(),
        );
        not_open(
            scheduler
                .init(
                    "trace".to_string(),
                    trace::Snapshot::code("task"),
                    Vec::new(),
                )
                .await
                .unwrap_err(),
        );
    }

    #[tokio::test]
    async fn successful_open_freezes_configuration_until_close() {
        let (url, ping_started, resume, server_task) = fake_server();
        let scheduler = Arc::new(configured(url));
        let opening = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.open(16).await }
        });
        tokio::time::timeout(Duration::from_secs(1), ping_started)
            .await
            .expect("open did not reach the controlled PING")
            .unwrap();
        resume.send(()).unwrap();
        opening.await.unwrap().unwrap();
        assert!(scheduler.opened.load(Ordering::Acquire));

        scheduler.close().await.unwrap();
        assert!(!scheduler.opened.load(Ordering::Acquire));
        let scheduler = Arc::try_unwrap(scheduler)
            .unwrap_or_else(|_| panic!("open task retained the Redis Scheduler"))
            .with_namespace("other")
            .unwrap();
        assert_eq!(scheduler.namespace(), "other");
        drop(scheduler);
        tokio::task::spawn_blocking(move || server_task.join().unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn close_waits_for_an_in_flight_claim_and_rejects_later_operations() {
        let HeldServer {
            url,
            ping_started,
            resume_ping,
            script_started,
            resume_script,
            registration_keys: _,
            task: server_task,
        } = held_server(include_str!("scripts/claim.lua"));
        let scheduler = Arc::new(configured(url));
        let opening = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.open(16).await }
        });
        tokio::time::timeout(Duration::from_secs(1), ping_started)
            .await
            .expect("open did not reach the controlled PING")
            .unwrap();
        resume_ping.send(()).unwrap();
        opening.await.unwrap().unwrap();

        let claiming = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.next_requests(1).await }
        });
        tokio::time::timeout(Duration::from_secs(1), script_started)
            .await
            .expect("claim did not reach the fake Redis server")
            .unwrap();
        let closing = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.close().await }
        });
        tokio::task::yield_now().await;
        assert!(scheduler.opened.load(Ordering::Acquire));
        assert!(!closing.is_finished());

        let later = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.has_pending_requests().await }
        });
        resume_script.send(()).unwrap();
        assert!(claiming.await.unwrap().unwrap().is_empty());
        closing.await.unwrap().unwrap();
        not_open(later.await.unwrap().unwrap_err());
        not_open(scheduler.next_requests(1).await.unwrap_err());

        drop(scheduler);
        tokio::task::spawn_blocking(move || server_task.join().unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancelling_close_while_an_operation_is_active_keeps_the_scheduler_open() {
        let HeldServer {
            url,
            ping_started,
            resume_ping,
            script_started,
            resume_script,
            registration_keys: _,
            task: server_task,
        } = held_server(include_str!("scripts/claim.lua"));
        let scheduler = Arc::new(configured(url));
        let opening = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.open(16).await }
        });
        tokio::time::timeout(Duration::from_secs(1), ping_started)
            .await
            .expect("open did not reach the controlled PING")
            .unwrap();
        resume_ping.send(()).unwrap();
        opening.await.unwrap().unwrap();

        let claiming = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.next_requests(1).await }
        });
        tokio::time::timeout(Duration::from_secs(1), script_started)
            .await
            .expect("claim did not reach the fake Redis server")
            .unwrap();
        let closing = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.close().await }
        });
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(scheduler.opened.load(Ordering::Acquire));
        closing.abort();
        assert!(closing.await.unwrap_err().is_cancelled());
        assert!(scheduler.opened.load(Ordering::Acquire));

        resume_script.send(()).unwrap();
        assert!(claiming.await.unwrap().unwrap().is_empty());
        scheduler.close().await.unwrap();

        drop(scheduler);
        tokio::task::spawn_blocking(move || server_task.join().unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancelling_close_during_offline_keeps_the_registration_recoverable() {
        let HeldServer {
            url,
            ping_started,
            resume_ping,
            script_started,
            resume_script,
            registration_keys: _,
            task: server_task,
        } = held_server(include_str!("scripts/offline.lua"));
        let scheduler = Arc::new(configured(url));
        let opening = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.open(16).await }
        });
        tokio::time::timeout(Duration::from_secs(1), ping_started)
            .await
            .expect("open did not reach the controlled PING")
            .unwrap();
        resume_ping.send(()).unwrap();
        opening.await.unwrap().unwrap();

        let closing = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.close().await }
        });
        tokio::time::timeout(Duration::from_secs(1), script_started)
            .await
            .expect("close did not reach the controlled offline update")
            .unwrap();
        assert!(scheduler.opened.load(Ordering::Acquire));
        assert!(scheduler.worker.heartbeat_stopped());
        assert!(!scheduler.worker.can_claim());

        closing.abort();
        assert!(closing.await.unwrap_err().is_cancelled());
        assert!(scheduler.opened.load(Ordering::Acquire));
        assert!(scheduler.worker.heartbeat_stopped());
        assert!(!scheduler.worker.can_claim());

        resume_script.send(()).unwrap();
        scheduler.close().await.unwrap();
        assert!(!scheduler.opened.load(Ordering::Acquire));
        assert!(!scheduler.worker.can_claim());
        assert!(scheduler.connection.lock().await.is_none());

        drop(scheduler);
        tokio::task::spawn_blocking(move || server_task.join().unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancelling_close_while_heartbeat_is_stopping_keeps_its_state() {
        let HeldServer {
            url,
            ping_started,
            resume_ping,
            script_started,
            resume_script,
            registration_keys: _,
            task: server_task,
        } = held_server(include_str!("scripts/heartbeat.lua"));
        let scheduler = Arc::new(
            configured(url)
                .with_heartbeat(Duration::from_millis(1), Duration::from_millis(100))
                .unwrap(),
        );
        let opening = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.open(16).await }
        });
        tokio::time::timeout(Duration::from_secs(1), ping_started)
            .await
            .expect("open did not reach the controlled PING")
            .unwrap();
        resume_ping.send(()).unwrap();
        opening.await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(1), script_started)
            .await
            .expect("heartbeat did not reach the fake Redis server")
            .unwrap();

        let closing = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.close().await }
        });
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!closing.is_finished());
        assert!(scheduler.opened.load(Ordering::Acquire));
        assert!(!scheduler.worker.heartbeat_stopped());
        assert!(!scheduler.worker.can_claim());

        closing.abort();
        assert!(closing.await.unwrap_err().is_cancelled());
        assert!(scheduler.opened.load(Ordering::Acquire));
        assert!(!scheduler.worker.is_configurable());

        resume_script.send(()).unwrap();
        scheduler.close().await.unwrap();
        assert!(!scheduler.opened.load(Ordering::Acquire));
        assert!(scheduler.worker.is_configurable());

        drop(scheduler);
        tokio::task::spawn_blocking(move || server_task.join().unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancelled_open_replays_one_registration_before_configuration_unfreezes() {
        let HeldServer {
            url,
            ping_started,
            resume_ping,
            script_started,
            resume_script,
            registration_keys,
            task: server_task,
        } = held_server(include_str!("scripts/register.lua"));
        let scheduler = Arc::new(configured(url));
        let opening = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.open(16).await }
        });
        tokio::time::timeout(Duration::from_secs(1), ping_started)
            .await
            .expect("open did not reach the controlled PING")
            .unwrap();
        resume_ping.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), script_started)
            .await
            .expect("open did not reach the controlled Worker registration")
            .unwrap();

        opening.abort();
        assert!(opening.await.unwrap_err().is_cancelled());
        assert!(!scheduler.opened.load(Ordering::Acquire));
        assert!(!scheduler.worker.is_configurable());
        assert!(scheduler.require_configurable().is_err());

        resume_script.send(()).unwrap();
        let error = scheduler.open(17).await.unwrap_err();
        assert!(error.to_string().contains("frozen with concurrency"));
        scheduler.open(16).await.unwrap();
        assert!(scheduler.opened.load(Ordering::Acquire));
        assert!(scheduler.worker.can_claim());
        scheduler.close().await.unwrap();
        assert!(scheduler.worker.is_configurable());

        drop(scheduler);
        tokio::task::spawn_blocking(move || server_task.join().unwrap())
            .await
            .unwrap();
        let keys = registration_keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], keys[1]);
    }

    #[tokio::test]
    async fn close_waiting_behind_open_leaves_the_scheduler_closed() {
        let (url, mut ping_started, resume, server_task) = fake_server();
        let scheduler = configured(url);
        {
            let active = scheduler.connection.lock().await;

            let open = scheduler.open(16);
            tokio::pin!(open);
            tokio::select! {
                biased;
                result = &mut open => panic!("open unexpectedly completed: {result:?}"),
                _ = tokio::task::yield_now() => {}
            }

            let close = scheduler.close();
            tokio::pin!(close);
            tokio::select! {
                biased;
                result = &mut close => panic!("close unexpectedly completed: {result:?}"),
                _ = tokio::task::yield_now() => {}
            }

            drop(active);
            let settled = async { tokio::join!(&mut open, &mut close) };
            tokio::pin!(settled);
            tokio::time::timeout(Duration::from_secs(1), async {
                tokio::select! {
                    _ = &mut ping_started => {}
                    result = &mut settled => panic!("open and close completed before PING was blocked: {result:?}"),
                }
            })
            .await
            .expect("open did not reach the controlled PING");
            resume.send(()).unwrap();
            let (opened, closed) = settled.await;
            opened.unwrap();
            closed.unwrap();

            assert!(scheduler.connection.lock().await.is_none());
            assert!(!scheduler.opened.load(Ordering::Acquire));
        }
        drop(scheduler);
        tokio::task::spawn_blocking(move || server_task.join().unwrap())
            .await
            .unwrap();
    }

    fn configured(url: impl Into<String>) -> Redis {
        Redis::new(url)
            .unwrap()
            .with_worker_id("worker-1")
            .unwrap()
            .with_worker_host("test-host")
            .unwrap()
            .with_worker_version("test")
            .unwrap()
    }

    fn opened() -> Redis {
        let scheduler = configured("redis://127.0.0.1:6379");
        scheduler.opened.store(true, Ordering::Release);
        scheduler
    }

    fn not_open(error: scheduler::Error) {
        assert_eq!(
            error.to_string(),
            "scheduler error: Redis Scheduler is not open"
        );
    }

    fn fake_server() -> (
        String,
        oneshot::Receiver<()>,
        mpsc::Sender<()>,
        thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (ping_sender, ping_started) = oneshot::channel();
        let (resume, resume_receiver) = mpsc::channel();
        let registrations = Arc::new(std::sync::Mutex::new(Vec::new()));
        let server_task = thread::spawn(move || {
            serve_redis(
                listener,
                ping_sender,
                resume_receiver,
                None,
                registrations,
                1,
            )
        });
        (
            format!("redis://{address}"),
            ping_started,
            resume,
            server_task,
        )
    }

    struct HeldServer {
        url: String,
        ping_started: oneshot::Receiver<()>,
        resume_ping: mpsc::Sender<()>,
        script_started: oneshot::Receiver<()>,
        resume_script: mpsc::Sender<()>,
        registration_keys: Arc<std::sync::Mutex<Vec<String>>>,
        task: thread::JoinHandle<()>,
    }

    fn held_server(script: &'static str) -> HeldServer {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (ping_sender, ping_started) = oneshot::channel();
        let (resume_ping, ping_receiver) = mpsc::channel();
        let (script_sender, script_started) = oneshot::channel();
        let (resume_script, script_receiver) = mpsc::channel();
        let hold = Hold {
            hash: redis::Script::new(script).get_hash().to_string(),
            started: Some(script_sender),
            resume: script_receiver,
        };
        let registration_keys = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = registration_keys.clone();
        let connection_limit = if script == include_str!("scripts/register.lua") {
            2
        } else {
            1
        };
        let server_task = thread::spawn(move || {
            serve_redis(
                listener,
                ping_sender,
                ping_receiver,
                Some(hold),
                recorded,
                connection_limit,
            )
        });
        HeldServer {
            url: format!("redis://{address}"),
            ping_started,
            resume_ping,
            script_started,
            resume_script,
            registration_keys,
            task: server_task,
        }
    }

    struct Hold {
        hash: String,
        started: Option<oneshot::Sender<()>>,
        resume: mpsc::Receiver<()>,
    }

    fn serve_redis(
        listener: TcpListener,
        ping_sender: oneshot::Sender<()>,
        resume: mpsc::Receiver<()>,
        mut hold: Option<Hold>,
        registration_keys: Arc<std::sync::Mutex<Vec<String>>>,
        connection_limit: usize,
    ) {
        let mut ping_sender = Some(ping_sender);
        for _ in 0..connection_limit {
            let (mut connection, _) = listener.accept().unwrap();
            connection
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            loop {
                let command = match read_command(&mut connection) {
                    Ok(command) => command,
                    Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                        ) =>
                    {
                        break;
                    }
                    Err(error) => panic!("fake Redis server failed to read a command: {error}"),
                };
                match command.first().map(String::as_str) {
                    Some(command) if command.eq_ignore_ascii_case("PING") => {
                        if let Some(ping_sender) = ping_sender.take() {
                            ping_sender.send(()).unwrap();
                            resume.recv().unwrap();
                        }
                        connection.write_all(b"+PONG\r\n").unwrap();
                    }
                    Some(name)
                        if name.eq_ignore_ascii_case("SCRIPT")
                            && command
                                .get(1)
                                .is_some_and(|action| action.eq_ignore_ascii_case("LOAD")) =>
                    {
                        let hash = redis::Script::new(&command[2]).get_hash().to_string();
                        write_bulk(&mut connection, &hash);
                    }
                    Some(name)
                        if name.eq_ignore_ascii_case("EVALSHA")
                            && command.get(1).is_some_and(|hash| {
                                hash == &redis::Script::new(include_str!("scripts/register.lua"))
                                    .get_hash()
                                    .to_string()
                            }) =>
                    {
                        registration_keys
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .push(command[10].clone());
                        let held = wait_if_held(&mut hold, &command[1]);
                        if connection
                            .write_all(b"*3\r\n:200\r\n$7\r\nsuccess\r\n$5\r\ntoken\r\n")
                            .is_err()
                        {
                            break;
                        }
                        if held && connection_limit > 1 {
                            let _ = connection.flush();
                            break;
                        }
                    }
                    Some(name)
                        if name.eq_ignore_ascii_case("EVALSHA")
                            && command.get(1).is_some_and(|hash| {
                                hash == &redis::Script::new(include_str!("scripts/claim.lua"))
                                    .get_hash()
                                    .to_string()
                            }) =>
                    {
                        wait_if_held(&mut hold, &command[1]);
                        connection
                            .write_all(
                                b"*6\r\n$0\r\n\r\n$0\r\n\r\n$0\r\n\r\n$0\r\n\r\n:1\r\n*0\r\n",
                            )
                            .unwrap();
                    }
                    Some(name)
                        if name.eq_ignore_ascii_case("EVALSHA")
                            && command.get(1).is_some_and(|hash| {
                                hash == &redis::Script::new(include_str!("scripts/offline.lua"))
                                    .get_hash()
                                    .to_string()
                            }) =>
                    {
                        wait_if_held(&mut hold, &command[1]);
                        connection.write_all(b"+OK\r\n").unwrap();
                    }
                    Some(name) if name.eq_ignore_ascii_case("EVALSHA") => {
                        wait_if_held(&mut hold, &command[1]);
                        connection.write_all(b"+OK\r\n").unwrap();
                    }
                    _ => connection.write_all(b"+OK\r\n").unwrap(),
                }
                connection.flush().unwrap();
            }
        }
    }

    fn wait_if_held(hold: &mut Option<Hold>, hash: &str) -> bool {
        let Some(hold) = hold.as_mut().filter(|hold| hold.hash == hash) else {
            return false;
        };
        let Some(started) = hold.started.take() else {
            return false;
        };
        started.send(()).unwrap();
        hold.resume.recv().unwrap();
        true
    }

    fn read_command(connection: &mut TcpStream) -> io::Result<Vec<String>> {
        let count = read_line(connection)?;
        assert_eq!(count.first(), Some(&b'*'));
        let count = std::str::from_utf8(&count[1..])
            .unwrap()
            .parse::<usize>()
            .unwrap();
        (0..count)
            .map(|_| {
                let length = read_line(connection)?;
                assert_eq!(length.first(), Some(&b'$'));
                let length = std::str::from_utf8(&length[1..])
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                let mut value = vec![0; length];
                connection.read_exact(&mut value)?;
                let mut ending = [0; 2];
                connection.read_exact(&mut ending)?;
                assert_eq!(ending, *b"\r\n");
                Ok(String::from_utf8(value).unwrap())
            })
            .collect()
    }

    fn read_line(connection: &mut TcpStream) -> io::Result<Vec<u8>> {
        let mut value = Vec::new();
        loop {
            let mut byte = [0; 1];
            connection.read_exact(&mut byte)?;
            if byte[0] == b'\r' {
                let mut newline = [0; 1];
                connection.read_exact(&mut newline)?;
                assert_eq!(newline, [b'\n']);
                return Ok(value);
            }
            value.push(byte[0]);
        }
    }

    fn write_bulk(connection: &mut TcpStream, value: &str) {
        write!(connection, "${}\r\n{value}\r\n", value.len()).unwrap();
    }
}
