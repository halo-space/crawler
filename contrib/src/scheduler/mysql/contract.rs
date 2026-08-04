use std::str::FromStr as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use spider::{net, payload, scheduler, trace};
use sqlx::MySqlPool;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};

use super::error::sqlx as sql_error;
use super::worker::Worker;

/// A MySQL 9 Scheduler backed by the database selected in its DSN.
///
/// The operator owns schema installation. [`scheduler::Scheduler::open`] only
/// establishes the pool, validates the required tables, and starts this
/// Scheduler's Worker lifecycle.
pub struct MySql {
    options: MySqlConnectOptions,
    database: String,
    pool: Mutex<Option<MySqlPool>>,
    pub(super) lease: scheduler::Lease,
    pub(super) worker: Worker,
    opened: AtomicBool,
    lifecycle: Mutex<()>,
    activity: RwLock<()>,
}

impl MySql {
    pub fn new(dsn: impl AsRef<str>) -> Result<Self, scheduler::Error> {
        let value = dsn.as_ref();
        let url = url::Url::parse(value).map_err(super::error::message)?;
        if url.scheme() != "mysql" || !url.has_host() {
            return Err(scheduler::Error::Message(
                "MySQL Scheduler DSN must be an absolute mysql URL".to_string(),
            ));
        }
        let database = url.path().trim_matches('/');
        if database.is_empty() || database.contains('/') {
            return Err(scheduler::Error::Message(
                "MySQL Scheduler DSN must select exactly one database".to_string(),
            ));
        }
        let options = MySqlConnectOptions::from_str(value).map_err(super::error::message)?;
        Ok(Self {
            options,
            database: database.to_string(),
            pool: Mutex::new(None),
            lease: scheduler::Lease::default(),
            worker: Worker::new(),
            opened: AtomicBool::new(false),
            lifecycle: Mutex::new(()),
            activity: RwLock::new(()),
        })
    }

    pub fn with_lease(mut self, lease: scheduler::Lease) -> Result<Self, scheduler::Error> {
        self.require_configurable()?;
        self.lease = lease;
        Ok(self)
    }

    pub fn with_worker_id(
        mut self,
        worker_id: impl Into<String>,
    ) -> Result<Self, scheduler::Error> {
        self.require_configurable()?;
        self.worker.set_id(worker_id)?;
        Ok(self)
    }

    pub fn with_worker_host(mut self, host: impl Into<String>) -> Result<Self, scheduler::Error> {
        self.require_configurable()?;
        self.worker.set_host(host)?;
        Ok(self)
    }

    pub fn with_worker_version(
        mut self,
        version: impl Into<String>,
    ) -> Result<Self, scheduler::Error> {
        self.require_configurable()?;
        self.worker.set_version(version)?;
        Ok(self)
    }

    pub fn with_modes(
        mut self,
        modes: impl IntoIterator<Item = net::Mode>,
    ) -> Result<Self, scheduler::Error> {
        self.require_configurable()?;
        self.worker.set_modes(modes)?;
        Ok(self)
    }

    pub fn with_heartbeat(
        mut self,
        interval: Duration,
        timeout: Duration,
    ) -> Result<Self, scheduler::Error> {
        self.require_configurable()?;
        self.worker.set_heartbeat(interval, timeout)?;
        Ok(self)
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    fn require_configurable(&self) -> Result<(), scheduler::Error> {
        if self.opened.load(Ordering::Acquire) || !self.worker.is_configurable() {
            Err(scheduler::Error::Message(
                "MySQL Scheduler cannot be reconfigured during an active Worker lifecycle"
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
                "MySQL Scheduler is not open".to_string(),
            ))
        }
    }

    async fn enter(&self) -> Result<RwLockReadGuard<'_, ()>, scheduler::Error> {
        let activity = self.activity.read().await;
        self.require_open()?;
        Ok(activity)
    }

    pub(super) async fn pool(&self) -> Result<MySqlPool, scheduler::Error> {
        self.pool
            .lock()
            .await
            .as_ref()
            .cloned()
            .ok_or_else(|| scheduler::Error::Message("MySQL Scheduler is not open".to_string()))
    }

    async fn connect(&self, concurrency: usize) -> Result<MySqlPool, scheduler::Error> {
        let max_connections = u32::try_from(concurrency)
            .unwrap_or(u32::MAX)
            .saturating_add(4)
            .max(5);
        let pool = MySqlPoolOptions::new()
            .min_connections(1)
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(5))
            .after_connect(|connection, _| {
                Box::pin(async move {
                    sqlx::query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect_with(self.options.clone())
            .await
            .map_err(sql_error)?;
        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .map_err(sql_error)?;
        super::schema::validate(&pool, &self.database).await?;
        Ok(pool)
    }
}

impl scheduler::Scheduler for MySql {
    fn lease(&self) -> Option<scheduler::Lease> {
        Some(self.lease)
    }

    async fn open(&self, concurrency: usize) -> Result<(), scheduler::Error> {
        let _lifecycle = self.lifecycle.lock().await;
        let _activity = self.activity.write().await;
        let mut active = self.pool.lock().await;
        if let Some(pool) = active.as_ref() {
            let current = self.worker.concurrency().ok_or_else(|| {
                scheduler::Error::Message(
                    "MySQL Scheduler Worker lifecycle state is missing".to_string(),
                )
            })?;
            if current != concurrency {
                return Err(scheduler::Error::Message(format!(
                    "MySQL Scheduler is already open with concurrency {current}; received {concurrency}"
                )));
            }
            sqlx::query("SELECT 1")
                .execute(pool)
                .await
                .map_err(sql_error)?;
            super::schema::validate(pool, &self.database).await?;
            self.opened.store(true, Ordering::Release);
            return Ok(());
        }

        self.worker.validate(concurrency)?;
        if let Some(current) = self.worker.concurrency()
            && current != concurrency
        {
            return Err(scheduler::Error::Message(format!(
                "MySQL Scheduler Worker lifecycle is frozen with concurrency {current}; received {concurrency}"
            )));
        }
        let pool = self.connect(concurrency).await?;
        self.worker.start(&pool, concurrency).await?;
        *active = Some(pool);
        self.opened.store(true, Ordering::Release);
        Ok(())
    }

    async fn close(&self) -> Result<(), scheduler::Error> {
        let _lifecycle = self.lifecycle.lock().await;
        let _activity = self.activity.write().await;
        let mut active = self.pool.lock().await;
        self.worker.stop_heartbeat().await;
        if let Some(pool) = active.as_ref()
            && let Err(error) = self.worker.offline(pool).await
        {
            tracing::warn!(error = %error, "MySQL Worker offline update failed");
        }
        self.worker.reset();
        if let Some(pool) = active.take() {
            pool.close().await;
        }
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

impl scheduler::Init for MySql {
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
