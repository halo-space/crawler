use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sha2::{Digest as _, Sha256};
use spider::{net, scheduler};
use sqlx::{MySql, MySqlPool, Row as _, Transaction};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::decode;
use super::error::{database_number, sqlx as sql_error};

pub(super) const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
pub(super) const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct Config {
    id: Option<String>,
    host: Option<String>,
    version: Option<String>,
    modes: Vec<net::Mode>,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            id: None,
            host: None,
            version: None,
            modes: vec![net::Mode::Http],
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            heartbeat_timeout: DEFAULT_HEARTBEAT_TIMEOUT,
        }
    }
}

pub(super) struct Worker {
    config: Config,
    can_claim: Arc<AtomicBool>,
    runtime: std::sync::Mutex<Runtime>,
}

#[derive(Default)]
struct Runtime {
    open_key: Option<String>,
    concurrency: Option<usize>,
    token: Option<String>,
    heartbeat: Option<Heartbeat>,
}

struct Heartbeat {
    stop: watch::Sender<bool>,
    stopped: watch::Receiver<bool>,
    task: JoinHandle<()>,
}

struct HeartbeatEnd(watch::Sender<bool>);

impl Drop for HeartbeatEnd {
    fn drop(&mut self) {
        let _ = self.0.send_replace(true);
    }
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        let _ = self.stop.send_replace(true);
        self.task.abort();
    }
}

impl Heartbeat {
    fn stop(&self) -> watch::Receiver<bool> {
        let _ = self.stop.send_replace(true);
        self.stopped.clone()
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.can_claim.store(false, Ordering::Release);
        let runtime = self
            .runtime
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        runtime.open_key = None;
        runtime.concurrency = None;
        runtime.token = None;
        runtime.heartbeat.take();
    }
}

impl Worker {
    pub(super) fn new() -> Self {
        Self {
            config: Config::default(),
            can_claim: Arc::new(AtomicBool::new(false)),
            runtime: std::sync::Mutex::new(Runtime::default()),
        }
    }

    pub(super) fn set_id(&mut self, value: impl Into<String>) -> Result<(), scheduler::Error> {
        self.config.id = Some(required("worker_id", value.into())?);
        Ok(())
    }

    pub(super) fn set_host(&mut self, value: impl Into<String>) -> Result<(), scheduler::Error> {
        self.config.host = Some(required("worker_host", value.into())?);
        Ok(())
    }

    pub(super) fn set_version(&mut self, value: impl Into<String>) -> Result<(), scheduler::Error> {
        self.config.version = Some(required("worker_version", value.into())?);
        Ok(())
    }

    pub(super) fn set_modes(
        &mut self,
        modes: impl IntoIterator<Item = net::Mode>,
    ) -> Result<(), scheduler::Error> {
        let mut http = false;
        let mut browser = false;
        for mode in modes {
            match mode {
                net::Mode::Http => http = true,
                net::Mode::Browser => browser = true,
            }
        }
        if !http && !browser {
            return Err(scheduler::Error::Message(
                "worker modes must not be empty".to_string(),
            ));
        }
        self.config.modes.clear();
        if http {
            self.config.modes.push(net::Mode::Http);
        }
        if browser {
            self.config.modes.push(net::Mode::Browser);
        }
        Ok(())
    }

    pub(super) fn set_heartbeat(
        &mut self,
        interval: Duration,
        timeout: Duration,
    ) -> Result<(), scheduler::Error> {
        let interval_ms = milliseconds("heartbeat interval", interval)?;
        let timeout_ms = milliseconds("heartbeat timeout", timeout)?;
        if interval_ms >= timeout_ms {
            return Err(scheduler::Error::Message(
                "heartbeat interval must be shorter than heartbeat timeout".to_string(),
            ));
        }
        self.config.heartbeat_interval = interval;
        self.config.heartbeat_timeout = timeout;
        Ok(())
    }

    pub(super) fn id(&self) -> Result<&str, scheduler::Error> {
        self.config
            .id
            .as_deref()
            .ok_or_else(|| missing("worker_id"))
    }

    pub(super) fn modes(&self) -> &[net::Mode] {
        &self.config.modes
    }

    pub(super) fn can_claim(&self) -> bool {
        self.can_claim.load(Ordering::Acquire)
    }

    pub(super) fn is_configurable(&self) -> bool {
        let runtime = self.runtime();
        runtime.concurrency.is_none() && runtime.open_key.is_none() && runtime.token.is_none()
    }

    pub(super) fn concurrency(&self) -> Option<usize> {
        self.runtime().concurrency
    }

    pub(super) fn validate(&self, concurrency: usize) -> Result<(), scheduler::Error> {
        if concurrency == 0 {
            return Err(scheduler::Error::Message(
                "worker concurrency must be positive".to_string(),
            ));
        }
        database_concurrency(concurrency)?;
        self.id()?;
        if self.config.host.is_none() {
            return Err(missing("worker_host"));
        }
        if self.config.version.is_none() {
            return Err(missing("worker_version"));
        }
        if self.config.modes.is_empty() {
            return Err(scheduler::Error::Message(
                "worker modes must not be empty".to_string(),
            ));
        }
        milliseconds("heartbeat interval", self.config.heartbeat_interval)?;
        milliseconds("heartbeat timeout", self.config.heartbeat_timeout)?;
        Ok(())
    }

    pub(super) async fn online(
        &self,
        transaction: &mut Transaction<'_, MySql>,
    ) -> Result<(bool, i64), scheduler::Error> {
        let worker_id = self.id()?;
        let row = sqlx::query(
            "SELECT host, version, modes, concurrency, heartbeat_timeout, last_heartbeat, \
                    token, offline_time \
             FROM workers WHERE worker_id = ? FOR SHARE",
        )
        .bind(worker_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(sql_error)?
        .ok_or_else(|| {
            scheduler::Error::Message("MySQL Worker registration was not found".to_string())
        })?;
        let now = database_time(transaction).await?;

        let host = decode::string(&row, "host")?;
        let version = decode::string(&row, "version")?;
        let token = decode::string(&row, "token")?;
        let modes = row
            .try_get::<serde_json::Value, _>("modes")
            .map_err(sql_error)?;
        let expected_modes = serde_json::to_value(&self.config.modes)
            .map_err(|error| scheduler::Error::Message(error.to_string()))?;
        let concurrency = row.try_get::<u32, _>("concurrency").map_err(sql_error)?;
        if host.is_empty()
            || version.is_empty()
            || token.is_empty()
            || modes != expected_modes
            || concurrency == 0
        {
            return Err(scheduler::Error::Message(
                "stored MySQL Worker metadata is incomplete or incompatible".to_string(),
            ));
        }

        let heartbeat_timeout = row
            .try_get::<i64, _>("heartbeat_timeout")
            .map_err(sql_error)?;
        let last_heartbeat = row.try_get::<i64, _>("last_heartbeat").map_err(sql_error)?;
        if heartbeat_timeout <= 0 || last_heartbeat < 0 || last_heartbeat > now {
            return Err(scheduler::Error::Message(
                "stored MySQL Worker heartbeat is invalid".to_string(),
            ));
        }
        if let Some(offline_time) = row
            .try_get::<Option<i64>, _>("offline_time")
            .map_err(sql_error)?
        {
            if offline_time <= 0 || offline_time > now {
                return Err(scheduler::Error::Message(
                    "stored MySQL Worker offline time is invalid".to_string(),
                ));
            }
            return Ok((false, now));
        }
        Ok((now.saturating_sub(last_heartbeat) < heartbeat_timeout, now))
    }

    pub(super) async fn start(
        &self,
        pool: &MySqlPool,
        concurrency: usize,
    ) -> Result<(), scheduler::Error> {
        self.validate(concurrency)?;
        let worker_id = self.id()?.to_string();
        let host = self
            .config
            .host
            .as_deref()
            .ok_or_else(|| missing("worker_host"))?;
        let version = self
            .config
            .version
            .as_deref()
            .ok_or_else(|| missing("worker_version"))?;
        let modes = serde_json::to_value(&self.config.modes)
            .map_err(|error| scheduler::Error::Message(error.to_string()))?;
        let concurrency_value = database_concurrency(concurrency)?;
        let heartbeat_timeout = milliseconds("heartbeat timeout", self.config.heartbeat_timeout)?;
        let open_key = self.open_key(concurrency)?;
        let token = registration_token(&worker_id, &open_key);
        let mut transaction = pool.begin().await.map_err(sql_error)?;
        let existing = sqlx::query(
            "SELECT host, version, modes, concurrency, heartbeat_timeout, \
                    last_heartbeat, token, offline_time \
             FROM workers WHERE worker_id = ? FOR UPDATE",
        )
        .bind(&worker_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(sql_error)?;
        let now = database_time(&mut transaction).await?;

        if let Some(row) = existing {
            let stored_token = decode::string(&row, "token")?;
            if stored_token == token {
                let stored_host = decode::string(&row, "host")?;
                let stored_version = decode::string(&row, "version")?;
                let stored_modes = row
                    .try_get::<serde_json::Value, _>("modes")
                    .map_err(sql_error)?;
                let stored_concurrency = row.try_get::<u32, _>("concurrency").map_err(sql_error)?;
                let stored_timeout = row
                    .try_get::<i64, _>("heartbeat_timeout")
                    .map_err(sql_error)?;
                if stored_host != host
                    || stored_version != version
                    || stored_modes != modes
                    || stored_concurrency != concurrency_value
                    || stored_timeout != heartbeat_timeout
                {
                    return Err(scheduler::Error::Message(
                        "stored MySQL Worker does not match the registration replay".to_string(),
                    ));
                }
                sqlx::query(
                    "UPDATE workers SET last_heartbeat = ?, offline_time = NULL, \
                            updated_time = CURRENT_TIMESTAMP(3) \
                     WHERE worker_id = ?",
                )
                .bind(now)
                .bind(&worker_id)
                .execute(&mut *transaction)
                .await
                .map_err(sql_error)?;
                transaction.commit().await.map_err(sql_error)?;
                self.confirm_started(pool.clone(), worker_id, token, concurrency);
                return Ok(());
            }

            let last_heartbeat = row.try_get::<i64, _>("last_heartbeat").map_err(sql_error)?;
            let stored_timeout = row
                .try_get::<i64, _>("heartbeat_timeout")
                .map_err(sql_error)?;
            let offline_time = row
                .try_get::<Option<i64>, _>("offline_time")
                .map_err(sql_error)?;
            if offline_time.is_none()
                && last_heartbeat >= 0
                && stored_timeout > 0
                && now.saturating_sub(last_heartbeat) < stored_timeout
            {
                return Err(worker_conflict());
            }
            sqlx::query("DELETE FROM workers WHERE worker_id = ?")
                .bind(&worker_id)
                .execute(&mut *transaction)
                .await
                .map_err(sql_error)?;
        }

        let result = sqlx::query(
            "INSERT INTO workers \
             (worker_id, host, ip, version, modes, concurrency, heartbeat_timeout, \
              last_heartbeat, token, offline_time, created_time, updated_time) \
             VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, NULL, \
                     CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))",
        )
        .bind(&worker_id)
        .bind(host)
        .bind(version)
        .bind(modes)
        .bind(concurrency_value)
        .bind(heartbeat_timeout)
        .bind(now)
        .bind(&token)
        .execute(&mut *transaction)
        .await;
        if let Err(error) = result {
            if database_number(&error) == Some(1062) {
                return Err(worker_conflict());
            }
            return Err(sql_error(error));
        }
        transaction.commit().await.map_err(sql_error)?;
        self.confirm_started(pool.clone(), worker_id, token, concurrency);
        Ok(())
    }

    fn confirm_started(
        &self,
        pool: MySqlPool,
        worker_id: String,
        token: String,
        concurrency: usize,
    ) {
        let heartbeat = start_heartbeat(
            pool,
            self.can_claim.clone(),
            worker_id,
            token.clone(),
            self.config.heartbeat_interval,
        );
        let mut runtime = self.runtime();
        drop(runtime.heartbeat.replace(heartbeat));
        runtime.open_key = None;
        runtime.concurrency = Some(concurrency);
        runtime.token = Some(token);
        self.can_claim.store(true, Ordering::Release);
    }

    pub(super) async fn stop_heartbeat(&self) {
        self.can_claim.store(false, Ordering::Release);
        let Some(mut stopped) = self.runtime().heartbeat.as_ref().map(Heartbeat::stop) else {
            return;
        };
        if *stopped.borrow() {
            return;
        }
        while stopped.changed().await.is_ok() {
            if *stopped.borrow() {
                return;
            }
        }
    }

    pub(super) async fn offline(&self, pool: &MySqlPool) -> Result<(), scheduler::Error> {
        let worker_id = self.id()?;
        let token = self.runtime().token.clone().ok_or_else(|| {
            scheduler::Error::Message("MySQL Worker registration token was not found".to_string())
        })?;
        let result = sqlx::query(
            "UPDATE workers SET \
                 offline_time = CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED), \
                 updated_time = CURRENT_TIMESTAMP(3) \
             WHERE worker_id = ? AND token = ? AND offline_time IS NULL",
        )
        .bind(worker_id)
        .bind(token)
        .execute(pool)
        .await
        .map_err(sql_error)?;
        if result.rows_affected() == 1 {
            Ok(())
        } else {
            Err(scheduler::Error::Message(
                "MySQL Worker offline identity does not match the active registration".to_string(),
            ))
        }
    }

    pub(super) fn reset(&self) {
        self.can_claim.store(false, Ordering::Release);
        let mut runtime = self.runtime();
        runtime.open_key = None;
        runtime.concurrency = None;
        runtime.token = None;
        runtime.heartbeat.take();
    }

    fn open_key(&self, concurrency: usize) -> Result<String, scheduler::Error> {
        let mut runtime = self.runtime();
        if let Some(active) = runtime.concurrency
            && active != concurrency
        {
            return Err(scheduler::Error::Message(format!(
                "MySQL Scheduler Worker lifecycle is frozen with concurrency {active}; received {concurrency}"
            )));
        }
        runtime.concurrency = Some(concurrency);
        Ok(runtime
            .open_key
            .get_or_insert_with(|| uuid::Uuid::now_v7().to_string())
            .clone())
    }

    fn runtime(&self) -> std::sync::MutexGuard<'_, Runtime> {
        self.runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn start_heartbeat(
    pool: MySqlPool,
    can_claim: Arc<AtomicBool>,
    worker_id: String,
    token: String,
    interval: Duration,
) -> Heartbeat {
    let (stop, mut stopping) = watch::channel(false);
    let (ended, stopped) = watch::channel(false);
    let task = tokio::spawn(async move {
        let _ended = HeartbeatEnd(ended);
        loop {
            if *stopping.borrow() {
                return;
            }
            tokio::select! {
                changed = stopping.changed() => {
                    if changed.is_err() || *stopping.borrow() {
                        return;
                    }
                }
                _ = tokio::time::sleep(interval) => {}
            }

            let result = sqlx::query(
                "UPDATE workers SET \
                     last_heartbeat = CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED), \
                     updated_time = CURRENT_TIMESTAMP(3) \
                 WHERE worker_id = ? AND token = ? AND offline_time IS NULL",
            )
            .bind(&worker_id)
            .bind(&token)
            .execute(&pool)
            .await
            .map_err(sql_error)
            .and_then(|result| {
                if result.rows_affected() == 1 {
                    Ok(())
                } else {
                    Err(scheduler::Error::Message(
                        "MySQL Worker heartbeat identity does not match the active registration"
                            .to_string(),
                    ))
                }
            });
            match result {
                Ok(()) => {
                    if !can_claim.swap(true, Ordering::AcqRel) {
                        tracing::info!(worker_id = %worker_id, "MySQL Worker heartbeat recovered");
                    }
                }
                Err(error) => {
                    can_claim.store(false, Ordering::Release);
                    tracing::warn!(
                        worker_id = %worker_id,
                        error = %error,
                        "MySQL Worker heartbeat failed; Request claims are paused"
                    );
                }
            }
        }
    });
    Heartbeat {
        stop,
        stopped,
        task,
    }
}

pub(super) async fn database_time(
    transaction: &mut Transaction<'_, MySql>,
) -> Result<i64, scheduler::Error> {
    sqlx::query_scalar::<_, i64>(
        "SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED)",
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(sql_error)
}

fn registration_token(worker_id: &str, open_key: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(worker_id.as_bytes());
    hash.update(b"\0");
    hash.update(open_key.as_bytes());
    let bytes = hash.finalize();
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
}

fn database_concurrency(value: usize) -> Result<u32, scheduler::Error> {
    u32::try_from(value).map_err(|_| {
        scheduler::Error::Message(
            "worker concurrency exceeds the MySQL unsigned integer range".to_string(),
        )
    })
}

fn milliseconds(name: &str, value: Duration) -> Result<i64, scheduler::Error> {
    let value = i64::try_from(value.as_millis()).map_err(|_| {
        scheduler::Error::Message(format!(
            "{name} does not fit a signed 64-bit millisecond value"
        ))
    })?;
    if value <= 0 {
        Err(scheduler::Error::Message(format!(
            "{name} must be positive"
        )))
    } else {
        Ok(value)
    }
}

fn required(name: &str, value: String) -> Result<String, scheduler::Error> {
    if value.trim().is_empty() {
        Err(missing(name))
    } else {
        Ok(value)
    }
}

fn missing(name: &str) -> scheduler::Error {
    scheduler::Error::Message(format!(
        "MySQL Scheduler requires {name}; configure it before open"
    ))
}

fn worker_conflict() -> scheduler::Error {
    scheduler::Error::Message(
        "MySQL Worker registration failed with code 100: worker_id is already online".to_string(),
    )
}
