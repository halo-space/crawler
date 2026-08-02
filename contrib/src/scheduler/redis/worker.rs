use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use spider::{net, scheduler};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::error::redis as redis_error;
use super::key::Keys;
use super::script::Scripts;

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

struct HeartbeatTask {
    connection: redis::aio::ConnectionManager,
    key: String,
    script: redis::Script,
    id: String,
    token: String,
    interval: Duration,
    can_claim: Arc<AtomicBool>,
}

impl Heartbeat {
    fn stop(&self) -> watch::Receiver<bool> {
        let _ = self.stop.send_replace(true);
        self.stopped.clone()
    }

    #[cfg(test)]
    fn is_stopped(&self) -> bool {
        *self.stopped.borrow()
    }
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct HeartbeatEnd(watch::Sender<bool>);

impl Drop for HeartbeatEnd {
    fn drop(&mut self) {
        let _ = self.0.send_replace(true);
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

    pub(super) fn set_id(&mut self, id: impl Into<String>) -> Result<(), scheduler::Error> {
        self.config.id = Some(required("worker_id", id.into())?);
        Ok(())
    }

    pub(super) fn set_host(&mut self, host: impl Into<String>) -> Result<(), scheduler::Error> {
        self.config.host = Some(required("worker_host", host.into())?);
        Ok(())
    }

    pub(super) fn set_version(
        &mut self,
        version: impl Into<String>,
    ) -> Result<(), scheduler::Error> {
        self.config.version = Some(required("worker_version", version.into())?);
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

    pub(super) async fn start(
        &self,
        connection: &mut redis::aio::ConnectionManager,
        keys: &Keys,
        scripts: &Scripts,
        concurrency: usize,
    ) -> Result<(), scheduler::Error> {
        self.validate(concurrency)?;
        let id = self.id()?.to_string();
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
        let modes = serde_json::to_string(&self.config.modes)
            .map_err(|error| scheduler::Error::Message(error.to_string()))?;
        let timeout = milliseconds("heartbeat timeout", self.config.heartbeat_timeout)?;
        let open_key = self.open_key(concurrency)?;
        let (code, message, token): (i64, String, String) = scripts
            .register
            .prepare_invoke()
            .key(keys.worker(&id))
            .arg(&id)
            .arg(host)
            .arg(version)
            .arg(modes)
            .arg(concurrency)
            .arg(timeout)
            .arg(open_key)
            .invoke_async(connection)
            .await
            .map_err(redis_error)?;
        if code != 200 {
            return Err(scheduler::Error::Message(format!(
                "Redis Worker registration failed with code {code}: {message}"
            )));
        }

        let (stop, stop_signal) = watch::channel(false);
        let (ended, stopped) = watch::channel(false);
        let heartbeat_task = HeartbeatTask {
            connection: connection.clone(),
            key: keys.worker(&id),
            script: scripts.heartbeat.clone(),
            id,
            token: token.clone(),
            interval: self.config.heartbeat_interval,
            can_claim: self.can_claim.clone(),
        };
        let heartbeat = Heartbeat {
            stop,
            stopped,
            task: tokio::spawn(async move {
                let _end = HeartbeatEnd(ended);
                heartbeat_task.run(stop_signal).await;
            }),
        };
        let mut runtime = self.runtime();
        drop(runtime.heartbeat.replace(heartbeat));
        runtime.open_key = None;
        runtime.concurrency = Some(concurrency);
        runtime.token = Some(token);
        self.can_claim.store(true, Ordering::Release);
        Ok(())
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

    pub(super) async fn offline(
        &self,
        connection: &mut redis::aio::ConnectionManager,
        keys: &Keys,
        scripts: &Scripts,
    ) -> Result<(), scheduler::Error> {
        let id = self.id()?;
        let token = self.runtime().token.clone().ok_or_else(|| {
            scheduler::Error::Message("Redis Worker registration token was not found".to_string())
        })?;
        let status: String = scripts
            .offline
            .prepare_invoke()
            .key(keys.worker(id))
            .arg(id)
            .arg(token)
            .invoke_async(connection)
            .await
            .map_err(redis_error)?;
        worker_result(status)
    }

    pub(super) fn reset(&self) {
        self.can_claim.store(false, Ordering::Release);
        let mut runtime = self.runtime();
        runtime.open_key = None;
        runtime.concurrency = None;
        runtime.token = None;
        runtime.heartbeat.take();
    }

    pub(super) fn validate(&self, concurrency: usize) -> Result<(), scheduler::Error> {
        if concurrency == 0 {
            return Err(scheduler::Error::Message(
                "worker concurrency must be positive".to_string(),
            ));
        }
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

    fn runtime(&self) -> std::sync::MutexGuard<'_, Runtime> {
        self.runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn concurrency(&self) -> Option<usize> {
        self.runtime().concurrency
    }

    #[cfg(test)]
    pub(super) fn heartbeat_stopped(&self) -> bool {
        self.runtime()
            .heartbeat
            .as_ref()
            .is_none_or(Heartbeat::is_stopped)
    }

    fn open_key(&self, concurrency: usize) -> Result<String, scheduler::Error> {
        let mut runtime = self.runtime();
        if let Some(active) = runtime.concurrency {
            if active != concurrency {
                return Err(scheduler::Error::Message(format!(
                    "Redis Scheduler Worker lifecycle is frozen with concurrency {active}; received {concurrency}"
                )));
            }
        } else {
            runtime.concurrency = Some(concurrency);
        }
        Ok(runtime
            .open_key
            .get_or_insert_with(|| uuid::Uuid::now_v7().to_string())
            .clone())
    }
}

impl HeartbeatTask {
    async fn run(mut self, mut stop: watch::Receiver<bool>) {
        loop {
            if *stop.borrow() {
                return;
            }
            tokio::select! {
                biased;
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        return;
                    }
                }
                _ = tokio::time::sleep(self.interval) => {}
            }
            if *stop.borrow() {
                return;
            }
            let result = self
                .script
                .prepare_invoke()
                .key(&self.key)
                .arg(&self.id)
                .arg(&self.token)
                .invoke_async::<String>(&mut self.connection)
                .await
                .map_err(redis_error)
                .and_then(worker_result);
            if *stop.borrow() {
                return;
            }
            match result {
                Ok(()) => {
                    if !self.can_claim.swap(true, Ordering::AcqRel) {
                        tracing::info!(worker_id = %self.id, "Redis Worker heartbeat recovered");
                    }
                }
                Err(error) => {
                    self.can_claim.store(false, Ordering::Release);
                    tracing::warn!(worker_id = %self.id, error = %error, "Redis Worker heartbeat failed");
                }
            }
        }
    }
}

fn worker_result(status: String) -> Result<(), scheduler::Error> {
    match status.as_str() {
        "OK" => Ok(()),
        "WORKER_NOT_FOUND" => Err(scheduler::Error::Message(
            "Redis Worker registration was not found".to_string(),
        )),
        "WORKER_ID_MISMATCH" => Err(scheduler::Error::Message(
            "stored Redis Worker identity does not match its key".to_string(),
        )),
        "WORKER_TOKEN_MISMATCH" => Err(scheduler::Error::Message(
            "Redis Worker registration was replaced".to_string(),
        )),
        "WORKER_OFFLINE" => Err(scheduler::Error::Message(
            "Redis Worker is offline".to_string(),
        )),
        "CORRUPT_WORKER" => Err(scheduler::Error::Message(
            "stored Redis Worker has an invalid type".to_string(),
        )),
        "CORRUPT_WORKER_METADATA" => Err(scheduler::Error::Message(
            "stored Redis Worker metadata is incomplete or invalid".to_string(),
        )),
        status => Err(scheduler::Error::Message(format!(
            "Redis Worker operation failed: {status}"
        ))),
    }
}

fn required(field: &str, value: String) -> Result<String, scheduler::Error> {
    if value.trim().is_empty() {
        Err(scheduler::Error::Message(format!(
            "{field} must not be empty"
        )))
    } else {
        Ok(value)
    }
}

fn missing(field: &str) -> scheduler::Error {
    scheduler::Error::Message(format!("Redis Scheduler requires {field}"))
}

fn milliseconds(field: &str, value: Duration) -> Result<i64, scheduler::Error> {
    let value = i64::try_from(value.as_millis())
        .map_err(|_| scheduler::Error::Message(format!("{field} exceeds Redis integer range")))?;
    if value == 0 {
        return Err(scheduler::Error::Message(format!(
            "{field} must be at least one millisecond"
        )));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distributed_worker_metadata_is_required_before_open() {
        let mut worker = Worker::new();
        assert!(
            worker
                .validate(1)
                .unwrap_err()
                .to_string()
                .contains("worker_id")
        );
        worker.set_id("worker-1").unwrap();
        assert!(worker.validate(1).unwrap_err().to_string().contains("host"));
        worker.set_host("crawler-node-01").unwrap();
        assert!(
            worker
                .validate(1)
                .unwrap_err()
                .to_string()
                .contains("version")
        );
        worker.set_version("1.0.0").unwrap();
        assert!(
            worker
                .validate(0)
                .unwrap_err()
                .to_string()
                .contains("concurrency")
        );
        worker.validate(1).unwrap();
    }

    #[test]
    fn modes_are_canonical_and_non_empty() {
        let mut worker = Worker::new();
        worker
            .set_modes([net::Mode::Browser, net::Mode::Http, net::Mode::Browser])
            .unwrap();
        assert_eq!(worker.modes(), [net::Mode::Http, net::Mode::Browser]);
        assert!(worker.set_modes([]).is_err());
    }

    #[test]
    fn heartbeat_policy_requires_a_real_offline_window() {
        let mut worker = Worker::new();
        assert!(
            worker
                .set_heartbeat(Duration::ZERO, Duration::from_secs(1))
                .is_err()
        );
        assert!(
            worker
                .set_heartbeat(Duration::from_nanos(1), Duration::from_secs(1))
                .is_err()
        );
        assert!(
            worker
                .set_heartbeat(Duration::from_secs(1), Duration::from_secs(1))
                .is_err()
        );
        worker
            .set_heartbeat(Duration::from_secs(1), Duration::from_secs(2))
            .unwrap();
    }

    #[test]
    fn pending_open_reuses_its_operation_key_until_registration_finishes() {
        let worker = Worker::new();
        let first = worker.open_key(4).unwrap();
        assert_eq!(worker.open_key(4).unwrap(), first);
        assert!(worker.open_key(5).is_err());

        worker.runtime().open_key = None;
        assert_ne!(worker.open_key(4).unwrap(), first);
    }
}
