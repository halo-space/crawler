use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use spider::{net, scheduler};

use super::{client, state, wire};

const SUCCESS: i32 = 200;

pub(super) struct Config {
    id: Option<String>,
    host: Option<String>,
    version: Option<String>,
    modes: Vec<net::Mode>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            id: None,
            host: None,
            version: None,
            modes: vec![net::Mode::Http],
        }
    }
}

impl Config {
    pub(super) fn set_id(&mut self, id: String) {
        self.id = Some(id);
    }

    pub(super) fn set_host(&mut self, host: String) {
        self.host = Some(host);
    }

    pub(super) fn set_version(&mut self, version: String) {
        self.version = Some(version);
    }

    pub(super) fn set_modes(&mut self, modes: impl IntoIterator<Item = net::Mode>) {
        self.modes = canonical_modes(modes);
    }

    pub(super) fn validate(&self, concurrency: usize) -> Result<(), scheduler::Error> {
        required("worker_id", self.id.as_deref())?;
        required("worker_host", self.host.as_deref())?;
        required("worker_version", self.version.as_deref())?;
        if concurrency == 0 {
            return Err(scheduler::Error::Message(
                "worker concurrency must be positive".to_string(),
            ));
        }
        if self.modes.is_empty() {
            return Err(scheduler::Error::Message(
                "worker modes must not be empty".to_string(),
            ));
        }
        Ok(())
    }

    pub(super) fn id(&self) -> Result<&str, scheduler::Error> {
        required("worker_id", self.id.as_deref())
    }

    pub(super) fn modes(&self) -> &[net::Mode] {
        &self.modes
    }

    fn registration(&self, concurrency: usize) -> Result<wire::Register, scheduler::Error> {
        self.validate(concurrency)?;
        Ok(wire::Register {
            worker_id: self.id()?.to_string(),
            host: required("worker_host", self.host.as_deref())?.to_string(),
            version: required("worker_version", self.version.as_deref())?.to_string(),
            modes: self.modes.clone(),
            concurrency,
        })
    }
}

pub(super) async fn register(
    client: &client::Client,
    config: &Config,
    concurrency: usize,
    operation_key: &str,
) -> Result<String, scheduler::Error> {
    let body = config.registration(concurrency)?;
    let response = client
        .post::<_, wire::WorkerResponse>("v1/worker/register", &body, Some(operation_key))
        .await?;
    success("register", &response)?;
    response
        .data
        .as_str()
        .filter(|token| !token.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            scheduler::Error::Message(
                "Worker register response data must be a non-empty token".to_string(),
            )
        })
}

pub(super) fn start_heartbeat(
    client: client::Client,
    runtime: Arc<state::Runtime>,
    epoch: u64,
    worker_id: String,
    token: String,
    interval: Duration,
) -> state::Heartbeat {
    let (stop, mut stopping) = tokio::sync::watch::channel(false);
    let (ended, stopped) = tokio::sync::watch::channel(false);
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
            if !runtime.is_open(epoch) {
                return;
            }

            let body = wire::Worker {
                worker_id: worker_id.clone(),
                token: token.clone(),
            };
            let result = client
                .post_once::<_, wire::WorkerResponse>("v1/worker/heartbeat", &body)
                .await
                .and_then(|response| success("heartbeat", &response));
            if !runtime.is_open(epoch) {
                return;
            }
            match result {
                Ok(()) => {
                    if !runtime.can_claim.swap(true, Ordering::AcqRel) {
                        tracing::info!(
                            worker_id = %worker_id,
                            "API Scheduler Worker heartbeat recovered"
                        );
                    }
                }
                Err(error) => {
                    runtime.can_claim.store(false, Ordering::Release);
                    tracing::warn!(
                        worker_id = %worker_id,
                        error = %error,
                        "API Scheduler Worker heartbeat failed; Request claims are paused"
                    );
                }
            }
        }
    });
    state::Heartbeat {
        stop,
        stopped,
        task,
    }
}

struct HeartbeatEnd(tokio::sync::watch::Sender<bool>);

impl Drop for HeartbeatEnd {
    fn drop(&mut self) {
        let _ = self.0.send_replace(true);
    }
}

pub(super) async fn offline(
    client: &client::Client,
    worker_id: &str,
    token: String,
) -> Result<(), scheduler::Error> {
    let body = wire::Worker {
        worker_id: worker_id.to_string(),
        token,
    };
    let response = client
        .post_once::<_, wire::WorkerResponse>("v1/worker/offline", &body)
        .await?;
    success("offline", &response)
}

fn success(operation: &str, response: &wire::WorkerResponse) -> Result<(), scheduler::Error> {
    if response.code == SUCCESS {
        Ok(())
    } else {
        Err(scheduler::Error::Message(format!(
            "Worker {operation} failed with code {}: {}",
            response.code, response.message
        )))
    }
}

fn required<'a>(name: &str, value: Option<&'a str>) -> Result<&'a str, scheduler::Error> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            scheduler::Error::Message(format!(
                "API Scheduler requires {name}; configure it before open"
            ))
        })
}

fn canonical_modes(modes: impl IntoIterator<Item = net::Mode>) -> Vec<net::Mode> {
    let mut values = Vec::with_capacity(2);
    for mode in modes {
        if !values.contains(&mode) {
            values.push(mode);
        }
    }
    values.sort_by_key(|mode| match mode {
        net::Mode::Http => 0,
        net::Mode::Browser => 1,
    });
    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_are_canonical() {
        assert_eq!(
            canonical_modes([net::Mode::Browser, net::Mode::Http, net::Mode::Browser]),
            [net::Mode::Http, net::Mode::Browser]
        );
    }
}
