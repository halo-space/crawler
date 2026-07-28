use std::sync::Arc;

use spider::{net, scheduler};
use tokio::sync::Mutex;

use super::{Api, client, state, wire};

impl Api {
    pub(super) async fn register(
        &self,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<Vec<net::Mode>, scheduler::Error> {
        let epoch = self.require_open()?;
        let modes = canonical_modes(worker_id, modes)?;
        let registration = {
            let mut workers = self.runtime.workers.lock().await;
            if let Some(worker) = workers.get(worker_id) {
                worker.registration.clone()
            } else {
                let registration = Arc::new(Mutex::new(state::Registration::new(modes.clone())));
                let task = tokio::spawn(heartbeat(
                    self.client.clone(),
                    self.runtime.clone(),
                    epoch,
                    worker_id.to_string(),
                    registration.clone(),
                ));
                workers.insert(
                    worker_id.to_string(),
                    state::Worker {
                        registration: registration.clone(),
                        task,
                    },
                );
                registration
            }
        };

        synchronize(&self.client, worker_id, &registration, &modes).await?;
        self.require_epoch(epoch)?;
        Ok(modes)
    }
}

async fn synchronize(
    client: &client::Client,
    worker_id: &str,
    registration: &Mutex<state::Registration>,
    modes: &[net::Mode],
) -> Result<(), scheduler::Error> {
    let mut registration = registration.lock().await;
    if registration.confirmed && registration.modes == modes {
        return Ok(());
    }

    registration.modes = modes.to_vec();
    let body = wire::Worker {
        worker_id: worker_id.to_string(),
        modes: registration.modes.clone(),
    };
    match client.post_empty("v1/worker/heartbeat", &body, None).await {
        Ok(()) => {
            registration.confirmed = true;
            Ok(())
        }
        Err(error) => {
            registration.confirmed = false;
            Err(error)
        }
    }
}

async fn heartbeat(
    client: client::Client,
    runtime: Arc<state::Runtime>,
    epoch: u64,
    worker_id: String,
    registration: Arc<Mutex<state::Registration>>,
) {
    loop {
        let interval = *runtime.heartbeat_interval.read().await;
        tokio::time::sleep(interval).await;
        if !runtime.is_open(epoch) {
            return;
        }
        let mut registration = registration.lock().await;
        let body = wire::Worker {
            worker_id: worker_id.clone(),
            modes: registration.modes.clone(),
        };
        let was_confirmed = registration.confirmed;
        match client.post_empty("v1/worker/heartbeat", &body, None).await {
            Ok(()) => registration.confirmed = true,
            Err(error) => {
                registration.confirmed = false;
                if was_confirmed {
                    tracing::warn!(
                        worker_id = %worker_id,
                        error = %error,
                        "API Scheduler Worker heartbeat failed"
                    );
                }
            }
        }
    }
}

pub(super) fn canonical_modes(
    worker_id: &str,
    modes: &[net::Mode],
) -> Result<Vec<net::Mode>, scheduler::Error> {
    if worker_id.trim().is_empty() {
        return Err(scheduler::Error::Message(
            "worker_id must not be empty".to_string(),
        ));
    }
    if modes.is_empty() {
        return Err(scheduler::Error::Message(
            "worker modes must not be empty".to_string(),
        ));
    }

    let mut http = false;
    let mut browser = false;
    for mode in modes {
        match mode {
            net::Mode::Http => http = true,
            net::Mode::Browser => browser = true,
        }
    }

    let mut values = Vec::with_capacity(2);
    if http {
        values.push(net::Mode::Http);
    }
    if browser {
        values.push(net::Mode::Browser);
    }
    Ok(values)
}
