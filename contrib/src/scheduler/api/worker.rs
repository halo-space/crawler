use std::sync::Arc;

use spider::{net, scheduler};
use tokio::sync::RwLock;

use super::{Api, client, state, wire};

impl Api {
    pub(super) async fn register(
        &self,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<Vec<net::Mode>, scheduler::Error> {
        let epoch = self.require_open()?;
        let modes = canonical_modes(worker_id, modes)?;
        let mut workers = self.runtime.workers.lock().await;
        if let Some(worker) = workers.get(worker_id)
            && *worker.modes.read().await == modes
        {
            return Ok(modes);
        }

        let body = wire::Worker {
            worker_id: worker_id.to_string(),
            modes: modes.clone(),
        };
        self.client
            .post_empty("v1/worker/heartbeat", &body, None)
            .await?;

        self.require_epoch(epoch)?;
        if let Some(worker) = workers.get(worker_id) {
            *worker.modes.write().await = modes.clone();
            return Ok(modes);
        }

        let advertised = Arc::new(RwLock::new(modes.clone()));
        let task = tokio::spawn(heartbeat(
            self.client.clone(),
            self.runtime.clone(),
            epoch,
            worker_id.to_string(),
            advertised.clone(),
        ));
        workers.insert(
            worker_id.to_string(),
            state::Worker {
                modes: advertised,
                task,
            },
        );
        Ok(modes)
    }
}

async fn heartbeat(
    client: client::Client,
    runtime: Arc<state::Runtime>,
    epoch: u64,
    worker_id: String,
    modes: Arc<RwLock<Vec<net::Mode>>>,
) {
    loop {
        let interval = *runtime.heartbeat_interval.read().await;
        tokio::time::sleep(interval).await;
        if !runtime.is_open(epoch) {
            return;
        }
        let body = wire::Worker {
            worker_id: worker_id.clone(),
            modes: modes.read().await.clone(),
        };
        let _ = client.post_empty("v1/worker/heartbeat", &body, None).await;
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
