use std::future::Future;
use std::time::Duration;

use axum::Router;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::store::MySql;
use crate::{Config, Error, handler};

mod cron;

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "server/http_test.rs"]
mod http_tests;

use cron::Cron;

const CRON_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

pub struct Server {
    config: Config,
    store: MySql,
}

impl Server {
    pub async fn from_config(config: Config) -> Result<Self, Error> {
        config.validate()?;
        let store = MySql::connect(&config).await?;
        Ok(Self { config, store })
    }

    pub(crate) fn router(&self) -> Router {
        handler::build(self.config.clone(), self.store.clone())
    }

    pub async fn serve(self) -> Result<(), Error> {
        let listener = tokio::net::TcpListener::bind(self.config.bind())
            .await
            .map_err(|error| Error::Unavailable(error.to_string()))?;
        self.serve_listener(listener, async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::error!(%error, "failed to listen for shutdown signal");
            }
        })
        .await
    }

    pub async fn serve_until<F>(self, shutdown: F) -> Result<(), Error>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let listener = tokio::net::TcpListener::bind(self.config.bind())
            .await
            .map_err(|error| Error::Unavailable(error.to_string()))?;
        self.serve_listener(listener, shutdown).await
    }

    pub async fn serve_listener<F>(
        self,
        listener: tokio::net::TcpListener,
        shutdown: F,
    ) -> Result<(), Error>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (stop, stopped) = watch::channel(false);
        let cron = Cron::new(
            self.store.clone(),
            self.config.namespace().to_string(),
            self.config.cron_interval(),
            self.config.dispatch_limit(),
            self.config.history().ttl,
            self.config.history().cleanup_limit,
        );
        let cron_task = tokio::spawn(cron.run(stopped));
        let stop_after_shutdown = stop.clone();
        let result = axum::serve(listener, self.router())
            .with_graceful_shutdown(async move {
                shutdown.await;
                let _ = stop_after_shutdown.send(true);
            })
            .await
            .map_err(|error| Error::Unavailable(error.to_string()));
        let _ = stop.send(true);
        drain_cron(cron_task, CRON_DRAIN_TIMEOUT).await?;
        result
    }
}

async fn drain_cron(mut task: JoinHandle<()>, timeout: Duration) -> Result<(), Error> {
    match tokio::time::timeout(timeout, &mut task).await {
        Ok(result) => {
            result.map_err(|error| Error::Unavailable(format!("Cron task failed: {error}")))
        }
        Err(_) => {
            tracing::warn!(
                timeout_ms = timeout.as_millis(),
                "Cron task did not stop before the shutdown deadline"
            );
            task.abort();
            match task.await {
                Ok(()) => Ok(()),
                Err(error) if error.is_cancelled() => Ok(()),
                Err(error) => Err(Error::Unavailable(format!("Cron task failed: {error}"))),
            }
        }
    }
}
