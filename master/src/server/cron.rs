use std::future::Future;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use crate::Error;
use crate::store::MySql;

pub(super) trait Store: Clone + Send + Sync + 'static {
    fn recover(&self, namespace: &str, now: i64) -> impl Future<Output = Result<(), Error>> + Send;

    fn dispatch(
        &self,
        namespace: &str,
        now: i64,
        limit: usize,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    fn cleanup(
        &self,
        namespace: &str,
        now: i64,
        retention: Duration,
        limit: usize,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

impl Store for MySql {
    async fn recover(&self, namespace: &str, now: i64) -> Result<(), Error> {
        MySql::recover(self, namespace, now).await.map(|_| ())
    }

    async fn dispatch(&self, namespace: &str, now: i64, limit: usize) -> Result<(), Error> {
        MySql::dispatch_due(self, namespace, now, limit)
            .await
            .map(|_| ())
    }

    async fn cleanup(
        &self,
        namespace: &str,
        now: i64,
        retention: Duration,
        limit: usize,
    ) -> Result<(), Error> {
        MySql::cleanup(self, namespace, now, retention, limit)
            .await
            .map(|_| ())
    }
}

pub(super) struct Cron<S> {
    store: S,
    namespace: String,
    interval: Duration,
    dispatch_limit: usize,
    history_retention: Duration,
    cleanup_limit: usize,
}

impl<S: Store> Cron<S> {
    pub(super) fn new(
        store: S,
        namespace: String,
        interval: Duration,
        dispatch_limit: usize,
        history_retention: Duration,
        cleanup_limit: usize,
    ) -> Self {
        Self {
            store,
            namespace,
            interval,
            dispatch_limit,
            history_retention,
            cleanup_limit,
        }
    }

    pub(super) async fn run(self, mut stopped: watch::Receiver<bool>) {
        let mut ticks = tokio::time::interval(self.interval);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticks.tick() => self.tick().await,
                changed = stopped.changed() => {
                    if changed.is_err() || *stopped.borrow() {
                        break;
                    }
                }
            }
        }
    }

    async fn tick(&self) {
        let now = now_millis();
        if let Err(error) = self.store.recover(&self.namespace, now).await {
            tracing::warn!(%error, "lease recovery failed");
        }
        if let Err(error) = self
            .store
            .dispatch(&self.namespace, now, self.dispatch_limit)
            .await
        {
            tracing::warn!(%error, "due Task dispatch failed");
        }
        if let Err(error) = self
            .store
            .cleanup(
                &self.namespace,
                now,
                self.history_retention,
                self.cleanup_limit,
            )
            .await
        {
            tracing::warn!(%error, "history cleanup failed");
        }
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}
