use std::sync::Arc;

use crate::spider::tx::Receiver;
use crate::{downloader, engine, middleware, scheduler};

pub const MAX_REQUEST_CONCURRENCY: usize = 16;
pub const MAX_EVENTS: usize = 32;
pub const DEFAULT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
pub const DEFAULT_WORKER_ID: &str = "worker-1";

pub struct Runtime<S, D, R, N = engine::NoInit> {
    scheduler: Arc<S>,
    downloader: Arc<D>,
    executor: Arc<R>,
    events: Option<Receiver>,
    registry: Arc<middleware::Registry>,
    middlewares: Arc<Vec<middleware::Spec>>,
    snapshots: Option<Arc<crate::item::snapshot::Store>>,
    concurrency: usize,
    limit: Option<usize>,
    event_limit: usize,
    init: N,
}

impl<S: scheduler::Scheduler, D, R> Runtime<S, D, R, engine::NoInit> {
    pub(super) fn new(
        scheduler: S,
        downloader: D,
        executor: R,
        events: Receiver,
        registry: middleware::Registry,
        middlewares: Vec<middleware::Spec>,
    ) -> Self {
        let snapshots = scheduler
            .dir()
            .map(crate::item::snapshot::Store::new)
            .map(Arc::new);
        Self {
            scheduler: Arc::new(scheduler),
            downloader: Arc::new(downloader),
            executor: Arc::new(executor),
            events: Some(events),
            registry: Arc::new(registry),
            middlewares: Arc::new(middlewares),
            snapshots,
            concurrency: MAX_REQUEST_CONCURRENCY,
            limit: None,
            event_limit: MAX_EVENTS,
            init: engine::NoInit,
        }
    }
}

impl<S, D, R, N> Runtime<S, D, R, N> {
    pub(super) fn with_init<T>(self, init: T) -> Runtime<S, D, R, T> {
        Runtime {
            scheduler: self.scheduler,
            downloader: self.downloader,
            executor: self.executor,
            events: self.events,
            registry: self.registry,
            middlewares: self.middlewares,
            snapshots: self.snapshots,
            concurrency: self.concurrency,
            limit: self.limit,
            event_limit: self.event_limit,
            init,
        }
    }
}

impl<S, D, R, N> Runtime<S, D, R, N>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    R: engine::contract::Execute + 'static,
    N: engine::init::Init<S> + 'static,
{
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_event_limit(mut self, limit: usize) -> Self {
        self.event_limit = limit;
        self
    }

    pub fn scheduler(&self) -> &S {
        self.scheduler.as_ref()
    }

    pub async fn open(&self) -> Result<(), crate::Error> {
        if let Err(error) = self.scheduler.open().await {
            return Err(crate::Error::Scheduler(error));
        }

        if let Err(error) = self.downloader.open().await {
            let _ = self.scheduler.close().await;
            return Err(crate::Error::Download(error));
        }

        if let Some(snapshots) = &self.snapshots
            && let Err(error) = snapshots.open().await
        {
            let _ = self.downloader.close().await;
            let _ = self.scheduler.close().await;
            return Err(error);
        }

        Ok(())
    }

    pub async fn close(&self) -> Result<(), crate::Error> {
        let download_result = self
            .downloader
            .close()
            .await
            .map_err(crate::Error::Download);
        let scheduler_result = self
            .scheduler
            .close()
            .await
            .map_err(crate::Error::Scheduler);

        download_result.and(scheduler_result)
    }

    pub async fn start(&mut self) -> Result<(), crate::Error> {
        self.validate_limits()?;
        self.open().await?;

        let execution = self.run().await;
        let closing = self.close().await;

        match (execution, closing) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), _) => Err(error),
            (Ok(()), Err(error)) => Err(error),
        }
    }

    async fn run(&mut self) -> Result<(), crate::Error> {
        let execution = match self.registry.before_spider(&self.middlewares).await {
            Ok(()) => self.coordinate().await,
            Err(error) => Err(crate::Error::Middleware(error)),
        };
        let after_spider = self
            .registry
            .after_spider(&self.middlewares)
            .await
            .map_err(crate::Error::Middleware);
        execution.and(after_spider)
    }

    async fn coordinate(&mut self) -> Result<(), crate::Error> {
        let Some(events) = self.events.take() else {
            return Err(crate::Error::message("engine already started"));
        };
        events.set_limit(self.event_limit);
        let init = self.init.init(self.scheduler.clone()).await?;
        engine::actor::Coordinator::new(
            self.scheduler.clone(),
            self.downloader.clone(),
            self.executor.clone(),
            self.registry.clone(),
            self.snapshots.clone(),
            self.concurrency,
            self.limit.unwrap_or(self.concurrency),
        )
        .run(events, init)
        .await
    }

    fn validate_limits(&self) -> Result<(), crate::Error> {
        if self.concurrency == 0 {
            return Err(crate::Error::message(
                "Request concurrency must be positive",
            ));
        }
        if self.limit == Some(0) {
            return Err(crate::Error::message(
                "Request claim limit must be positive",
            ));
        }
        if self.event_limit == 0 {
            return Err(crate::Error::message("Event limit must be positive"));
        }
        Ok(())
    }
}
