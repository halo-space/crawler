use std::sync::Arc;

use kameo::actor::Spawn;

use crate::spider::tx::{Event, Events};
use crate::{downloader, engine, middleware, scheduler};

pub const MAX_REQUEST_CONCURRENCY: usize = 16;
pub const MAX_EVENTS: usize = 32;
pub const DEFAULT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
pub const DEFAULT_WORKER_ID: &str = "worker-1";

pub struct Runtime<S, D, E, N = engine::NoInit> {
    scheduler: Arc<S>,
    downloader: Arc<D>,
    executor: Arc<E>,
    events: Option<Events>,
    registry: Arc<middleware::Registry>,
    middlewares: Arc<Vec<middleware::Spec>>,
    snapshots: Option<Arc<crate::item::snapshot::Store>>,
    concurrency: usize,
    claim_limit: Option<usize>,
    event_limit: usize,
    init: N,
}

impl<S: scheduler::Scheduler, D, E> Runtime<S, D, E, engine::NoInit> {
    pub(super) fn new(
        scheduler: S,
        downloader: D,
        executor: E,
        events: Events,
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
            claim_limit: None,
            event_limit: MAX_EVENTS,
            init: engine::NoInit,
        }
    }
}

impl<S, D, E, N> Runtime<S, D, E, N> {
    pub(super) fn with_init<T>(self, init: T) -> Runtime<S, D, E, T> {
        Runtime {
            scheduler: self.scheduler,
            downloader: self.downloader,
            executor: self.executor,
            events: self.events,
            registry: self.registry,
            middlewares: self.middlewares,
            snapshots: self.snapshots,
            concurrency: self.concurrency,
            claim_limit: self.claim_limit,
            event_limit: self.event_limit,
            init,
        }
    }
}

impl<S, D, E, N> Runtime<S, D, E, N>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
    N: engine::init::Init<S> + 'static,
{
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.claim_limit = Some(limit);
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
        let actor = engine::actor::Engine::new(
            self.scheduler.clone(),
            self.downloader.clone(),
            self.executor.clone(),
            self.registry.clone(),
            self.snapshots.clone(),
            events.clone(),
            engine::actor::Limits::new(
                self.concurrency,
                self.claim_limit.unwrap_or(self.concurrency),
            ),
        );
        let prepared =
            engine::actor::Engine::<S, D, E>::prepare_with_mailbox(kameo::mailbox::unbounded());
        events.bind(prepared.actor_ref().clone().reply_recipient::<Event>())?;
        let handle = prepared.spawn((actor, init));
        let (actor, reason) = handle
            .await
            .map_err(|error| crate::Error::message(error.to_string()))?
            .map_err(|error| crate::Error::message(error.to_string()))?;
        if !reason.is_normal() {
            return Err(crate::Error::message(reason.to_string()));
        }
        actor.into_result()
    }

    fn validate_limits(&self) -> Result<(), crate::Error> {
        if self.concurrency == 0 {
            return Err(crate::Error::message(
                "Request concurrency must be positive",
            ));
        }
        if self.claim_limit == Some(0) {
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
