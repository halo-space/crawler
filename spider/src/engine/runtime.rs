use std::sync::Arc;

use kameo::actor::Spawn;

use super::worker::Worker;
use crate::spider::tx::{Event, Events};
use crate::{downloader, engine, middleware, scheduler};

pub const MAX_REQUEST_CONCURRENCY: usize = 16;
pub const MAX_EVENTS: usize = 32;
pub const DEFAULT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

pub(super) struct Setup<S, D, E, O> {
    pub(super) scheduler: S,
    pub(super) downloader: D,
    pub(super) executor: E,
    pub(super) store: O,
    pub(super) events: Events,
    pub(super) registry: middleware::Registry,
    pub(super) middlewares: Vec<middleware::Spec>,
    pub(super) worker: Worker,
}

pub struct Runtime<S, D, E, N = engine::NoInit, O = crate::item::Jsonl> {
    scheduler: Arc<S>,
    downloader: Arc<D>,
    executor: Arc<E>,
    store: Arc<O>,
    events: Option<Events>,
    registry: Arc<middleware::Registry>,
    middlewares: Arc<Vec<middleware::Spec>>,
    concurrency: usize,
    claim_limit: Option<usize>,
    event_limit: usize,
    worker: Worker,
    init: N,
}

impl<S: scheduler::Scheduler, D, E, O> Runtime<S, D, E, engine::NoInit, O>
where
    O: crate::item::Store + 'static,
{
    pub(super) fn new(setup: Setup<S, D, E, O>) -> Self {
        Self {
            scheduler: Arc::new(setup.scheduler),
            downloader: Arc::new(setup.downloader),
            executor: Arc::new(setup.executor),
            store: Arc::new(setup.store),
            events: Some(setup.events),
            registry: Arc::new(setup.registry),
            middlewares: Arc::new(setup.middlewares),
            concurrency: MAX_REQUEST_CONCURRENCY,
            claim_limit: None,
            event_limit: MAX_EVENTS,
            worker: setup.worker,
            init: engine::NoInit,
        }
    }
}

impl<S, D, E, N, O> Runtime<S, D, E, N, O> {
    pub(super) fn with_init<T>(self, init: T) -> Runtime<S, D, E, T, O> {
        Runtime {
            scheduler: self.scheduler,
            downloader: self.downloader,
            executor: self.executor,
            store: self.store,
            events: self.events,
            registry: self.registry,
            middlewares: self.middlewares,
            concurrency: self.concurrency,
            claim_limit: self.claim_limit,
            event_limit: self.event_limit,
            worker: self.worker,
            init,
        }
    }
}

impl<S, D, E, N, O> Runtime<S, D, E, N, O>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
    N: engine::init::Init<S> + 'static,
    O: crate::item::Store + 'static,
{
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn with_claim_limit(mut self, limit: usize) -> Self {
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

    pub fn store(&self) -> &O {
        self.store.as_ref()
    }

    pub async fn open(&self) -> Result<(), crate::Error> {
        if let Err(error) = self.scheduler.open().await {
            return Err(crate::Error::Scheduler(error));
        }

        if let Err(error) = self.downloader.open().await {
            if let Err(close_error) = self.scheduler.close().await {
                tracing::warn!(
                    error = %close_error,
                    "failed to close Scheduler after Downloader open failed"
                );
            }
            return Err(crate::Error::Download(error));
        }

        if let Err(error) = self.store.open().await {
            if let Err(close_error) = self.downloader.close().await {
                tracing::warn!(
                    error = %close_error,
                    "failed to close Downloader after Item Store open failed"
                );
            }
            if let Err(close_error) = self.scheduler.close().await {
                tracing::warn!(
                    error = %close_error,
                    "failed to close Scheduler after Item Store open failed"
                );
            }
            return Err(crate::Error::Item(error));
        }

        Ok(())
    }

    pub async fn close(&self) -> Result<(), crate::Error> {
        let mut first_error = None;
        if let Err(error) = self.downloader.close().await {
            first_error.get_or_insert(crate::Error::Download(error));
        }
        if let Err(error) = self.store.close().await {
            first_error.get_or_insert(crate::Error::Item(error));
        }
        if let Err(error) = self.scheduler.close().await {
            first_error.get_or_insert(crate::Error::Scheduler(error));
        }
        first_error.map_or(Ok(()), Err)
    }

    pub async fn start(&mut self) -> Result<(), crate::Error> {
        self.validate()?;
        self.open().await?;

        let execution = self.execute_lifecycle().await;
        let closing = self.close().await;

        match (execution, closing) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), _) => Err(error),
            (Ok(()), Err(error)) => Err(error),
        }
    }

    async fn execute_lifecycle(&mut self) -> Result<(), crate::Error> {
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
            self.store.clone(),
            self.registry.clone(),
            events.clone(),
            engine::actor::Config::new(
                self.concurrency,
                self.claim_limit.unwrap_or(self.concurrency),
                self.worker.clone(),
            ),
        );
        let prepared =
            engine::actor::Engine::<S, D, E, O>::prepare_with_mailbox(kameo::mailbox::unbounded());
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

    fn validate(&self) -> Result<(), crate::Error> {
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
        self.worker.validate()
    }
}
