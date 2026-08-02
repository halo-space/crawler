use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use kameo::actor::Spawn;

use crate::spider::tx::{Event, Events};
use crate::{downloader, engine, middleware, scheduler};

pub const MAX_REQUEST_CONCURRENCY: usize = 16;
pub const MAX_EVENTS: usize = 32;
pub const DEFAULT_IDLE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

type ShutdownSignal = Pin<Box<dyn Future<Output = Result<(), crate::Error>> + Send>>;

pub(super) struct Setup<S, D, E, O> {
    pub(super) scheduler: S,
    pub(super) downloader: D,
    pub(super) executor: E,
    pub(super) store: O,
    pub(super) events: Events,
    pub(super) registry: middleware::Registry,
    pub(super) middlewares: Vec<middleware::Spec>,
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
    idle_interval: std::time::Duration,
    tracing: crate::trace::Tracing,
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
            idle_interval: DEFAULT_IDLE_INTERVAL,
            tracing: crate::trace::Tracing::default(),
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
            idle_interval: self.idle_interval,
            tracing: self.tracing,
            init,
        }
    }

    /// Selects runtime tracing for subsequent Request execution.
    pub fn with_tracing(mut self, tracing: crate::trace::Tracing) -> Self {
        self.tracing = tracing;
        self
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

    pub fn with_idle_interval(mut self, interval: std::time::Duration) -> Self {
        self.idle_interval = interval;
        self
    }

    pub fn scheduler(&self) -> &S {
        self.scheduler.as_ref()
    }

    pub fn store(&self) -> &O {
        self.store.as_ref()
    }

    pub async fn open(&self) -> Result<(), crate::Error> {
        if let Err(error) = self.scheduler.open(self.concurrency).await {
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
        self.start_with(false).await
    }

    #[doc(hidden)]
    pub async fn start_until_idle(&mut self) -> Result<(), crate::Error> {
        self.start_with(true).await
    }

    async fn start_with(&mut self, exit_when_idle: bool) -> Result<(), crate::Error> {
        let mut shutdown: ShutdownSignal = Box::pin(shutdown_signal());
        self.start_with_shutdown(exit_when_idle, &mut shutdown)
            .await
    }

    async fn start_with_shutdown(
        &mut self,
        exit_when_idle: bool,
        shutdown: &mut ShutdownSignal,
    ) -> Result<(), crate::Error> {
        self.validate()?;
        let Some(shutdown_requested) = open_while_listening(self.open(), shutdown).await? else {
            return Ok(());
        };

        let execution = if shutdown_requested {
            Ok(())
        } else {
            self.execute_lifecycle(exit_when_idle, shutdown).await
        };
        let closing = self.close().await;

        match (execution, closing) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), _) => Err(error),
            (Ok(()), Err(error)) => Err(error),
        }
    }

    async fn execute_lifecycle(
        &mut self,
        exit_when_idle: bool,
        shutdown: &mut ShutdownSignal,
    ) -> Result<(), crate::Error> {
        let before_spider = complete_while_listening(
            async {
                self.registry
                    .before_spider(&self.middlewares)
                    .await
                    .map_err(crate::Error::Middleware)
            },
            shutdown,
        )
        .await;
        let execution = match before_spider {
            Ok((Some(()), false)) => self.coordinate(exit_when_idle, shutdown).await,
            Ok((_, true)) => Ok(()),
            Ok((None, false)) => unreachable!(),
            Err(error) => Err(error),
        };
        let after_spider = self
            .registry
            .after_spider(&self.middlewares)
            .await
            .map_err(crate::Error::Middleware);
        execution.and(after_spider)
    }

    async fn coordinate(
        &mut self,
        exit_when_idle: bool,
        shutdown: &mut ShutdownSignal,
    ) -> Result<(), crate::Error> {
        let Some(events) = self.events.take() else {
            return Err(crate::Error::message("engine already started"));
        };
        events.set_limit(self.event_limit);
        let (init, shutdown_requested) =
            complete_while_listening(self.init.init(self.scheduler.clone()), shutdown).await?;
        if shutdown_requested {
            return Ok(());
        }
        let Some(init) = init else {
            return Ok(());
        };
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
                self.idle_interval,
                self.tracing,
                exit_when_idle,
            ),
        );
        let prepared =
            engine::actor::Engine::<S, D, E, O>::prepare_with_mailbox(kameo::mailbox::unbounded());
        let actor_ref = prepared.actor_ref().clone();
        events.bind(actor_ref.clone().reply_recipient::<Event>())?;
        let mut handle = prepared.spawn((actor, init));
        let stopped = tokio::select! {
            stopped = &mut handle => stopped,
            signal = shutdown.as_mut() => {
                if let Err(error) = actor_ref.tell(engine::actor::Shutdown).await
                    && !actor_ref.is_alive()
                {
                    tracing::debug!(error = %error, "Engine Actor stopped before shutdown signal was delivered");
                }
                let stopped = handle.await;
                signal?;
                stopped
            }
        };
        let (actor, reason) = stopped
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
        if self.idle_interval.is_zero() {
            return Err(crate::Error::message("Idle interval must be positive"));
        }
        Ok(())
    }
}

async fn open_while_listening(
    opening: impl Future<Output = Result<(), crate::Error>>,
    shutdown: &mut ShutdownSignal,
) -> Result<Option<bool>, crate::Error> {
    let (opened, shutdown_requested) = complete_while_listening(opening, shutdown).await?;
    Ok(opened.map(|()| shutdown_requested))
}

async fn complete_while_listening<T>(
    stage: impl Future<Output = Result<T, crate::Error>>,
    shutdown: &mut ShutdownSignal,
) -> Result<(Option<T>, bool), crate::Error> {
    tokio::pin!(stage);
    let started = std::sync::atomic::AtomicBool::new(false);
    let tracked = std::future::poll_fn(|context| {
        started.store(true, std::sync::atomic::Ordering::Relaxed);
        stage.as_mut().poll(context)
    });
    tokio::pin!(tracked);

    tokio::select! {
        biased;
        signal = shutdown.as_mut() => {
            signal?;
            if started.load(std::sync::atomic::Ordering::Relaxed) {
                Ok((Some(tracked.await?), true))
            } else {
                Ok((None, true))
            }
        }
        result = &mut tracked => Ok((Some(result?), false)),
    }
}

async fn shutdown_signal() -> Result<(), crate::Error> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|error| crate::Error::message(error.to_string()))?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.map_err(|error| crate::Error::message(error.to_string()))
            }
            _ = terminate.recv() => Ok(()),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .map_err(|error| crate::Error::message(error.to_string()))
    }
}

#[cfg(test)]
mod tests;
