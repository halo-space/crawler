use std::sync::Arc;

use kameo::actor::{Actor, ActorRef};

use crate::spider::tx::Events;
use crate::{downloader, engine, middleware, scheduler};

mod claim;
mod output;
mod request;
mod shutdown;
mod start;
mod task;
mod wait;

pub(super) use shutdown::Shutdown;

pub(super) struct Config {
    concurrency: usize,
    claim_limit: usize,
    idle_interval: std::time::Duration,
    tracing: crate::trace::Tracing,
    exit_when_idle: bool,
}

impl Config {
    pub(super) fn new(
        concurrency: usize,
        claim_limit: usize,
        idle_interval: std::time::Duration,
        tracing: crate::trace::Tracing,
        exit_when_idle: bool,
    ) -> Self {
        Self {
            concurrency,
            claim_limit,
            idle_interval,
            tracing,
            exit_when_idle,
        }
    }
}

pub(super) struct Engine<S, D, E, O> {
    scheduler: Arc<S>,
    downloader: Arc<D>,
    executor: Arc<E>,
    store: Arc<O>,
    registry: Arc<middleware::Registry>,
    events: Events,
    config: Config,

    // Concurrent Request work. Its length is the source of truth for free execution slots.
    requests: task::Tasks,
    // Tx output remains tracked after the Event permit is released by its Actor handler.
    outputs: task::Tasks,

    // Executor startup gates Scheduler claims.
    startup: Option<task::Task>,
    // Only one Scheduler claim may run at a time.
    claim: Option<task::Task>,
    // Local work changed while the current claim was running.
    claim_stale: bool,
    // An empty claim schedules one delayed poll.
    poll: Option<task::Task>,
    // Final shutdown waits for accepted Tx Events to become idle.
    idle: Option<task::Task>,

    // SIGINT/SIGTERM stops new claims while accepted work drains.
    stopping: bool,
    // Runtime returns the first error after all accepted work has drained.
    error: Option<crate::Error>,
}

impl<S, D, E, O> Engine<S, D, E, O>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
    O: crate::item::Store + 'static,
{
    pub(super) fn new(
        scheduler: Arc<S>,
        downloader: Arc<D>,
        executor: Arc<E>,
        store: Arc<O>,
        registry: Arc<middleware::Registry>,
        events: Events,
        config: Config,
    ) -> Self {
        Self {
            scheduler,
            downloader,
            executor,
            store,
            registry,
            events,
            config,
            requests: task::Tasks::default(),
            outputs: task::Tasks::default(),
            startup: None,
            claim: None,
            claim_stale: false,
            poll: None,
            idle: None,
            stopping: false,
            error: None,
        }
    }

    pub(super) fn into_result(self) -> Result<(), crate::Error> {
        self.error.map_or(Ok(()), Err)
    }

    fn record_error(&mut self, error: crate::Error) {
        if self.error.is_none() {
            self.error = Some(error);
        }
        if let Some(poll) = self.poll.take() {
            poll.abort();
        }
    }

    fn finishes_when_idle(&self) -> bool {
        self.config.exit_when_idle || self.error.is_some()
    }

    fn invalidate_claim(&mut self) {
        if self.claim.is_some() {
            self.claim_stale = true;
        }
    }

    /// Starts eligible work and reports when a requested or error-driven shutdown has drained.
    fn advance(&mut self, actor_ref: &ActorRef<Self>) -> bool {
        if self.startup.is_none()
            && self.claim.is_none()
            && self.poll.is_none()
            && !self.stopping
            && self.error.is_none()
            && self.requests.len() < self.config.concurrency
        {
            let available = self.config.concurrency - self.requests.len();
            claim::spawn(
                self,
                actor_ref.clone(),
                self.config.claim_limit.min(available),
            );
        }

        if !self.stopping && self.error.is_none() {
            return false;
        }

        if self.startup.is_some()
            || self.claim.is_some()
            || self.poll.is_some()
            || !self.requests.is_empty()
            || !self.outputs.is_empty()
        {
            return false;
        }

        if !self.events.is_idle() {
            if self.idle.is_none() {
                wait::idle(self, actor_ref.clone());
            }
            return false;
        }

        self.idle.is_none()
    }
}

impl<S, D, E, O> Actor for Engine<S, D, E, O>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
    O: crate::item::Store + 'static,
{
    type Args = (Self, engine::init::Output);
    type Error = kameo::error::Infallible;

    async fn on_start(
        (mut state, init): Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        if init == engine::init::Output::Start {
            start::spawn(&mut state, actor_ref);
        } else {
            state.advance(&actor_ref);
        }
        Ok(state)
    }
}
