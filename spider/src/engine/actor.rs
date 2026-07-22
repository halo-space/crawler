use std::sync::Arc;

use kameo::actor::{Actor, ActorRef};

use crate::spider::tx::Events;
use crate::{downloader, engine, middleware, scheduler};

mod claim;
mod output;
mod request;
mod start;
mod task;
mod wait;

pub(super) struct Config {
    concurrency: usize,
    claim_limit: usize,
    worker: super::worker::Worker,
}

impl Config {
    pub(super) fn new(
        concurrency: usize,
        claim_limit: usize,
        worker: super::worker::Worker,
    ) -> Self {
        Self {
            concurrency,
            claim_limit,
            worker,
        }
    }
}

pub(super) struct Engine<S, D, E> {
    scheduler: Arc<S>,
    downloader: Arc<D>,
    executor: Arc<E>,
    registry: Arc<middleware::Registry>,
    snapshots: Option<Arc<crate::item::snapshot::Store>>,
    events: Events,
    config: Config,

    // Concurrent Request work. Its length is the source of truth for free execution slots.
    requests: task::Tasks,
    // Tx output remains tracked after the Event permit is released by its Actor handler.
    outputs: task::Tasks,

    // Executor startup gates Scheduler claims.
    startup: Option<task::Task>,
    // Only one Scheduler claim and pending-state check may run at a time.
    claim: Option<task::Task>,
    // Work changed during the current claim, so an empty result needs rechecking.
    claim_stale: bool,
    // An empty claim with pending work schedules one delayed poll.
    poll: Option<task::Task>,
    // Final shutdown waits for cloned Tx producers and accepted Events to become idle.
    idle: Option<task::Task>,

    // The last empty claim confirmed that the Scheduler has no pending Request.
    exhausted: bool,
    // A terminal claim error blocks new claims while already accepted work drains.
    claims_blocked: bool,
    // Runtime returns the first error after all accepted work has drained.
    error: Option<crate::Error>,
}

impl<S, D, E> Engine<S, D, E>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
{
    pub(super) fn new(
        scheduler: Arc<S>,
        downloader: Arc<D>,
        executor: Arc<E>,
        registry: Arc<middleware::Registry>,
        snapshots: Option<Arc<crate::item::snapshot::Store>>,
        events: Events,
        config: Config,
    ) -> Self {
        Self {
            scheduler,
            downloader,
            executor,
            registry,
            snapshots,
            events,
            config,
            requests: task::Tasks::default(),
            outputs: task::Tasks::default(),
            startup: None,
            claim: None,
            claim_stale: false,
            poll: None,
            idle: None,
            exhausted: false,
            claims_blocked: false,
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
    }

    fn invalidate_claim(&mut self) {
        if self.claim.is_some() {
            self.claim_stale = true;
        }
    }

    /// Starts eligible work and returns whether every work source has drained.
    fn advance(&mut self, actor_ref: &ActorRef<Self>) -> bool {
        if self.startup.is_none()
            && self.claim.is_none()
            && self.poll.is_none()
            && !self.claims_blocked
            && !self.exhausted
            && self.requests.len() < self.config.concurrency
        {
            let available = self.config.concurrency - self.requests.len();
            claim::spawn(
                self,
                actor_ref.clone(),
                self.config.claim_limit.min(available),
            );
        }

        if self.startup.is_some()
            || self.claim.is_some()
            || self.poll.is_some()
            || !self.requests.is_empty()
            || !self.outputs.is_empty()
            || (!self.exhausted && !self.claims_blocked)
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

impl<S, D, E> Actor for Engine<S, D, E>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
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
