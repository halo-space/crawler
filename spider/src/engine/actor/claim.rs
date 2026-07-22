use std::sync::Arc;

use kameo::actor::ActorRef;
use kameo::message::{Context, Message};

use super::{Engine, request, task, wait};
use crate::{downloader, engine, scheduler};

const MAX_ATTEMPTS: usize = 3;
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(25);

enum Output {
    Requests(Vec<crate::net::Request>),
    Pending,
    Exhausted,
}

pub(super) struct Done {
    id: task::Id,
    result: Result<Output, crate::Error>,
}

pub(super) fn spawn<S, D, E>(
    engine: &mut Engine<S, D, E>,
    actor_ref: ActorRef<Engine<S, D, E>>,
    limit: usize,
) where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
{
    engine.claim_stale = false;
    let scheduler = engine.scheduler.clone();
    let worker_id = engine.config.worker.id.clone();
    let modes = engine.config.worker.modes.clone();
    let handle = tokio::spawn(async move {
        let result = task::protect(next(scheduler, limit, worker_id, modes)).await;
        let id = tokio::task::id();
        let _ = actor_ref.tell(Done { id, result }).await;
    });
    engine.claim = Some(task::Task::new(handle));
}

async fn next<S>(
    scheduler: Arc<S>,
    limit: usize,
    worker_id: String,
    modes: Vec<crate::net::Mode>,
) -> Result<Output, crate::Error>
where
    S: scheduler::Scheduler,
{
    let requests = retry(|| scheduler.next_requests(limit, &worker_id, &modes)).await?;
    if requests.is_empty() {
        if retry(|| scheduler.has_pending_requests(&worker_id, &modes)).await? {
            Ok(Output::Pending)
        } else {
            Ok(Output::Exhausted)
        }
    } else {
        Ok(Output::Requests(requests))
    }
}

async fn retry<Fut, T>(mut operation: impl FnMut() -> Fut) -> Result<T, crate::Error>
where
    Fut: std::future::Future<Output = Result<T, scheduler::Error>>,
{
    for attempt in 0..MAX_ATTEMPTS {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if error.is_transient() && attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(error) => return Err(crate::Error::Scheduler(error)),
        }
    }
    unreachable!()
}

impl<S, D, E> Message<Done> for Engine<S, D, E>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
{
    type Reply = ();

    async fn handle(&mut self, done: Done, ctx: &mut Context<Self, Self::Reply>) {
        if !self
            .claim
            .as_ref()
            .is_some_and(|task| task.matches(done.id))
        {
            return;
        }
        self.claim = None;
        let stale = std::mem::take(&mut self.claim_stale);
        match done.result {
            Ok(Output::Requests(requests)) => {
                self.exhausted = false;
                for next in requests {
                    request::spawn(self, ctx.actor_ref().clone(), next);
                }
            }
            Ok(Output::Pending) => {
                self.exhausted = false;
                wait::poll(self, ctx.actor_ref().clone());
            }
            Ok(Output::Exhausted) => {
                self.exhausted = !stale;
            }
            Err(error) => {
                self.claims_blocked = true;
                self.record_error(error);
            }
        }
        if self.advance(ctx.actor_ref()) {
            ctx.stop();
        }
    }
}
