use kameo::actor::ActorRef;
use kameo::message::{Context, Message};

use super::{Engine, task};
use crate::{downloader, engine, scheduler};

pub(super) struct Done {
    id: task::Id,
    result: Result<(), crate::Error>,
}

pub(super) fn spawn<S, D, E>(
    engine: &mut Engine<S, D, E>,
    actor_ref: ActorRef<Engine<S, D, E>>,
    request: crate::net::Request,
) where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
{
    let scheduler = engine.scheduler.clone();
    let downloader = engine.downloader.clone();
    let executor = engine.executor.clone();
    let registry = engine.registry.clone();
    let handle = tokio::spawn(async move {
        let result = task::protect(engine::request::task::execute(
            request, scheduler, downloader, executor, registry,
        ))
        .await;
        let id = tokio::task::id();
        let _ = actor_ref.tell(Done { id, result }).await;
    });
    engine.requests.insert(handle);
}

impl<S, D, E> Message<Done> for Engine<S, D, E>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
{
    type Reply = ();

    async fn handle(&mut self, done: Done, ctx: &mut Context<Self, Self::Reply>) {
        if !self.requests.remove(done.id) {
            return;
        }
        self.invalidate_claim();
        self.exhausted = false;
        if let Err(error) = done.result {
            self.record_error(error);
        }
        if self.advance(ctx.actor_ref()) {
            ctx.stop();
        }
    }
}
