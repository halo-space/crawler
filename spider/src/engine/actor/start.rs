use kameo::actor::ActorRef;
use kameo::message::{Context, Message};

use super::{Engine, task};
use crate::{downloader, engine, scheduler};

pub(super) struct Done {
    id: task::Id,
    result: Result<(), crate::Error>,
}

pub(super) fn spawn<S, D, E, O>(
    engine: &mut Engine<S, D, E, O>,
    actor_ref: ActorRef<Engine<S, D, E, O>>,
) where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
    O: crate::item::Store + 'static,
{
    let executor = engine.executor.clone();
    let handle = tokio::spawn(async move {
        let result = task::protect(async move { executor.start().await }).await;
        let id = tokio::task::id();
        let _ = actor_ref.tell(Done { id, result }).await;
    });
    engine.startup = Some(task::Task::new(handle));
}

impl<S, D, E, O> Message<Done> for Engine<S, D, E, O>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
    O: crate::item::Store + 'static,
{
    type Reply = ();

    async fn handle(&mut self, done: Done, ctx: &mut Context<Self, Self::Reply>) {
        if !self
            .startup
            .as_ref()
            .is_some_and(|task| task.matches(done.id))
        {
            return;
        }
        self.startup = None;
        self.exhausted = false;
        if let Err(error) = done.result {
            self.record_error(error);
        }
        if self.advance(ctx.actor_ref()) {
            ctx.stop();
        }
    }
}
