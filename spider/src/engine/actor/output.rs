use kameo::message::{Context, Message};
use kameo::reply::DelegatedReply;

use super::{Engine, task};
use crate::spider::tx::Event;
use crate::{downloader, engine, scheduler};

pub(super) struct Done {
    id: task::Id,
}

impl<S, D, E> Message<Event> for Engine<S, D, E>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
{
    type Reply = DelegatedReply<Result<(), String>>;

    async fn handle(&mut self, event: Event, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.invalidate_claim();
        let event = event.accept();
        let scheduler = self.scheduler.clone();
        let registry = self.registry.clone();
        let snapshots = self.snapshots.clone();
        let actor_ref = ctx.actor_ref().clone();
        let (delegated, reply) = ctx.reply_sender();

        let handle = tokio::spawn(async move {
            let result =
                task::protect(engine::event::handle(event, scheduler, registry, snapshots)).await;
            if let Some(reply) = reply {
                let response = match &result {
                    Ok(()) => Ok(()),
                    Err(error) => Err(error.to_string()),
                };
                reply.send(response);
            }
            let id = tokio::task::id();
            let _ = actor_ref.tell(Done { id }).await;
        });
        self.outputs.insert(handle);
        self.exhausted = false;
        delegated
    }
}

impl<S, D, E> Message<Done> for Engine<S, D, E>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
{
    type Reply = ();

    async fn handle(&mut self, done: Done, ctx: &mut Context<Self, Self::Reply>) {
        if !self.outputs.remove(done.id) {
            return;
        }
        self.invalidate_claim();
        self.exhausted = false;
        if self.advance(ctx.actor_ref()) {
            ctx.stop();
        }
    }
}
