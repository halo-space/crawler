use kameo::message::{Context, Message};
use kameo::reply::DelegatedReply;

use super::{Engine, task};
use crate::spider::tx::Event;
use crate::{downloader, engine, item, scheduler};

pub(super) struct Done {
    id: task::Id,
    error: Option<crate::Error>,
}

impl<S, D, E, O> Message<Event> for Engine<S, D, E, O>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
    O: item::Store + 'static,
{
    type Reply = DelegatedReply<Result<(), String>>;

    async fn handle(&mut self, event: Event, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.invalidate_claim();
        let event = event.accept();
        let scheduler = self.scheduler.clone();
        let store = self.store.clone();
        let registry = self.registry.clone();
        let actor_ref = ctx.actor_ref().clone();
        let (delegated, reply) = ctx.reply_sender();

        let handle = tokio::spawn(async move {
            let result =
                task::protect(engine::event::handle(event, scheduler, store, registry)).await;
            let error = match (result, reply) {
                (Ok(()), Some(reply)) => {
                    let _ = send_reply(reply.boxed(), Ok(()));
                    None
                }
                (Ok(()), None) => None,
                (Err(error), Some(reply)) => {
                    if send_reply(reply.boxed(), Err(error.to_string())) {
                        None
                    } else {
                        Some(error)
                    }
                }
                (Err(error), None) => Some(error),
            };
            let id = tokio::task::id();
            let _ = actor_ref.tell(Done { id, error }).await;
        });
        self.outputs.insert(handle);
        self.exhausted = false;
        delegated
    }
}

impl<S, D, E, O> Message<Done> for Engine<S, D, E, O>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
    O: item::Store + 'static,
{
    type Reply = ();

    async fn handle(&mut self, done: Done, ctx: &mut Context<Self, Self::Reply>) {
        if !self.outputs.remove(done.id) {
            return;
        }
        self.invalidate_claim();
        self.exhausted = false;
        if let Some(error) = done.error {
            self.record_error(error);
        }
        if self.advance(ctx.actor_ref()) {
            ctx.stop();
        }
    }
}

fn send_reply(reply: kameo::reply::BoxReplySender, value: Result<(), String>) -> bool {
    let value = value
        .map(|value| Box::new(value) as kameo::message::BoxReply)
        .map_err(|error| {
            kameo::error::SendError::HandlerError(Box::new(error) as Box<dyn std::any::Any + Send>)
        });
    reply.send(value).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_reports_when_the_original_receiver_is_gone() {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        assert!(send_reply(sender, Ok(())));
        assert!(receiver.await.is_ok());

        let (sender, receiver) = tokio::sync::oneshot::channel();
        drop(receiver);
        assert!(!send_reply(sender, Err("output failed".to_string())));
    }
}
