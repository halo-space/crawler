use kameo::message::{Context, Message};

use super::Engine;
use crate::{downloader, engine, scheduler};

pub(crate) struct Shutdown;

impl<S, D, E, O> Message<Shutdown> for Engine<S, D, E, O>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
    O: crate::item::Store + 'static,
{
    type Reply = ();

    async fn handle(&mut self, _shutdown: Shutdown, ctx: &mut Context<Self, Self::Reply>) {
        self.stopping = true;
        if let Some(poll) = self.poll.take() {
            poll.abort();
        }
        if self.advance(ctx.actor_ref()) {
            ctx.stop();
        }
    }
}
