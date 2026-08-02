use kameo::actor::ActorRef;
use kameo::message::{Context, Message};

use super::{Engine, task};
use crate::{downloader, engine, scheduler};

pub(super) struct Poll {
    id: task::Id,
    result: Result<(), crate::Error>,
}

pub(super) struct Idle {
    id: task::Id,
    result: Result<(), crate::Error>,
}

pub(super) fn poll<S, D, E, O>(
    engine: &mut Engine<S, D, E, O>,
    actor_ref: ActorRef<Engine<S, D, E, O>>,
) where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
    O: crate::item::Store + 'static,
{
    let interval = if engine.finishes_when_idle() {
        std::time::Duration::from_millis(1)
    } else {
        engine.config.idle_interval
    };
    let id = task::Id::new();
    let done_id = id.clone();
    let handle = tokio::spawn(async move {
        let result = task::protect(async {
            tokio::time::sleep(interval).await;
            Ok(())
        })
        .await;
        let _ = actor_ref
            .tell(Poll {
                id: done_id,
                result,
            })
            .await;
    });
    engine.poll = Some(task::Task::new(id, handle));
}

pub(super) fn idle<S, D, E, O>(
    engine: &mut Engine<S, D, E, O>,
    actor_ref: ActorRef<Engine<S, D, E, O>>,
) where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
    O: crate::item::Store + 'static,
{
    let events = engine.events.clone();
    let id = task::Id::new();
    let done_id = id.clone();
    let handle = tokio::spawn(async move {
        let result = task::protect(async move {
            events.wait_until_idle().await;
            Ok(())
        })
        .await;
        let _ = actor_ref
            .tell(Idle {
                id: done_id,
                result,
            })
            .await;
    });
    engine.idle = Some(task::Task::new(id, handle));
}

impl<S, D, E, O> Message<Poll> for Engine<S, D, E, O>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
    O: crate::item::Store + 'static,
{
    type Reply = ();

    async fn handle(&mut self, done: Poll, ctx: &mut Context<Self, Self::Reply>) {
        if !self
            .poll
            .as_ref()
            .is_some_and(|task| task.matches(&done.id))
        {
            return;
        }
        self.poll = None;
        if let Err(error) = done.result {
            self.record_error(error);
        }
        if self.advance(ctx.actor_ref()) {
            ctx.stop();
        }
    }
}

impl<S, D, E, O> Message<Idle> for Engine<S, D, E, O>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
    O: crate::item::Store + 'static,
{
    type Reply = ();

    async fn handle(&mut self, done: Idle, ctx: &mut Context<Self, Self::Reply>) {
        if !self
            .idle
            .as_ref()
            .is_some_and(|task| task.matches(&done.id))
        {
            return;
        }
        self.idle = None;
        if let Err(error) = done.result {
            self.record_error(error);
        }
        if self.advance(ctx.actor_ref()) {
            ctx.stop();
        }
    }
}
