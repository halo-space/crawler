#[cfg(feature = "runtime-tracing")]
use fastrace::future::FutureExt as _;
use kameo::actor::ActorRef;
use kameo::message::{Context, Message};
use tokio::time::Instant;

use super::{Engine, task};
use crate::{downloader, engine, scheduler};

pub(super) struct Done {
    id: task::Id,
    result: Result<(), crate::Error>,
}

pub(super) fn spawn<S, D, E, O>(
    engine: &mut Engine<S, D, E, O>,
    actor_ref: ActorRef<Engine<S, D, E, O>>,
    request: crate::net::Request,
    claim_started: Instant,
) where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
    O: crate::item::Store + 'static,
{
    let scheduler = engine.scheduler.clone();
    let downloader = engine.downloader.clone();
    let executor = engine.executor.clone();
    let registry = engine.registry.clone();
    #[cfg(feature = "runtime-tracing")]
    let span = engine
        .config
        .tracing
        .request_span(&request, &request.leased_by);
    #[cfg(not(feature = "runtime-tracing"))]
    let _ = engine.config.tracing;
    let id = task::Id::new();
    let done_id = id.clone();
    let future = async move {
        let result = task::protect(engine::request::task::execute(
            request,
            claim_started,
            scheduler,
            downloader,
            executor,
            registry,
        ))
        .await;
        crate::trace::record_result(&result, crate::trace::error_class);
        let _ = actor_ref
            .tell(Done {
                id: done_id,
                result,
            })
            .await;
    };
    #[cfg(feature = "runtime-tracing")]
    let handle = tokio::spawn(future.in_span(span));
    #[cfg(not(feature = "runtime-tracing"))]
    let handle = tokio::spawn(future);
    engine.requests.insert(id, handle);
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
        if !self.requests.remove(&done.id) {
            return;
        }
        self.invalidate_claim();
        if let Err(error) = done.result {
            self.record_error(error);
        }
        if self.advance(ctx.actor_ref()) {
            ctx.stop();
        }
    }
}
