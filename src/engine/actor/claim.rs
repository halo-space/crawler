use std::sync::Arc;
use tokio::time::Instant;

use kameo::actor::ActorRef;
use kameo::message::{Context, Message};

use super::{Engine, request, task, wait};
use crate::{downloader, engine, scheduler};

const MAX_ATTEMPTS: usize = 3;
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(25);

enum Output {
    Requests {
        requests: Vec<crate::net::Request>,
        claim_started: Instant,
    },
    Empty {
        pending: bool,
    },
}

pub(super) struct Done {
    id: task::Id,
    result: Result<Output, crate::Error>,
}

pub(super) fn spawn<S, D, E, O>(
    engine: &mut Engine<S, D, E, O>,
    actor_ref: ActorRef<Engine<S, D, E, O>>,
    limit: usize,
) where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
    O: crate::item::Store + 'static,
{
    engine.claim_stale = false;
    let scheduler = engine.scheduler.clone();
    let check_pending = engine.finishes_when_idle();
    let id = task::Id::new();
    let done_id = id.clone();
    let handle = tokio::spawn(async move {
        let result = task::protect(next(scheduler, limit, check_pending)).await;
        let _ = actor_ref
            .tell(Done {
                id: done_id,
                result,
            })
            .await;
    });
    engine.claim = Some(task::Task::new(id, handle));
}

async fn next<S>(
    scheduler: Arc<S>,
    limit: usize,
    check_pending: bool,
) -> Result<Output, crate::Error>
where
    S: scheduler::Scheduler,
{
    let (requests, claim_started) = claim(scheduler.as_ref(), limit).await?;
    if requests.is_empty() {
        let pending = if check_pending {
            retry(|| scheduler.has_pending_requests()).await?
        } else {
            false
        };
        Ok(Output::Empty { pending })
    } else {
        Ok(Output::Requests {
            requests,
            claim_started,
        })
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

async fn claim<S>(
    scheduler: &S,
    limit: usize,
) -> Result<(Vec<crate::net::Request>, Instant), crate::Error>
where
    S: scheduler::Scheduler,
{
    let claim_started = Instant::now();
    for attempt in 0..MAX_ATTEMPTS {
        match scheduler.next_requests(limit).await {
            Ok(requests) => return Ok((requests, claim_started)),
            Err(error) if error.is_transient() && attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(error) => return Err(crate::Error::Scheduler(error)),
        }
    }
    unreachable!()
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
            .claim
            .as_ref()
            .is_some_and(|task| task.matches(&done.id))
        {
            return;
        }
        self.claim = None;
        let stale = std::mem::take(&mut self.claim_stale);
        match done.result {
            Ok(Output::Requests {
                requests,
                claim_started,
            }) => {
                for next in requests {
                    request::spawn(self, ctx.actor_ref().clone(), next, claim_started);
                }
            }
            Ok(Output::Empty { pending }) => {
                if self.finishes_when_idle()
                    && !pending
                    && !stale
                    && self.requests.is_empty()
                    && self.outputs.is_empty()
                    && self.events.is_idle()
                {
                    self.stopping = true;
                } else if !self.stopping && self.error.is_none() {
                    wait::poll(self, ctx.actor_ref().clone());
                }
            }
            Err(error) => {
                self.record_error(error);
            }
        }
        if self.advance(ctx.actor_ref()) {
            ctx.stop();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::{net, payload, trace};

    struct TestScheduler {
        attempts: Mutex<Vec<Instant>>,
        requests: Mutex<Option<Vec<net::Request>>>,
    }

    impl TestScheduler {
        fn new(requests: Vec<net::Request>) -> Self {
            Self {
                attempts: Mutex::new(Vec::new()),
                requests: Mutex::new(Some(requests)),
            }
        }

        fn attempts(&self) -> Vec<Instant> {
            self.attempts.lock().unwrap().clone()
        }
    }

    impl scheduler::Scheduler for TestScheduler {
        async fn open(&self, _concurrency: usize) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn close(&self) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn push(&self, _payload: payload::Payload) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn trace(
            &self,
            _trace_id: &str,
        ) -> Result<Option<trace::Snapshot>, scheduler::Error> {
            Ok(None)
        }

        async fn next_requests(
            &self,
            _limit: usize,
        ) -> Result<Vec<net::Request>, scheduler::Error> {
            let attempt = {
                let mut attempts = self.attempts.lock().unwrap();
                attempts.push(Instant::now());
                attempts.len()
            };
            if attempt == 1 {
                return Err(scheduler::Error::Unavailable(
                    "transient claim failure".to_string(),
                ));
            }
            tokio::time::sleep(RETRY_DELAY).await;
            Ok(self.requests.lock().unwrap().take().unwrap())
        }

        async fn has_pending_requests(&self) -> Result<bool, scheduler::Error> {
            Ok(false)
        }

        async fn ack(&self, _payload: &payload::Payload) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn release(&self, _payload: &payload::Payload) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn refresh_lease(&self, _payload: &payload::Payload) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn success(&self, _payload: &payload::Payload) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn failure(&self, _payload: &payload::Payload) -> Result<(), scheduler::Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn transient_retry_keeps_the_first_claim_start_for_the_batch() {
        let requests = vec![
            net::Request::follow("https://example.com/one").unwrap(),
            net::Request::follow("https://example.com/two").unwrap(),
        ];
        let expected_ids = requests
            .iter()
            .map(|request| request.id.clone())
            .collect::<Vec<_>>();
        let scheduler = TestScheduler::new(requests);

        let (claimed, claim_started) = claim(&scheduler, expected_ids.len()).await.unwrap();

        let attempts = scheduler.attempts();
        assert_eq!(attempts.len(), 2);
        assert!(claim_started <= attempts[0]);
        assert!(claim_started < attempts[1]);
        assert_eq!(
            claimed
                .iter()
                .map(|request| request.id.clone())
                .collect::<Vec<_>>(),
            expected_ids
        );
    }
}
