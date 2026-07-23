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
    let (requests, claim_started) = claim(scheduler.as_ref(), limit, &worker_id, &modes).await?;
    if requests.is_empty() {
        if retry(|| scheduler.has_pending_requests(&worker_id, &modes)).await? {
            Ok(Output::Pending)
        } else {
            Ok(Output::Exhausted)
        }
    } else {
        Ok(Output::Requests {
            requests,
            claim_started,
        })
    }
}

async fn claim<S>(
    scheduler: &S,
    limit: usize,
    worker_id: &str,
    modes: &[crate::net::Mode],
) -> Result<(Vec<crate::net::Request>, Instant), crate::Error>
where
    S: scheduler::Scheduler,
{
    for attempt in 0..MAX_ATTEMPTS {
        let claim_started = Instant::now();
        match scheduler.next_requests(limit, worker_id, modes).await {
            Ok(requests) => return Ok((requests, claim_started)),
            Err(error) if error.is_transient() && attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(error) => return Err(crate::Error::Scheduler(error)),
        }
    }
    unreachable!()
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
            Ok(Output::Requests {
                requests,
                claim_started,
            }) => {
                self.exhausted = false;
                for next in requests {
                    request::spawn(self, ctx.actor_ref().clone(), next, claim_started);
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
        async fn open(&self) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn close(&self) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn push(&self, _payload: payload::Payload) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn push_items(&self, _payload: &payload::Payload) -> Result<(), scheduler::Error> {
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
            _worker_id: &str,
            _modes: &[net::Mode],
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

        async fn has_pending_requests(
            &self,
            _worker_id: &str,
            _modes: &[net::Mode],
        ) -> Result<bool, scheduler::Error> {
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
    async fn transient_retry_uses_successful_claim_start_for_the_batch() {
        let requests = vec![
            net::Request::follow("https://example.com/one").unwrap(),
            net::Request::follow("https://example.com/two").unwrap(),
        ];
        let expected_ids = requests
            .iter()
            .map(|request| request.id.clone())
            .collect::<Vec<_>>();
        let scheduler = TestScheduler::new(requests);

        let (claimed, claim_started) = claim(
            &scheduler,
            expected_ids.len(),
            "worker-1",
            &[net::Mode::Http],
        )
        .await
        .unwrap();

        let attempts = scheduler.attempts();
        assert_eq!(attempts.len(), 2);
        assert!(claim_started >= attempts[0] + RETRY_DELAY);
        assert!(claim_started <= attempts[1]);
        assert_eq!(
            claimed
                .iter()
                .map(|request| request.id.clone())
                .collect::<Vec<_>>(),
            expected_ids
        );
    }
}
