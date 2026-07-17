use std::sync::Arc;

use tokio::task::{JoinHandle, JoinSet};

use crate::spider::tx::{Event, Receiver};
use crate::{downloader, engine, middleware, net, scheduler};

const MAX_SCHEDULER_ATTEMPTS: usize = 3;
const SCHEDULER_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(25);

type Claim = JoinHandle<Result<Vec<net::Request>, scheduler::Error>>;

pub(super) struct Coordinator<S, D, R> {
    scheduler: Arc<S>,
    downloader: Arc<D>,
    executor: Arc<R>,
    registry: Arc<middleware::Registry>,
    snapshots: Option<Arc<crate::item::snapshot::Store>>,
    concurrency: usize,
    limit: usize,
}

impl<S, D, R> Coordinator<S, D, R>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    R: engine::contract::Execute + 'static,
{
    pub(super) fn new(
        scheduler: Arc<S>,
        downloader: Arc<D>,
        executor: Arc<R>,
        registry: Arc<middleware::Registry>,
        snapshots: Option<Arc<crate::item::snapshot::Store>>,
        concurrency: usize,
        limit: usize,
    ) -> Self {
        Self {
            scheduler,
            downloader,
            executor,
            registry,
            snapshots,
            concurrency,
            limit,
        }
    }

    pub(super) async fn run(
        &self,
        mut events: Receiver,
        init: engine::init::Output,
    ) -> Result<(), crate::Error> {
        let mut outputs = JoinSet::new();
        let mut reported = None;
        if init == engine::init::Output::Start
            && let Err(error) = self.start(&mut events, &mut outputs).await
        {
            reported = Some(error);
        }

        let mut requests = JoinSet::new();
        let mut claim: Option<Claim> = None;
        let mut exhausted = false;
        let mut claims_blocked = false;
        let mut next_claim = tokio::time::Instant::now();

        loop {
            while let Ok(event) = events.try_recv() {
                self.spawn_output(&mut outputs, event);
                exhausted = false;
            }

            if !claims_blocked
                && !exhausted
                && claim.is_none()
                && requests.len() < self.concurrency
                && tokio::time::Instant::now() >= next_claim
            {
                let available = self.concurrency - requests.len();
                claim = Some(self.claim(self.limit.min(available)));
            }

            if claim.is_none()
                && requests.is_empty()
                && outputs.is_empty()
                && events.is_empty()
                && events.producers_are_idle()
                && (claims_blocked || exhausted)
            {
                return reported.map_or(Ok(()), Err);
            }

            let wait_until = next_claim;
            let producers_idle = events.producers_are_idle();
            let producers = events.producer_wait();
            tokio::pin!(producers);
            tokio::select! {
                result = async { claim.as_mut().expect("claim branch is guarded").await }, if claim.is_some() => {
                    claim = None;
                    match result {
                        Ok(Ok(claimed)) => {
                            if claimed.is_empty() {
                                match self.has_pending_requests().await {
                                    Ok(pending) => exhausted = !pending,
                                    Err(error) => {
                                        claims_blocked = true;
                                        reported.get_or_insert(crate::Error::Scheduler(error));
                                    }
                                }
                                if !exhausted && !claims_blocked {
                                    next_claim = tokio::time::Instant::now()
                                        + super::runtime::DEFAULT_POLL_INTERVAL;
                                }
                            } else {
                                exhausted = false;
                                for request in claimed {
                                    self.spawn_request(&mut requests, request);
                                }
                            }
                        }
                        Ok(Err(error)) => {
                            claims_blocked = true;
                            reported.get_or_insert(crate::Error::Scheduler(error));
                        }
                        Err(error) => {
                            claims_blocked = true;
                            reported.get_or_insert_with(|| crate::Error::message(error.to_string()));
                        }
                    }
                }
                result = requests.join_next(), if !requests.is_empty() => {
                    exhausted = false;
                    match result {
                        Some(Ok(Ok(()))) => {}
                        Some(Ok(Err(error))) => { reported.get_or_insert(error); }
                        Some(Err(error)) => {
                            reported.get_or_insert_with(|| crate::Error::message(error.to_string()));
                        }
                        None => {}
                    }
                }
                result = outputs.join_next(), if !outputs.is_empty() => {
                    exhausted = false;
                    if let Some(Err(error)) = result {
                        reported.get_or_insert_with(|| crate::Error::message(error.to_string()));
                    }
                }
                event = events.recv() => {
                    if let Some(event) = event {
                        self.spawn_output(&mut outputs, event);
                        exhausted = false;
                    } else if !claims_blocked {
                        claims_blocked = true;
                        reported.get_or_insert(crate::Error::message(
                            "event channel closed before local work completed",
                        ));
                    }
                }
                _ = tokio::time::sleep_until(wait_until),
                    if !claims_blocked
                        && claim.is_none()
                        && requests.len() < self.concurrency
                        && tokio::time::Instant::now() < next_claim => {}
                _ = &mut producers,
                    if claim.is_none()
                        && requests.is_empty()
                        && outputs.is_empty()
                        && events.is_empty()
                        && !producers_idle => {}
            }
        }
    }

    fn claim(&self, limit: usize) -> Claim {
        let scheduler = self.scheduler.clone();
        tokio::spawn(async move {
            for attempt in 0..MAX_SCHEDULER_ATTEMPTS {
                match scheduler.next_requests(limit).await {
                    Ok(requests) => return Ok(requests),
                    Err(error) if error.is_transient() && attempt + 1 < MAX_SCHEDULER_ATTEMPTS => {
                        tokio::time::sleep(SCHEDULER_RETRY_DELAY).await;
                    }
                    Err(error) => return Err(error),
                }
            }
            unreachable!()
        })
    }

    async fn has_pending_requests(&self) -> Result<bool, scheduler::Error> {
        for attempt in 0..MAX_SCHEDULER_ATTEMPTS {
            match self.scheduler.has_pending_requests().await {
                Ok(pending) => return Ok(pending),
                Err(error) if error.is_transient() && attempt + 1 < MAX_SCHEDULER_ATTEMPTS => {
                    tokio::time::sleep(SCHEDULER_RETRY_DELAY).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!()
    }

    fn spawn_request(
        &self,
        requests: &mut JoinSet<Result<(), crate::Error>>,
        request: net::Request,
    ) {
        requests.spawn(engine::worker::run(
            request,
            self.scheduler.clone(),
            self.downloader.clone(),
            self.executor.clone(),
            self.registry.clone(),
        ));
    }

    async fn start(
        &self,
        events: &mut Receiver,
        outputs: &mut JoinSet<()>,
    ) -> Result<(), crate::Error> {
        let startup = self.executor.start();
        tokio::pin!(startup);
        loop {
            tokio::select! {
                result = &mut startup => return result,
                Some(result) = outputs.join_next(), if !outputs.is_empty() => {
                    result.map_err(|error| crate::Error::message(error.to_string()))?;
                }
                event = events.recv() => {
                    let Some(event) = event else {
                        return Err(crate::Error::message(
                            "event channel closed during engine start",
                        ));
                    };
                    self.spawn_output(outputs, event);
                }
            }
        }
    }

    fn spawn_output(&self, tasks: &mut JoinSet<()>, event: Event) {
        let scheduler = self.scheduler.clone();
        let registry = self.registry.clone();
        let snapshots = self.snapshots.clone();
        tasks.spawn(async move {
            engine::event::handle(event, scheduler, registry, snapshots).await;
        });
    }
}
