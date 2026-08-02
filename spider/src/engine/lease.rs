use std::future::Future;
use std::time::Duration;

use crate::{net, payload, scheduler};

const RETRY_DELAY: Duration = Duration::from_millis(25);

/// Runs one Request execution and submits its final settlement while the
/// acknowledged lease remains valid.
///
/// Ownership loss before execution completes prevents settlement. Once the
/// final Payload exists, the settlement response is authoritative and a
/// concurrent refresh error only stops further refreshes.
pub(super) async fn execute_with_lease<S, F>(
    scheduler: &S,
    request: &net::Request,
    claim_started: tokio::time::Instant,
    execution: impl Future<Output = payload::Payload>,
    settle: impl FnOnce(payload::Payload) -> F,
) -> Result<(), crate::Error>
where
    S: scheduler::Scheduler,
    F: Future<Output = Result<(), crate::Error>>,
{
    let Some(policy) = scheduler.lease() else {
        return settle(execution.await).await;
    };

    let mut payload = payload::Payload::for_request(request, request.leased_by.clone());
    payload.state = net::State::Processing;
    let deadline = expires_at(Some(policy), claim_started, request.id.as_str())?
        .expect("a Scheduler lease must produce a deadline");
    let refresh = refresh(scheduler, payload, policy, deadline);
    tokio::pin!(execution);
    tokio::pin!(refresh);
    let payload = tokio::select! {
        biased;
        error = &mut refresh => return Err(crate::Error::Scheduler(error)),
        payload = &mut execution => payload,
    };

    let settlement = settle(payload);
    tokio::pin!(settlement);
    tokio::select! {
        biased;
        result = &mut settlement => result,
        _ = &mut refresh => settlement.await,
    }
}

/// Refreshes until the current execution no longer owns a valid lease.
async fn refresh<S>(
    scheduler: &S,
    payload: payload::Payload,
    policy: scheduler::Lease,
    mut deadline: tokio::time::Instant,
) -> scheduler::Error
where
    S: scheduler::Scheduler,
{
    loop {
        match refresh_until(scheduler, &payload, deadline, policy.interval()).await {
            Ok(refresh_started) => {
                deadline = match checked_deadline(refresh_started, policy.timeout(), &payload.id) {
                    Ok(deadline) => deadline,
                    Err(error) => return error,
                };
                let next_refresh = refresh_started
                    .checked_add(policy.interval())
                    .unwrap_or(deadline)
                    .min(deadline);
                tokio::time::sleep_until(next_refresh).await;
            }
            Err(error) => return error,
        }
    }
}

async fn refresh_until<S>(
    scheduler: &S,
    payload: &payload::Payload,
    deadline: tokio::time::Instant,
    interval: Duration,
) -> Result<tokio::time::Instant, scheduler::Error>
where
    S: scheduler::Scheduler,
{
    let mut attempt = 1;
    loop {
        let started_at = tokio::time::Instant::now();
        if started_at >= deadline {
            return Err(scheduler::Error::LeaseExpired(payload.id.clone()));
        }
        let refresh = async {
            tokio::time::timeout_at(deadline, scheduler.refresh_lease(payload))
                .await
                .unwrap_or_else(|_| Err(scheduler::Error::LeaseExpired(payload.id.clone())))
        };
        let result = crate::trace::operation(
            "scheduler.refresh_lease",
            Some(attempt),
            refresh,
            crate::trace::scheduler_error_class,
        )
        .await;
        match result {
            Ok(()) => return Ok(started_at),
            Err(error) if error.is_ownership_loss() => return Err(error),
            Err(error) if error.is_transient() => {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    return Err(scheduler::Error::LeaseExpired(payload.id.clone()));
                }
                attempt += 1;
                let retry_at = now
                    .checked_add(RETRY_DELAY.min(interval))
                    .unwrap_or(deadline)
                    .min(deadline);
                tokio::time::sleep_until(retry_at).await;
            }
            Err(error) => return Err(error),
        }
    }
}

pub(super) fn expires_at(
    policy: Option<scheduler::Lease>,
    started: tokio::time::Instant,
    request_id: &str,
) -> Result<Option<tokio::time::Instant>, scheduler::Error> {
    policy
        .map(|policy| checked_deadline(started, policy.timeout(), request_id))
        .transpose()
}

fn checked_deadline(
    start: tokio::time::Instant,
    timeout: Duration,
    request_id: &str,
) -> Result<tokio::time::Instant, scheduler::Error> {
    start.checked_add(timeout).ok_or_else(|| {
        scheduler::Error::Message(format!(
            "lease duration exceeds the runtime clock range for Request {request_id}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct TestScheduler {
        policy: scheduler::Lease,
        refreshes: AtomicUsize,
        successes: usize,
        first_delay: Duration,
        refresh_failure: RefreshFailure,
    }

    #[derive(Clone, Copy)]
    enum RefreshFailure {
        Unavailable,
        LeaseMismatch,
    }

    impl TestScheduler {
        fn new(policy: scheduler::Lease) -> Self {
            Self {
                policy,
                refreshes: AtomicUsize::new(0),
                successes: usize::MAX,
                first_delay: Duration::ZERO,
                refresh_failure: RefreshFailure::Unavailable,
            }
        }

        fn unavailable(policy: scheduler::Lease) -> Self {
            Self {
                policy,
                refreshes: AtomicUsize::new(0),
                successes: 0,
                first_delay: Duration::ZERO,
                refresh_failure: RefreshFailure::Unavailable,
            }
        }

        fn slow_then_unavailable(policy: scheduler::Lease, delay: Duration) -> Self {
            Self {
                policy,
                refreshes: AtomicUsize::new(0),
                successes: 1,
                first_delay: delay,
                refresh_failure: RefreshFailure::Unavailable,
            }
        }

        fn ownership_loss_after(policy: scheduler::Lease, successes: usize) -> Self {
            Self {
                policy,
                refreshes: AtomicUsize::new(0),
                successes,
                first_delay: Duration::ZERO,
                refresh_failure: RefreshFailure::LeaseMismatch,
            }
        }
    }

    impl scheduler::Scheduler for TestScheduler {
        fn lease(&self) -> Option<scheduler::Lease> {
            Some(self.policy)
        }

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
        ) -> Result<Option<crate::trace::Snapshot>, scheduler::Error> {
            Ok(None)
        }

        async fn next_requests(
            &self,
            _limit: usize,
        ) -> Result<Vec<net::Request>, scheduler::Error> {
            Ok(Vec::new())
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

        async fn refresh_lease(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
            let attempt = self.refreshes.fetch_add(1, Ordering::SeqCst);
            if attempt >= self.successes {
                return match self.refresh_failure {
                    RefreshFailure::Unavailable => {
                        Err(scheduler::Error::Unavailable("offline".to_string()))
                    }
                    RefreshFailure::LeaseMismatch => {
                        Err(scheduler::Error::LeaseMismatch(payload.id.clone()))
                    }
                };
            }
            if attempt == 0 {
                tokio::time::sleep(self.first_delay).await;
            }
            Ok(())
        }

        async fn success(&self, _payload: &payload::Payload) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn failure(&self, _payload: &payload::Payload) -> Result<(), scheduler::Error> {
            Ok(())
        }
    }

    fn claimed_request() -> net::Request {
        let mut request = net::Request::follow("https://example.com").unwrap();
        request.version = 1;
        request.state = net::State::Processing;
        request.leased_by = "worker-1".to_string();
        request.lease_time = crate::utils::time::now_millis();
        request
    }

    async fn settle(_payload: payload::Payload) -> Result<(), crate::Error> {
        Ok(())
    }

    #[tokio::test]
    async fn starts_refresh_before_immediate_execution_completes() {
        let policy =
            scheduler::Lease::new(Duration::from_secs(1), Duration::from_millis(500)).unwrap();
        let scheduler = TestScheduler::new(policy);
        let request = claimed_request();

        let result = execute_with_lease(
            &scheduler,
            &request,
            tokio::time::Instant::now(),
            async { payload::Payload::new() },
            settle,
        )
        .await;

        result.unwrap();
        assert_eq!(scheduler.refreshes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn slow_initial_refresh_does_not_pause_execution() {
        let policy =
            scheduler::Lease::new(Duration::from_millis(200), Duration::from_millis(50)).unwrap();
        let scheduler = TestScheduler::slow_then_unavailable(policy, Duration::from_millis(100));
        let request = claimed_request();

        tokio::time::timeout(
            Duration::from_millis(25),
            execute_with_lease(
                &scheduler,
                &request,
                tokio::time::Instant::now(),
                async { payload::Payload::new() },
                settle,
            ),
        )
        .await
        .expect("lease refresh must run alongside request execution")
        .unwrap();

        assert_eq!(scheduler.refreshes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn initial_refresh_uses_the_claim_start_not_the_stored_lease_time() {
        let policy =
            scheduler::Lease::new(Duration::from_millis(100), Duration::from_millis(50)).unwrap();
        let scheduler = TestScheduler::unavailable(policy);
        let mut request = claimed_request();
        request.lease_time = i64::MAX;
        let claim_started = tokio::time::Instant::now() - Duration::from_millis(90);

        let result = tokio::time::timeout(
            Duration::from_millis(50),
            execute_with_lease(
                &scheduler,
                &request,
                claim_started,
                std::future::pending::<payload::Payload>(),
                settle,
            ),
        )
        .await
        .expect("the claim-start deadline must bound refresh retries");

        assert!(matches!(
            result,
            Err(crate::Error::Scheduler(scheduler::Error::LeaseExpired(id))) if id == request.id
        ));
        assert!(scheduler.refreshes.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn slow_refresh_does_not_extend_the_lease_from_response_time() {
        let policy =
            scheduler::Lease::new(Duration::from_millis(120), Duration::from_millis(20)).unwrap();
        let scheduler = TestScheduler::slow_then_unavailable(policy, Duration::from_millis(80));
        let request = claimed_request();

        let result = tokio::time::timeout(
            Duration::from_millis(160),
            execute_with_lease(
                &scheduler,
                &request,
                tokio::time::Instant::now(),
                std::future::pending::<payload::Payload>(),
                settle,
            ),
        )
        .await
        .expect("a slow response must not move the lease deadline forward");

        assert!(matches!(
            result,
            Err(crate::Error::Scheduler(scheduler::Error::LeaseExpired(id))) if id == request.id
        ));
        assert!(scheduler.refreshes.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn near_deadline_success_does_not_delay_the_next_refresh() {
        let policy =
            scheduler::Lease::new(Duration::from_millis(300), Duration::from_millis(200)).unwrap();
        let scheduler = TestScheduler::slow_then_unavailable(policy, Duration::from_millis(270));
        let request = claimed_request();

        let result = tokio::time::timeout(
            Duration::from_millis(360),
            execute_with_lease(
                &scheduler,
                &request,
                tokio::time::Instant::now(),
                std::future::pending::<payload::Payload>(),
                settle,
            ),
        )
        .await
        .expect("a refresh response near the deadline must not add another full interval");

        assert!(matches!(
            result,
            Err(crate::Error::Scheduler(scheduler::Error::LeaseExpired(id))) if id == request.id
        ));
        assert!(scheduler.refreshes.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn settlement_continues_after_refresh_loses_ownership() {
        let policy =
            scheduler::Lease::new(Duration::from_millis(200), Duration::from_millis(5)).unwrap();
        let scheduler = TestScheduler::ownership_loss_after(policy, 1);
        let request = claimed_request();

        let result = execute_with_lease(
            &scheduler,
            &request,
            tokio::time::Instant::now(),
            async { payload::Payload::new() },
            |_| async {
                tokio::time::sleep(Duration::from_millis(25)).await;
                Ok(())
            },
        )
        .await;

        result.unwrap();
        assert!(scheduler.refreshes.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn settlement_error_wins_after_refresh_loses_ownership() {
        let policy =
            scheduler::Lease::new(Duration::from_millis(200), Duration::from_millis(5)).unwrap();
        let scheduler = TestScheduler::ownership_loss_after(policy, 1);
        let request = claimed_request();

        let result = execute_with_lease(
            &scheduler,
            &request,
            tokio::time::Instant::now(),
            async { payload::Payload::new() },
            |_| async {
                tokio::time::sleep(Duration::from_millis(25)).await;
                Err(crate::Error::Scheduler(scheduler::Error::VersionMismatch(
                    "settlement".to_string(),
                )))
            },
        )
        .await;

        assert!(matches!(
            result,
            Err(crate::Error::Scheduler(scheduler::Error::VersionMismatch(id)))
                if id == "settlement"
        ));
        assert!(scheduler.refreshes.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn deadline_overflow_is_returned_as_a_scheduler_error() {
        let request_id = "request-overflow";

        let error =
            checked_deadline(tokio::time::Instant::now(), Duration::MAX, request_id).unwrap_err();

        assert!(matches!(error, scheduler::Error::Message(message)
            if message.contains(request_id) && message.contains("runtime clock range")));
    }
}
