mod claims;
pub(crate) mod fixture;
mod initialization;
mod items;
mod leases;
pub(crate) mod payload;
mod replay;
mod rules;
mod settlement;

pub use fixture::Timing;

/// Runs every backend-neutral Scheduler scenario against fresh isolated instances.
///
/// The factory is called once per scenario. Each instance must use a disposable local directory
/// and, for remote backends, a unique namespace owned and cleaned up by the backend fixture. A
/// lease-backed fixture should configure a short test lease so expiry recovery remains fast.
pub async fn run<S, F>(create: F, initializes_run: bool, timing: Timing)
where
    S: spider::Scheduler + spider::scheduler::Init + 'static,
    F: Fn() -> S,
{
    initialization::lifecycle_and_trace(create(), initializes_run).await;
    initialization::initialization_is_atomic(create()).await;
    replay::request_replay_is_atomic(create()).await;
    initialization::trace_ownership_is_atomic(create()).await;
    rules::claimed_requests_preserve_trace(create()).await;
    rules::existing_trace_runs_without_local_seed(create()).await;
    claims::claims_are_capability_scoped(create()).await;
    claims::concurrent_capability_claims_are_atomic(create()).await;
    claims::delayed_requests_wait_for_next_time(create(), timing).await;
    settlement::execution_identity_is_enforced(create()).await;
    settlement::release_before_ack_preserves_queue_retry(create()).await;
    settlement::failure_owns_queue_retry(create()).await;
    items::submission_is_isolated(create()).await;
}

/// Runs the lease-specific scenarios for a fixture that explicitly exposes a lease policy.
pub async fn lease<S>(scheduler: S, timing: Timing)
where
    S: spider::Scheduler,
{
    leases::contract(scheduler, timing).await;
}
