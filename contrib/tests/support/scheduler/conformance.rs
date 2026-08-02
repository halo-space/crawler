mod claims;
pub(crate) mod fixture;
mod initialization;
mod leases;
mod limits;
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
    initialization::requests_are_atomic(create()).await;
    initialization::unbound_requests_are_atomic(create()).await;
    replay::initial_request_validation_is_atomic(create()).await;
    replay::unbound_push_is_atomic(create()).await;
    replay::request_replay_is_atomic(create()).await;
    initialization::trace_ownership_is_atomic(create()).await;
    rules::claimed_requests_preserve_trace(create()).await;
    rules::existing_trace_runs_without_local_seed(create()).await;
    claims::claims_use_frozen_capabilities_and_priority(create()).await;
    claims::concurrent_claims_are_atomic(create()).await;
    claims::delayed_requests_wait_for_next_time(create(), timing).await;
    limits::request_retry_limit_is_enforced(create()).await;
    settlement::execution_generation_is_enforced(create()).await;
    settlement::release_before_ack_preserves_queue_retry(create()).await;
    settlement::failure_owns_terminal_settlement(create()).await;
}

/// Runs the lease-specific scenarios for a fixture that explicitly exposes a lease policy.
pub async fn lease<S>(scheduler: S, single_worker: bool, timing: Timing)
where
    S: spider::Scheduler + spider::scheduler::Init,
{
    leases::contract(scheduler, single_worker, timing).await;
}
