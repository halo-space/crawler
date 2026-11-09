use std::future::Future;

#[cfg(feature = "runtime-tracing")]
use fastrace::future::FutureExt as _;
#[cfg(feature = "runtime-tracing")]
use fastrace::prelude::{LocalSpan, Span, SpanContext};
#[cfg(any(feature = "runtime-tracing", test))]
use sha2::{Digest as _, Sha256};

#[cfg(any(feature = "runtime-tracing", test))]
const MAX_IDENTITY_BYTES: usize = 128;

#[cfg(feature = "runtime-tracing")]
pub(crate) type RuntimeContext = SpanContext;
#[cfg(not(feature = "runtime-tracing"))]
pub(crate) type RuntimeContext = ();

/// Selects which Request executions produce runtime traces.
///
/// This value configures only span creation. The application remains responsible
/// for installing and flushing the process-global `fastrace` reporter.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Tracing {
    ratio: f64,
}

impl Tracing {
    /// Traces every Request executed by this Runtime.
    pub const fn all() -> Self {
        Self { ratio: 1.0 }
    }

    /// Deterministically samples Request executions using a ratio in `0.0..=1.0`.
    pub fn sample(ratio: f64) -> Result<Self, crate::Error> {
        if !ratio.is_finite() || !(0.0..=1.0).contains(&ratio) {
            return Err(crate::Error::message(
                "tracing sample ratio must be between 0.0 and 1.0",
            ));
        }
        Ok(Self { ratio })
    }

    #[cfg(any(feature = "runtime-tracing", test))]
    fn samples(&self, request: &crate::net::Request, worker_id: &str) -> bool {
        if self.ratio == 0.0 {
            return false;
        }
        if self.ratio == 1.0 {
            return true;
        }

        let mut hash = Sha256::new();
        hash.update((request.id.len() as u64).to_be_bytes());
        hash.update(request.id.as_bytes());
        hash.update(request.version.to_be_bytes());
        hash.update((worker_id.len() as u64).to_be_bytes());
        hash.update(worker_id.as_bytes());
        let digest = hash.finalize();
        let sample = u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 prefix")) >> 11;
        let threshold = (self.ratio * ((1_u64 << 53) as f64)) as u64;
        sample < threshold
    }

    #[cfg(feature = "runtime-tracing")]
    pub(crate) fn request_span(&self, request: &crate::net::Request, worker_id: &str) -> Span {
        if !self.samples(request, worker_id) {
            return Span::noop();
        }

        let mode = match request.mode {
            crate::net::Mode::Http => "http",
            crate::net::Mode::Browser => "browser",
        };
        Span::root("crawler.request", SpanContext::random()).with_properties(|| {
            [
                ("crawler.task_id", bounded_identity(&request.task_id)),
                ("crawler.trace_id", bounded_identity(&request.trace_id)),
                ("crawler.request_id", bounded_identity(&request.id)),
                ("crawler.node", bounded_identity(request.node_key())),
                ("crawler.version", request.version.to_string()),
                ("crawler.worker_id", bounded_identity(worker_id)),
                ("crawler.mode", mode.to_string()),
            ]
        })
    }
}

#[cfg(any(feature = "runtime-tracing", test))]
fn bounded_identity(value: &str) -> String {
    if value.len() <= MAX_IDENTITY_BYTES && !value.chars().any(char::is_control) {
        return value.to_string();
    }

    let digest = Sha256::digest(value.as_bytes());
    let mut encoded = String::with_capacity("sha256:".len() + digest.len() * 2);
    encoded.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        write!(encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

impl Default for Tracing {
    fn default() -> Self {
        Self { ratio: 0.0 }
    }
}

pub(crate) fn current_context() -> Option<RuntimeContext> {
    #[cfg(feature = "runtime-tracing")]
    {
        SpanContext::current_local_parent()
    }
    #[cfg(not(feature = "runtime-tracing"))]
    {
        None
    }
}

#[cfg(not(feature = "runtime-tracing"))]
pub(crate) fn operation<T, E, F, C>(
    name: &'static str,
    attempt: Option<usize>,
    future: F,
    error_class: C,
) -> F
where
    F: Future<Output = Result<T, E>>,
    C: FnOnce(&E) -> &'static str,
{
    let _ = (name, attempt, error_class);
    future
}

#[cfg(feature = "runtime-tracing")]
pub(crate) fn operation<T, E, F, C>(
    name: &'static str,
    attempt: Option<usize>,
    future: F,
    error_class: C,
) -> impl Future<Output = Result<T, E>>
where
    F: Future<Output = Result<T, E>>,
    C: FnOnce(&E) -> &'static str,
{
    let mut span = Span::enter_with_local_parent(name);
    if let Some(attempt) = attempt {
        span = span.with_property(|| ("retry.attempt", attempt.to_string()));
    }
    Box::pin(
        async move {
            let result = future.await;
            record_result(&result, error_class);
            result
        }
        .in_span(span),
    )
}

#[cfg(not(feature = "runtime-tracing"))]
pub(crate) fn output<T, E, F, C>(
    name: &'static str,
    count: usize,
    parent: Option<RuntimeContext>,
    future: F,
    error_class: C,
) -> F
where
    F: Future<Output = Result<T, E>>,
    C: FnOnce(&E) -> &'static str,
{
    let _ = (name, count, parent, error_class);
    future
}

#[cfg(feature = "runtime-tracing")]
pub(crate) fn output<T, E, F, C>(
    name: &'static str,
    count: usize,
    parent: Option<RuntimeContext>,
    future: F,
    error_class: C,
) -> impl Future<Output = Result<T, E>>
where
    F: Future<Output = Result<T, E>>,
    C: FnOnce(&E) -> &'static str,
{
    let span = parent.map_or_else(Span::noop, |parent| Span::root(name, parent));
    let span = span.with_property(|| ("output.count", count.to_string()));
    Box::pin(
        async move {
            let result = future.await;
            record_result(&result, error_class);
            result
        }
        .in_span(span),
    )
}

pub(crate) fn record_result<T, E>(
    result: &Result<T, E>,
    error_class: impl FnOnce(&E) -> &'static str,
) {
    #[cfg(feature = "runtime-tracing")]
    {
        match result {
            Ok(_) => LocalSpan::add_property(|| ("span.status_code", "ok")),
            Err(error) => LocalSpan::add_properties(|| {
                [
                    ("span.status_code", "error"),
                    ("error.type", error_class(error)),
                ]
            }),
        }
    }
    #[cfg(not(feature = "runtime-tracing"))]
    {
        let _ = (result, error_class);
    }
}

pub(crate) fn record_http_method(method: &crate::net::Method) {
    #[cfg(feature = "runtime-tracing")]
    {
        let method = match method {
            crate::net::Method::Get => "GET",
            crate::net::Method::Post => "POST",
            crate::net::Method::Put => "PUT",
            crate::net::Method::Patch => "PATCH",
            crate::net::Method::Delete => "DELETE",
            crate::net::Method::Head => "HEAD",
            crate::net::Method::Options => "OPTIONS",
        };
        LocalSpan::add_property(|| ("http.request.method", method));
    }
    #[cfg(not(feature = "runtime-tracing"))]
    let _ = method;
}

pub(crate) fn record_http_status(status: crate::net::StatusCode) {
    #[cfg(feature = "runtime-tracing")]
    LocalSpan::add_property(|| ("http.response.status_code", status.0.to_string()));
    #[cfg(not(feature = "runtime-tracing"))]
    let _ = status;
}

pub(crate) fn error_class(error: &crate::Error) -> &'static str {
    match error {
        crate::Error::Ai(_) => "ai",
        crate::Error::Download(_) => "download",
        crate::Error::Config(_) => "config",
        crate::Error::Item(_) => "item",
        crate::Error::Net(_) => "net",
        crate::Error::Middleware(_) => "middleware",
        crate::Error::Scheduler(_) => "scheduler",
        crate::Error::Spider(_) => "spider",
        crate::Error::Selector(_) => "selector",
        crate::Error::Graph(_) => "graph",
        crate::Error::Stats(_) => "stats",
        crate::Error::Message(_) => "engine",
    }
}

pub(crate) fn scheduler_error_class(error: &crate::scheduler::Error) -> &'static str {
    match error {
        crate::scheduler::Error::IdentityMismatch { .. } => "identity_mismatch",
        crate::scheduler::Error::LeaseMismatch(_) => "lease_mismatch",
        crate::scheduler::Error::LeaseExpired(_) => "lease_expired",
        crate::scheduler::Error::NotAcknowledged(_) => "not_acknowledged",
        crate::scheduler::Error::StateMismatch(_) => "state_mismatch",
        crate::scheduler::Error::VersionMismatch(_) => "version_mismatch",
        crate::scheduler::Error::RequestNotFound(_) => "request_not_found",
        crate::scheduler::Error::TraceNotFound(_) => "trace_not_found",
        crate::scheduler::Error::InvalidTrace { .. } => "invalid_trace",
        crate::scheduler::Error::InvalidRequest { .. } => "invalid_request",
        crate::scheduler::Error::Unavailable(_) => "unavailable",
        crate::scheduler::Error::Message(_) => "scheduler",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_ratio_is_validated() {
        assert_eq!(Tracing::default().ratio, 0.0);
        assert_eq!(Tracing::all().ratio, 1.0);
        assert!(Tracing::sample(0.0).is_ok());
        assert!(Tracing::sample(1.0).is_ok());
        assert!(Tracing::sample(-0.1).is_err());
        assert!(Tracing::sample(1.1).is_err());
        assert!(Tracing::sample(f64::NAN).is_err());
        assert!(Tracing::sample(f64::INFINITY).is_err());
    }

    #[test]
    fn sampling_is_deterministic() {
        let tracing = Tracing::sample(0.5).unwrap();
        let mut outcomes = Vec::new();
        for id in 0..64 {
            let request = crate::net::Request::follow("https://example.com")
                .unwrap()
                .with_id(format!("request-{id}"));
            outcomes.push(tracing.samples(&request, "worker-1"));
        }

        assert!(outcomes.iter().any(|sampled| *sampled));
        assert!(outcomes.iter().any(|sampled| !sampled));
        for (id, expected) in outcomes.into_iter().enumerate() {
            let request = crate::net::Request::follow("https://example.com")
                .unwrap()
                .with_id(format!("request-{id}"));
            assert_eq!(tracing.samples(&request, "worker-1"), expected);
        }
    }

    #[test]
    fn sampling_honors_exact_boundaries() {
        let request = crate::net::Request::follow("https://example.com").unwrap();

        assert!(!Tracing::sample(0.0).unwrap().samples(&request, "worker-1"));
        assert!(Tracing::all().samples(&request, "worker-1"));
    }

    #[test]
    fn runtime_identities_are_bounded_without_changing_short_values() {
        assert_eq!(bounded_identity("request-1"), "request-1");

        let secret = "sensitive".repeat(MAX_IDENTITY_BYTES);
        let bounded = bounded_identity(&secret);
        assert!(bounded.starts_with("sha256:"));
        assert_eq!(bounded.len(), "sha256:".len() + 64);
        assert!(!bounded.contains("sensitive"));

        let bounded = bounded_identity("worker\nforged-property");
        assert!(bounded.starts_with("sha256:"));
        assert!(!bounded.contains('\n'));
    }

    #[cfg(feature = "runtime-tracing")]
    #[test]
    fn sampled_requests_receive_independent_runtime_traces() {
        let first = crate::net::Request::follow("https://example.com/one").unwrap();
        let second = crate::net::Request::follow("https://example.com/two").unwrap();
        let first = Tracing::all().request_span(&first, "worker-1");
        let second = Tracing::all().request_span(&second, "worker-1");
        let first = SpanContext::from_span(&first).unwrap();
        let second = SpanContext::from_span(&second).unwrap();

        assert_ne!(first.trace_id, second.trace_id);
    }

    #[cfg(feature = "runtime-tracing")]
    #[test]
    fn result_records_failure_class_and_retry_attempt() {
        use fastrace::local::LocalCollector;
        use fastrace::prelude::{LocalSpan, SpanContext};

        let collector = LocalCollector::start();
        let span = LocalSpan::enter_with_local_parent("test.retry")
            .with_property(|| ("retry.attempt", "2"));
        record_result(&Err::<(), _>("provider-secret"), |_| "provider");
        drop(span);

        let records = collector.collect().to_span_records(SpanContext::random());
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].name, "test.retry");
        assert_eq!(property(&records[0], "retry.attempt"), Some("2"));
        assert_eq!(property(&records[0], "span.status_code"), Some("error"));
        assert_eq!(property(&records[0], "error.type"), Some("provider"));
        assert!(records.iter().all(|record| {
            record
                .properties
                .iter()
                .all(|(_, value)| !value.contains("provider-secret"))
        }));
    }

    #[cfg(feature = "runtime-tracing")]
    #[tokio::test]
    async fn unsampled_requests_produce_no_operation_spans() {
        use fastrace::future::FutureExt as _;
        use fastrace::local::LocalCollector;
        use fastrace::prelude::SpanContext;

        let request = crate::net::Request::follow("https://example.com").unwrap();
        let collector = LocalCollector::start();
        let root = Tracing::default().request_span(&request, "worker-1");
        async {
            operation(
                "test.unsampled",
                Some(1),
                async { Ok::<_, &'static str>(()) },
                |_| "test",
            )
            .await
        }
        .in_span(root)
        .await
        .unwrap();

        assert!(
            collector
                .collect()
                .to_span_records(SpanContext::random())
                .is_empty()
        );
    }

    #[cfg(feature = "runtime-tracing")]
    fn property<'a>(record: &'a fastrace::collector::SpanRecord, name: &str) -> Option<&'a str> {
        record
            .properties
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_ref())
    }
}
