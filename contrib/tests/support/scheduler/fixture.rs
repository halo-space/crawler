use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use spider::{Scheduler, net};

pub(crate) const WORKER_A: &str = "worker-a";
pub(crate) const WORKER_B: &str = "worker-b";
pub(crate) const HTTP: &[net::Mode] = &[net::Mode::Http];
pub(super) const BROWSER: &[net::Mode] = &[net::Mode::Browser];
pub(super) const ALL: &[net::Mode] = &[net::Mode::Http, net::Mode::Browser];

/// Timing chosen by a Scheduler conformance fixture.
///
/// Remote backends can use wider values than Memory to account for transport and clock latency.
#[derive(Clone, Copy)]
pub struct Timing {
    lease_timeout: Duration,
    lease_refresh: Duration,
    lease_margin: Duration,
    delayed_request: Duration,
    delayed_margin: Duration,
}

impl Timing {
    pub const fn new(
        lease_timeout: Duration,
        lease_refresh: Duration,
        lease_margin: Duration,
        delayed_request: Duration,
        delayed_margin: Duration,
    ) -> Self {
        Self {
            lease_timeout,
            lease_refresh,
            lease_margin,
            delayed_request,
            delayed_margin,
        }
    }

    #[allow(dead_code)]
    pub const fn memory() -> Self {
        Self::new(
            Duration::from_millis(500),
            Duration::from_millis(100),
            Duration::from_millis(50),
            Duration::from_millis(200),
            Duration::from_millis(50),
        )
    }

    pub const fn lease(self) -> (Duration, Duration) {
        (self.lease_timeout, self.lease_refresh)
    }

    pub(super) fn after_refresh(self, timeout: Duration) -> Duration {
        timeout / 2 + self.lease_margin
    }

    pub(super) fn after_expiry(self, timeout: Duration) -> Duration {
        timeout + self.lease_margin
    }

    pub(super) fn delayed_request(self) -> Duration {
        self.delayed_request
    }

    pub(super) fn after_delay(self) -> Duration {
        self.delayed_request + self.delayed_margin
    }
}

pub(super) async fn open<S>(scheduler: &S)
where
    S: Scheduler,
{
    scheduler.open().await.unwrap();
}

pub(super) async fn close<S>(scheduler: &S)
where
    S: Scheduler,
{
    let dir = scheduler.dir().map(PathBuf::from);
    scheduler.close().await.unwrap();
    if let Some(dir) = dir {
        match tokio::fs::remove_dir_all(dir).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("failed to remove Scheduler fixture directory: {error}"),
        }
    }
}

/// Executes two Scheduler operations from separate runtime threads after both are ready.
///
/// A direct `tokio::join!` is not a race for synchronous implementations such as Memory: one
/// future can finish before its peer ever starts. This helper makes the conformance assertions
/// exercise the actual shared-state contention boundary.
pub(super) async fn race<S, F, G, Left, Right, LeftFuture, RightFuture>(
    scheduler: Arc<S>,
    left: F,
    right: G,
) -> (Left, Right)
where
    S: Scheduler + 'static,
    F: FnOnce(Arc<S>) -> LeftFuture + Send + 'static,
    G: FnOnce(Arc<S>) -> RightFuture + Send + 'static,
    Left: Send + 'static,
    Right: Send + 'static,
    LeftFuture: Future<Output = Left> + Send + 'static,
    RightFuture: Future<Output = Right> + Send + 'static,
{
    let barrier = Arc::new(Barrier::new(3));
    let left_scheduler = scheduler.clone();
    let left_barrier = barrier.clone();
    let left = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create a Scheduler conformance runtime");
        left_barrier.wait();
        runtime.block_on(left(left_scheduler))
    });
    let right_barrier = barrier.clone();
    let right = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create a Scheduler conformance runtime");
        right_barrier.wait();
        runtime.block_on(right(scheduler))
    });

    tokio::task::spawn_blocking(move || {
        barrier.wait();
        (
            left.join().expect("left Scheduler operation panicked"),
            right.join().expect("right Scheduler operation panicked"),
        )
    })
    .await
    .expect("Scheduler operation join task panicked")
}
