use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::watch;

use super::cron::{Cron, Store};
use super::drain_cron;
use crate::Error;

#[derive(Clone, Default)]
struct Fixture {
    recovered: Arc<AtomicUsize>,
    dispatched: Arc<AtomicUsize>,
    cleaned: Arc<AtomicUsize>,
}

impl Store for Fixture {
    async fn recover(&self, _namespace: &str, _now: i64) -> Result<(), Error> {
        self.recovered.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn dispatch(&self, _namespace: &str, _now: i64, _limit: usize) -> Result<(), Error> {
        self.dispatched.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn cleanup(
        &self,
        _namespace: &str,
        _now: i64,
        _ttl: Duration,
        _limit: usize,
    ) -> Result<(), Error> {
        self.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn cron_runs_recovery_dispatch_and_cleanup_then_stops() {
    let fixture = Fixture::default();
    let observed = fixture.clone();
    let (stop, stopped) = watch::channel(false);
    let cron = Cron::new(
        fixture,
        "crawler".to_string(),
        Duration::from_millis(1),
        4,
        Duration::from_secs(1),
        4,
    );
    let task = tokio::spawn(cron.run(stopped));

    tokio::time::timeout(Duration::from_secs(1), async {
        while observed.recovered.load(Ordering::SeqCst) == 0
            || observed.dispatched.load(Ordering::SeqCst) == 0
            || observed.cleaned.load(Ordering::SeqCst) == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn drain_waits_for_a_stopped_cron() {
    let task = tokio::spawn(async {});

    drain_cron(task, Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn drain_aborts_a_cron_that_does_not_stop() {
    struct Dropped(Arc<AtomicUsize>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let dropped = Arc::new(AtomicUsize::new(0));
    let guard = Dropped(Arc::clone(&dropped));
    let task = tokio::spawn(async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    });

    drain_cron(task, Duration::from_millis(1)).await.unwrap();

    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}
