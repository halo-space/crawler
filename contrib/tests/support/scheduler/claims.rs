use std::sync::Arc;

use spider::{Scheduler, net, payload};

use super::{
    fixture::{ALL, BROWSER, HTTP, Timing, WORKER_A, WORKER_B, close, open_run, race},
    payload::request,
    settlement::succeed,
};

pub(super) async fn claims_are_capability_scoped<S>(scheduler: S)
where
    S: Scheduler + spider::scheduler::Init,
{
    open_run(&scheduler).await;
    let mut low = request("http-low", "https://example.com/http/low");
    low.priority = 1;
    let mut first = request("http-first", "https://example.com/http/first");
    first.priority = 10;
    let mut second = request("http-second", "https://example.com/http/second");
    second.priority = 10;
    let mut browser = request("browser", "https://example.com/browser").mode(net::Mode::Browser);
    browser.priority = 100;
    scheduler
        .push(payload::Payload::new().requests(vec![low, first, second, browser]))
        .await
        .unwrap();

    assert!(
        scheduler
            .next_requests(0, WORKER_A, HTTP)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(scheduler.next_requests(1, " ", HTTP).await.is_err());
    assert!(scheduler.next_requests(1, WORKER_A, &[]).await.is_err());
    assert!(scheduler.has_pending_requests(" ", HTTP).await.is_err());
    assert!(scheduler.has_pending_requests(WORKER_A, &[]).await.is_err());

    let http = scheduler.next_requests(2, WORKER_A, HTTP).await.unwrap();
    assert_eq!(http.len(), 2);
    assert_eq!(http[0].id, "http-first");
    assert_eq!(http[1].id, "http-second");
    assert!(http.iter().all(|request| {
        request.mode == net::Mode::Http
            && request.state == net::State::Processing
            && request.leased_by == WORKER_A
            && request.version == 1
            && request.lease_time > 0
    }));
    let remaining = scheduler.next_requests(2, WORKER_A, HTTP).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, "http-low");
    let browser = scheduler.next_requests(1, WORKER_B, BROWSER).await.unwrap();
    assert_eq!(browser.len(), 1);
    assert_eq!(browser[0].id, "browser");

    for request in http.iter().chain(&remaining).chain(&browser) {
        succeed(&scheduler, request).await;
    }

    let mut http = request("all-http", "https://example.com/all/http");
    http.priority = 10;
    let mut browser =
        request("all-browser", "https://example.com/all/browser").mode(net::Mode::Browser);
    browser.priority = 20;
    let mut low = request("all-low", "https://example.com/all/low");
    low.priority = 1;
    scheduler
        .push(payload::Payload::new().requests(vec![http, browser, low]))
        .await
        .unwrap();
    let all = scheduler.next_requests(3, WORKER_A, ALL).await.unwrap();
    assert_eq!(
        all.iter()
            .map(|request| request.id.as_str())
            .collect::<Vec<_>>(),
        ["all-browser", "all-http", "all-low"]
    );
    for request in &all {
        succeed(&scheduler, request).await;
    }

    let pending = request("pending-global", "https://example.com/pending");
    scheduler
        .push(payload::Payload::new().requests(vec![pending]))
        .await
        .unwrap();
    let processing = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(
        scheduler
            .has_pending_requests(WORKER_B, HTTP)
            .await
            .unwrap()
    );
    assert!(
        !scheduler
            .has_pending_requests(WORKER_B, BROWSER)
            .await
            .unwrap()
    );
    succeed(&scheduler, &processing).await;
    assert!(
        !scheduler
            .has_pending_requests(WORKER_B, HTTP)
            .await
            .unwrap()
    );
    close(&scheduler).await;
}

pub(super) async fn concurrent_capability_claims_are_atomic<S>(scheduler: S)
where
    S: Scheduler + spider::scheduler::Init + 'static,
{
    open_run(&scheduler).await;
    let mut requests = Vec::with_capacity(32);
    for index in 0..32 {
        requests.push(request(
            &format!("concurrent-{index}"),
            &format!("https://example.com/concurrent/http/{index}"),
        ));
    }
    scheduler
        .push(payload::Payload::new().requests(requests))
        .await
        .unwrap();

    let scheduler = Arc::new(scheduler);
    let (claimed_by_a, claimed_by_b) = race(
        scheduler.clone(),
        |scheduler| async move { scheduler.next_requests(16, WORKER_A, HTTP).await },
        |scheduler| async move { scheduler.next_requests(16, WORKER_B, HTTP).await },
    )
    .await;
    let mut claimed_by_a = claimed_by_a.unwrap();
    let claimed_by_b = claimed_by_b.unwrap();
    assert!(claimed_by_a.len() <= 16);
    assert!(claimed_by_b.len() <= 16);
    assert!(
        claimed_by_a
            .iter()
            .all(|request| { request.mode == net::Mode::Http && request.leased_by == WORKER_A })
    );
    assert!(
        claimed_by_b
            .iter()
            .all(|request| { request.mode == net::Mode::Http && request.leased_by == WORKER_B })
    );
    let mut ids = claimed_by_a
        .iter()
        .chain(&claimed_by_b)
        .map(|request| request.id.clone())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(ids.len(), claimed_by_a.len() + claimed_by_b.len());
    while ids.len() < 32 {
        let next = scheduler.next_requests(16, WORKER_A, HTTP).await.unwrap();
        assert!(!next.is_empty(), "Scheduler lost unclaimed Requests");
        assert!(next.len() <= 16);
        assert!(
            next.iter().all(|request| {
                request.mode == net::Mode::Http && request.leased_by == WORKER_A
            })
        );
        for request in &next {
            assert!(
                ids.insert(request.id.clone()),
                "Request was claimed twice: {}",
                request.id
            );
        }
        claimed_by_a.extend(next);
    }
    assert_eq!(ids.len(), 32);
    for request in claimed_by_a.iter().chain(&claimed_by_b) {
        succeed(scheduler.as_ref(), request).await;
    }
    close(scheduler.as_ref()).await;
}

pub(super) async fn delayed_requests_wait_for_next_time<S>(scheduler: S, timing: Timing)
where
    S: Scheduler + spider::scheduler::Init,
{
    open_run(&scheduler).await;
    let mut delayed = request("delayed", "https://example.com/delayed");
    delayed.next_time = now_millis() + timing.delayed_request().as_millis() as i64;
    scheduler
        .push(payload::Payload::new().requests(vec![delayed]))
        .await
        .unwrap();
    assert!(
        scheduler
            .next_requests(1, WORKER_A, HTTP)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        scheduler
            .has_pending_requests(WORKER_A, HTTP)
            .await
            .unwrap()
    );

    tokio::time::sleep(timing.after_delay()).await;
    let claimed = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    succeed(&scheduler, &claimed).await;
    close(&scheduler).await;
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
