use spider::{Scheduler, net, payload};

use super::{key, request, server, settlement, worker};

const EXCLUDED_READY: usize = 129;
const EVENT_PAGE: usize = 129;

#[tokio::test]
async fn worker_cursors_pass_excluded_pages_and_finish_when_none_are_eligible() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("worker-cursors");
    let scheduler = server.redis(&namespace);
    scheduler.open().await.unwrap();

    let mut requests = (0..EXCLUDED_READY)
        .map(|index| {
            let mut request = request::new(
                &format!("excluded-{index}"),
                &format!("https://example.com/excluded/{index}"),
            );
            request.priority = 10;
            request.max_retry_count = 2;
            request
        })
        .collect::<Vec<_>>();
    let delayed_id = "excluded-delayed";
    let mut delayed = request::new(delayed_id, "https://example.com/excluded/delayed");
    delayed.next_time = now_millis() + 60_000;
    delayed.max_retry_count = 2;
    requests.push(delayed);
    let eligible_id = "eligible-after-excluded-page";
    requests.push(request::new(
        eligible_id,
        "https://example.com/eligible-after-excluded-page",
    ));
    scheduler
        .push(payload::Payload::new().requests(requests))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    let mut pipe = redis::pipe();
    pipe.atomic();
    for index in 0..EXCLUDED_READY {
        exclude(&mut pipe, &namespace, &format!("excluded-{index}"), "http");
    }
    exclude(&mut pipe, &namespace, delayed_id, "http");
    pipe.query_async::<()>(&mut connection).await.unwrap();

    assert!(
        scheduler
            .next_requests(1, worker::A, worker::HTTP)
            .await
            .unwrap()
            .is_empty(),
        "the first bounded scan must stop after one excluded page"
    );
    let mut later = request::new("later-low-priority", "https://example.com/later");
    later.priority = -10;
    scheduler
        .push(payload::Payload::new().requests(vec![later]))
        .await
        .unwrap();
    let eligible = scheduler
        .next_requests(1, worker::A, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .expect("the claim cursor must reach the eligible Request");
    assert_eq!(eligible.id, eligible_id);
    settlement::succeed(&scheduler, &eligible).await;

    let later = scheduler
        .next_requests(1, worker::A, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .expect("a lower-priority enqueue must not reset completed cursor progress");
    assert_eq!(later.id, "later-low-priority");
    settlement::succeed(&scheduler, &later).await;

    assert!(
        !scheduler
            .has_pending_requests(worker::A, worker::HTTP)
            .await
            .unwrap(),
        "pending must compare the queue with failed-Worker exclusions exactly"
    );

    let available_to_b = scheduler
        .next_requests(1, worker::B, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .expect("another Worker must be able to claim an excluded Request");
    assert_eq!(available_to_b.failed_workers, [worker::A]);
    settlement::succeed(&scheduler, &available_to_b).await;

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn a_new_high_priority_request_invalidates_an_exclusion_cursor() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("cursor-priority");
    let scheduler = server.redis(&namespace);
    scheduler.open().await.unwrap();

    let requests = (0..EXCLUDED_READY)
        .map(|index| {
            let mut request = request::new(
                &format!("priority-excluded-{index}"),
                &format!("https://example.com/priority-excluded/{index}"),
            );
            request.priority = 10;
            request.max_retry_count = 2;
            request
        })
        .collect::<Vec<_>>();
    scheduler
        .push(payload::Payload::new().requests(requests))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    let mut pipe = redis::pipe();
    pipe.atomic();
    for index in 0..EXCLUDED_READY {
        exclude(
            &mut pipe,
            &namespace,
            &format!("priority-excluded-{index}"),
            "http",
        );
    }
    pipe.query_async::<()>(&mut connection).await.unwrap();

    assert!(
        scheduler
            .next_requests(1, worker::A, worker::HTTP)
            .await
            .unwrap()
            .is_empty()
    );

    let mut urgent = request::new("urgent", "https://example.com/urgent");
    urgent.priority = 100;
    scheduler
        .push(payload::Payload::new().requests(vec![urgent]))
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(1, worker::A, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .expect("a new higher-priority Request must reset the scan cursor");
    assert_eq!(claimed.id, "urgent");
    settlement::succeed(&scheduler, &claimed).await;

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn an_invalid_ready_event_resets_the_cursor_before_claiming() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("invalid-ready-event");
    let scheduler = server.redis(&namespace);
    scheduler.open().await.unwrap();

    let requests = excluded("invalid-event-excluded", EXCLUDED_READY, 10);
    scheduler
        .push(payload::Payload::new().requests(requests))
        .await
        .unwrap();
    exclude_all(
        &server,
        &namespace,
        "invalid-event-excluded",
        EXCLUDED_READY,
        "http",
    )
    .await;
    assert!(
        scheduler
            .next_requests(1, worker::A, worker::HTTP)
            .await
            .unwrap()
            .is_empty()
    );

    let mut urgent = request::new("invalid-event-urgent", "https://example.com/urgent");
    urgent.priority = 100;
    scheduler
        .push(payload::Payload::new().requests(vec![urgent]))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    let request_key = key::request(&namespace, "invalid-event-urgent");
    let event = redis::cmd("HGET")
        .arg(&request_key)
        .arg("ready_event")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    redis::pipe()
        .atomic()
        .cmd("ZREM")
        .arg(format!("{namespace}:ready_events:http"))
        .arg(&event)
        .ignore()
        .cmd("ZADD")
        .arg(format!("{namespace}:ready_events:http"))
        .arg(0)
        .arg("zz-invalid-ready-event")
        .ignore()
        .query_async::<()>(&mut connection)
        .await
        .unwrap();

    let claimed = scheduler
        .next_requests(1, worker::A, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .expect("an invalid ready event must conservatively reset the cursor");
    assert_eq!(claimed.id, "invalid-event-urgent");
    settlement::succeed(&scheduler, &claimed).await;
    let valid_event = redis::cmd("ZSCORE")
        .arg(format!("{namespace}:ready_events:http"))
        .arg(event)
        .query_async::<Option<f64>>(&mut connection)
        .await
        .unwrap();
    assert!(valid_event.is_none());
    let invalid_event = redis::cmd("ZSCORE")
        .arg(format!("{namespace}:ready_events:http"))
        .arg("zz-invalid-ready-event")
        .query_async::<Option<f64>>(&mut connection)
        .await
        .unwrap();
    assert!(invalid_event.is_none());
    let stored = redis::cmd("HGET")
        .arg(request_key)
        .arg("ready_event")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    assert!(stored.is_empty());

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn an_unresolved_mode_cannot_be_skipped_for_a_lower_priority_mode() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("cross-mode-priority");
    let scheduler = server.redis(&namespace);
    scheduler.open().await.unwrap();

    let mut requests = (0..65)
        .map(|index| {
            let mut request = request::new(
                &format!("http-excluded-{index}"),
                &format!("https://example.com/http-excluded/{index}"),
            );
            request.priority = 100;
            request.max_retry_count = 2;
            request
        })
        .collect::<Vec<_>>();
    let mut browser = request::new("browser", "https://example.com/browser");
    browser.mode = net::Mode::Browser;
    browser.priority = 1;
    requests.push(browser);
    scheduler
        .push(payload::Payload::new().requests(requests))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    let mut pipe = redis::pipe();
    pipe.atomic();
    for index in 0..65 {
        exclude(
            &mut pipe,
            &namespace,
            &format!("http-excluded-{index}"),
            "http",
        );
    }
    pipe.query_async::<()>(&mut connection).await.unwrap();

    const MODES: &[net::Mode] = &[net::Mode::Http, net::Mode::Browser];
    assert!(
        scheduler
            .next_requests(1, worker::A, MODES)
            .await
            .unwrap()
            .is_empty(),
        "Browser cannot win until the higher-priority HTTP prefix is resolved"
    );
    let claimed = scheduler
        .next_requests(1, worker::A, MODES)
        .await
        .unwrap()
        .pop()
        .expect("the Browser Request becomes eligible after HTTP is proven excluded");
    assert_eq!(claimed.id, "browser");
    settlement::succeed(&scheduler, &claimed).await;

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn low_priority_event_pages_do_not_starve_existing_work() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("low-priority-event-pages");
    let scheduler = server.redis(&namespace);
    scheduler.open().await.unwrap();

    let mut requests = excluded("event-excluded", EXCLUDED_READY, 10);
    requests.push(request::new(
        "event-eligible",
        "https://example.com/event-eligible",
    ));
    scheduler
        .push(payload::Payload::new().requests(requests))
        .await
        .unwrap();
    exclude_all(
        &server,
        &namespace,
        "event-excluded",
        EXCLUDED_READY,
        "http",
    )
    .await;
    assert!(
        scheduler
            .next_requests(1, worker::A, worker::HTTP)
            .await
            .unwrap()
            .is_empty()
    );

    let low = (0..EVENT_PAGE * 2)
        .map(|index| {
            let mut request = request::new(
                &format!("event-low-{index}"),
                &format!("https://example.com/event-low/{index}"),
            );
            request.priority = -10;
            request
        })
        .collect();
    scheduler
        .push(payload::Payload::new().requests(low))
        .await
        .unwrap();

    let claimed = scheduler
        .next_requests(1, worker::A, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .expect("one public claim must drain bounded low-priority event pages");
    assert_eq!(claimed.id, "event-eligible");
    settlement::succeed(&scheduler, &claimed).await;

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn browser_events_do_not_reset_an_http_cursor() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("cross-mode-events");
    let scheduler = server.redis(&namespace);
    scheduler.open().await.unwrap();

    let mut requests = excluded("cross-mode-excluded", EXCLUDED_READY, 10);
    requests.push(request::new(
        "cross-mode-eligible",
        "https://example.com/cross-mode-eligible",
    ));
    scheduler
        .push(payload::Payload::new().requests(requests))
        .await
        .unwrap();
    exclude_all(
        &server,
        &namespace,
        "cross-mode-excluded",
        EXCLUDED_READY,
        "http",
    )
    .await;
    assert!(
        scheduler
            .next_requests(1, worker::A, worker::HTTP)
            .await
            .unwrap()
            .is_empty()
    );

    let browser = (0..EVENT_PAGE)
        .map(|index| {
            let mut request = request::new(
                &format!("browser-event-{index}"),
                &format!("https://example.com/browser-event/{index}"),
            );
            request.mode = net::Mode::Browser;
            request.priority = 100;
            request
        })
        .collect();
    scheduler
        .push(payload::Payload::new().requests(browser))
        .await
        .unwrap();

    let claimed = scheduler
        .next_requests(1, worker::A, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .expect("Browser events must not reset HTTP scan progress");
    assert_eq!(claimed.id, "cross-mode-eligible");
    settlement::succeed(&scheduler, &claimed).await;

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn delayed_promotion_crossing_an_event_page_preserves_priority() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("promotion-event-page");
    let scheduler = server.redis(&namespace);
    scheduler.open().await.unwrap();

    let due = now_millis() + 300;
    let mut requests = excluded("promotion-excluded", EXCLUDED_READY, 10);
    requests.push(request::new(
        "promotion-eligible",
        "https://example.com/promotion-eligible",
    ));
    for index in 0..127 {
        let mut request = request::new(
            &format!("promotion-low-{index}"),
            &format!("https://example.com/promotion-low/{index}"),
        );
        request.priority = -10;
        request.next_time = due;
        requests.push(request);
    }
    let mut urgent = request::new("promotion-urgent", "https://example.com/promotion-urgent");
    urgent.priority = 100;
    urgent.next_time = due;
    requests.push(urgent);
    scheduler
        .push(payload::Payload::new().requests(requests))
        .await
        .unwrap();
    exclude_all(
        &server,
        &namespace,
        "promotion-excluded",
        EXCLUDED_READY,
        "http",
    )
    .await;
    assert!(
        scheduler
            .next_requests(1, worker::A, worker::HTTP)
            .await
            .unwrap()
            .is_empty()
    );

    let mut low = request::new("promotion-page-prefix", "https://example.com/page-prefix");
    low.priority = -10;
    scheduler
        .push(payload::Payload::new().requests(vec![low]))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;

    let claimed = scheduler
        .next_requests(1, worker::A, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .expect("one public claim must drain promotion event pages and invalidate the cursor");
    assert_eq!(claimed.id, "promotion-urgent");
    settlement::succeed(&scheduler, &claimed).await;

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

fn excluded(prefix: &str, count: usize, priority: i32) -> Vec<net::Request> {
    (0..count)
        .map(|index| {
            let mut request = request::new(
                &format!("{prefix}-{index}"),
                &format!("https://example.com/{prefix}/{index}"),
            );
            request.priority = priority;
            request.max_retry_count = 2;
            request
        })
        .collect()
}

async fn exclude_all(
    server: &server::Handle,
    namespace: &str,
    prefix: &str,
    count: usize,
    mode: &str,
) {
    let mut connection = server.connection().await;
    let mut pipe = redis::pipe();
    pipe.atomic();
    for index in 0..count {
        exclude(&mut pipe, namespace, &format!("{prefix}-{index}"), mode);
    }
    pipe.query_async::<()>(&mut connection).await.unwrap();
}

fn exclude(pipe: &mut redis::Pipeline, namespace: &str, id: &str, mode: &str) {
    let request = key::request(namespace, id);
    let token = key::token(id);
    let worker_token = worker::A
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    pipe.cmd("HSET")
        .arg(&request)
        .arg("retry_count")
        .arg(1)
        .ignore()
        .cmd("RPUSH")
        .arg(format!("{request}:failed_workers"))
        .arg(worker::A)
        .ignore()
        .cmd("ZADD")
        .arg(format!("{namespace}:pending_exclusions:{mode}"))
        .arg(0)
        .arg(format!("{worker_token}|{token}"))
        .ignore();
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
