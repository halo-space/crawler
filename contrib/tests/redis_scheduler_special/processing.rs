use std::time::Duration;

use spider::{Scheduler, net, payload};

use super::common::{
    HTTP, WORKER_A, WORKER_B, namespace, processing_key, processing_payload, request, request_key,
    scheduler, succeed, success_payload, token,
};
use crate::redis_fixture::Fixture;

const BROWSER: &[net::Mode] = &[net::Mode::Browser];
const BOTH: &[net::Mode] = &[net::Mode::Http, net::Mode::Browser];

#[tokio::test]
async fn push_rejects_a_wrong_type_processing_index_before_writing() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("processing-push-type");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    let browser = processing_key(&namespace, "browser");
    let mut connection = fixture.connection().await;
    redis::cmd("SET")
        .arg(&browser)
        .arg("wrong-type")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();

    assert!(
        scheduler
            .push(
                payload::Payload::new()
                    .requests(vec![request("push-type", "https://example.com/push-type",)])
            )
            .await
            .is_err()
    );
    let exists = redis::cmd("EXISTS")
        .arg(request_key(&namespace, "push-type"))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert_eq!(exists, 0);

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn processing_indices_are_mode_scoped_and_removed_on_settlement() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("processing-modes");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    let http = request("processing-http", "https://example.com/http");
    let mut browser = request("processing-browser", "https://example.com/browser");
    browser.mode = net::Mode::Browser;
    browser.max_retry_count = 1;
    scheduler
        .push(payload::Payload::new().requests(vec![http, browser]))
        .await
        .unwrap();

    let http = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let browser = scheduler
        .next_requests(1, WORKER_B, BROWSER)
        .await
        .unwrap()
        .pop()
        .unwrap();

    let http_index = processing_key(&namespace, "http");
    let browser_index = processing_key(&namespace, "browser");
    let mut connection = fixture.connection().await;
    assert!(
        score(&mut connection, &http_index, &http.id)
            .await
            .is_some()
    );
    assert!(
        score(&mut connection, &browser_index, &browser.id)
            .await
            .is_some()
    );
    assert!(
        score(&mut connection, &browser_index, &http.id)
            .await
            .is_none()
    );
    assert!(
        score(&mut connection, &http_index, &browser.id)
            .await
            .is_none()
    );
    assert!(
        scheduler
            .has_pending_requests(WORKER_A, HTTP)
            .await
            .unwrap()
    );
    assert!(
        scheduler
            .has_pending_requests(WORKER_B, BROWSER)
            .await
            .unwrap()
    );

    succeed(&scheduler, &http).await;
    assert!(
        score(&mut connection, &http_index, &http.id)
            .await
            .is_none()
    );
    assert!(
        !scheduler
            .has_pending_requests(WORKER_A, HTTP)
            .await
            .unwrap()
    );
    assert!(
        scheduler
            .has_pending_requests(WORKER_B, BROWSER)
            .await
            .unwrap()
    );

    scheduler.ack(&processing_payload(&browser)).await.unwrap();
    let mut failure =
        payload::Payload::for_request(&browser, browser.leased_by.clone()).failed("failed");
    failure.start_time = Some(1);
    failure.end_time = Some(2);
    scheduler.failure(&failure).await.unwrap();
    assert!(
        score(&mut connection, &browser_index, &browser.id)
            .await
            .is_none()
    );
    assert!(
        !scheduler
            .has_pending_requests(WORKER_B, BROWSER)
            .await
            .unwrap()
    );

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn refresh_restores_the_index_and_updates_its_lease_score() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("processing-refresh");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    scheduler
        .push(payload::Payload::new().requests(vec![request(
            "refresh-index",
            "https://example.com/refresh-index",
        )]))
        .await
        .unwrap();
    let request = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let active = processing_payload(&request);
    scheduler.ack(&active).await.unwrap();

    let index = processing_key(&namespace, "http");
    let other_index = processing_key(&namespace, "browser");
    let key = request_key(&namespace, &request.id);
    let mut connection = fixture.connection().await;
    let initial = score(&mut connection, &index, &request.id).await.unwrap();
    let stored = redis::cmd("HGET")
        .arg(&key)
        .arg("lease_time")
        .query_async::<i64>(&mut connection)
        .await
        .unwrap();
    assert_eq!(initial, stored);

    redis::cmd("ZREM")
        .arg(&index)
        .arg(token(&request.id))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("ZADD")
        .arg(&other_index)
        .arg(initial)
        .arg(token(&request.id))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert!(
        !scheduler
            .has_pending_requests(WORKER_B, HTTP)
            .await
            .unwrap()
    );
    tokio::time::sleep(Duration::from_millis(2)).await;
    scheduler.refresh_lease(&active).await.unwrap();

    let refreshed = score(&mut connection, &index, &request.id).await.unwrap();
    let stored = redis::cmd("HGET")
        .arg(&key)
        .arg("lease_time")
        .query_async::<i64>(&mut connection)
        .await
        .unwrap();
    assert_eq!(refreshed, stored);
    assert!(refreshed > initial);
    assert!(
        score(&mut connection, &other_index, &request.id)
            .await
            .is_none()
    );
    assert!(
        scheduler
            .has_pending_requests(WORKER_B, HTTP)
            .await
            .unwrap()
    );

    redis::cmd("ZADD")
        .arg(&other_index)
        .arg(refreshed)
        .arg(token(&request.id))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    scheduler.success(&success_payload(&request)).await.unwrap();
    assert!(score(&mut connection, &index, &request.id).await.is_none());
    assert!(
        score(&mut connection, &other_index, &request.id)
            .await
            .is_none()
    );
    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn expired_processing_score_recovers_the_request() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("processing-expiry");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    let mut original = request(
        "expired-processing",
        "https://example.com/expired-processing",
    );
    original.max_retry_count = 3;
    scheduler
        .push(payload::Payload::new().requests(vec![original]))
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    scheduler.ack(&processing_payload(&claimed)).await.unwrap();

    let index = processing_key(&namespace, "http");
    let mut connection = fixture.connection().await;
    redis::cmd("HSET")
        .arg(request_key(&namespace, &claimed.id))
        .arg("lease_time")
        .arg(0)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("ZADD")
        .arg(&index)
        .arg(0)
        .arg(token(&claimed.id))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let recovered = scheduler
        .next_requests(1, WORKER_B, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(recovered.id, claimed.id);
    assert_eq!(recovered.version, claimed.version + 1);
    assert_eq!(recovered.retry_count, claimed.retry_count + 1);
    assert_eq!(recovered.failed_workers, [WORKER_A]);
    let stored = redis::cmd("HGET")
        .arg(request_key(&namespace, &recovered.id))
        .arg("lease_time")
        .query_async::<i64>(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        score(&mut connection, &index, &recovered.id).await,
        Some(stored)
    );

    succeed(&scheduler, &recovered).await;
    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn claim_removes_orphans_and_repairs_a_request_in_the_wrong_mode_index() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("processing-repair");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    let http_index = processing_key(&namespace, "http");
    let browser_index = processing_key(&namespace, "browser");
    let mut connection = fixture.connection().await;
    redis::cmd("ZADD")
        .arg(&http_index)
        .arg(4_000_000_000_000_i64)
        .arg(token("missing-request"))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert!(
        scheduler
            .has_pending_requests(WORKER_A, HTTP)
            .await
            .unwrap()
    );
    assert!(
        scheduler
            .next_requests(1, WORKER_A, HTTP)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        score(&mut connection, &http_index, "missing-request")
            .await
            .is_none()
    );
    assert!(
        !scheduler
            .has_pending_requests(WORKER_A, HTTP)
            .await
            .unwrap()
    );

    scheduler
        .push(payload::Payload::new().requests(vec![request(
            "wrong-mode-index",
            "https://example.com/wrong-mode-index",
        )]))
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let lease_time = redis::cmd("HGET")
        .arg(request_key(&namespace, &claimed.id))
        .arg("lease_time")
        .query_async::<i64>(&mut connection)
        .await
        .unwrap();
    redis::cmd("ZADD")
        .arg(&http_index)
        .arg(lease_time + 2)
        .arg(token(&claimed.id))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("ZADD")
        .arg(&browser_index)
        .arg(lease_time + 1)
        .arg(token(&claimed.id))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    assert!(
        scheduler
            .next_requests(1, WORKER_B, HTTP)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        score(&mut connection, &http_index, &claimed.id).await,
        Some(lease_time)
    );
    assert!(
        score(&mut connection, &browser_index, &claimed.id)
            .await
            .is_none()
    );
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

    succeed(&scheduler, &claimed).await;
    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn expired_recovery_limit_is_shared_across_modes() {
    const PER_MODE: usize = 129;

    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("processing-recovery-limit");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    let mut requests = Vec::with_capacity(PER_MODE * 2);
    for index in 0..PER_MODE {
        requests.push(request(
            &format!("recovery-http-{index}"),
            "https://example.com/recovery/http",
        ));
        let mut browser = request(
            &format!("recovery-browser-{index}"),
            "https://example.com/recovery/browser",
        );
        browser.mode = net::Mode::Browser;
        requests.push(browser);
    }
    scheduler
        .push(payload::Payload::new().requests(requests))
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(PER_MODE * 2, WORKER_A, BOTH)
        .await
        .unwrap();
    assert_eq!(claimed.len(), PER_MODE * 2);

    let mut connection = fixture.connection().await;
    for request in &claimed {
        redis::cmd("HSET")
            .arg(request_key(&namespace, &request.id))
            .arg("lease_time")
            .arg(0)
            .query_async::<usize>(&mut connection)
            .await
            .unwrap();
        redis::cmd("ZADD")
            .arg(processing_key(
                &namespace,
                match request.mode {
                    net::Mode::Http => "http",
                    net::Mode::Browser => "browser",
                },
            ))
            .arg(0)
            .arg(token(&request.id))
            .query_async::<usize>(&mut connection)
            .await
            .unwrap();
    }

    let recovered = scheduler.next_requests(1, WORKER_B, BOTH).await.unwrap();
    assert_eq!(recovered.len(), 1);
    let mut counts = Vec::new();
    for mode in ["http", "browser"] {
        counts.push(
            redis::cmd("ZCARD")
                .arg(processing_key(&namespace, mode))
                .query_async::<usize>(&mut connection)
                .await
                .unwrap(),
        );
    }
    counts.sort_unstable();
    assert_eq!(counts, [65, 66]);

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn opposite_mode_type_errors_do_not_partially_refresh_or_settle() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("processing-opposite-type");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    scheduler
        .push(payload::Payload::new().requests(vec![request(
            "opposite-type",
            "https://example.com/opposite-type",
        )]))
        .await
        .unwrap();
    let request = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let active = processing_payload(&request);
    scheduler.ack(&active).await.unwrap();

    let key = request_key(&namespace, &request.id);
    let http = processing_key(&namespace, "http");
    let browser = processing_key(&namespace, "browser");
    let mut connection = fixture.connection().await;
    let lease_time = redis::cmd("HGET")
        .arg(&key)
        .arg("lease_time")
        .query_async::<i64>(&mut connection)
        .await
        .unwrap();
    let lease_score = score(&mut connection, &http, &request.id).await;
    redis::cmd("SET")
        .arg(&browser)
        .arg("wrong-type")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(2)).await;
    assert!(scheduler.refresh_lease(&active).await.is_err());
    assert_eq!(
        redis::cmd("HGET")
            .arg(&key)
            .arg("lease_time")
            .query_async::<i64>(&mut connection)
            .await
            .unwrap(),
        lease_time
    );
    assert_eq!(
        score(&mut connection, &http, &request.id).await,
        lease_score
    );

    assert!(scheduler.success(&success_payload(&request)).await.is_err());
    assert_eq!(
        redis::cmd("HGET")
            .arg(&key)
            .arg("state")
            .query_async::<String>(&mut connection)
            .await
            .unwrap(),
        "processing"
    );
    assert_eq!(
        score(&mut connection, &http, &request.id).await,
        lease_score
    );

    redis::cmd("DEL")
        .arg(&browser)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    scheduler.success(&success_payload(&request)).await.unwrap();

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

async fn score(
    connection: &mut redis::aio::MultiplexedConnection,
    key: &str,
    request_id: &str,
) -> Option<i64> {
    redis::cmd("ZSCORE")
        .arg(key)
        .arg(token(request_id))
        .query_async::<Option<f64>>(connection)
        .await
        .unwrap()
        .map(|score| score as i64)
}
