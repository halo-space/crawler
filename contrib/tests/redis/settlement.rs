use contrib::scheduler::redis::Redis;
use spider::{Scheduler, net, payload};

use super::{request, server, worker};

#[tokio::test]
async fn settlement_rejects_a_scheduler_that_does_not_own_the_lease() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("settlement-lease-owner");
    let owner = server.redis(&namespace);
    let other = server.redis_as(&namespace, worker::B);
    server::open(&owner).await;
    server::open(&other).await;
    super::run::init(&owner).await;

    let mut failed = request::new(
        "lease-owner-failure",
        "https://example.com/lease-owner-failure",
    );
    failed.max_retry_count = 1;
    owner
        .push(payload::Payload::new().requests(vec![
            request::new("lease-owner-ack", "https://example.com/lease-owner-ack"),
            request::new(
                "lease-owner-release",
                "https://example.com/lease-owner-release",
            ),
            request::new(
                "lease-owner-refresh",
                "https://example.com/lease-owner-refresh",
            ),
            request::new(
                "lease-owner-success",
                "https://example.com/lease-owner-success",
            ),
            failed,
        ]))
        .await
        .unwrap();
    let mut claimed = owner.next_requests(5).await.unwrap();
    assert_eq!(claimed.len(), 5);

    let acked = take(&mut claimed, "lease-owner-ack");
    let ack = processing(&acked);
    assert_lease_mismatch(other.ack(&ack).await.unwrap_err(), &acked.id);
    let mut owner_ack = processing(&acked);
    owner_ack.worker_id = worker::B.to_string();
    owner.ack(&owner_ack).await.unwrap();
    owner.success(&success(&acked)).await.unwrap();

    let released = take(&mut claimed, "lease-owner-release");
    let release = processing(&released);
    assert_lease_mismatch(other.release(&release).await.unwrap_err(), &released.id);
    let mut owner_release = processing(&released);
    owner_release.worker_id = worker::B.to_string();
    owner.release(&owner_release).await.unwrap();

    let refreshed = take(&mut claimed, "lease-owner-refresh");
    owner.ack(&processing(&refreshed)).await.unwrap();
    let refresh = processing(&refreshed);
    assert_lease_mismatch(
        other.refresh_lease(&refresh).await.unwrap_err(),
        &refreshed.id,
    );
    let mut owner_refresh = processing(&refreshed);
    owner_refresh.worker_id = worker::B.to_string();
    owner.refresh_lease(&owner_refresh).await.unwrap();
    owner.success(&success(&refreshed)).await.unwrap();

    let succeeded = take(&mut claimed, "lease-owner-success");
    owner.ack(&processing(&succeeded)).await.unwrap();
    let succeeded_payload = success(&succeeded);
    assert_lease_mismatch(
        other.success(&succeeded_payload).await.unwrap_err(),
        &succeeded.id,
    );
    let mut owner_success = success(&succeeded);
    owner_success.worker_id = worker::B.to_string();
    owner.success(&owner_success).await.unwrap();
    owner.success(&owner_success).await.unwrap();
    assert_lease_mismatch(
        other.success(&owner_success).await.unwrap_err(),
        &succeeded.id,
    );

    let failed = take(&mut claimed, "lease-owner-failure");
    owner.ack(&processing(&failed)).await.unwrap();
    let other_failure = failure(&failed, "boom");
    assert_lease_mismatch(other.failure(&other_failure).await.unwrap_err(), &failed.id);
    let mut owner_failure = failure(&failed, "boom");
    owner_failure.worker_id = worker::B.to_string();
    owner.failure(&owner_failure).await.unwrap();
    owner.failure(&owner_failure).await.unwrap();
    assert_lease_mismatch(other.failure(&owner_failure).await.unwrap_err(), &failed.id);

    assert!(claimed.is_empty());
    owner.close().await.unwrap();
    other.close().await.unwrap();
    server.clear(&namespace).await;
}

pub(super) fn processing(request: &net::Request) -> payload::Payload {
    let mut payload = payload::Payload::for_request(request, request.leased_by.clone());
    payload.state = net::State::Processing;
    payload
}

pub(super) fn success(request: &net::Request) -> payload::Payload {
    let mut payload = payload::Payload::for_request(request, request.leased_by.clone());
    payload.start_time = Some(1);
    payload.end_time = Some(2);
    payload
}

fn failure(request: &net::Request, error: &str) -> payload::Payload {
    let mut payload =
        payload::Payload::for_request(request, request.leased_by.clone()).failed(error);
    payload.start_time = Some(1);
    payload.end_time = Some(2);
    payload
}

pub(super) async fn succeed(scheduler: &Redis, request: &net::Request) {
    scheduler.ack(&processing(request)).await.unwrap();
    scheduler.success(&success(request)).await.unwrap();
}

fn take(requests: &mut Vec<net::Request>, id: &str) -> net::Request {
    let index = requests
        .iter()
        .position(|request| request.id == id)
        .unwrap();
    requests.swap_remove(index)
}

fn assert_lease_mismatch(error: spider::scheduler::Error, id: &str) {
    assert!(matches!(
        error,
        spider::scheduler::Error::LeaseMismatch(request_id) if request_id == id
    ));
}
