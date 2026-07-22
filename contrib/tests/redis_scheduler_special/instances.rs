use std::collections::HashSet;

use spider::{Scheduler, payload};

use super::common::{HTTP, WORKER_A, WORKER_B, namespace, request, scheduler, succeed};
use crate::redis_fixture::Fixture;

#[tokio::test]
async fn separate_instances_coordinate_replay_and_concurrent_claims() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("multi-instance");
    let left = scheduler(&fixture, &namespace);
    let right = scheduler(&fixture, &namespace);
    left.open().await.unwrap();
    right.open().await.unwrap();

    let replay = request("replay", "https://example.com/replay");
    let (left_push, right_push) = tokio::join!(
        left.push(payload::Payload::new().requests(vec![replay.clone()])),
        right.push(payload::Payload::new().requests(vec![replay]))
    );
    left_push.unwrap();
    right_push.unwrap();
    let (left_claim, right_claim) = tokio::join!(
        left.next_requests(1, WORKER_A, HTTP),
        right.next_requests(1, WORKER_B, HTTP)
    );
    let replayed = left_claim
        .unwrap()
        .into_iter()
        .chain(right_claim.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(replayed.len(), 1);
    succeed(&left, &replayed[0]).await;

    let requests = (0..32)
        .map(|index| {
            request(
                &format!("multi-{index}"),
                &format!("https://example.com/multi/{index}"),
            )
        })
        .collect();
    left.push(payload::Payload::new().requests(requests))
        .await
        .unwrap();
    let (left_claim, right_claim) = tokio::join!(
        left.next_requests(16, WORKER_A, HTTP),
        right.next_requests(16, WORKER_B, HTTP)
    );
    let left_claim = left_claim.unwrap();
    let right_claim = right_claim.unwrap();
    assert_eq!(left_claim.len(), 16);
    assert_eq!(right_claim.len(), 16);
    let ids = left_claim
        .iter()
        .chain(&right_claim)
        .map(|request| request.id.as_str())
        .collect::<HashSet<_>>();
    assert_eq!(ids.len(), 32);
    for request in left_claim.iter().chain(&right_claim) {
        succeed(&left, request).await;
    }

    left.close().await.unwrap();
    right.close().await.unwrap();
    fixture.clear(&namespace).await;
}
