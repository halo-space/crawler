use std::collections::HashSet;

use spider::{Scheduler, payload};

use super::{request, server, settlement, worker};

#[tokio::test]
async fn separate_instances_coordinate_replay_and_concurrent_claims() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("multi-instance");
    let left = server.redis(&namespace);
    let right = server.redis_as(&namespace, worker::B);
    server::open(&left).await;
    super::run::init(&left).await;
    server::open(&right).await;
    super::run::init(&right).await;

    let replay = request::new("replay", "https://example.com/replay");
    let (left_push, right_push) = tokio::join!(
        left.push(payload::Payload::new().requests(vec![replay.clone()])),
        right.push(payload::Payload::new().requests(vec![replay]))
    );
    left_push.unwrap();
    right_push.unwrap();
    let (left_claim, right_claim) = tokio::join!(left.next_requests(1), right.next_requests(1));
    let left_claim = left_claim.unwrap();
    let right_claim = right_claim.unwrap();
    let replayed = left_claim.iter().chain(&right_claim).collect::<Vec<_>>();
    assert_eq!(replayed.len(), 1);
    if let Some(request) = left_claim.first() {
        settlement::succeed(&left, request).await;
    } else {
        settlement::succeed(&right, &right_claim[0]).await;
    }

    let requests = (0..32)
        .map(|index| {
            request::new(
                &format!("multi-{index}"),
                &format!("https://example.com/multi/{index}"),
            )
        })
        .collect();
    left.push(payload::Payload::new().requests(requests))
        .await
        .unwrap();
    let (left_claim, right_claim) = tokio::join!(left.next_requests(16), right.next_requests(16));
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
    for request in &left_claim {
        settlement::succeed(&left, request).await;
    }
    for request in &right_claim {
        settlement::succeed(&right, request).await;
    }

    left.close().await.unwrap();
    right.close().await.unwrap();
    server.clear(&namespace).await;
}
