#[path = "support/redis.rs"]
mod redis_fixture;
mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use contrib::scheduler::redis::Redis;
use spider::scheduler::Lease;

const TIMING: support::scheduler::Timing = support::scheduler::Timing::new(
    Duration::from_secs(2),
    Duration::from_millis(250),
    Duration::from_millis(300),
    Duration::from_millis(300),
    Duration::from_millis(200),
);

#[tokio::test]
async fn conforms_to_the_scheduler_contract() {
    let Some(fixture) = redis_fixture::Fixture::connect().await else {
        return;
    };
    let url = fixture.url().to_string();
    let namespaces = Arc::new(Mutex::new(Vec::new()));
    let lease = Lease::new(TIMING.lease().0, TIMING.lease().1).unwrap();

    let result = tokio::spawn({
        let url = url.clone();
        let namespaces = namespaces.clone();
        async move {
            support::scheduler::run(
                {
                    let url = url.clone();
                    let namespaces = namespaces.clone();
                    move || scheduler(&url, &namespaces, lease)
                },
                false,
                TIMING,
            )
            .await;
            support::scheduler::lease(scheduler(&url, &namespaces, lease), TIMING).await;
        }
    })
    .await;

    let namespaces = std::mem::take(&mut *namespaces.lock().unwrap());
    for namespace in namespaces {
        fixture.clear(&namespace).await;
    }
    result.unwrap();
}

fn scheduler(url: &str, namespaces: &Arc<Mutex<Vec<String>>>, lease: Lease) -> Redis {
    let namespace = format!("crawler-test-redis-{}", uuid::Uuid::now_v7().simple());
    namespaces.lock().unwrap().push(namespace.clone());
    Redis::new(url)
        .unwrap()
        .with_namespace(namespace)
        .unwrap()
        .with_lease(lease)
}
