#[path = "support/scheduler/conformance.rs"]
mod conformance;
#[path = "support/redis.rs"]
mod redis_fixture;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use contrib::scheduler::redis::Redis;
use spider::scheduler::Lease;

const TIMING: conformance::Timing = conformance::Timing::new(
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
            conformance::run(
                {
                    let url = url.clone();
                    let namespaces = namespaces.clone();
                    move || scheduler(&url, &namespaces, lease)
                },
                false,
                TIMING,
            )
            .await;
            conformance::lease(scheduler(&url, &namespaces, lease), TIMING).await;
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
