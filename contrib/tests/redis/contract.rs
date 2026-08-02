use std::sync::{Arc, Mutex};
use std::time::Duration;

use contrib::scheduler::redis::Redis;
use spider::scheduler::Lease;

use super::{conformance, server::Handle};

const TIMING: conformance::Timing = conformance::Timing::new(
    Duration::from_secs(2),
    Duration::from_millis(250),
    Duration::from_millis(300),
    Duration::from_millis(300),
    Duration::from_millis(200),
);

#[tokio::test]
async fn all_operations_conform() {
    let Some(server) = Handle::connect().await else {
        return;
    };
    let url = server.url().to_string();
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
                    move || isolated(&url, &namespaces, lease)
                },
                false,
                TIMING,
            )
            .await;
            conformance::lease(isolated(&url, &namespaces, lease), false, TIMING).await;
        }
    })
    .await;

    let namespaces = std::mem::take(&mut *namespaces.lock().unwrap());
    for namespace in namespaces {
        server.clear(&namespace).await;
    }
    result.unwrap();
}

fn isolated(url: &str, namespaces: &Arc<Mutex<Vec<String>>>, lease: Lease) -> Redis {
    let namespace = format!("crawler-test-redis-{}", uuid::Uuid::now_v7().simple());
    namespaces.lock().unwrap().push(namespace.clone());
    Redis::new(url)
        .unwrap()
        .with_namespace(namespace)
        .unwrap()
        .with_worker_id("worker-a")
        .unwrap()
        .with_worker_host("crawler-test-host")
        .unwrap()
        .with_worker_version("test")
        .unwrap()
        .with_modes([spider::net::Mode::Http, spider::net::Mode::Browser])
        .unwrap()
        .with_lease(lease)
        .unwrap()
}
