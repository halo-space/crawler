use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use contrib::scheduler::mysql::MySql;
use spider::scheduler::Lease;

use super::{conformance, server};

const DATABASES: usize = 17;
const TIMING: conformance::Timing = conformance::Timing::new(
    Duration::from_secs(2),
    Duration::from_millis(250),
    Duration::from_millis(300),
    Duration::from_millis(300),
    Duration::from_millis(200),
);

#[tokio::test]
async fn all_operations_conform() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let mut databases = Vec::with_capacity(DATABASES);
    for index in 0..DATABASES {
        databases.push(server.database(&format!("contract-{index}")).await);
    }
    let urls = Arc::new(Mutex::new(
        databases
            .iter()
            .map(|database| database.url().to_string())
            .collect::<VecDeque<_>>(),
    ));
    let lease = Lease::new(TIMING.lease().0, TIMING.lease().1).unwrap();

    let result = tokio::spawn({
        let urls = urls.clone();
        async move {
            conformance::run(
                {
                    let urls = urls.clone();
                    move || isolated(&urls, lease)
                },
                false,
                TIMING,
            )
            .await;
            conformance::lease(isolated(&urls, lease), false, TIMING).await;
        }
    })
    .await;

    for database in databases {
        database.remove().await;
    }
    result.unwrap();
    assert!(urls.lock().unwrap().is_empty());
}

fn isolated(urls: &Arc<Mutex<VecDeque<String>>>, lease: Lease) -> MySql {
    let url = urls
        .lock()
        .unwrap()
        .pop_front()
        .expect("MySQL conformance fixture exhausted its isolated databases");
    MySql::new(url)
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
