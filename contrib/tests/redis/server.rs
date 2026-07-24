use std::time::Duration;

use contrib::scheduler::redis::Redis;

const DEFAULT_URL: &str = "redis://127.0.0.1:6379";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

pub(super) struct Handle {
    url: String,
}

impl Handle {
    pub(super) async fn connect() -> Option<Self> {
        let url = std::env::var("CRAWLER_REDIS_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
        let client = redis::Client::open(url.as_str())
            .unwrap_or_else(|error| panic!("invalid CRAWLER_REDIS_URL {url:?}: {error}"));
        let mut connection = match tokio::time::timeout(
            CONNECT_TIMEOUT,
            client.get_multiplexed_async_connection(),
        )
        .await
        {
            Ok(Ok(connection)) => connection,
            Ok(Err(error)) if error.kind() == redis::ErrorKind::Io => {
                return unavailable(&url, error);
            }
            Ok(Err(error)) => panic!("failed to connect to configured Redis at {url}: {error}"),
            Err(_) => {
                eprintln!(
                    "skipping Redis Scheduler integration test: Redis at {url} did not connect within {CONNECT_TIMEOUT:?}"
                );
                return None;
            }
        };
        let pong = tokio::time::timeout(
            CONNECT_TIMEOUT,
            redis::cmd("PING").query_async::<String>(&mut connection),
        )
        .await;
        match pong {
            Ok(Ok(pong)) if pong == "PONG" => Some(Self { url }),
            Ok(Ok(reply)) => panic!("Redis at {url} returned unexpected PING reply {reply:?}"),
            Ok(Err(error)) if error.kind() == redis::ErrorKind::Io => unavailable(&url, error),
            Ok(Err(error)) => panic!("Redis at {url} rejected PING: {error}"),
            Err(_) => unavailable_timeout(&url, "answer PING"),
        }
    }

    pub(super) fn url(&self) -> &str {
        &self.url
    }

    pub(super) async fn connection(&self) -> redis::aio::MultiplexedConnection {
        redis::Client::open(self.url.as_str())
            .expect("a previously reachable Redis URL must remain valid")
            .get_multiplexed_async_connection()
            .await
            .expect("failed to reconnect to Redis for integration-test inspection")
    }

    pub(super) async fn clear(&self, namespace: &str) {
        let mut connection = self.connection().await;
        let pattern = format!("{namespace}:*");
        let mut cursor = 0_u64;
        let mut all_keys = Vec::new();
        loop {
            let (next, keys) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query_async::<(u64, Vec<String>)>(&mut connection)
                .await
                .expect("failed to scan Redis integration-test keys");
            all_keys.extend(keys);
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        for keys in all_keys.chunks(100) {
            redis::cmd("DEL")
                .arg(keys)
                .query_async::<usize>(&mut connection)
                .await
                .expect("failed to remove Redis integration-test keys");
        }
    }

    pub(super) fn redis(&self, namespace: &str) -> Redis {
        Redis::new(self.url())
            .unwrap()
            .with_namespace(namespace)
            .unwrap()
    }
}

pub(super) fn namespace(label: &str) -> String {
    format!(
        "crawler-test-redis-{label}-{}",
        uuid::Uuid::now_v7().simple()
    )
}

fn unavailable(url: &str, error: redis::RedisError) -> Option<Handle> {
    eprintln!("skipping Redis Scheduler integration test: Redis at {url} is unavailable: {error}");
    None
}

fn unavailable_timeout(url: &str, operation: &str) -> Option<Handle> {
    eprintln!(
        "skipping Redis Scheduler integration test: Redis at {url} did not {operation} within {CONNECT_TIMEOUT:?}"
    );
    None
}
