use std::time::Duration;

use base64::Engine as _;
use contrib::middleware::dedup::{Options, Redis as Dedup};
use spider::middleware::{Middleware as _, Next, Spec};
use spider::net::Request;

const DEFAULT_URL: &str = "redis://127.0.0.1:6379";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

#[tokio::test]
async fn concurrent_workers_only_admit_one_fingerprint() {
    let Some(url) = redis_url().await else {
        return;
    };
    let task = unique("concurrent");
    let node = "detail";
    let key = bucket(&task, node);
    clear(&url, &[&key]).await;
    let first = Dedup::new(&url, Options::new(10_000, 0.001).unwrap()).unwrap();
    let second = Dedup::new(&url, Options::new(10_000, 0.001).unwrap()).unwrap();
    let spec = spec();
    let mut left = request(&task, node);
    left.trace_id = "trace-a".to_string();
    let mut right = request(&task, node);
    right.trace_id = "trace-b".to_string();

    let (left, right) = tokio::join!(
        first.before_scheduler(left, &spec),
        second.before_scheduler(right, &spec)
    );
    let continued = [left.unwrap(), right.unwrap()]
        .into_iter()
        .filter(|result| matches!(result, Next::Continue(_)))
        .count();

    assert_eq!(continued, 1);
    clear(&url, &[&key]).await;
}

#[tokio::test]
async fn isolates_task_and_node_but_shares_membership_across_traces() {
    let Some(url) = redis_url().await else {
        return;
    };
    let first_task = unique("task-a");
    let second_task = unique("task-b");
    let keys = [
        bucket(&first_task, "detail"),
        bucket(&second_task, "detail"),
        bucket(&first_task, "page"),
    ];
    clear(&url, &keys.iter().map(String::as_str).collect::<Vec<_>>()).await;
    let dedup = Dedup::new(&url, Options::new(10_000, 0.001).unwrap()).unwrap();
    let first = spec().key("first");
    let second = spec().key("second");

    let mut initial = request(&first_task, "detail");
    initial.trace_id = "trace-a".to_string();
    assert!(matches!(
        dedup.before_scheduler(initial, &first).await.unwrap(),
        Next::Continue(_)
    ));

    let mut same_bucket = request(&first_task, "detail");
    same_bucket.trace_id = "trace-b".to_string();
    assert!(matches!(
        dedup.before_scheduler(same_bucket, &second).await.unwrap(),
        Next::Skip
    ));

    for value in [
        request(&second_task, "detail"),
        request(&first_task, "page"),
    ] {
        assert!(matches!(
            dedup.before_scheduler(value, &spec()).await.unwrap(),
            Next::Continue(_)
        ));
    }

    clear(&url, &keys.iter().map(String::as_str).collect::<Vec<_>>()).await;
}

#[tokio::test]
async fn rejects_a_non_bloom_bucket_without_changing_it() {
    let Some(url) = redis_url().await else {
        return;
    };
    let task = unique("wrong-type");
    let key = bucket(&task, "detail");
    let mut connection = connection(&url).await;
    redis::cmd("SET")
        .arg(&key)
        .arg("owned-by-another-capability")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let dedup = Dedup::new(&url, Options::new(10_000, 0.001).unwrap()).unwrap();

    let error = dedup
        .before_scheduler(request(&task, "detail"), &spec())
        .await
        .unwrap_err();

    assert!(error.to_string().contains("Redis Dedup operation failed"));
    assert_eq!(
        redis::cmd("GET")
            .arg(&key)
            .query_async::<String>(&mut connection)
            .await
            .unwrap(),
        "owned-by-another-capability"
    );
    clear(&url, &[&key]).await;
}

#[tokio::test]
async fn disabled_and_unfiltered_requests_do_not_create_a_bucket() {
    let Some(url) = redis_url().await else {
        return;
    };
    let task = unique("bypass");
    let key = bucket(&task, "detail");
    clear(&url, &[&key]).await;
    let dedup = Dedup::new(&url, Options::new(10_000, 0.001).unwrap()).unwrap();
    let disabled = Spec::new("dedup").args(serde_json::json!({
        "key": ["$request.url"],
        "ttl": 0
    }));
    assert!(matches!(
        dedup
            .before_scheduler(request(&task, "detail"), &disabled)
            .await
            .unwrap(),
        Next::Continue(_)
    ));

    let mut unfiltered = request(&task, "detail");
    unfiltered.dont_filter = true;
    assert!(matches!(
        dedup.before_scheduler(unfiltered, &spec()).await.unwrap(),
        Next::Continue(_)
    ));

    assert!(
        !redis::cmd("EXISTS")
            .arg(&key)
            .query_async::<bool>(&mut connection(&url).await)
            .await
            .unwrap()
    );
}

fn spec() -> Spec {
    Spec::new("dedup").args(serde_json::json!({
        "key": ["$request.url"],
        "ttl": -1
    }))
}

fn request(task: &str, node: &str) -> Request {
    let mut request = Request::follow("https://example.com/article")
        .unwrap()
        .node(node);
    request.task_id = task.to_string();
    request
}

fn unique(label: &str) -> String {
    format!("crawler-test-dedup-{label}-{}", uuid::Uuid::now_v7())
}

fn bucket(task: &str, node: &str) -> String {
    let encoder = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    format!("dedup:{}:{}", encoder.encode(task), encoder.encode(node))
}

async fn redis_url() -> Option<String> {
    let (url, required) = match std::env::var("CRAWLER_REDIS_URL") {
        Ok(url) => (url, true),
        Err(std::env::VarError::NotPresent) => (DEFAULT_URL.to_string(), false),
        Err(error) => panic!("invalid CRAWLER_REDIS_URL: {error}"),
    };
    let client = redis::Client::open(url.as_str())
        .unwrap_or_else(|error| panic!("invalid CRAWLER_REDIS_URL {url:?}: {error}"));
    let result = tokio::time::timeout(CONNECT_TIMEOUT, async {
        let mut connection = client.get_multiplexed_async_connection().await?;
        redis::cmd("BF.EXISTS")
            .arg(unique("probe"))
            .arg("probe")
            .query_async::<bool>(&mut connection)
            .await
    })
    .await;
    match result {
        Ok(Ok(_)) => Some(url),
        Ok(Err(error)) if required => {
            panic!("configured Redis Bloom at {url} is unavailable: {error}")
        }
        Ok(Err(error)) => {
            eprintln!("skipping Redis Dedup test: Redis Bloom at {url} is unavailable: {error}");
            None
        }
        Err(_) if required => panic!("configured Redis Bloom at {url} did not answer in time"),
        Err(_) => {
            eprintln!("skipping Redis Dedup test: Redis Bloom at {url} did not answer in time");
            None
        }
    }
}

async fn connection(url: &str) -> redis::aio::MultiplexedConnection {
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

async fn clear(url: &str, keys: &[&str]) {
    if keys.is_empty() {
        return;
    }
    redis::cmd("DEL")
        .arg(keys)
        .query_async::<usize>(&mut connection(url).await)
        .await
        .unwrap();
}
