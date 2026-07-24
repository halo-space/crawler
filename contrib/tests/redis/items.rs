use redis::streams::StreamRangeReply;
use spider::scheduler::Init;
use spider::{Scheduler, item, payload, trace};

use super::{server, worker};

#[tokio::test]
async fn stream_appends_one_entry_per_payload_and_preserves_replays() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("items");
    let scheduler = server.redis(&namespace);
    scheduler.open().await.unwrap();

    let mut payload = payload::Payload::new().items(vec![record("first"), record("second")]);
    payload.id = "source-request".to_string();
    payload.task_id = "item-task".to_string();
    payload.version = 9;
    payload.worker_id = worker::A.to_string();
    payload.node = "detail".to_string();
    scheduler.push_items(&payload).await.unwrap();

    let first = payloads(&server, &namespace).await;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0]["records"].as_array().unwrap().len(), 2);
    assert_eq!(first[0]["version"], 9);

    scheduler.push_items(&payload).await.unwrap();
    let replayed = payloads(&server, &namespace).await;
    assert_eq!(replayed.len(), 2);
    assert_eq!(replayed[0], replayed[1]);

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn stream_carries_rules_version_and_timezone() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("item-metadata");
    let scheduler = server.redis(&namespace);
    scheduler.open().await.unwrap();

    let rules = spider::config::Config::from_yaml(
        r#"
spider:
  name: metadata
  version: "2026.07"
  timezone: Asia/Shanghai
  start: [{node: index, url: https://example.com}]
graph:
  nodes:
    index: {}
  edges: []
"#,
    )
    .unwrap();
    scheduler
        .init(
            "metadata-trace".to_string(),
            trace::Snapshot::rules("metadata-task", rules),
            Vec::new(),
        )
        .await
        .unwrap();

    let mut payload = payload::Payload::new().items(vec![record("metadata")]);
    payload.id = "metadata-request".to_string();
    payload.task_id = "metadata-task".to_string();
    payload.trace_id = "metadata-trace".to_string();
    payload.version = 3;
    payload.worker_id = worker::A.to_string();
    payload.node = "index".to_string();
    scheduler.push_items(&payload).await.unwrap();

    let records = payloads(&server, &namespace).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["config_version"], "2026.07");
    assert_eq!(records[0]["timezone"], "Asia/Shanghai");

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

fn record(value: &str) -> Box<dyn item::Item> {
    let mut item = item::Map::new(item::Values::from([("value".to_string(), value.into())]));
    *item::Item::state_mut(&mut item).id_mut() = format!("item-{value}");
    Box::new(item)
}

async fn payloads(server: &server::Handle, namespace: &str) -> Vec<serde_json::Value> {
    let mut connection = server.connection().await;
    redis::cmd("XRANGE")
        .arg(format!("{namespace}:items"))
        .arg("-")
        .arg("+")
        .query_async::<StreamRangeReply>(&mut connection)
        .await
        .unwrap()
        .ids
        .into_iter()
        .map(|record| {
            let payload = record
                .get::<String>("payload")
                .expect("Item Stream entry must contain a payload field");
            serde_json::from_str(&payload).expect("Item Stream payload must be valid JSON")
        })
        .collect()
}
