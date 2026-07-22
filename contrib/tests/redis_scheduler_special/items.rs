use spider::scheduler::Init;
use spider::{Scheduler, payload, trace};

use super::common::{WORKER_A, item, namespace, scheduler, stream_payloads};
use crate::redis_fixture::Fixture;

#[tokio::test]
async fn item_stream_appends_one_entry_per_payload_and_preserves_replays() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("items");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    let mut items = payload::Payload::new().items(vec![item("first"), item("second")]);
    items.id = "source-request".to_string();
    items.task_id = "item-task".to_string();
    items.version = 9;
    items.worker_id = WORKER_A.to_string();
    items.node = "detail".to_string();
    scheduler.push_items(&items).await.unwrap();

    let first = stream_payloads(&fixture, &namespace).await;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0]["records"].as_array().unwrap().len(), 2);
    assert_eq!(first[0]["version"], 9);

    scheduler.push_items(&items).await.unwrap();
    let replayed = stream_payloads(&fixture, &namespace).await;
    assert_eq!(replayed.len(), 2);
    assert_eq!(replayed[0], replayed[1]);

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn item_stream_carries_rules_version_and_timezone() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("item-metadata");
    let scheduler = scheduler(&fixture, &namespace);
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

    let mut items = payload::Payload::new().items(vec![item("metadata")]);
    items.id = "metadata-request".to_string();
    items.task_id = "metadata-task".to_string();
    items.trace_id = "metadata-trace".to_string();
    items.version = 3;
    items.worker_id = WORKER_A.to_string();
    items.node = "index".to_string();
    scheduler.push_items(&items).await.unwrap();

    let records = stream_payloads(&fixture, &namespace).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["config_version"], "2026.07");
    assert_eq!(records[0]["timezone"], "Asia/Shanghai");

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}
