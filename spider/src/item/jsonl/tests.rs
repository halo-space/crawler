use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::item::{Item, Store};

static TEMP_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Serialize)]
struct TestItem {
    #[serde(skip)]
    state: item::State,
    value: i64,
}

impl Item for TestItem {
    fn from_values(_values: item::Values) -> Result<Self, item::Error> {
        Ok(Self {
            state: item::State::default(),
            value: 0,
        })
    }

    fn state(&self) -> &item::State {
        &self.state
    }

    fn state_mut(&mut self) -> &mut item::State {
        &mut self.state
    }
}

struct FailingItem {
    state: item::State,
}

impl Serialize for FailingItem {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom("cannot serialize Item"))
    }
}

impl Item for FailingItem {
    fn from_values(_values: item::Values) -> Result<Self, item::Error> {
        Ok(Self {
            state: item::State::default(),
        })
    }

    fn state(&self) -> &item::State {
        &self.state
    }

    fn state_mut(&mut self) -> &mut item::State {
        &mut self.state
    }
}

fn item(id: &str, value: i64) -> TestItem {
    let mut state = item::State::default();
    *state.id_mut() = id.to_string();
    TestItem { state, value }
}

fn failing_item(id: &str) -> FailingItem {
    let mut state = item::State::default();
    *state.id_mut() = id.to_string();
    FailingItem { state }
}

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!(
        "spider-jsonl-store-{}-{}",
        std::process::id(),
        TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

fn payload(items: Vec<TestItem>) -> payload::Payload {
    boxed_payload(
        items
            .into_iter()
            .map(|value| Box::new(value) as Box<dyn Item>)
            .collect(),
    )
}

fn boxed_payload(items: Vec<Box<dyn Item>>) -> payload::Payload {
    let mut payload = payload::Payload::new().items(items);
    payload.id = "request-1".to_string();
    payload.task_id = "task/one".to_string();
    payload.trace_id = "trace-1".to_string();
    payload.version = 1;
    payload.worker_id = "worker-1".to_string();
    payload.node = "detail".to_string();
    payload
}

async fn snapshot_files(dir: &Path, task_id: &str) -> Vec<PathBuf> {
    let root = dir
        .join("data")
        .join("items")
        .join("snapshots")
        .join(crate::utils::path::segment(task_id));
    let Ok(mut hours) = tokio::fs::read_dir(root).await else {
        return Vec::new();
    };
    let mut files = Vec::new();
    while let Some(hour) = hours.next_entry().await.unwrap() {
        if !hour.file_type().await.unwrap().is_dir() {
            continue;
        }
        let mut entries = tokio::fs::read_dir(hour.path()).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            if entry.file_type().await.unwrap().is_file() {
                files.push(entry.path());
            }
        }
    }
    files
}

async fn output_files(dir: &Path, task_id: &str) -> Vec<PathBuf> {
    let root = dir
        .join("data")
        .join("items")
        .join("output")
        .join(crate::utils::path::segment(task_id));
    let Ok(mut entries) = tokio::fs::read_dir(root).await else {
        return Vec::new();
    };
    let mut files = Vec::new();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let path = entry.path();
        if entry.file_type().await.unwrap().is_file()
            && path
                .extension()
                .is_some_and(|extension| extension == "jsonl")
        {
            files.push(path);
        }
    }
    files.sort_by(|left, right| left.file_name().cmp(&right.file_name()));
    files
}

async fn output_content(dir: &Path, task_id: &str) -> String {
    let mut output = String::new();
    for path in output_files(dir, task_id).await {
        output.push_str(&tokio::fs::read_to_string(path).await.unwrap());
    }
    output
}

async fn output_is_empty(dir: &Path) -> bool {
    let mut tasks = tokio::fs::read_dir(dir.join("data/items/output"))
        .await
        .unwrap();
    tasks.next_entry().await.unwrap().is_none()
}

#[tokio::test]
async fn writes_framework_id_beside_business_data() {
    let dir = temp_dir();
    let store = Jsonl::with_dir(&dir);
    store.open().await.unwrap();
    store
        .submit(&payload(vec![item("item-1", 7)]))
        .await
        .unwrap();
    store.close().await.unwrap();

    let content = output_content(&dir, "task/one").await;
    let value: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
    assert_eq!(
        value,
        serde_json::json!({"id": "item-1", "data": {"value": 7}})
    );
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn empty_submission_is_a_no_op() {
    let dir = temp_dir();
    let store = Jsonl::with_dir(&dir);
    store.submit(&payload(Vec::new())).await.unwrap();
    assert!(
        !tokio::fs::try_exists(dir.join("data/items/output"))
            .await
            .unwrap()
    );
    tokio::fs::remove_dir_all(dir).await.unwrap_or(());
}

#[tokio::test]
async fn rejects_empty_run_and_framework_ids_without_writing() {
    let dir = temp_dir();
    let store = Jsonl::with_dir(&dir);
    store.open().await.unwrap();

    let mut missing_task = payload(vec![item("item-1", 1)]);
    missing_task.task_id.clear();
    let error = store.submit(&missing_task).await.unwrap_err();
    assert!(error.to_string().contains("task id"));

    let mut missing_trace = payload(vec![item("item-1", 1)]);
    missing_trace.trace_id.clear();
    let error = store.submit(&missing_trace).await.unwrap_err();
    assert!(error.to_string().contains("trace id"));

    let error = store.submit(&payload(vec![item("", 1)])).await.unwrap_err();
    assert!(error.to_string().contains("framework Item ID"));

    assert!(output_is_empty(&dir).await);
    store.close().await.unwrap();
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn rejects_requests_and_completion_fields_without_writing() {
    let dir = temp_dir();
    let store = Jsonl::with_dir(&dir);
    store.open().await.unwrap();

    let mut with_request = payload(vec![item("item-1", 1)]);
    with_request
        .requests
        .push(crate::net::Request::follow("https://example.com").unwrap());
    let error = store.submit(&with_request).await.unwrap_err();
    assert!(error.to_string().contains("unrelated fields"));

    let mut with_completion = payload(vec![item("item-2", 2)]);
    with_completion.error = Some("download failed".to_string());
    let error = store.submit(&with_completion).await.unwrap_err();
    assert!(error.to_string().contains("unrelated fields"));

    assert!(output_is_empty(&dir).await);
    store.close().await.unwrap();
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn rejects_non_empty_submission_after_close() {
    let dir = temp_dir();
    let store = Jsonl::with_dir(&dir);
    store.open().await.unwrap();
    store.close().await.unwrap();

    let error = store
        .submit(&payload(vec![item("item-1", 1)]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not open"));
    assert!(output_is_empty(&dir).await);
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn close_waits_for_open_file_access() {
    let dir = temp_dir();
    let store = Arc::new(Jsonl::with_dir(&dir));
    store.open().await.unwrap();
    let submission = store.opened.read().await;
    let closing = {
        let store = store.clone();
        tokio::spawn(async move { store.close().await })
    };

    tokio::task::yield_now().await;
    assert!(!closing.is_finished());
    drop(submission);
    closing.await.unwrap().unwrap();

    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn serializes_concurrent_submissions_without_interleaving_lines() {
    const SUBMISSIONS: usize = 32;

    let dir = temp_dir();
    let store = Arc::new(Jsonl::with_dir(&dir));
    store.open().await.unwrap();
    let mut tasks = Vec::with_capacity(SUBMISSIONS);
    for index in 0..SUBMISSIONS {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            let payload = payload(vec![
                item(&format!("item-{index}-a"), index as i64),
                item(&format!("item-{index}-b"), index as i64),
            ]);
            store.submit(&payload).await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    store.close().await.unwrap();

    let content = output_content(&dir, "task/one").await;
    let mut ids = HashSet::new();
    for line in content.lines() {
        let record: serde_json::Value = serde_json::from_str(line).unwrap();
        ids.insert(record["id"].as_str().unwrap().to_string());
    }
    assert_eq!(ids.len(), SUBMISSIONS * 2);
    assert_eq!(content.lines().count(), SUBMISSIONS * 2);
    let lines = content.lines().collect::<Vec<_>>();
    for submission in lines.chunks_exact(2) {
        let first: serde_json::Value = serde_json::from_str(submission[0]).unwrap();
        let second: serde_json::Value = serde_json::from_str(submission[1]).unwrap();
        let first = first["id"].as_str().unwrap();
        let second = second["id"].as_str().unwrap();
        assert_eq!(first.strip_suffix("-a"), second.strip_suffix("-b"));
    }

    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn bounds_the_open_file_cache_across_many_tasks() {
    let dir = temp_dir();
    let store = Jsonl::with_dir(&dir);
    store.open().await.unwrap();

    for index in 0..96 {
        let mut payload = payload(vec![item(&format!("item-{index}"), index)]);
        payload.task_id = format!("task-{index}");
        store.submit(&payload).await.unwrap();
    }

    assert!(store.cached_files().await <= super::output::MAX_CACHED_FILES);
    store.close().await.unwrap();
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn serializes_an_uncached_path_when_every_cache_entry_is_busy() {
    const SUBMISSIONS: usize = 16;
    const ITEMS: usize = 64;

    let dir = temp_dir();
    let store = Arc::new(Jsonl::with_dir(&dir));
    store.open().await.unwrap();
    for index in 0..super::output::MAX_CACHED_FILES {
        let mut payload = payload(vec![item(&format!("cached-{index}"), index as i64)]);
        payload.task_id = format!("cached-task-{index}");
        store.submit(&payload).await.unwrap();
    }
    let held = store.output.hold_cached_files().await;
    assert_eq!(held.len(), super::output::MAX_CACHED_FILES);

    let mut tasks = Vec::with_capacity(SUBMISSIONS);
    for submission in 0..SUBMISSIONS {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            let items = (0..ITEMS)
                .map(|index| {
                    item(
                        &format!("submission-{submission}-item-{index}"),
                        index as i64,
                    )
                })
                .collect();
            let mut payload = payload(items);
            payload.task_id = "uncached-task".to_string();
            store.submit(&payload).await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }

    let output = output_content(&dir, "uncached-task").await;
    let lines = output.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), SUBMISSIONS * ITEMS);
    for group in lines.chunks_exact(ITEMS) {
        let first: serde_json::Value = serde_json::from_str(group[0]).unwrap();
        let prefix = first["id"]
            .as_str()
            .unwrap()
            .split("-item-")
            .next()
            .unwrap();
        assert!(group.iter().all(|line| {
            let record: serde_json::Value = serde_json::from_str(line).unwrap();
            record["id"].as_str().unwrap().split("-item-").next() == Some(prefix)
        }));
    }

    drop(held);
    store.close().await.unwrap();
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn accepts_the_same_payload_again_without_business_deduplication() {
    let dir = temp_dir();
    let store = Jsonl::with_dir(&dir);
    let payload = payload(vec![item("item-1", 1)]);
    store.open().await.unwrap();

    store.submit(&payload).await.unwrap();
    store.submit(&payload).await.unwrap();
    store.close().await.unwrap();

    let content = output_content(&dir, "task/one").await;
    assert_eq!(content.lines().count(), 2);
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn serialization_failure_does_not_write_a_partial_submission() {
    let dir = temp_dir();
    let store = Jsonl::with_dir(&dir);
    store.open().await.unwrap();
    let payload = boxed_payload(vec![
        Box::new(item("item-1", 1)),
        Box::new(failing_item("item-2")),
    ]);

    let error = store.submit(&payload).await.unwrap_err();
    assert!(matches!(error, item::Error::Serialize(_)));
    assert!(output_files(&dir, "task/one").await.is_empty());

    store.close().await.unwrap();
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn write_failure_keeps_one_snapshot_until_success() {
    let dir = temp_dir();
    let store = Jsonl::with_dir(&dir);
    let payload = payload(vec![item("item-1", 1), item("item-2", 2)]);
    store.open().await.unwrap();
    store.set_write_failure(true);

    for attempt in 1..=2 {
        let error = store.submit(&payload).await.unwrap_err();
        assert!(error.to_string().contains("injected Item write failure"));
        assert_eq!(
            output_content(&dir, "task/one").await.lines().count(),
            attempt * 2
        );
    }

    let snapshots = snapshot_files(&dir, "task/one").await;
    assert_eq!(snapshots.len(), 1);
    let snapshot: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&snapshots[0]).await.unwrap()).unwrap();
    assert_eq!(snapshot["items"].as_array().unwrap().len(), 2);

    store.set_write_failure(false);
    store.submit(&payload).await.unwrap();
    assert!(snapshot_files(&dir, "task/one").await.is_empty());
    let content = output_content(&dir, "task/one").await;
    assert_eq!(content.lines().count(), 6);

    store.close().await.unwrap();
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn equal_payload_projections_share_only_the_recovery_snapshot() {
    let dir = temp_dir();
    let store = Jsonl::with_dir(&dir);
    let first = payload(vec![item("item-1", 1)]);
    let second = payload(vec![item("item-1", 1)]);
    store.open().await.unwrap();
    store.set_write_failure(true);

    store.submit(&first).await.unwrap_err();
    store.submit(&second).await.unwrap_err();
    assert_eq!(snapshot_files(&dir, "task/one").await.len(), 1);

    store.set_write_failure(false);
    store.submit(&first).await.unwrap();
    store.submit(&second).await.unwrap();
    assert!(snapshot_files(&dir, "task/one").await.is_empty());
    let output = output_content(&dir, "task/one").await;
    assert_eq!(output.lines().count(), 4);

    store.close().await.unwrap();
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn snapshot_association_isolates_payload_and_item_fields() {
    let dir = temp_dir();
    let store = Jsonl::with_dir(&dir);
    let baseline = payload(vec![item("item-1", 1)]);

    let mut request_id = payload(vec![item("item-1", 1)]);
    request_id.id = "request-2".to_string();
    let mut trace_id = payload(vec![item("item-1", 1)]);
    trace_id.trace_id = "trace-2".to_string();
    let mut version = payload(vec![item("item-1", 1)]);
    version.version = 2;
    let mut worker_id = payload(vec![item("item-1", 1)]);
    worker_id.worker_id = "worker-2".to_string();
    let mut node = payload(vec![item("item-1", 1)]);
    node.node = "other".to_string();
    let item_id = payload(vec![item("item-2", 1)]);
    let item_data = payload(vec![item("item-1", 2)]);

    store.open().await.unwrap();
    store.set_write_failure(true);
    store.submit(&baseline).await.unwrap_err();
    let baseline_files = snapshot_files(&dir, "task/one").await;
    assert_eq!(baseline_files.len(), 1);
    let baseline_file = baseline_files[0].clone();

    for changed in [
        request_id, trace_id, version, worker_id, node, item_id, item_data,
    ] {
        store.submit(&changed).await.unwrap_err();
    }

    let failed_files = snapshot_files(&dir, "task/one")
        .await
        .into_iter()
        .collect::<HashSet<_>>();
    assert_eq!(failed_files.len(), 8);
    assert!(failed_files.contains(&baseline_file));

    store.set_write_failure(false);
    store.submit(&baseline).await.unwrap();
    let remaining = snapshot_files(&dir, "task/one")
        .await
        .into_iter()
        .collect::<HashSet<_>>();
    let mut expected = failed_files;
    assert!(expected.remove(&baseline_file));
    assert_eq!(remaining, expected);

    store.close().await.unwrap();
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn snapshot_cleanup_failure_does_not_change_submit_success() {
    let dir = temp_dir();
    let store = Jsonl::with_dir(&dir);
    let payload = payload(vec![item("item-1", 1)]);
    store.open().await.unwrap();
    store.set_write_failure(true);
    store.submit(&payload).await.unwrap_err();

    let snapshots = snapshot_files(&dir, "task/one").await;
    assert_eq!(snapshots.len(), 1);
    tokio::fs::remove_file(&snapshots[0]).await.unwrap();
    tokio::fs::create_dir(&snapshots[0]).await.unwrap();

    store.set_write_failure(false);
    store.submit(&payload).await.unwrap();
    assert!(tokio::fs::try_exists(&snapshots[0]).await.unwrap());

    store.close().await.unwrap();
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn reopening_does_not_adopt_or_clean_old_snapshots() {
    let dir = temp_dir();
    let payload = payload(vec![item("item-1", 1)]);
    let first = Jsonl::with_dir(&dir);
    first.open().await.unwrap();
    first.set_write_failure(true);
    first.submit(&payload).await.unwrap_err();
    first.close().await.unwrap();
    assert_eq!(snapshot_files(&dir, "task/one").await.len(), 1);

    let second = Jsonl::with_dir(&dir);
    second.open().await.unwrap();
    second.submit(&payload).await.unwrap();
    second.close().await.unwrap();

    assert_eq!(snapshot_files(&dir, "task/one").await.len(), 1);
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn repeated_open_keeps_the_current_snapshot_session() {
    let dir = temp_dir();
    let store = Jsonl::with_dir(&dir);
    let payload = payload(vec![item("item-1", 1)]);
    store.open().await.unwrap();
    store.set_write_failure(true);
    store.submit(&payload).await.unwrap_err();
    assert_eq!(snapshot_files(&dir, "task/one").await.len(), 1);

    store.open().await.unwrap();
    store.set_write_failure(false);
    store.submit(&payload).await.unwrap();

    assert!(snapshot_files(&dir, "task/one").await.is_empty());
    store.close().await.unwrap();
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn snapshot_failure_preserves_the_original_store_error() {
    let dir = temp_dir();
    let store = Jsonl::with_dir(&dir);
    store.open().await.unwrap();
    let snapshot_task = dir
        .join("data/items/snapshots")
        .join(crate::utils::path::segment("task/one"));
    tokio::fs::write(&snapshot_task, b"blocks snapshot directory")
        .await
        .unwrap();
    store.set_write_failure(true);

    let error = store
        .submit(&payload(vec![item("item-1", 1)]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected Item write failure"));
    let output = output_content(&dir, "task/one").await;
    assert_eq!(output.lines().count(), 1);
    assert!(snapshot_files(&dir, "task/one").await.is_empty());

    store.close().await.unwrap();
    tokio::fs::remove_dir_all(dir).await.unwrap_or(());
}
