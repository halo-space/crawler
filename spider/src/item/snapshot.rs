use std::path::{Path, PathBuf};

use chrono::Local;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use crate::payload;

pub(crate) struct Store {
    dir: PathBuf,
}

impl Store {
    pub(crate) fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    pub(crate) async fn open(&self) -> Result<(), crate::Error> {
        tokio::fs::create_dir_all(self.dir.join("data").join("items").join("snapshots"))
            .await
            .map_err(crate::item::Error::from)
            .map_err(crate::Error::from)
    }

    pub(crate) async fn write(
        &self,
        payload: &payload::Payload,
        error: &str,
    ) -> Result<PathBuf, crate::Error> {
        let path = self.path(payload);
        let items = payload
            .items
            .iter()
            .map(|item| {
                serde_json::to_value(item.as_ref()).map(|data| {
                    serde_json::json!({
                        "id": item.id(),
                        "data": data,
                    })
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(crate::item::Error::from)?;
        let snapshot = serde_json::json!({
            "id": payload.id,
            "task_id": payload.task_id,
            "trace_id": payload.trace_id,
            "version": payload.version,
            "worker_id": payload.worker_id,
            "node": payload.node,
            "error": error,
            "failed_time": crate::utils::time::now_millis(),
            "items": items,
        });
        let bytes = serde_json::to_vec_pretty(&snapshot).map_err(crate::item::Error::from)?;
        publish(&path, &bytes).await?;

        Ok(path)
    }

    pub(crate) async fn remove(&self, path: &Path) -> Result<(), crate::Error> {
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(crate::item::Error::from(error).into()),
        }
    }

    fn path(&self, payload: &payload::Payload) -> PathBuf {
        let hour = Local::now().format("%Y-%m-%d-%H").to_string();
        let task_id = crate::utils::path::segment(&payload.task_id);
        let file = format!("{}.json", uuid::Uuid::now_v7());

        self.dir
            .join("data")
            .join("items")
            .join("snapshots")
            .join(&task_id)
            .join(hour)
            .join(file)
    }
}

async fn publish(path: &Path, bytes: &[u8]) -> Result<(), crate::Error> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(crate::item::Error::from)?;
    }
    let temporary = temporary_path(path);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .await
        .map_err(crate::item::Error::from)?;
    let result = async {
        file.write_all(bytes).await?;
        file.flush().await?;
        drop(file);
        tokio::fs::rename(&temporary, path).await
    }
    .await;
    if let Err(error) = result {
        let cleanup = tokio::fs::remove_file(&temporary).await;
        if let Err(cleanup_error) = cleanup
            && cleanup_error.kind() != std::io::ErrorKind::NotFound
        {
            return Err(crate::item::Error::Message(format!(
                "failure snapshot write failed: {error}; temporary file cleanup failed: {cleanup_error}"
            ))
            .into());
        }
        return Err(crate::item::Error::from(error).into());
    }

    Ok(())
}

fn temporary_path(path: &Path) -> PathBuf {
    let file = path
        .file_name()
        .expect("failure snapshot path must have a file name")
        .to_string_lossy();
    path.with_file_name(format!(".{file}.{}.tmp", uuid::Uuid::now_v7()))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use serde_json::Value;

    use super::*;

    static TEMP_ID: AtomicU64 = AtomicU64::new(1);

    #[derive(serde::Serialize)]
    struct TestItem {
        #[serde(skip)]
        state: crate::item::State,
        value: i64,
    }

    impl crate::item::Item for TestItem {
        fn from_values(mut values: crate::item::Values) -> Result<Self, crate::item::Error> {
            let value = values
                .shift_remove("value")
                .and_then(|value| value.as_i64())
                .ok_or_else(|| crate::item::Error::Message("value must be an int".to_string()))?;
            Ok(Self {
                state: crate::item::State::default(),
                value,
            })
        }

        fn state(&self) -> &crate::item::State {
            &self.state
        }

        fn state_mut(&mut self) -> &mut crate::item::State {
            &mut self.state
        }
    }

    fn item(id: &str, value: i64) -> TestItem {
        let mut state = crate::item::State::default();
        *state.id_mut() = id.to_string();
        TestItem { state, value }
    }

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "spider-failure-snapshot-{}-{}",
            std::process::id(),
            TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }

    async fn temporary_files(dir: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        let mut entries = tokio::fs::read_dir(dir).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            if entry.file_name().to_string_lossy().ends_with(".tmp") {
                files.push(entry.path());
            }
        }
        files
    }

    #[tokio::test]
    async fn writes_complete_items_and_removes_snapshot() {
        let runtime_dir = temp_dir();
        let snapshots = Store::new(&runtime_dir);
        let mut payload = payload::Payload::new().items(vec![Box::new(item("item-1", 7))]);
        payload.id = "request/1".to_string();
        payload.task_id = "task/1".to_string();
        payload.trace_id = "trace-1".to_string();
        payload.version = 2;

        snapshots.open().await.unwrap();
        let path = snapshots.write(&payload, "disk full").await.unwrap();
        let value: Value = serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();

        assert_eq!(
            value["items"],
            serde_json::json!([{"id": "item-1", "data": {"value": 7}}])
        );
        assert_eq!(value["error"], "disk full");
        assert!(value.get("schema_version").is_none());
        assert!(path.to_string_lossy().contains("task_1"));
        assert!(temporary_files(path.parent().unwrap()).await.is_empty());

        snapshots.remove(&path).await.unwrap();
        assert!(!tokio::fs::try_exists(&path).await.unwrap());
        tokio::fs::remove_dir_all(runtime_dir).await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_writes_create_distinct_snapshots() {
        let runtime_dir = temp_dir();
        let snapshots = Store::new(&runtime_dir);
        let mut first = payload::Payload::new().items(vec![Box::new(item("item-1", 1))]);
        first.id = "request".to_string();
        first.task_id = "task".to_string();
        let mut second = payload::Payload::new().items(vec![Box::new(item("item-2", 2))]);
        second.id = "request".to_string();
        second.task_id = "task".to_string();

        let (first_path, second_path) = tokio::join!(
            snapshots.write(&first, "first"),
            snapshots.write(&second, "second")
        );
        let first_path = first_path.unwrap();
        let second_path = second_path.unwrap();
        assert_ne!(first_path, second_path);
        let first: Value =
            serde_json::from_slice(&tokio::fs::read(&first_path).await.unwrap()).unwrap();
        let second: Value =
            serde_json::from_slice(&tokio::fs::read(&second_path).await.unwrap()).unwrap();
        assert_eq!(first["items"][0]["data"]["value"], 1);
        assert_eq!(second["items"][0]["data"]["value"], 2);
        assert!(
            temporary_files(first_path.parent().unwrap())
                .await
                .is_empty()
        );

        tokio::fs::remove_dir_all(runtime_dir).await.unwrap();
    }

    #[tokio::test]
    async fn publish_failure_removes_the_temporary_file() {
        let runtime_dir = temp_dir();
        let path = runtime_dir.join("snapshot.json");
        tokio::fs::create_dir_all(&path).await.unwrap();

        assert!(publish(&path, b"snapshot").await.is_err());
        assert!(temporary_files(&runtime_dir).await.is_empty());

        tokio::fs::remove_dir_all(runtime_dir).await.unwrap();
    }
}
