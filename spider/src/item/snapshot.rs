use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use chrono::Local;
use tokio::sync::{Mutex, MutexGuard};

use crate::payload;

pub(crate) struct Store {
    dir: PathBuf,
    locks: Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>,
}

impl Store {
    pub(crate) fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            locks: Mutex::new(HashMap::new()),
        }
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
        let lock = self.file_lock(&path).await;
        let _guard = lock.lock().await;
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
            "schema_version": 1,
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

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(crate::item::Error::from)?;
        }
        tokio::fs::write(&path, bytes)
            .await
            .map_err(crate::item::Error::from)?;

        Ok(path)
    }

    pub(crate) async fn remove(&self, path: &Path) -> Result<(), crate::Error> {
        let lock = self.file_lock(path).await;
        let _guard = lock.lock().await;
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

    async fn file_lock(&self, path: &Path) -> Arc<Mutex<()>> {
        let mut locks = self.locks().await;
        locks.retain(|_, lock| lock.strong_count() > 0);

        if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
            return lock;
        }

        let lock = Arc::new(Mutex::new(()));
        locks.insert(path.to_path_buf(), Arc::downgrade(&lock));
        lock
    }

    async fn locks(&self) -> MutexGuard<'_, HashMap<PathBuf, Weak<Mutex<()>>>> {
        self.locks.lock().await
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
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

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
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
        assert!(path.to_string_lossy().contains("task_1"));

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

        tokio::fs::remove_dir_all(runtime_dir).await.unwrap();
    }
}
