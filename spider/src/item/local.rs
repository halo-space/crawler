use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Local as LocalTime;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::payload;

#[derive(Debug)]
pub(crate) struct Writer {
    dir: PathBuf,
    files: Mutex<HashMap<PathBuf, Arc<Mutex<File>>>>,
}

impl Writer {
    pub(crate) fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            files: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    fn current_path(&self, task_id: &str) -> PathBuf {
        let hour = LocalTime::now().format("%Y-%m-%d-%H");
        self.path_for_hour(task_id, &hour.to_string())
    }

    fn path_for_hour(&self, task_id: &str, hour: &str) -> PathBuf {
        let task_id = crate::utils::path::segment(task_id);
        self.dir
            .join("data")
            .join("items")
            .join("output")
            .join(task_id)
            .join(format!("{hour}.jsonl"))
    }

    async fn file(&self, path: &Path) -> Result<Arc<Mutex<File>>, crate::Error> {
        if let Some(file) = self.files.lock().await.get(path).cloned() {
            return Ok(file);
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(crate::item::Error::from)?;
        let file = Arc::new(Mutex::new(file));
        let (file, obsolete) = {
            let mut files = self.files.lock().await;
            if let Some(file) = files.get(path) {
                return Ok(file.clone());
            }

            let task_dir = path.parent();
            let current_path = files
                .keys()
                .find(|candidate| candidate.parent() == task_dir)
                .cloned();
            let obsolete = current_path
                .filter(|current| current.as_path() < path)
                .and_then(|current| files.remove(&current))
                .into_iter()
                .collect::<Vec<_>>();
            if files.keys().all(|candidate| candidate.parent() != task_dir) {
                files.insert(path.to_path_buf(), file.clone());
            }
            (file, obsolete)
        };

        for file in obsolete {
            file.lock()
                .await
                .flush()
                .await
                .map_err(crate::item::Error::from)?;
        }

        Ok(file)
    }

    async fn close_files(&self) -> Result<(), crate::Error> {
        let files = self
            .files
            .lock()
            .await
            .drain()
            .map(|(_, file)| file)
            .collect::<Vec<_>>();
        let mut first_error = None;
        for file in files {
            if let Err(error) = file.lock().await.flush().await
                && first_error.is_none()
            {
                first_error = Some(crate::item::Error::from(error).into());
            }
        }

        first_error.map_or(Ok(()), Err)
    }
}

impl Default for Writer {
    fn default() -> Self {
        Self::new(".")
    }
}

impl Writer {
    pub(crate) async fn open(&self) -> Result<(), crate::Error> {
        tokio::fs::create_dir_all(self.dir.join("data").join("items").join("output"))
            .await
            .map_err(crate::item::Error::from)
            .map_err(crate::Error::from)
    }

    pub(crate) async fn close(&self) -> Result<(), crate::Error> {
        self.close_files().await
    }

    pub(crate) async fn write(&self, payload: &payload::Payload) -> Result<(), crate::Error> {
        let path = self.current_path(&payload.task_id);
        let mut bytes = Vec::new();
        for item in &payload.items {
            let mut line = serde_json::to_vec(item.as_ref()).map_err(crate::item::Error::from)?;
            line.push(b'\n');
            bytes.extend(line);
        }
        if bytes.is_empty() {
            return Ok(());
        }

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(crate::item::Error::from)?;
        }

        let file = self.file(&path).await?;
        let mut file = file.lock().await;
        let original_len = file
            .metadata()
            .await
            .map_err(crate::item::Error::from)?
            .len();
        let write_result = async {
            file.write_all(&bytes).await?;
            file.flush().await
        }
        .await;
        if let Err(error) = write_result {
            let rollback_result = async {
                file.set_len(original_len).await?;
                file.seek(std::io::SeekFrom::End(0)).await?;
                Ok::<(), std::io::Error>(())
            }
            .await;
            if let Err(rollback_error) = rollback_result {
                return Err(crate::item::Error::Message(format!(
                    "item write failed: {error}; rollback failed: {rollback_error}"
                ))
                .into());
            }
            return Err(crate::item::Error::from(error).into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use serde_json::Value;

    use super::*;
    use crate::item::Item;

    static TEMP_ID: AtomicU64 = AtomicU64::new(1);

    #[derive(serde::Serialize)]
    struct TestItem {
        #[serde(skip)]
        state: crate::item::State,
        label: String,
        value: i64,
    }

    impl TestItem {
        fn new(value: i64) -> Self {
            Self {
                state: crate::item::State::default(),
                label: format!("item-{value}"),
                value,
            }
        }
    }

    impl Item for TestItem {
        fn from_values(mut values: crate::item::Values) -> Result<Self, crate::item::Error> {
            let value = values
                .shift_remove("value")
                .and_then(|value| value.as_i64())
                .ok_or_else(|| crate::item::Error::Message("value must be an int".to_string()))?;
            Ok(Self::new(value))
        }

        fn state(&self) -> &crate::item::State {
            &self.state
        }

        fn state_mut(&mut self) -> &mut crate::item::State {
            &mut self.state
        }
    }

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "spider-local-items-{}-{}",
            std::process::id(),
            TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn payload(task_id: &str, value: i64) -> payload::Payload {
        let mut payload = payload::Payload::new().items(vec![Box::new(TestItem::new(value))]);
        payload.task_id = task_id.to_string();
        payload
    }

    #[tokio::test]
    async fn writes_jsonl_to_sanitized_task_path() {
        let runtime_dir = temp_dir();
        let storage = Writer::new(&runtime_dir);
        let path = storage.current_path("task/one");

        storage.open().await.unwrap();
        storage.write(&payload("task/one", 7)).await.unwrap();
        storage.close().await.unwrap();
        assert!(storage.files.lock().await.is_empty());

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(
            content.lines().collect::<Vec<_>>(),
            [r#"{"label":"item-7","value":7}"#]
        );
        assert!(path.to_string_lossy().contains("task_one"));

        tokio::fs::remove_dir_all(runtime_dir).await.unwrap();
    }

    #[tokio::test]
    async fn serializes_concurrent_writes_to_the_same_file() {
        let runtime_dir = temp_dir();
        let storage = Writer::new(&runtime_dir);
        let path = storage.current_path("same-task");
        let first = payload("same-task", 1);
        let second = payload("same-task", 2);

        let (first_result, second_result) =
            tokio::join!(storage.write(&first), storage.write(&second));
        first_result.unwrap();
        second_result.unwrap();
        storage.close().await.unwrap();

        let content = tokio::fs::read_to_string(path).await.unwrap();
        let mut values = content
            .lines()
            .map(|line| {
                serde_json::from_str::<Value>(line).unwrap()["value"]
                    .as_i64()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, [1, 2]);

        tokio::fs::remove_dir_all(runtime_dir).await.unwrap();
    }

    #[tokio::test]
    async fn payload_task_id_controls_the_output_path() {
        let runtime_dir = temp_dir();
        let storage = Writer::new(&runtime_dir);
        let path = storage.current_path("request-task");
        let fallback_path = storage.current_path("item-task");
        let mut payload = payload("request-task", 7);
        payload.items[0]
            .vals_mut()
            .insert("task_id".to_string(), Value::from("item-task"));

        storage.write(&payload).await.unwrap();
        storage.close().await.unwrap();

        assert!(tokio::fs::try_exists(path).await.unwrap());
        assert!(!tokio::fs::try_exists(fallback_path).await.unwrap());

        tokio::fs::remove_dir_all(runtime_dir).await.unwrap();
    }

    #[tokio::test]
    async fn evicts_obsolete_hourly_writer_for_the_same_task() {
        let runtime_dir = temp_dir();
        let storage = Writer::new(&runtime_dir);
        let first_path = storage.path_for_hour("task", "2026-07-10-10");
        let second_path = storage.path_for_hour("task", "2026-07-10-11");
        tokio::fs::create_dir_all(first_path.parent().unwrap())
            .await
            .unwrap();

        storage.file(&first_path).await.unwrap();
        assert_eq!(storage.files.lock().await.len(), 1);
        storage.file(&second_path).await.unwrap();

        let files = storage.files.lock().await;
        assert_eq!(files.len(), 1);
        assert!(files.contains_key(&second_path));
        drop(files);

        storage.close().await.unwrap();
        tokio::fs::remove_dir_all(runtime_dir).await.unwrap();
    }

    #[tokio::test]
    async fn late_old_hour_writer_does_not_evict_the_current_hour() {
        let runtime_dir = temp_dir();
        let storage = Writer::new(&runtime_dir);
        let first_path = storage.path_for_hour("task", "2026-07-10-10");
        let second_path = storage.path_for_hour("task", "2026-07-10-11");
        tokio::fs::create_dir_all(first_path.parent().unwrap())
            .await
            .unwrap();

        storage.file(&second_path).await.unwrap();
        storage.file(&first_path).await.unwrap();

        let files = storage.files.lock().await;
        assert_eq!(files.len(), 1);
        assert!(files.contains_key(&second_path));
        drop(files);

        storage.close().await.unwrap();
        tokio::fs::remove_dir_all(runtime_dir).await.unwrap();
    }
}
