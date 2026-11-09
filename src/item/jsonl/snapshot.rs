use std::path::{Path, PathBuf};
use std::sync::RwLock;

use chrono::Local;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, MutexGuard};

use super::Record;
use crate::{item, payload};

const LOCK_SHARDS: usize = 64;

#[derive(Debug)]
pub(super) struct Store {
    dir: PathBuf,
    session: RwLock<Session>,
    locks: [Mutex<()>; LOCK_SHARDS],
}

impl Store {
    pub(super) fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            session: RwLock::new(Session::new()),
            locks: std::array::from_fn(|_| Mutex::new(())),
        }
    }

    pub(super) async fn open(&self) -> Result<(), item::Error> {
        tokio::fs::create_dir_all(self.dir.join("data").join("items").join("snapshots")).await?;
        *self.session.write().map_err(|_| {
            item::Error::Message("Item snapshot session lock is poisoned".to_string())
        })? = Session::new();
        Ok(())
    }

    pub(super) fn association(
        &self,
        payload: &payload::Payload,
        records: &[Record],
    ) -> Result<Association, item::Error> {
        let encoded = canonical(&Key {
            id: &payload.id,
            task_id: &payload.task_id,
            trace_id: &payload.trace_id,
            version: payload.version,
            worker_id: &payload.worker_id,
            node: &payload.node,
            items: records,
        })?;
        let content = Sha256::digest(encoded);
        let session = self
            .session
            .read()
            .map_err(|_| {
                item::Error::Message("Item snapshot session lock is poisoned".to_string())
            })?
            .clone();
        let mut chain = Sha256::new();
        chain.update(session.id.as_bytes());
        chain.update(content);
        let chain = chain.finalize();
        let shard = usize::from(chain[0]) % LOCK_SHARDS;
        let content = hex(&content);
        let chain = hex(&chain);
        let name = format!("{content}-{chain}");
        Ok(Association {
            name,
            hour: session.hour,
            shard,
        })
    }

    pub(super) async fn lock<'a>(&'a self, association: &Association) -> MutexGuard<'a, ()> {
        self.locks[association.shard].lock().await
    }

    pub(super) async fn write(
        &self,
        payload: &payload::Payload,
        association: &Association,
        records: &[Record],
        error: &str,
    ) {
        let path = self.path(payload, association);
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            return;
        }
        if let Err(snapshot_error) = publish(&path, payload, records, error).await {
            tracing::warn!(
                error = %snapshot_error,
                task_id = %payload.task_id,
                request_id = %payload.id,
                "failed to persist Item failure snapshot"
            );
        }
    }

    pub(super) async fn remove(&self, payload: &payload::Payload, association: &Association) {
        let path = self.path(payload, association);
        if let Err(error) = tokio::fs::remove_file(path).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(error = %error, "failed to remove Item failure snapshot");
        }
    }

    fn path(&self, payload: &payload::Payload, association: &Association) -> PathBuf {
        self.dir
            .join("data")
            .join("items")
            .join("snapshots")
            .join(crate::utils::path::segment(&payload.task_id))
            .join(&association.hour)
            .join(format!("{}.json", association.name))
    }
}

#[derive(Clone, Debug)]
struct Session {
    id: String,
    hour: String,
}

impl Session {
    fn new() -> Self {
        Self {
            id: uuid::Uuid::now_v7().simple().to_string(),
            hour: Local::now().format("%Y-%m-%d-%H").to_string(),
        }
    }
}

#[derive(Debug)]
pub(super) struct Association {
    name: String,
    hour: String,
    shard: usize,
}

#[derive(Serialize)]
struct Snapshot<'a> {
    id: &'a str,
    task_id: &'a str,
    trace_id: &'a str,
    version: i64,
    worker_id: &'a str,
    node: &'a str,
    error: &'a str,
    failed_time: i64,
    items: &'a [Record],
}

#[derive(Serialize)]
struct Key<'a> {
    id: &'a str,
    task_id: &'a str,
    trace_id: &'a str,
    version: i64,
    worker_id: &'a str,
    node: &'a str,
    items: &'a [Record],
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn canonical(value: &impl Serialize) -> Result<Vec<u8>, item::Error> {
    let value = sort(serde_json::to_value(value)?);
    serde_json::to_vec(&value).map_err(item::Error::from)
}

fn sort(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(sort).collect())
        }
        serde_json::Value::Object(values) => {
            let mut values = values.into_iter().collect::<Vec<_>>();
            values.sort_by(|left, right| left.0.cmp(&right.0));
            let mut sorted = serde_json::Map::new();
            for (name, value) in values {
                sorted.insert(name, sort(value));
            }
            serde_json::Value::Object(sorted)
        }
        value => value,
    }
}

#[cfg(test)]
mod tests;

async fn publish(
    path: &Path,
    payload: &payload::Payload,
    records: &[Record],
    error: &str,
) -> Result<(), item::Error> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("snapshot.json");
    let temporary = path.with_file_name(format!(".{filename}.{}.tmp", uuid::Uuid::now_v7()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .await?;
    let result = async {
        let snapshot = Snapshot {
            id: &payload.id,
            task_id: &payload.task_id,
            trace_id: &payload.trace_id,
            version: payload.version,
            worker_id: &payload.worker_id,
            node: &payload.node,
            error,
            failed_time: crate::utils::time::now_millis(),
            items: records,
        };
        let bytes = serde_json::to_vec(&snapshot)?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        drop(file);
        tokio::fs::rename(&temporary, path).await?;
        Ok::<(), item::Error>(())
    }
    .await;
    if let Err(error) = result {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error);
    }
    Ok(())
}
