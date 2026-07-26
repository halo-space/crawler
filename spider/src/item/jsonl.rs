use std::path::{Path, PathBuf};

use serde::Serialize;
use tokio::sync::RwLock;

use crate::{item, payload};

mod output;
mod snapshot;

#[derive(Debug)]
pub struct Jsonl {
    dir: PathBuf,
    output: output::Writer,
    snapshots: snapshot::Store,
    opened: RwLock<bool>,
}

impl Jsonl {
    pub fn new() -> Self {
        Self::with_dir(".")
    }

    pub fn with_dir(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        Self {
            output: output::Writer::new(dir.clone()),
            snapshots: snapshot::Store::new(dir.clone()),
            dir,
            opened: RwLock::new(false),
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    #[cfg(test)]
    fn set_write_failure(&self, enabled: bool) {
        self.output.set_write_failure(enabled);
    }

    #[cfg(test)]
    async fn cached_files(&self) -> usize {
        self.output.cached_files().await
    }
}

impl Default for Jsonl {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize)]
pub(super) struct Record {
    id: String,
    data: serde_json::Value,
}

impl item::Store for Jsonl {
    async fn open(&self) -> Result<(), item::Error> {
        let mut opened = self.opened.write().await;
        if *opened {
            return Ok(());
        }
        self.output.open().await?;
        self.snapshots.open().await?;
        *opened = true;
        Ok(())
    }

    async fn close(&self) -> Result<(), item::Error> {
        let mut opened = self.opened.write().await;
        *opened = false;
        self.output.close().await
    }

    async fn submit(&self, payload: &payload::Payload) -> Result<(), item::Error> {
        payload
            .validate_store()
            .map_err(|message| item::Error::Message(message.to_string()))?;
        if payload.items.is_empty() {
            return Ok(());
        }
        let opened = self.opened.read().await;
        if !*opened {
            return Err(item::Error::Message("Item Store is not open".to_string()));
        }

        let mut records = Vec::with_capacity(payload.items.len());
        for value in &payload.items {
            records.push(Record {
                id: value.id().to_string(),
                data: serde_json::to_value(value.as_ref())?,
            });
        }
        let association = self.snapshots.association(payload, &records)?;
        let mut bytes = Vec::new();
        for record in &records {
            serde_json::to_writer(&mut bytes, record)?;
            bytes.push(b'\n');
        }
        let _snapshot_lock = self.snapshots.lock(&association).await;
        if let Err(error) = self.output.write(&payload.task_id, &bytes).await {
            self.snapshots
                .write(payload, &association, &records, &error.to_string())
                .await;
            return Err(error);
        }
        self.snapshots.remove(payload, &association).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
