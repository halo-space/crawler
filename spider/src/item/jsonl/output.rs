use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::Local;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::item;

pub(super) const MAX_CACHED_FILES: usize = 64;
const WRITE_LOCKS: usize = 64;

#[derive(Debug)]
pub(super) struct Writer {
    dir: PathBuf,
    files: Mutex<HashMap<PathBuf, CachedFile>>,
    locks: [Mutex<()>; WRITE_LOCKS],
    #[cfg(test)]
    fail_after_write: AtomicBool,
}

#[derive(Debug)]
struct CachedFile {
    file: Arc<Mutex<File>>,
    touched: Instant,
}

impl Writer {
    pub(super) fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            files: Mutex::new(HashMap::new()),
            locks: std::array::from_fn(|_| Mutex::new(())),
            #[cfg(test)]
            fail_after_write: AtomicBool::new(false),
        }
    }

    pub(super) async fn open(&self) -> Result<(), item::Error> {
        tokio::fs::create_dir_all(self.dir.join("data").join("items").join("output")).await?;
        Ok(())
    }

    pub(super) async fn close(&self) -> Result<(), item::Error> {
        let files = self
            .files
            .lock()
            .await
            .drain()
            .map(|(_, cached)| cached.file)
            .collect::<Vec<_>>();
        let mut first_error = None;
        for file in files {
            if let Err(error) = file.lock().await.flush().await
                && first_error.is_none()
            {
                first_error = Some(item::Error::from(error));
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(super) async fn write(&self, task_id: &str, bytes: &[u8]) -> Result<(), item::Error> {
        let path = self.current_path(task_id);
        let _path_lock = self.locks[lock_index(&path)].lock().await;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = self.file(&path).await?;
        let mut file = file.lock().await;
        let result = async {
            file.write_all(bytes).await?;
            #[cfg(test)]
            if self.fail_after_write.load(Ordering::Acquire) {
                return Err(std::io::Error::other("injected Item write failure"));
            }
            file.flush().await?;
            Ok::<(), std::io::Error>(())
        }
        .await;
        if let Err(error) = result {
            tracing::error!(path = %path.display(), error = %error, "failed to write Item output");
            return Err(item::Error::from(error));
        }
        Ok(())
    }

    pub(super) fn current_path(&self, task_id: &str) -> PathBuf {
        let hour = Local::now().format("%Y-%m-%d-%H").to_string();
        self.path(task_id, &hour)
    }

    fn path(&self, task_id: &str, hour: &str) -> PathBuf {
        self.dir
            .join("data")
            .join("items")
            .join("output")
            .join(crate::utils::path::segment(task_id))
            .join(format!("{hour}.jsonl"))
    }

    async fn file(&self, path: &Path) -> Result<Arc<Mutex<File>>, item::Error> {
        {
            let mut files = self.files.lock().await;
            if let Some(cached) = files.get_mut(path) {
                cached.touched = Instant::now();
                return Ok(cached.file.clone());
            }
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        let file = Arc::new(Mutex::new(file));
        let obsolete = {
            let mut files = self.files.lock().await;
            if let Some(cached) = files.get_mut(path) {
                cached.touched = Instant::now();
                return Ok(cached.file.clone());
            }
            let task_dir = path.parent();
            let mut obsolete = files
                .iter()
                .filter(|(candidate, cached)| {
                    candidate.parent() == task_dir
                        && candidate.as_path() != path
                        && Arc::strong_count(&cached.file) == 1
                })
                .map(|(path, _)| path.clone())
                .collect::<Vec<_>>()
                .into_iter()
                .filter_map(|path| files.remove(&path))
                .map(|cached| cached.file)
                .collect::<Vec<_>>();

            if files.len() >= MAX_CACHED_FILES
                && let Some(lru) = files
                    .iter()
                    .filter(|(_, cached)| Arc::strong_count(&cached.file) == 1)
                    .min_by_key(|(_, cached)| cached.touched)
                    .map(|(path, _)| path.clone())
                && let Some(cached) = files.remove(&lru)
            {
                obsolete.push(cached.file);
            }

            if files.len() < MAX_CACHED_FILES {
                files.insert(
                    path.to_path_buf(),
                    CachedFile {
                        file: file.clone(),
                        touched: Instant::now(),
                    },
                );
            }
            obsolete
        };
        for file in obsolete {
            file.lock().await.flush().await?;
        }
        Ok(file)
    }

    #[cfg(test)]
    pub(super) fn set_write_failure(&self, enabled: bool) {
        self.fail_after_write.store(enabled, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) async fn cached_files(&self) -> usize {
        self.files.lock().await.len()
    }

    #[cfg(test)]
    pub(super) async fn hold_cached_files(&self) -> Vec<Arc<Mutex<File>>> {
        self.files
            .lock()
            .await
            .values()
            .map(|cached| cached.file.clone())
            .collect()
    }
}

fn lock_index(path: &Path) -> usize {
    path.as_os_str()
        .as_encoded_bytes()
        .iter()
        .fold(0_usize, |hash, byte| {
            hash.wrapping_mul(31).wrapping_add(usize::from(*byte))
        })
        % WRITE_LOCKS
}
