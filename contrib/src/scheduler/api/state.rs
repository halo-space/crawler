use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use spider::trace;
use tokio::sync::{Mutex, RwLock, oneshot};
use tokio::task::JoinHandle;

use super::wire;

pub(super) struct Runtime {
    pub(super) opened: AtomicBool,
    pub(super) epoch: AtomicU64,
    pub(super) lifecycle: Mutex<()>,
    pub(super) activity: RwLock<()>,
    pub(super) heartbeat: std::sync::Mutex<Option<Heartbeat>>,
    registration: std::sync::Mutex<Registration>,
    pub(super) token: std::sync::Mutex<Option<String>>,
    pub(super) can_claim: AtomicBool,
    pub(super) traces: Mutex<TraceCache>,
}

pub(super) struct Heartbeat {
    pub(super) stop: oneshot::Sender<()>,
    pub(super) task: JoinHandle<()>,
}

#[derive(Default)]
struct Registration {
    key: Option<String>,
    concurrency: Option<usize>,
}

impl Runtime {
    pub(super) fn new(trace_cache_capacity: usize, trace_cache_bytes: usize) -> Self {
        Self {
            opened: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            lifecycle: Mutex::new(()),
            activity: RwLock::new(()),
            heartbeat: std::sync::Mutex::new(None),
            registration: std::sync::Mutex::new(Registration::default()),
            token: std::sync::Mutex::new(None),
            can_claim: AtomicBool::new(false),
            traces: Mutex::new(TraceCache::new(trace_cache_capacity, trace_cache_bytes)),
        }
    }

    pub(super) fn is_open(&self, epoch: u64) -> bool {
        self.opened.load(Ordering::Acquire) && self.epoch.load(Ordering::Acquire) == epoch
    }

    pub(super) fn set_token(&self, token: String) {
        *self
            .token
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(token);
    }

    pub(super) fn token(&self) -> Option<String> {
        self.token
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(super) fn take_token(&self) -> Option<String> {
        self.token
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    pub(super) fn check_concurrency(&self, concurrency: usize) -> Result<(), String> {
        let registration = self
            .registration
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(active) = registration.concurrency {
            if active == concurrency {
                Ok(())
            } else {
                Err(format!(
                    "API Scheduler Worker lifecycle is frozen with concurrency {active}; received {concurrency}"
                ))
            }
        } else {
            Ok(())
        }
    }

    pub(super) fn concurrency(&self) -> Option<usize> {
        self.registration
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .concurrency
    }

    pub(super) fn open_key(&self, concurrency: usize) -> Result<String, String> {
        let mut registration = self
            .registration
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(active) = registration.concurrency
            && active != concurrency
        {
            return Err(format!(
                "API Scheduler Worker lifecycle is frozen with concurrency {active}; received {concurrency}"
            ));
        }
        registration.concurrency = Some(concurrency);
        Ok(registration
            .key
            .get_or_insert_with(|| uuid::Uuid::now_v7().to_string())
            .clone())
    }

    pub(super) fn finish_registration(&self) {
        self.registration
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .key = None;
    }

    pub(super) fn clear_registration(&self) {
        *self
            .registration
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Registration::default();
    }

    pub(super) fn is_configurable(&self) -> bool {
        !self.opened.load(Ordering::Acquire)
            && self.concurrency().is_none()
            && self
                .token
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none()
    }

    pub(super) fn set_heartbeat(&self, heartbeat: Heartbeat) {
        *self
            .heartbeat
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(heartbeat);
    }

    pub(super) fn take_heartbeat(&self) -> Option<Heartbeat> {
        self.heartbeat
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    pub(super) fn abandon(&self) {
        self.opened.store(false, Ordering::Release);
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.can_claim.store(false, Ordering::Release);
        self.clear_registration();
        self.take_token();
        if let Some(heartbeat) = self.take_heartbeat() {
            heartbeat.abort();
        }
    }
}

impl Heartbeat {
    pub(super) async fn stop(self) {
        let _ = self.stop.send(());
        let _ = self.task.await;
    }

    fn abort(self) {
        let _ = self.stop.send(());
        self.task.abort();
    }
}

pub(super) struct TraceCache {
    capacity: usize,
    max_bytes: usize,
    bytes: usize,
    clock: u64,
    values: HashMap<String, TraceEntry>,
}

struct TraceEntry {
    snapshot: Arc<trace::Snapshot>,
    digest: String,
    bytes: usize,
    used: u64,
}

impl TraceCache {
    pub(super) fn new(capacity: usize, max_bytes: usize) -> Self {
        Self {
            capacity,
            max_bytes,
            bytes: 0,
            clock: 0,
            values: HashMap::with_capacity(capacity),
        }
    }

    pub(super) fn get(&mut self, id: &str) -> Option<Arc<trace::Snapshot>> {
        self.clock = self.clock.saturating_add(1);
        let entry = self.values.get_mut(id)?;
        entry.used = self.clock;
        Some(entry.snapshot.clone())
    }

    pub(super) fn insert(
        &mut self,
        id: String,
        snapshot: trace::Snapshot,
    ) -> Result<Arc<trace::Snapshot>, String> {
        self.clock = self.clock.saturating_add(1);
        let (digest, bytes) =
            wire::canonical_fingerprint(&snapshot).map_err(|error| error.to_string())?;
        if let Some(entry) = self.values.get_mut(&id) {
            if entry.digest != digest {
                return Err("immutable Trace Snapshot changed between responses".to_string());
            }
            entry.used = self.clock;
            return Ok(entry.snapshot.clone());
        }
        let snapshot = Arc::new(snapshot);
        if self.capacity == 0 || self.max_bytes == 0 || bytes > self.max_bytes {
            return Ok(snapshot);
        }
        while self.values.len() >= self.capacity
            || self.bytes.saturating_add(bytes) > self.max_bytes
        {
            let Some(oldest) = self
                .values
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(id, _)| id.clone())
            else {
                break;
            };
            if let Some(removed) = self.values.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(removed.bytes);
            }
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.values.insert(
            id,
            TraceEntry {
                snapshot: snapshot.clone(),
                digest,
                bytes,
                used: self.clock,
            },
        );
        Ok(snapshot)
    }

    pub(super) fn clear(&mut self) {
        self.bytes = 0;
        self.clock = 0;
        self.values.clear();
    }
}
