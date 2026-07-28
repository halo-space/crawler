use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use spider::{net, trace};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

use super::wire;

pub(super) struct Runtime {
    pub(super) opened: AtomicBool,
    pub(super) epoch: AtomicU64,
    pub(super) lifecycle: Mutex<()>,
    pub(super) activity: RwLock<()>,
    pub(super) heartbeat_interval: RwLock<Duration>,
    pub(super) workers: Mutex<HashMap<String, Worker>>,
    pub(super) traces: Mutex<TraceCache>,
}

impl Runtime {
    pub(super) fn new(
        heartbeat_interval: Duration,
        trace_cache_capacity: usize,
        trace_cache_bytes: usize,
    ) -> Self {
        Self {
            opened: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            lifecycle: Mutex::new(()),
            activity: RwLock::new(()),
            heartbeat_interval: RwLock::new(heartbeat_interval),
            workers: Mutex::new(HashMap::new()),
            traces: Mutex::new(TraceCache::new(trace_cache_capacity, trace_cache_bytes)),
        }
    }

    pub(super) fn is_open(&self, epoch: u64) -> bool {
        self.opened.load(Ordering::Acquire) && self.epoch.load(Ordering::Acquire) == epoch
    }
}

pub(super) struct Worker {
    pub(super) registration: Arc<Mutex<Registration>>,
    pub(super) task: JoinHandle<()>,
}

pub(super) struct Registration {
    pub(super) modes: Vec<net::Mode>,
    pub(super) confirmed: bool,
}

impl Registration {
    pub(super) fn new(modes: Vec<net::Mode>) -> Self {
        Self {
            modes,
            confirmed: false,
        }
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
