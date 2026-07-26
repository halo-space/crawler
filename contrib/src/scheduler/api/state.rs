use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use spider::{net, trace};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::wire;

pub(super) struct Runtime {
    pub(super) opened: AtomicBool,
    pub(super) epoch: AtomicU64,
    pub(super) lifecycle: Mutex<()>,
    pub(super) activity: RwLock<()>,
    pub(super) heartbeat_interval: RwLock<Duration>,
    pub(super) workers: Mutex<HashMap<String, Worker>>,
    pub(super) operations: Mutex<Operations>,
    pub(super) traces: Mutex<TraceCache>,
}

pub(super) const OPERATION_TTL: Duration = Duration::from_secs(5 * 60);
pub(super) const OPERATION_CAPACITY: usize = 4096;

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
            operations: Mutex::new(Operations::new(OPERATION_TTL, OPERATION_CAPACITY)),
            traces: Mutex::new(TraceCache::new(trace_cache_capacity, trace_cache_bytes)),
        }
    }

    pub(super) fn is_open(&self, epoch: u64) -> bool {
        self.opened.load(Ordering::Acquire) && self.epoch.load(Ordering::Acquire) == epoch
    }
}

pub(super) struct Worker {
    pub(super) modes: Arc<RwLock<Vec<net::Mode>>>,
    pub(super) task: JoinHandle<()>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct Operation {
    task: Option<tokio::task::Id>,
    action: Action,
}

impl Operation {
    pub(super) fn new(action: Action) -> Self {
        Self {
            task: tokio::task::try_id(),
            action,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum Action {
    Init(String),
}

pub(super) struct Operations {
    ttl: Duration,
    capacity: usize,
    values: HashMap<Operation, OperationEntry>,
}

struct OperationEntry {
    key: String,
    expires_at: Instant,
}

impl Operations {
    pub(super) fn new(ttl: Duration, capacity: usize) -> Self {
        Self {
            ttl,
            capacity,
            values: HashMap::with_capacity(capacity),
        }
    }

    pub(super) fn key(&mut self, operation: Operation) -> Result<String, spider::scheduler::Error> {
        let now = Instant::now();
        self.values.retain(|_, entry| entry.expires_at > now);

        if operation.task.is_none() {
            return Ok(uuid::Uuid::now_v7().to_string());
        }

        if let Some(entry) = self.values.get(&operation) {
            return Ok(entry.key.clone());
        }
        if self.values.len() >= self.capacity {
            return Err(spider::scheduler::Error::Unavailable(format!(
                "API Scheduler has {} unresolved Init operations",
                self.capacity
            )));
        }

        let key = uuid::Uuid::now_v7().to_string();
        self.values.insert(
            operation,
            OperationEntry {
                key: key.clone(),
                expires_at: now + self.ttl,
            },
        );
        Ok(key)
    }

    pub(super) fn remove(&mut self, operation: &Operation) {
        self.values.remove(operation);
    }

    pub(super) fn clear(&mut self) {
        self.values.clear();
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
