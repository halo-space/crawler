use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use serde_json::Value;

use crate::stats::Counter;

#[derive(Default)]
pub(crate) struct Delta {
    counters: Mutex<HashMap<String, Counter>>,
}

impl Delta {
    pub(crate) fn total(&self, name: &str, count: usize) {
        self.update(name, |counter| counter.total += count as i64);
    }

    pub(crate) fn done(&self, name: &str, count: usize) {
        self.update(name, |counter| counter.done += count as i64);
    }

    pub(crate) fn filter(&self, name: &str, count: usize) {
        self.update(name, |counter| counter.filter += count as i64);
    }

    pub(crate) fn dedup(&self, name: &str, count: usize) {
        self.update(name, |counter| counter.dedup += count as i64);
    }

    pub(crate) fn validate(&self, name: &str, count: usize) {
        self.update(name, |counter| counter.validate += count as i64);
    }

    pub(crate) fn download(&self, name: &str, count: usize) {
        self.update(name, |counter| counter.download += count as i64);
    }

    pub(crate) fn snapshot(&self) -> HashMap<String, Value> {
        self.lock()
            .iter()
            .filter(|(_, counter)| !counter.is_empty())
            .filter_map(|(name, counter)| {
                serde_json::to_value(counter)
                    .ok()
                    .map(|value| (name.clone(), value))
            })
            .collect()
    }

    fn update(&self, name: &str, update: impl FnOnce(&mut Counter)) {
        let mut counters = self.lock();
        update(counters.entry(name.to_string()).or_default());
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Counter>> {
        self.counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulates_and_serializes_counters() {
        let delta = Delta::default();
        delta.total("detail", 2);
        delta.done("detail", 1);
        delta.validate("detail", 1);

        assert_eq!(
            delta.snapshot()["detail"],
            serde_json::json!({
                "total": 2,
                "done": 1,
                "filter": 0,
                "dedup": 0,
                "validate": 1,
                "download": 0
            })
        );
    }
}
