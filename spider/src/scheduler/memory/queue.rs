use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::{net, scheduler};

pub(super) fn snapshot(request: net::Request) -> Result<net::request::Snapshot, scheduler::Error> {
    net::request::Snapshot::try_from(request).map_err(scheduler::Error::Message)
}

#[derive(Debug, Default)]
pub(super) struct Queue {
    ready: BinaryHeap<Ready>,
    delayed: BinaryHeap<Delayed>,
    sequence: u64,
}

impl Queue {
    pub(super) fn len(&self) -> usize {
        self.ready.len() + self.delayed.len()
    }

    pub(super) fn push(&mut self, snapshot: net::request::Snapshot, now: i64) {
        let entry = Entry {
            snapshot,
            sequence: self.sequence,
        };
        self.sequence = self.sequence.wrapping_add(1);
        if entry.snapshot.next_time <= now {
            self.ready.push(Ready(entry));
        } else {
            self.delayed.push(Delayed(entry));
        }
    }

    pub(super) fn pop(&mut self, now: i64) -> Option<net::request::Snapshot> {
        while self
            .delayed
            .peek()
            .is_some_and(|entry| entry.0.snapshot.next_time <= now)
        {
            let entry = self.delayed.pop()?.0;
            self.ready.push(Ready(entry));
        }
        self.ready.pop().map(|entry| entry.0.snapshot)
    }
}

#[derive(Debug)]
struct Entry {
    snapshot: net::request::Snapshot,
    sequence: u64,
}

#[derive(Debug)]
struct Ready(Entry);

impl PartialEq for Ready {
    fn eq(&self, other: &Self) -> bool {
        self.0.sequence == other.0.sequence
    }
}

impl Eq for Ready {}

impl PartialOrd for Ready {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ready {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .snapshot
            .priority
            .cmp(&other.0.snapshot.priority)
            .then_with(|| other.0.sequence.cmp(&self.0.sequence))
    }
}

#[derive(Debug)]
struct Delayed(Entry);

impl PartialEq for Delayed {
    fn eq(&self, other: &Self) -> bool {
        self.0.sequence == other.0.sequence
    }
}

impl Eq for Delayed {}

impl PartialOrd for Delayed {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Delayed {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .0
            .snapshot
            .next_time
            .cmp(&self.0.snapshot.next_time)
            .then_with(|| other.0.sequence.cmp(&self.0.sequence))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_requests_use_priority_then_fifo() {
        let mut queue = Queue::default();
        let mut low = net::Request::follow("https://example.com/low").unwrap();
        low.priority = 1;
        let mut first = net::Request::follow("https://example.com/first").unwrap();
        first.priority = 10;
        let mut second = net::Request::follow("https://example.com/second").unwrap();
        second.priority = 10;
        for request in [low, first, second] {
            queue.push(snapshot(request).unwrap(), 1);
        }

        assert!(
            queue
                .pop(1)
                .is_some_and(|snapshot| snapshot.url.ends_with("/first"))
        );
        assert!(
            queue
                .pop(1)
                .is_some_and(|snapshot| snapshot.url.ends_with("/second"))
        );
        assert!(
            queue
                .pop(1)
                .is_some_and(|snapshot| snapshot.url.ends_with("/low"))
        );
    }

    #[test]
    fn delayed_requests_are_promoted_by_time() {
        let mut queue = Queue::default();
        let mut request = net::Request::follow("https://example.com/later").unwrap();
        request.next_time = 20;
        queue.push(snapshot(request).unwrap(), 10);

        assert!(queue.pop(19).is_none());
        assert!(queue.pop(20).is_some());
    }
}
