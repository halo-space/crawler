use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::{net, scheduler};

pub(super) fn snapshot(request: net::Request) -> Result<net::request::Snapshot, scheduler::Error> {
    net::request::Snapshot::try_from(request).map_err(scheduler::Error::Message)
}

#[derive(Debug, Default)]
pub(super) struct Queue {
    domains: Vec<Domain>,
    sequence: u64,
}

impl Queue {
    pub(super) fn len(&self) -> usize {
        self.domains.iter().map(Domain::len).sum()
    }

    pub(super) fn push(&mut self, snapshot: net::request::Snapshot, now: i64) {
        let mode = snapshot.mode.clone();
        let entry = Entry {
            snapshot,
            sequence: self.sequence,
        };
        self.sequence = self.sequence.wrapping_add(1);
        self.domain(&mode).push(entry, now);
    }

    pub(super) fn take(
        &mut self,
        now: i64,
        limit: usize,
        supported_modes: &[net::Mode],
    ) -> Vec<net::request::Snapshot> {
        for domain in &mut self.domains {
            if supported_modes.contains(&domain.mode) {
                domain.promote(now);
            }
        }

        let mut selected = Vec::with_capacity(limit.min(self.len()));
        while selected.len() < limit {
            let Some(index) = self.next_supported(supported_modes) else {
                break;
            };
            let entry = self.domains[index]
                .ready
                .pop()
                .expect("selected Request must still exist");
            selected.push(entry.0.snapshot);
        }
        selected
    }

    pub(super) fn contains_supported(&self, supported_modes: &[net::Mode]) -> bool {
        self.domains
            .iter()
            .any(|domain| !domain.is_empty() && supported_modes.contains(&domain.mode))
    }

    fn domain(&mut self, mode: &net::Mode) -> &mut Domain {
        if let Some(index) = self.domains.iter().position(|domain| &domain.mode == mode) {
            return &mut self.domains[index];
        }
        self.domains.push(Domain::new(mode.clone()));
        self.domains
            .last_mut()
            .expect("inserted Request domain must exist")
    }

    fn next_supported(&self, supported_modes: &[net::Mode]) -> Option<usize> {
        self.domains
            .iter()
            .enumerate()
            .filter(|(_, domain)| supported_modes.contains(&domain.mode))
            .filter_map(|(index, domain)| domain.ready.peek().map(|entry| (index, entry)))
            .max_by(|(_, left), (_, right)| left.cmp(right))
            .map(|(index, _)| index)
    }
}

#[derive(Debug)]
struct Domain {
    mode: net::Mode,
    ready: BinaryHeap<Ready>,
    delayed: BinaryHeap<Delayed>,
}

impl Domain {
    fn new(mode: net::Mode) -> Self {
        Self {
            mode,
            ready: BinaryHeap::new(),
            delayed: BinaryHeap::new(),
        }
    }

    fn len(&self) -> usize {
        self.ready.len() + self.delayed.len()
    }

    fn is_empty(&self) -> bool {
        self.ready.is_empty() && self.delayed.is_empty()
    }

    fn push(&mut self, entry: Entry, now: i64) {
        if entry.snapshot.next_time <= now {
            self.ready.push(Ready(entry));
        } else {
            self.delayed.push(Delayed(entry));
        }
    }

    fn promote(&mut self, now: i64) {
        while self
            .delayed
            .peek()
            .is_some_and(|entry| entry.0.snapshot.next_time <= now)
        {
            let entry = self
                .delayed
                .pop()
                .expect("peeked delayed Request must still exist")
                .0;
            self.ready.push(Ready(entry));
        }
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
                .take(1, 1, &[net::Mode::Http])
                .pop()
                .is_some_and(|snapshot| snapshot.url.ends_with("/first"))
        );
        assert!(
            queue
                .take(1, 1, &[net::Mode::Http])
                .pop()
                .is_some_and(|snapshot| snapshot.url.ends_with("/second"))
        );
        assert!(
            queue
                .take(1, 1, &[net::Mode::Http])
                .pop()
                .is_some_and(|snapshot| snapshot.url.ends_with("/low"))
        );
    }

    #[test]
    fn delayed_requests_are_promoted_by_time() {
        let mut queue = Queue::default();
        let mut request = net::Request::follow("https://example.com/later").unwrap();
        request.next_time = 20;
        queue.push(snapshot(request).unwrap(), 10);

        assert!(queue.take(19, 1, &[net::Mode::Http]).is_empty());
        assert_eq!(queue.take(20, 1, &[net::Mode::Http]).len(), 1);
    }

    #[test]
    fn capability_filter_preserves_incompatible_snapshots_and_order() {
        let mut queue = Queue::default();
        let mut browser_first = net::Request::follow("https://example.com/browser-first")
            .unwrap()
            .mode(net::Mode::Browser);
        browser_first.priority = 10;
        let mut browser_second = net::Request::follow("https://example.com/browser-second")
            .unwrap()
            .mode(net::Mode::Browser);
        browser_second.priority = 10;
        let mut http = net::Request::follow("https://example.com/http").unwrap();
        http.priority = 1;
        for request in [browser_first, browser_second, http] {
            queue.push(snapshot(request).unwrap(), 1);
        }

        let claimed = queue.take(1, 1, &[net::Mode::Http]).pop().unwrap();
        assert!(claimed.url.ends_with("/http"));

        for suffix in ["/browser-first", "/browser-second"] {
            let untouched = queue.take(1, 1, &[net::Mode::Browser]).pop().unwrap();
            assert!(untouched.url.ends_with(suffix));
            assert_eq!(untouched.state, net::State::Pending);
            assert_eq!(untouched.version, 0);
            assert!(untouched.leased_by.is_empty());
            assert_eq!(untouched.lease_time, 0);
        }
    }

    #[test]
    fn claim_returns_multiple_eligible_requests_behind_incompatible_work() {
        let mut queue = Queue::default();
        for index in 0..4 {
            let mut browser = net::Request::follow(format!("https://example.com/browser/{index}"))
                .unwrap()
                .mode(net::Mode::Browser);
            browser.priority = 10;
            queue.push(snapshot(browser).unwrap(), 1);
        }
        for index in 0..3 {
            let mut http =
                net::Request::follow(format!("https://example.com/http/{index}")).unwrap();
            http.priority = 1;
            queue.push(snapshot(http).unwrap(), 1);
        }

        let claimed = queue.take(1, 3, &[net::Mode::Http]);

        assert_eq!(
            claimed
                .iter()
                .map(|snapshot| snapshot.url.as_str())
                .collect::<Vec<_>>(),
            [
                "https://example.com/http/0",
                "https://example.com/http/1",
                "https://example.com/http/2"
            ]
        );
        assert!(queue.contains_supported(&[net::Mode::Browser]));
        assert!(!queue.contains_supported(&[net::Mode::Http]));
    }

    #[test]
    fn supported_modes_share_global_priority_and_fifo_order() {
        let mut queue = Queue::default();
        let mut http_first = net::Request::follow("https://example.com/http-first").unwrap();
        http_first.priority = 10;
        let mut browser_second = net::Request::follow("https://example.com/browser-second")
            .unwrap()
            .mode(net::Mode::Browser);
        browser_second.priority = 10;
        let mut browser_high = net::Request::follow("https://example.com/browser-high")
            .unwrap()
            .mode(net::Mode::Browser);
        browser_high.priority = 20;
        for request in [http_first, browser_second, browser_high] {
            queue.push(snapshot(request).unwrap(), 1);
        }

        let claimed = queue.take(1, 3, &[net::Mode::Http, net::Mode::Browser]);

        assert_eq!(
            claimed
                .iter()
                .map(|snapshot| snapshot.url.as_str())
                .collect::<Vec<_>>(),
            [
                "https://example.com/browser-high",
                "https://example.com/http-first",
                "https://example.com/browser-second"
            ]
        );
    }

    #[test]
    fn delayed_incompatible_request_remains_unchanged() {
        let mut queue = Queue::default();
        let mut browser = net::Request::follow("https://example.com/browser")
            .unwrap()
            .mode(net::Mode::Browser);
        browser.next_time = 20;
        queue.push(snapshot(browser).unwrap(), 10);

        assert!(queue.take(19, 1, &[net::Mode::Http]).is_empty());
        assert!(queue.take(20, 1, &[net::Mode::Http]).is_empty());
        let untouched = queue.take(20, 1, &[net::Mode::Browser]).pop().unwrap();

        assert_eq!(untouched.state, net::State::Pending);
        assert_eq!(untouched.version, 0);
        assert!(untouched.leased_by.is_empty());
        assert_eq!(untouched.lease_time, 0);
    }
}
