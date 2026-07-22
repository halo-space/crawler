use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::{middleware, net};

pub(super) type Occurrences = Arc<Mutex<State>>;

#[derive(Default)]
pub(super) struct State {
    counters: HashMap<[u8; 32], Counter>,
}

#[derive(Default)]
struct Counter {
    next: u64,
    available: BTreeSet<u64>,
}

pub(crate) struct Reservation {
    occurrences: Occurrences,
    slots: Vec<([u8; 32], u64)>,
    committed: bool,
}

/// Assigns replay-stable IDs to framework-created child Requests.
///
/// The allocator is scoped to one parent Request execution. A new parse retry
/// receives a new allocator, so the same logical output gets the same ID while
/// two intentional identical outputs in one execution receive different IDs.
pub(super) fn assign(
    requests: &mut [net::Request],
    parent_id: &str,
    occurrences: &Occurrences,
) -> Result<Reservation, crate::Error> {
    let mut reservation = Reservation::new(occurrences.clone());
    if parent_id.is_empty() {
        return Ok(reservation);
    }

    let mut assigned = Vec::new();
    for (index, request) in requests.iter().enumerate() {
        if !request.has_unassigned_id() {
            continue;
        }

        let fingerprint = fingerprint(request)?;
        let occurrence = reservation.reserve(fingerprint)?;
        let mut hasher = Sha256::new();
        hasher.update(b"spider-request-output-v1\0");
        hasher.update(parent_id.as_bytes());
        hasher.update([0]);
        hasher.update(fingerprint);
        hasher.update(occurrence.to_be_bytes());
        let id = format!("req_{:x}", hasher.finalize());
        assigned.push((index, id));
    }

    for (index, id) in assigned {
        requests[index].assign_id(id);
    }
    Ok(reservation)
}

impl Reservation {
    fn new(occurrences: Occurrences) -> Self {
        Self {
            occurrences,
            slots: Vec::new(),
            committed: false,
        }
    }

    fn reserve(&mut self, fingerprint: [u8; 32]) -> Result<u64, crate::Error> {
        let mut state = self
            .occurrences
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let counter = state.counters.entry(fingerprint).or_default();
        let occurrence = if let Some(occurrence) = counter.available.pop_first() {
            occurrence
        } else {
            let occurrence = counter.next;
            counter.next = counter.next.checked_add(1).ok_or_else(|| {
                crate::Error::message("Request output occurrence counter overflowed")
            })?;
            occurrence
        };
        self.slots.push((fingerprint, occurrence));
        Ok(occurrence)
    }

    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.committed || self.slots.is_empty() {
            return;
        }
        let mut state = self
            .occurrences
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (fingerprint, occurrence) in self.slots.drain(..) {
            if let Some(counter) = state.counters.get_mut(&fingerprint) {
                counter.available.insert(occurrence);
            }
        }
    }
}

fn fingerprint(request: &net::Request) -> Result<[u8; 32], crate::Error> {
    let value = serde_json::to_value(View {
        task_id: &request.task_id,
        trace_id: &request.trace_id,
        node: request.node_key(),
        protocol: &request.protocol,
        url: &request.url,
        method: &request.method,
        headers: &request.headers,
        body: Body::new(&request.body),
        cookies: request.cookies.records(),
        vals: &request.vals,
        kwargs: &request.kwargs,
        priority: request.priority,
        dont_filter: request.dont_filter,
        next_time: request.next_time,
        mode: &request.mode,
        timeout: request.timeout,
        max_body_bytes: request.max_body_bytes,
        proxy: &request.proxy,
        tls: &request.tls,
        middlewares: &request.middlewares,
        max_retry_count: request.max_retry_count,
    })
    .map_err(|error| crate::Error::message(format!("cannot derive Request ID: {error}")))?;
    let value = canonicalize(value);
    let bytes = serde_json::to_vec(&value)
        .map_err(|error| crate::Error::message(format!("cannot derive Request ID: {error}")))?;
    Ok(Sha256::digest(bytes).into())
}

#[derive(Serialize)]
struct View<'a> {
    task_id: &'a str,
    trace_id: &'a str,
    node: &'a str,
    protocol: &'a net::Protocol,
    url: &'a str,
    method: &'a net::Method,
    headers: &'a net::Headers,
    body: Body<'a>,
    cookies: net::cookies::Records<'a>,
    vals: &'a HashMap<String, Value>,
    kwargs: &'a HashMap<String, Value>,
    priority: i32,
    dont_filter: bool,
    next_time: i64,
    mode: &'a net::Mode,
    timeout: Option<u64>,
    max_body_bytes: Option<u64>,
    proxy: &'a Option<net::ProxyConfig>,
    tls: &'a Option<net::TlsConfig>,
    middlewares: &'a [middleware::Spec],
    max_retry_count: i32,
}

#[derive(Serialize)]
enum Body<'a> {
    Empty,
    Bytes { len: u64, sha256: [u8; 32] },
    Text(&'a str),
    Json(&'a Value),
}

impl<'a> Body<'a> {
    fn new(body: &'a net::Body) -> Self {
        match body {
            net::Body::Empty => Self::Empty,
            net::Body::Bytes(bytes) => Self::Bytes {
                len: bytes.len() as u64,
                sha256: Sha256::digest(bytes).into(),
            },
            net::Body::Text(value) => Self::Text(value),
            net::Body::Json(value) => Self::Json(value),
        }
    }
}

fn canonicalize(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize).collect()),
        Value::Object(values) => {
            let mut entries = values.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            let mut object = Map::with_capacity(entries.len());
            for (key, value) in entries {
                object.insert(key, canonicalize(value));
            }
            Value::Object(object)
        }
        value => value,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn requests() -> Vec<net::Request> {
        vec![net::Request::follow("https://example.com/detail").unwrap()]
    }

    #[test]
    fn replay_reuses_ids_with_a_fresh_allocator() {
        let mut first = requests();
        let first_allocator = Occurrences::default();
        assign(&mut first, "req-parent", &first_allocator).unwrap();

        let mut second = requests();
        let second_allocator = Occurrences::default();
        assign(&mut second, "req-parent", &second_allocator).unwrap();

        assert_eq!(first[0].id, second[0].id);
    }

    #[test]
    fn identical_outputs_in_one_execution_get_distinct_ids() {
        let mut requests = vec![
            net::Request::follow("https://example.com/detail").unwrap(),
            net::Request::follow("https://example.com/detail").unwrap(),
        ];
        let allocator = Occurrences::default();
        assign(&mut requests, "req-parent", &allocator).unwrap();

        assert_ne!(requests[0].id, requests[1].id);
    }

    #[test]
    fn map_insertion_order_does_not_change_the_id() {
        let mut first = net::Request::follow("https://example.com/detail").unwrap();
        first.vals.insert("b".to_string(), Value::from(2));
        first.vals.insert("a".to_string(), Value::from(1));
        let mut second = net::Request::follow("https://example.com/detail").unwrap();
        second.vals.insert("a".to_string(), Value::from(1));
        second.vals.insert("b".to_string(), Value::from(2));

        assign(
            std::slice::from_mut(&mut first),
            "req-parent",
            &Occurrences::default(),
        )
        .unwrap();
        assign(
            std::slice::from_mut(&mut second),
            "req-parent",
            &Occurrences::default(),
        )
        .unwrap();

        assert_eq!(first.id, second.id);
    }

    #[test]
    fn explicit_ids_are_preserved() {
        let mut assigned = net::Request::follow("https://example.com/direct").unwrap();
        assigned.id = "direct-request".to_string();
        let mut requests = vec![
            net::Request::follow("https://example.com/detail")
                .unwrap()
                .with_id("business-request"),
            assigned,
        ];
        assign(&mut requests, "req-parent", &Occurrences::default()).unwrap();

        assert_eq!(requests[0].id, "business-request");
        assert_eq!(requests[1].id, "direct-request");
    }

    #[test]
    fn detached_outputs_keep_their_generated_ids() {
        let mut requests = requests();
        let before = requests[0].id.clone();
        assign(&mut requests, "", &Occurrences::default()).unwrap();

        assert_eq!(requests[0].id, before);
    }

    #[test]
    fn next_time_is_part_of_the_child_specification() {
        let mut immediate = requests();
        let mut delayed = requests();
        delayed[0].next_time = 42;
        assign(
            std::slice::from_mut(&mut immediate[0]),
            "req-parent",
            &Occurrences::default(),
        )
        .unwrap();
        assign(
            std::slice::from_mut(&mut delayed[0]),
            "req-parent",
            &Occurrences::default(),
        )
        .unwrap();

        assert_ne!(immediate[0].id, delayed[0].id);
    }

    #[test]
    fn failed_assignment_returns_occurrences_to_the_allocator() {
        let occurrences = Occurrences::default();
        let mut first = requests();
        let first_id = {
            let reservation = assign(&mut first, "req-parent", &occurrences).unwrap();
            let id = first[0].id.clone();
            drop(reservation);
            id
        };

        let mut retry = requests();
        let reservation = assign(&mut retry, "req-parent", &occurrences).unwrap();
        assert_eq!(retry[0].id, first_id);
        reservation.commit();

        let mut distinct = requests();
        let reservation = assign(&mut distinct, "req-parent", &occurrences).unwrap();
        assert_ne!(distinct[0].id, first_id);
        reservation.commit();
    }

    #[tokio::test]
    async fn cookie_expiry_does_not_change_replayed_ids() {
        let url = url::Url::parse("https://example.com/detail").unwrap();
        let mut cookies = net::Cookies::new();
        let mut headers = net::Headers::new();
        headers
            .try_append("set-cookie", "sid=one; Max-Age=1; Path=/")
            .unwrap();
        cookies.store_response(&url, &headers);

        let mut first = requests();
        first[0].cookies = cookies.clone();
        assign(&mut first, "req-parent", &Occurrences::default()).unwrap();

        tokio::time::sleep(Duration::from_millis(1_100)).await;
        assert!(cookies.is_empty());

        let mut replay = requests();
        replay[0].cookies = cookies;
        assign(&mut replay, "req-parent", &Occurrences::default()).unwrap();

        assert_eq!(first[0].id, replay[0].id);
    }
}
