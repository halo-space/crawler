use std::collections::HashMap;
use std::io;

use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{middleware, net, scheduler};

pub(super) fn of(snapshot: &net::request::Snapshot) -> Result<[u8; 32], scheduler::Error> {
    let view = View::new(snapshot);
    let mut hasher = Sha256::new();
    hasher.update(b"memory-request-snapshot-v1\0");
    serde_json::to_writer(Sink(&mut hasher), &view).map_err(|error| {
        scheduler::Error::Message(format!("Request Snapshot cannot be serialized: {error}"))
    })?;
    Ok(hasher.finalize().into())
}

#[derive(Serialize)]
struct View<'a> {
    id: &'a str,
    task_id: &'a str,
    trace_id: &'a str,
    node: &'a str,
    protocol: &'a net::Protocol,
    url: &'a str,
    method: &'a net::Method,
    headers: &'a net::Headers,
    body: Body<'a>,
    cookies: net::cookies::Records<'a>,
    vals: Values<'a>,
    kwargs: Values<'a>,
    priority: i32,
    dont_filter: bool,
    mode: &'a net::Mode,
    timeout: Option<u64>,
    max_body_bytes: Option<u64>,
    proxy: &'a Option<net::ProxyConfig>,
    tls: &'a Option<net::TlsConfig>,
    middlewares: Specs<'a>,
    state: net::State,
    version: i64,
    next_time: i64,
    leased_by: &'a str,
    lease_time: i64,
    retry_count: i32,
    max_retry_count: i32,
    failed_workers: &'a [String],
}

impl<'a> View<'a> {
    fn new(snapshot: &'a net::request::Snapshot) -> Self {
        let net::request::Snapshot {
            id,
            task_id,
            trace_id,
            node,
            protocol,
            url,
            method,
            headers,
            body,
            cookies,
            vals,
            kwargs,
            priority,
            dont_filter,
            mode,
            timeout,
            max_body_bytes,
            proxy,
            tls,
            middlewares,
            state,
            version,
            next_time,
            leased_by,
            lease_time,
            retry_count,
            max_retry_count,
            failed_workers,
        } = snapshot;
        Self {
            id,
            task_id,
            trace_id,
            node,
            protocol,
            url,
            method,
            headers,
            body: Body::new(body),
            cookies: cookies.records(),
            vals: Values(vals),
            kwargs: Values(kwargs),
            priority: *priority,
            dont_filter: *dont_filter,
            mode,
            timeout: *timeout,
            max_body_bytes: *max_body_bytes,
            proxy,
            tls,
            middlewares: Specs(middlewares),
            state: *state,
            version: *version,
            next_time: *next_time,
            leased_by,
            lease_time: *lease_time,
            retry_count: *retry_count,
            max_retry_count: *max_retry_count,
            failed_workers,
        }
    }
}

#[derive(Serialize)]
enum Body<'a> {
    Empty,
    Bytes { len: u64, sha256: [u8; 32] },
    Text(&'a str),
    Json(Canonical<'a>),
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
            net::Body::Json(value) => Self::Json(Canonical(value)),
        }
    }
}

struct Values<'a>(&'a HashMap<String, Value>);

impl Serialize for Values<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut keys = self.0.keys().collect::<Vec<_>>();
        keys.sort_unstable();
        let mut map = serializer.serialize_map(Some(keys.len()))?;
        for key in keys {
            map.serialize_entry(key, &Canonical(&self.0[key]))?;
        }
        map.end()
    }
}

struct Specs<'a>(&'a [middleware::Spec]);

impl Serialize for Specs<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for spec in self.0 {
            sequence.serialize_element(&Spec {
                hook: &spec.hook,
                name: &spec.name,
                key: &spec.key,
                order: spec.order,
                skip: spec.skip,
                args: Canonical(&spec.args),
            })?;
        }
        sequence.end()
    }
}

#[derive(Serialize)]
struct Spec<'a> {
    hook: &'a Option<String>,
    name: &'a str,
    key: &'a Option<String>,
    order: Option<i32>,
    skip: bool,
    args: Canonical<'a>,
}

struct Canonical<'a>(&'a Value);

impl Serialize for Canonical<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            Value::Null => serializer.serialize_unit(),
            Value::Bool(value) => serializer.serialize_bool(*value),
            Value::Number(value) => value.serialize(serializer),
            Value::String(value) => serializer.serialize_str(value),
            Value::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    sequence.serialize_element(&Canonical(value))?;
                }
                sequence.end()
            }
            Value::Object(values) => {
                let mut keys = values.keys().collect::<Vec<_>>();
                keys.sort_unstable();
                let mut map = serializer.serialize_map(Some(keys.len()))?;
                for key in keys {
                    map.serialize_entry(key, &Canonical(&values[key]))?;
                }
                map.end()
            }
        }
    }
}

struct Sink<'a>(&'a mut Sha256);

impl io::Write for Sink<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn snapshot(body: net::Body) -> net::request::Snapshot {
        let mut request = net::Request::follow("https://example.com").unwrap();
        request.body = body;
        net::request::Snapshot::try_from(request).unwrap()
    }

    fn assert_changed(
        base: &net::request::Snapshot,
        field: &str,
        change: impl FnOnce(&mut net::request::Snapshot),
    ) {
        let expected = of(base).unwrap();
        let mut changed = base.clone();
        change(&mut changed);
        assert_ne!(expected, of(&changed).unwrap(), "{field} was not hashed");
    }

    #[test]
    fn binary_body_is_hashed_without_expanding_it_to_json_numbers() {
        let bytes = Bytes::from(vec![0xA5; 8 * 1024 * 1024]);
        let body = net::Body::Bytes(bytes);
        let encoded = serde_json::to_vec(&Body::new(&body)).unwrap();
        let snapshot = snapshot(body);
        let first = of(&snapshot).unwrap();
        let second = of(&snapshot).unwrap();
        let mut json = snapshot.clone();
        json.body = net::Body::Json(serde_json::json!([]));

        assert!(encoded.len() < 256);
        assert_eq!(first, second);
        assert_ne!(first, of(&json).unwrap());
    }

    #[test]
    fn request_execution_fields_participate_in_the_digest() {
        let base = snapshot(net::Body::Empty);
        assert_changed(&base, "headers", |snapshot| {
            snapshot.headers.try_set("x-test", "one").unwrap();
        });
        assert_changed(&base, "cookies", |snapshot| {
            snapshot
                .cookies
                .insert(
                    &url::Url::parse("https://example.com").unwrap(),
                    "sid",
                    "one",
                )
                .unwrap();
        });
        assert_changed(&base, "vals", |snapshot| {
            snapshot
                .vals
                .insert("value".to_string(), serde_json::json!({"b": 2, "a": 1}));
        });
        assert_changed(&base, "kwargs", |snapshot| {
            snapshot
                .kwargs
                .insert("value".to_string(), serde_json::json!([1, 2]));
        });
        assert_changed(&base, "mode", |snapshot| {
            snapshot.mode = net::Mode::Browser;
        });
        assert_changed(&base, "timeout", |snapshot| {
            snapshot.timeout = Some(1_000);
        });
        assert_changed(&base, "max_body_bytes", |snapshot| {
            snapshot.max_body_bytes = Some(1_024);
        });
        assert_changed(&base, "proxy", |snapshot| {
            snapshot.proxy = Some(net::ProxyConfig {
                url: "http://127.0.0.1:8080".to_string(),
            });
        });
        assert_changed(&base, "tls", |snapshot| {
            snapshot.tls = Some(net::TlsConfig {
                accept_invalid_certs: true,
            });
        });
        assert_changed(&base, "middlewares", |snapshot| {
            snapshot.middlewares.push(
                middleware::Spec::new("retry")
                    .hook("before_request")
                    .args(serde_json::json!({"count": 2})),
            );
        });
    }
}
