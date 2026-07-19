use serde_json::Value;
use std::collections::HashMap;

use crate::{middleware, net, trace};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub id: String,
    pub task_id: String,
    pub trace_id: String,
    pub node: String,
    pub protocol: net::Protocol,
    pub url: String,
    pub method: net::Method,
    pub headers: net::Headers,
    pub body: net::Body,
    pub cookies: net::Cookies,
    pub vals: HashMap<String, Value>,
    pub kwargs: HashMap<String, Value>,
    pub priority: i32,
    pub dont_filter: bool,
    pub mode: net::Mode,
    pub timeout: Option<u64>,
    pub proxy: Option<net::ProxyConfig>,
    pub tls: Option<net::TlsConfig>,
    pub middlewares: Vec<middleware::Spec>,
    pub state: net::State,
    pub version: i64,
    pub next_time: i64,
    pub leased_by: String,
    pub lease_time: i64,
    pub retry_count: i32,
    pub max_retry_count: i32,
    pub failed_workers: Vec<String>,
}

impl TryFrom<net::Request> for Snapshot {
    type Error = String;

    fn try_from(request: net::Request) -> Result<Self, Self::Error> {
        let node = request.node_key().to_string();
        let snapshot = Self {
            id: request.id,
            task_id: request.task_id,
            trace_id: request.trace_id,
            node,
            protocol: request.protocol,
            url: request.url,
            method: request.method,
            headers: request.headers,
            body: request.body,
            cookies: request.cookies,
            vals: request.vals,
            kwargs: request.kwargs,
            priority: request.priority,
            dont_filter: request.dont_filter,
            mode: request.mode,
            timeout: request.timeout,
            proxy: request.proxy,
            tls: request.tls,
            middlewares: request.middlewares,
            state: request.state,
            version: request.version,
            next_time: request.next_time,
            leased_by: request.leased_by,
            lease_time: request.lease_time,
            retry_count: request.retry_count,
            max_retry_count: request.max_retry_count,
            failed_workers: request.failed_workers,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }
}

impl Snapshot {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.id.is_empty() {
            return Err("Request Snapshot id must not be empty".to_string());
        }
        if self.task_id.is_empty() != self.trace_id.is_empty() {
            return Err(
                "Request Snapshot task_id and trace_id must both be set or both be empty"
                    .to_string(),
            );
        }
        if self.node.is_empty() {
            return Err("Request Snapshot node must not be empty".to_string());
        }
        let parsed = url::Url::parse(&self.url)
            .map_err(|error| format!("Request Snapshot URL is invalid: {error}"))?;
        if !parsed.has_host() {
            return Err("Request Snapshot URL must have a host".to_string());
        }
        let protocol = match parsed.scheme() {
            "http" | "https" => net::Protocol::Http,
            scheme => {
                return Err(format!(
                    "Request Snapshot URL uses unsupported protocol: {scheme}"
                ));
            }
        };
        if self.protocol != protocol {
            return Err("Request Snapshot protocol does not match its URL".to_string());
        }
        for (name, value) in &self.headers {
            reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|error| format!("Request Snapshot header name is invalid: {error}"))?;
            reqwest::header::HeaderValue::from_str(value)
                .map_err(|error| format!("Request Snapshot header value is invalid: {error}"))?;
        }
        if !self.cookies.is_empty() {
            for name in self.cookies.keys() {
                reqwest::header::HeaderName::from_bytes(name.as_bytes())
                    .map_err(|error| format!("Request Snapshot cookie name is invalid: {error}"))?;
            }
            let cookies = self
                .cookies
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect::<Vec<_>>()
                .join("; ");
            reqwest::header::HeaderValue::from_str(&cookies)
                .map_err(|error| format!("Request Snapshot cookie is invalid: {error}"))?;
        }
        if let Some(proxy) = &self.proxy {
            super::transport::validate_proxy_url(&proxy.url)
                .map_err(|message| format!("Request Snapshot proxy URL {message}"))?;
        }
        if self.state != net::State::Pending {
            return Err("queued Request Snapshot state must be pending".to_string());
        }
        if self.version < 0 {
            return Err("Request Snapshot version must not be negative".to_string());
        }
        if self.next_time < 0 {
            return Err("Request Snapshot next_time must not be negative".to_string());
        }
        if !self.leased_by.is_empty() || self.lease_time != 0 {
            return Err("queued Request Snapshot must not have an active lease".to_string());
        }
        if self.retry_count < 0
            || self.max_retry_count <= 0
            || self.retry_count >= self.max_retry_count
        {
            return Err("Request Snapshot retry fields are invalid".to_string());
        }
        let mut workers = std::collections::HashSet::with_capacity(self.failed_workers.len());
        if self
            .failed_workers
            .iter()
            .any(|worker| worker.is_empty() || !workers.insert(worker))
        {
            return Err(
                "Request Snapshot failed_workers must contain unique non-empty values".to_string(),
            );
        }
        if self.failed_workers.len() > self.retry_count as usize {
            return Err("Request Snapshot failed_workers cannot exceed retry_count".to_string());
        }
        for spec in &self.middlewares {
            crate::middleware::check(spec)
                .map_err(|error| format!("Request Snapshot has invalid middleware: {error}"))?;
        }
        Ok(())
    }

    pub fn restore(
        self,
        trace: Option<std::sync::Arc<trace::Snapshot>>,
    ) -> Result<net::Request, String> {
        self.validate()?;
        if !self.trace_id.is_empty() {
            let Some(trace) = trace.as_ref() else {
                return Err("Request Snapshot requires a Trace Snapshot".to_string());
            };
            if trace.task_id != self.task_id {
                return Err("Request Snapshot task_id does not match Trace Snapshot".to_string());
            }
            if let Some(config) = trace.dsl.as_ref()
                && !config.graph.nodes.contains_key(&self.node)
            {
                return Err(format!(
                    "Request Snapshot node does not exist in Trace Snapshot: {}",
                    self.node
                ));
            }
        }

        let mut request = self.into_request()?;
        if let Some(trace) = trace {
            request.set_snapshot(trace);
        }
        Ok(request)
    }

    pub(crate) fn into_request(self) -> Result<net::Request, String> {
        let mut request = net::Request::follow(self.url.clone())
            .map_err(|error| format!("Request Snapshot URL is invalid: {error}"))?;
        request.id = self.id;
        request.task_id = self.task_id;
        request.trace_id = self.trace_id;
        request.protocol = self.protocol;
        request.url = self.url;
        request.method = self.method;
        request.headers = self.headers;
        request.body = self.body;
        request.cookies = self.cookies;
        request.vals = self.vals;
        request.kwargs = self.kwargs;
        request.priority = self.priority;
        request.dont_filter = self.dont_filter;
        request.mode = self.mode;
        request.timeout = self.timeout;
        request.proxy = self.proxy;
        request.tls = self.tls;
        request.middlewares = self.middlewares;
        request.state = self.state;
        request.version = self.version;
        request.next_time = self.next_time;
        request.leased_by = self.leased_by;
        request.lease_time = self.lease_time;
        request.retry_count = self.retry_count;
        request.max_retry_count = self.max_retry_count;
        request.failed_workers = self.failed_workers;
        request.set_node(self.node);
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules_trace() -> std::sync::Arc<trace::Snapshot> {
        let config = crate::config::Config::from_yaml(
            r#"
spider:
  name: books
  start:
    - node: detail
      url: https://example.com
graph:
  nodes:
    detail: {}
  edges: []
"#,
        )
        .unwrap();
        std::sync::Arc::new(trace::Snapshot::rules("task-1", config))
    }

    #[test]
    fn rules_request_round_trip_preserves_executable_fields() {
        let mut request = net::Request::follow("https://example.com/detail/1").unwrap();
        request.id = "req-1".to_string();
        request.task_id = "task-1".to_string();
        request.trace_id = "trace-1".to_string();
        request.priority = 7;
        request.timeout = Some(3_000);
        request
            .vals
            .insert("category".to_string(), Value::String("rust".to_string()));
        request.set_node("detail");

        let snapshot = Snapshot::try_from(request).unwrap();
        let encoded = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(encoded["protocol"], "http");
        assert_eq!(encoded["method"], "GET");
        assert_eq!(encoded["mode"], "http");
        assert_eq!(encoded["state"], "pending");
        assert!(encoded.get("schema_version").is_none());
        assert!(encoded.get("created_time").is_none());
        assert!(encoded.get("updated_time").is_none());
        let decoded = serde_json::from_value::<Snapshot>(encoded).unwrap();
        let restored = decoded.restore(Some(rules_trace())).unwrap();

        assert_eq!(restored.id, "req-1");
        assert_eq!(restored.task_id, "task-1");
        assert_eq!(restored.trace_id, "trace-1");
        assert_eq!(restored.node_key(), "detail");
        assert_eq!(restored.priority, 7);
        assert_eq!(restored.timeout, Some(3_000));
        assert_eq!(restored.vals["category"], "rust");
        assert!(restored.snapshot().is_some());
    }

    #[test]
    fn code_request_round_trip_contains_only_the_stable_node() {
        let request = net::Request::follow("https://example.com/detail")
            .unwrap()
            .node("detail");

        let snapshot = Snapshot::try_from(request).unwrap();
        let encoded = serde_json::to_value(&snapshot).unwrap();

        assert_eq!(encoded["node"], "detail");
        assert!(encoded.get("handler").is_none());
        let restored = serde_json::from_value::<Snapshot>(encoded)
            .unwrap()
            .restore(None)
            .unwrap();
        assert_eq!(restored.node_key(), "detail");
        assert!(restored.snapshot().is_none());
    }

    #[test]
    fn restore_rejects_invalid_runtime_state_and_failed_workers() {
        let request = net::Request::follow("https://example.com").unwrap();
        let mut snapshot = Snapshot::try_from(request).unwrap();
        snapshot.state = net::State::Processing;
        assert!(snapshot.clone().restore(None).is_err());

        snapshot.state = net::State::Pending;
        snapshot.failed_workers = vec!["worker-1".to_string(), "worker-1".to_string()];
        assert!(snapshot.restore(None).is_err());
    }

    #[test]
    fn restore_rejects_an_unsupported_proxy_protocol() {
        let request = net::Request::follow("https://example.com").unwrap();
        let mut snapshot = Snapshot::try_from(request).unwrap();
        snapshot.proxy = Some(net::ProxyConfig {
            url: "ftp://proxy.example.com".to_string(),
        });

        let error = snapshot.restore(None).unwrap_err();

        assert!(error.contains("unsupported protocol: ftp"));
    }

    #[test]
    fn snapshot_rejects_an_invalid_cookie_name() {
        let request = net::Request::follow("https://example.com")
            .unwrap()
            .cookie("bad;name", "value");

        let error = Snapshot::try_from(request).unwrap_err();

        assert!(error.contains("cookie name is invalid"));
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let request = net::Request::follow("https://example.com").unwrap();
        let mut encoded = serde_json::to_value(Snapshot::try_from(request).unwrap()).unwrap();
        encoded["kind"] = Value::from("code");

        assert!(serde_json::from_value::<Snapshot>(encoded).is_err());
    }
}
