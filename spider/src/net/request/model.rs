use std::collections::HashMap;
use std::fmt;

use serde_json::Value;
use url::Url;

use crate::middleware;
use crate::net::{Body, Cookies, Error, Headers, Method, Protocol};

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    Http,
    Browser,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    #[default]
    Pending,
    Processing,
    Done,
    Failed,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyConfig {
    pub url: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    pub accept_invalid_certs: bool,
}

#[derive(Clone)]
pub struct Request {
    pub id: String,
    pub task_id: String,
    pub trace_id: String,
    pub version: i64,

    pub protocol: Protocol,
    pub url: String,
    pub method: Method,
    pub headers: Headers,
    pub body: Body,
    pub cookies: Cookies,

    pub vals: HashMap<String, Value>,
    pub kwargs: HashMap<String, Value>,

    pub priority: i32,
    pub dont_filter: bool,

    pub mode: Mode,
    pub timeout: Option<u64>,
    pub proxy: Option<ProxyConfig>,
    pub tls: Option<TlsConfig>,

    pub middlewares: Vec<middleware::Spec>,

    pub state: State,
    pub next_time: i64,
    pub leased_by: String,
    pub lease_time: i64,
    pub retry_count: i32,
    pub max_retry_count: i32,
    pub failed_workers: Vec<String>,

    node: String,
    snapshot: Option<std::sync::Arc<crate::trace::Snapshot>>,
    allowed_domains: Vec<String>,
}

impl Request {
    pub fn follow(url: impl Into<String>) -> Result<Self, Error> {
        let url = url.into();
        let parsed = Url::parse(&url)?;

        if !parsed.has_host() {
            return Err(Error::UrlNotAbsolute(url));
        }

        let protocol = match parsed.scheme() {
            "http" | "https" => Protocol::Http,
            scheme => return Err(Error::UnsupportedProtocol(scheme.to_string())),
        };

        Ok(Self {
            id: next_id(),
            task_id: String::new(),
            trace_id: String::new(),
            version: 0,
            protocol,
            url,
            method: Method::Get,
            headers: Headers::new(),
            body: Body::Empty,
            cookies: Cookies::new(),
            vals: HashMap::new(),
            kwargs: HashMap::new(),
            priority: 0,
            dont_filter: false,
            mode: Mode::Http,
            timeout: None,
            proxy: None,
            tls: None,
            middlewares: Vec::new(),
            state: State::Pending,
            next_time: 0,
            leased_by: String::new(),
            lease_time: 0,
            retry_count: 0,
            max_retry_count: 1,
            failed_workers: Vec::new(),
            node: "index".to_string(),
            snapshot: None,
            allowed_domains: Vec::new(),
        })
    }

    pub fn node(mut self, node: impl Into<String>) -> Self {
        self.node = node.into();
        self
    }

    pub(crate) fn set_node(&mut self, node: impl Into<String>) {
        self.node = node.into();
    }

    pub(crate) fn set_snapshot(&mut self, snapshot: std::sync::Arc<crate::trace::Snapshot>) {
        self.snapshot = Some(snapshot);
    }

    pub(crate) fn snapshot(&self) -> Option<&std::sync::Arc<crate::trace::Snapshot>> {
        self.snapshot.as_ref()
    }

    pub fn node_key(&self) -> &str {
        &self.node
    }

    pub(crate) fn set_allowed_domains(&mut self, domains: Vec<String>) {
        self.allowed_domains = domains;
    }

    pub(crate) fn allows(&self, url: &Url) -> bool {
        if self.allowed_domains.is_empty() {
            return true;
        }
        let Some(host) = url.host_str() else {
            return false;
        };
        self.allowed_domains.iter().any(|domain| {
            host.eq_ignore_ascii_case(domain)
                || (host.len() > domain.len()
                    && host.as_bytes()[host.len() - domain.len() - 1] == b'.'
                    && host[host.len() - domain.len()..].eq_ignore_ascii_case(domain))
        })
    }

    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    pub fn cookie(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.cookies.insert(key.into(), value.into());
        self
    }

    pub fn method(mut self, method: Method) -> Self {
        self.method = method;
        self
    }

    pub fn body(mut self, body: Body) -> Self {
        self.body = body;
        self
    }

    pub fn mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    pub fn vals(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.vals.insert(key.into(), value.into());
        self
    }

    pub fn kwargs(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.kwargs.insert(key.into(), value.into());
        self
    }

    pub fn with_middleware(mut self, spec: middleware::Spec) -> Self {
        self.middlewares.push(spec);
        self
    }

    pub fn with_retry(self, count: usize, backoff: impl IntoIterator<Item = u64>) -> Self {
        self.with_middleware(middleware::Spec::new("retry").args(serde_json::json!({
            "count": count,
            "backoff": backoff.into_iter().collect::<Vec<_>>(),
        })))
    }

    pub fn with_rate_limit(self, group: impl Into<String>, qps: f64) -> Self {
        self.with_middleware(
            middleware::Spec::new("rate_limit")
                .hook("before_download")
                .args(serde_json::json!({
                    "group": group.into(),
                    "qps": qps,
                })),
        )
    }

    pub fn with_dedup<I, K>(self, key: I, ttl: Option<u64>) -> Self
    where
        I: IntoIterator<Item = K>,
        K: Into<String>,
    {
        let ttl = ttl.map_or_else(|| Value::from(-1), Value::from);
        self.with_middleware(
            middleware::Spec::new("dedup")
                .hook("before_scheduler")
                .args(serde_json::json!({
                    "rules": {
                        "request": {
                            "key": key.into_iter().map(Into::into).collect::<Vec<String>>(),
                            "normalize": {"enabled": true},
                            "ttl": ttl,
                        }
                    }
                })),
        )
    }
}

fn next_id() -> String {
    format!("req_{}", uuid::Uuid::now_v7())
}

impl fmt::Debug for Request {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Request")
            .field("id", &self.id)
            .field("task_id", &self.task_id)
            .field("trace_id", &self.trace_id)
            .field("version", &self.version)
            .field("protocol", &self.protocol)
            .field("url", &self.url)
            .field("node", &self.node_key())
            .field("method", &self.method)
            .field("headers", &self.headers)
            .field("body", &self.body)
            .field("cookies", &self.cookies)
            .field("vals", &self.vals)
            .field("kwargs", &self.kwargs)
            .field("priority", &self.priority)
            .field("dont_filter", &self.dont_filter)
            .field("mode", &self.mode)
            .field("timeout", &self.timeout)
            .field("proxy", &self.proxy)
            .field("tls", &self.tls)
            .field("middlewares", &self.middlewares)
            .field("state", &self.state)
            .field("next_time", &self.next_time)
            .field("leased_by", &self.leased_by)
            .field("lease_time", &self.lease_time)
            .field("retry_count", &self.retry_count)
            .field("max_retry_count", &self.max_retry_count)
            .field("failed_workers", &self.failed_workers)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn follow_requires_absolute_url() {
        let error = Request::follow("/detail").unwrap_err();
        assert!(matches!(error, Error::UrlParse(_)));
    }

    #[test]
    fn follow_defaults_to_index_node() {
        let request = Request::follow("https://example.com").unwrap();

        assert_eq!(request.node_key(), "index");
        assert_eq!(request.method, Method::Get);
        assert_eq!(request.mode, Mode::Http);
        let id = uuid::Uuid::parse_str(request.id.strip_prefix("req_").unwrap()).unwrap();
        assert_eq!(id.get_version(), Some(uuid::Version::SortRand));
    }

    #[test]
    fn middleware_helpers_write_specs() {
        let request = Request::follow("https://example.com")
            .unwrap()
            .with_retry(2, [10, 20])
            .with_rate_limit("example", 2.0)
            .with_dedup(["$request.url"], Some(1_000));

        assert_eq!(
            request
                .middlewares
                .iter()
                .map(|spec| spec.name.as_str())
                .collect::<Vec<_>>(),
            ["retry", "rate_limit", "dedup"]
        );
        assert_eq!(request.middlewares[0].args["count"], 2);
        assert_eq!(request.middlewares[1].args["group"], "example");
        assert_eq!(
            request.middlewares[2].args["rules"]["request"]["key"],
            serde_json::json!(["$request.url"])
        );
    }

    #[test]
    fn node_is_a_single_stable_key() {
        let code = Request::follow("https://example.com")
            .unwrap()
            .node("detail");
        assert_eq!(code.node_key(), "detail");

        let mut rules = code;
        rules.set_node("list");
        assert_eq!(rules.node_key(), "list");
    }
}
