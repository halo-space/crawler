use bytes::Bytes;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use url::Url;

use crate::net::{Cookies, Error, Headers, Request, StatusCode};
use crate::selector;
use crate::{ai, middleware};

mod encoding;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum HttpVersion {
    Http10,
    #[default]
    Http11,
    Http2,
    Http3,
}

#[derive(Clone)]
pub struct Response {
    pub request: Request,
    pub url: String,
    pub status: StatusCode,
    pub reason: Option<String>,
    pub version: HttpVersion,
    pub redirects: Vec<String>,
    pub headers: Headers,
    pub cookies: Cookies,
    pub body: Bytes,
    pub vals: HashMap<String, Value>,
    pub kwargs: HashMap<String, Value>,
    pub middlewares: Vec<middleware::Spec>,
    pub(crate) ai: Option<Arc<ai::OpenAI>>,
}

impl fmt::Debug for Response {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Response")
            .field("request", &self.request)
            .field("origin", &debug_origin(&self.url))
            .field("status", &self.status)
            .field("has_reason", &self.reason.is_some())
            .field("version", &self.version)
            .field("redirects_len", &self.redirects.len())
            .field("headers_len", &self.headers.len())
            .field("cookies_len", &self.cookies.len())
            .field("body_len", &self.body.len())
            .field("vals_len", &self.vals.len())
            .field("kwargs_len", &self.kwargs.len())
            .field("middlewares_len", &self.middlewares.len())
            .finish()
    }
}

fn debug_origin(value: &str) -> String {
    Url::parse(value)
        .map(|url| url.origin().ascii_serialization())
        .unwrap_or_else(|_| "<invalid URL>".to_string())
}

impl Response {
    /// Creates a response and copies the parsing context carried by its request.
    pub fn new(request: Request, status: StatusCode, body: impl Into<Bytes>) -> Self {
        Self {
            url: request.url.clone(),
            vals: request.vals.clone(),
            kwargs: request.kwargs.clone(),
            middlewares: request.middlewares.clone(),
            request,
            status,
            reason: None,
            version: HttpVersion::default(),
            redirects: Vec::new(),
            headers: Headers::new(),
            cookies: Cookies::new(),
            body: body.into(),
            ai: None,
        }
    }

    pub fn body(&self) -> &Bytes {
        &self.body
    }

    pub fn text(&self) -> Result<String, Error> {
        Ok(encoding::decode(&self.body, &self.headers))
    }

    pub fn json<T>(&self) -> Result<T, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        selector::json::parse(self)
    }

    pub fn css(&self) -> Result<scrape_core::Soup, Error> {
        selector::css::parse(self)
    }

    /// Extracts one JSON object through the OpenAI provider selected by the Engine.
    pub async fn ai(&self, expr: &str) -> Result<Value, selector::Error> {
        selector::ai::select(self.ai.as_deref(), self, expr).await
    }

    pub(crate) fn attach_ai(&mut self, openai: Option<Arc<ai::OpenAI>>) {
        self.ai = openai;
    }

    pub fn urljoin(&self, url: &str) -> Result<String, Error> {
        let base = Url::parse(&self.url)?;
        Ok(base.join(url)?.to_string())
    }

    pub fn follow(&self, url: &str) -> Result<Request, Error> {
        let target = self.urljoin(url)?;
        let target_url = Url::parse(&target)?;
        let same_origin = Url::parse(&self.url)?.origin() == target_url.origin();
        let mut request = Request::follow(target)?;
        request.task_id = self.request.task_id.clone();
        request.trace_id = self.request.trace_id.clone();
        request.vals = self.vals.clone();
        if same_origin {
            request.headers = self.request.headers.clone();
            request.cookies = self.cookies.clone();
        } else {
            request.cookies = self.cookies.for_url(&target_url);
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn html_response(body: &str) -> Response {
        Response {
            request: Request::follow("https://example.com/list/").unwrap(),
            url: "https://example.com/list/".to_string(),
            status: StatusCode(200),
            reason: Some("OK".to_string()),
            version: HttpVersion::Http11,
            redirects: Vec::new(),
            headers: Headers::new(),
            cookies: Cookies::new(),
            body: Bytes::from(body.to_string()),
            vals: HashMap::new(),
            kwargs: HashMap::new(),
            middlewares: Vec::new(),
            ai: None,
        }
    }

    fn encoded_response(body: &'static [u8], content_type: &str) -> Response {
        let mut response = html_response("");
        response
            .headers
            .try_set("content-type", content_type)
            .unwrap();
        response.body = Bytes::from_static(body);
        response
    }

    #[test]
    fn new_copies_request_parse_context() {
        let mut request = Request::follow("https://example.com/list/").unwrap();
        request
            .vals
            .insert("category".to_string(), Value::from("books"));
        request.kwargs.insert("page".to_string(), Value::from(2));
        request.middlewares.push(middleware::Spec::new("custom"));

        let response = Response::new(request, StatusCode(201), "body");

        assert_eq!(response.url, "https://example.com/list/");
        assert_eq!(response.status, StatusCode(201));
        assert_eq!(response.body(), &Bytes::from_static(b"body"));
        assert_eq!(response.vals["category"], Value::from("books"));
        assert_eq!(response.kwargs["page"], Value::from(2));
        assert_eq!(response.middlewares.len(), 1);
        assert_eq!(response.version, HttpVersion::Http11);
        assert!(response.headers.is_empty());
        assert!(response.cookies.is_empty());
        assert!(response.ai.is_none());
    }

    #[test]
    fn follow_completes_relative_url_and_inherits_selected_fields() {
        let mut source = Request::follow("https://example.com/list/").unwrap();
        source
            .kwargs
            .insert("arg".to_string(), Value::from("keep-out"));
        source.headers.try_set("accept", "text/html").unwrap();

        let mut headers = Headers::new();
        headers.try_set("content-type", "text/html").unwrap();

        let mut cookies = Cookies::new();
        cookies
            .insert(
                &Url::parse("https://example.com/list/").unwrap(),
                "sid",
                "1",
            )
            .unwrap();

        let mut vals = HashMap::new();
        vals.insert("category".to_string(), Value::from("books"));

        let response = Response {
            request: source,
            url: "https://example.com/list/".to_string(),
            status: StatusCode(200),
            reason: Some("OK".to_string()),
            version: HttpVersion::Http11,
            redirects: Vec::new(),
            headers,
            cookies,
            body: Bytes::new(),
            vals,
            kwargs: HashMap::new(),
            middlewares: Vec::new(),
            ai: None,
        };

        let next = response.follow("../detail/1").unwrap();

        assert_eq!(next.url, "https://example.com/detail/1");
        assert_eq!(next.vals.get("category"), Some(&Value::from("books")));
        assert_eq!(
            next.headers
                .get("accept")
                .and_then(|value| value.to_str().ok()),
            Some("text/html")
        );
        assert_eq!(
            next.cookies.get(&Url::parse(&next.url).unwrap(), "sid"),
            Some("1")
        );
        assert!(next.kwargs.is_empty());
        assert_eq!(next.priority, 0);
        assert!(!next.dont_filter);
        assert_eq!(next.mode, crate::net::Mode::Http);
    }

    #[test]
    fn follow_drops_headers_and_cookies_across_origins() {
        let mut response = html_response("");
        response
            .request
            .headers
            .try_set("authorization", "secret")
            .unwrap();
        let source_url = Url::parse(&response.url).unwrap();
        response
            .request
            .cookies
            .insert(&source_url, "sid", "1")
            .unwrap();
        response
            .cookies
            .insert(&source_url, "response", "2")
            .unwrap();

        let next = response.follow("https://other.example/detail").unwrap();

        assert!(next.headers.is_empty());
        assert!(next.cookies.is_empty());
        assert_eq!(next.task_id, response.request.task_id);
        assert_eq!(next.trace_id, response.request.trace_id);
    }

    #[test]
    fn cross_origin_follow_keeps_only_target_applicable_domain_cookies() {
        let mut response = html_response("");
        response
            .request
            .headers
            .try_set("authorization", "secret")
            .unwrap();
        let mut set_cookie = Headers::new();
        set_cookie
            .try_append("set-cookie", "shared=1; Domain=example.com; Path=/")
            .unwrap();
        response
            .cookies
            .store_response(&Url::parse(&response.url).unwrap(), &set_cookie);

        let next = response.follow("https://api.example.com/detail").unwrap();
        let next_url = Url::parse(&next.url).unwrap();

        assert!(next.headers.is_empty());
        assert_eq!(next.cookies.get(&next_url, "shared"), Some("1"));
        assert_eq!(next.cookies.len(), 1);
    }

    #[test]
    fn queued_siblings_keep_independent_cookie_snapshots() {
        let mut response = html_response("");
        let origin = Url::parse(&response.url).unwrap();
        response.cookies.insert(&origin, "sid", "one").unwrap();

        let first = response.follow("/detail/1").unwrap();
        response.cookies.insert(&origin, "sid", "two").unwrap();
        let second = response.follow("/detail/2").unwrap();

        assert_eq!(
            first.cookies.get(&Url::parse(&first.url).unwrap(), "sid"),
            Some("one")
        );
        assert_eq!(
            second.cookies.get(&Url::parse(&second.url).unwrap(), "sid"),
            Some("two")
        );
    }

    #[test]
    fn css_extracts_text_and_attributes() {
        let response = html_response(
            r#"
            <html>
              <body>
                <article class="book"><h1>Book A</h1><a href="/a">A</a></article>
                <article class="book"><h1>Book B</h1><a href="/b">B</a></article>
              </body>
            </html>
            "#,
        );

        let soup = response.css().unwrap();
        let titles = soup
            .select("article.book h1")
            .unwrap()
            .into_iter()
            .map(|node| node.text())
            .collect::<Vec<_>>();
        assert_eq!(titles, vec!["Book A".to_string(), "Book B".to_string()]);
        assert_eq!(
            soup.select("article.book a")
                .unwrap()
                .into_iter()
                .filter_map(|node| node.get("href").map(str::to_string))
                .collect::<Vec<_>>(),
            vec!["/a".to_string(), "/b".to_string()]
        );
    }

    #[test]
    fn css_supports_scrapy_style_chained_extractors() {
        let response = html_response(
            r#"
            <html>
              <body>
                <article class="book"><h1>Book A</h1><a href="/a">A</a></article>
                <article class="book"><h1>Book B</h1><a href="/b">B</a></article>
              </body>
            </html>
            "#,
        );

        let soup = response.css().unwrap();
        assert_eq!(
            soup.select("article.book h1")
                .unwrap()
                .into_iter()
                .map(|node| node.text())
                .collect::<Vec<_>>(),
            vec!["Book A".to_string(), "Book B".to_string()]
        );
        assert_eq!(
            soup.select("article.book a")
                .unwrap()
                .into_iter()
                .filter_map(|node| node.get("href").map(str::to_string))
                .collect::<Vec<_>>(),
            vec!["/a".to_string(), "/b".to_string()]
        );
        assert!(
            soup.find("article.book")
                .unwrap()
                .unwrap()
                .outer_html()
                .contains("<h1>Book A</h1>")
        );
    }

    #[test]
    fn selector_errors_are_returned_when_values_are_read() {
        let response = html_response("<html></html>");

        let soup = response.css().unwrap();
        assert!(soup.select("article[").is_err());
    }

    #[test]
    fn json_deserializes_response_body() {
        let response = html_response(r#"{"title":"Book A"}"#);
        let value: serde_json::Value = response.json().unwrap();

        assert_eq!(value.get("title"), Some(&Value::from("Book A")));
    }

    #[test]
    fn text_and_css_decode_without_changing_body_bytes() {
        const BODY: &[u8] = b"<html><h1>\xB9\xF0\xC1\xD6\xC3\xD7\xB7\xDB</h1></html>";
        let response = encoded_response(BODY, "text/html; charset=gbk");

        assert!(response.text().unwrap().contains("桂林米粉"));
        assert_eq!(
            response.css().unwrap().find("h1").unwrap().unwrap().text(),
            "桂林米粉"
        );
        assert_eq!(response.body().as_ref(), BODY);
    }

    #[test]
    fn json_uses_the_same_charset_resolution_as_text() {
        const BODY: &[u8] = b"{\"title\":\"\xB9\xF0\xC1\xD6\"}";
        let response = encoded_response(BODY, "application/json; charset=gbk");
        let value: serde_json::Value = response.json().unwrap();

        assert_eq!(value.get("title"), Some(&Value::from("桂林")));
        assert_eq!(response.body().as_ref(), BODY);
    }

    #[tokio::test]
    async fn ai_validates_the_prompt_and_requires_a_client() {
        let response = html_response("body");

        assert_eq!(
            response.ai("  ").await.unwrap_err(),
            selector::Error::Ai("prompt cannot be empty".to_string())
        );
        assert_eq!(
            response.ai("extract data").await.unwrap_err(),
            selector::Error::Ai("AI provider is not configured".to_string())
        );
    }

    #[test]
    fn debug_redacts_response_content_and_url_credentials() {
        let mut response = html_response("response-body-secret");
        response.request = Request::follow(
            "https://request-user:request-password@example.com/private?api_key=request-secret",
        )
        .unwrap()
        .header("authorization", "request-header-secret")
        .unwrap()
        .cookie("session", "request-cookie-secret")
        .unwrap();
        response.request.proxy = Some(crate::net::ProxyConfig {
            url: "http://proxy-user:proxy-password@proxy.example:8080".to_string(),
        });
        response.url =
            "https://response-user:response-password@example.com/path?token=response-secret"
                .to_string();
        response.reason = Some("reason-secret".to_string());
        response
            .redirects
            .push("https://example.com/?token=redirect-secret".to_string());
        response
            .headers
            .try_set("set-cookie", "header-secret")
            .unwrap();
        let response_url = Url::parse(&response.url).unwrap();
        response
            .cookies
            .insert(&response_url, "session", "cookie-secret")
            .unwrap();
        response
            .vals
            .insert("token".to_string(), Value::from("vals-secret"));
        response
            .kwargs
            .insert("api_key".to_string(), Value::from("kwargs-secret"));
        response.middlewares.push(
            middleware::Spec::new("custom")
                .args(serde_json::json!({"api_key": "middleware-secret"})),
        );
        response.attach_ai(Some(Arc::new(
            ai::OpenAI::new(
                "https://provider.example/v1",
                "ai-provider-secret",
                "model-secret",
            )
            .unwrap(),
        )));

        let debug = format!("{response:?}");

        for secret in [
            "request-user",
            "request-password",
            "request-secret",
            "request-header-secret",
            "request-cookie-secret",
            "response-user",
            "response-password",
            "response-secret",
            "reason-secret",
            "redirect-secret",
            "header-secret",
            "cookie-secret",
            "response-body-secret",
            "vals-secret",
            "kwargs-secret",
            "middleware-secret",
            "proxy-user",
            "proxy-password",
            "ai-provider-secret",
            "model-secret",
            "provider.example",
        ] {
            assert!(!debug.contains(secret), "Debug exposed {secret}: {debug}");
        }
        assert!(debug.contains("https://example.com"));
        assert!(debug.contains("http://proxy.example:8080"));
        assert!(debug.contains("redirects_len: 1"));
    }
}
