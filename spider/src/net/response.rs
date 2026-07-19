use bytes::Bytes;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use url::Url;

use crate::middleware;
use crate::net::{Cookies, Error, Headers, Request, StatusCode};
use crate::selector;

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
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    pub fn text(&self) -> Result<String, Error> {
        String::from_utf8(self.body.to_vec()).map_err(Error::from)
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

    pub fn urljoin(&self, url: &str) -> Result<String, Error> {
        let base = Url::parse(&self.url)?;
        Ok(base.join(url)?.to_string())
    }

    pub fn follow(&self, url: &str) -> Result<Request, Error> {
        let target = self.urljoin(url)?;
        let same_origin = Url::parse(&self.url)?.origin() == Url::parse(&target)?.origin();
        let mut request = Request::follow(target)?;
        request.task_id = self.request.task_id.clone();
        request.trace_id = self.request.trace_id.clone();
        request.vals = self.vals.clone();
        if same_origin {
            request.headers = self.request.headers.clone();
            request.cookies = self.request.cookies.clone();
            request.cookies.extend(self.cookies.clone());
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
        }
    }

    #[test]
    fn follow_completes_relative_url_and_inherits_selected_fields() {
        let mut source = Request::follow("https://example.com/list/").unwrap();
        source
            .kwargs
            .insert("arg".to_string(), Value::from("keep-out"));
        source
            .headers
            .insert("accept".to_string(), "text/html".to_string());

        let mut headers = Headers::new();
        headers.insert("content-type".to_string(), "text/html".to_string());

        let mut cookies = Cookies::new();
        cookies.insert("sid".to_string(), "1".to_string());

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
        };

        let next = response.follow("../detail/1").unwrap();

        assert_eq!(next.url, "https://example.com/detail/1");
        assert_eq!(next.vals.get("category"), Some(&Value::from("books")));
        assert_eq!(
            next.headers.get("accept").map(String::as_str),
            Some("text/html")
        );
        assert_eq!(next.cookies.get("sid").map(String::as_str), Some("1"));
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
            .insert("authorization".to_string(), "secret".to_string());
        response
            .request
            .cookies
            .insert("sid".to_string(), "1".to_string());
        response
            .cookies
            .insert("response".to_string(), "2".to_string());

        let next = response.follow("https://other.example/detail").unwrap();

        assert!(next.headers.is_empty());
        assert!(next.cookies.is_empty());
        assert_eq!(next.task_id, response.request.task_id);
        assert_eq!(next.trace_id, response.request.trace_id);
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
    fn debug_redacts_response_content_and_url_credentials() {
        let mut response = html_response("response-body-secret");
        response.request = Request::follow(
            "https://request-user:request-password@example.com/private?api_key=request-secret",
        )
        .unwrap()
        .header("authorization", "request-header-secret")
        .cookie("session", "request-cookie-secret");
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
            .insert("set-cookie".to_string(), "header-secret".to_string());
        response
            .cookies
            .insert("session".to_string(), "cookie-secret".to_string());
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
        ] {
            assert!(!debug.contains(secret), "Debug exposed {secret}: {debug}");
        }
        assert!(debug.contains("https://example.com"));
        assert!(debug.contains("http://proxy.example:8080"));
        assert!(debug.contains("redirects_len: 1"));
    }
}
