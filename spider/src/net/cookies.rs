use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt;

use cookie_store::{Cookie, CookieDomain, CookieStore, RawCookie};
use http::HeaderValue;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
use url::Url;

use super::Headers;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid cookie name: {0}")]
    Name(String),

    #[error("invalid cookie value")]
    Value,

    #[error("cookie store rejected the cookie: {0}")]
    Store(#[from] cookie_store::CookieError),
}

#[derive(Clone, Default)]
pub struct Cookies {
    store: CookieStore,
}

impl Cookies {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &mut self,
        url: &Url,
        name: impl AsRef<str>,
        value: impl AsRef<str>,
    ) -> Result<(), Error> {
        let name = name.as_ref();
        let value = value.as_ref();
        validate(name, value)?;
        let cookie = RawCookie::build((name.to_string(), value.to_string()))
            .path("/")
            .build();
        self.store.insert_raw(&cookie, url)?;
        Ok(())
    }

    pub fn get<'a>(&'a self, url: &Url, name: &str) -> Option<&'a str> {
        self.get_all(url, name).into_iter().next()
    }

    pub fn get_all<'a>(&'a self, url: &Url, name: &str) -> Vec<&'a str> {
        self.matching(url)
            .into_iter()
            .filter_map(|cookie| (cookie.name() == name).then_some(cookie.value()))
            .collect()
    }

    pub fn contains(&self, url: &Url, name: &str) -> bool {
        self.get(url, name).is_some()
    }

    pub fn len(&self) -> usize {
        self.store.iter_unexpired().count()
    }

    pub fn is_empty(&self) -> bool {
        self.store.iter_unexpired().next().is_none()
    }

    pub fn clear(&mut self) {
        self.store.clear();
    }

    pub(crate) fn request_header(
        &self,
        url: &Url,
    ) -> Result<Option<HeaderValue>, http::header::InvalidHeaderValue> {
        let value = self
            .matching(url)
            .into_iter()
            .map(|cookie| format!("{}={}", cookie.name(), cookie.value()))
            .collect::<Vec<_>>()
            .join("; ");
        if value.is_empty() {
            Ok(None)
        } else {
            HeaderValue::from_str(&value).map(Some)
        }
    }

    pub(crate) fn store_response(&mut self, url: &Url, headers: &Headers) {
        for value in headers.get_all(http::header::SET_COOKIE).iter() {
            let Ok(value) = std::str::from_utf8(value.as_bytes()) else {
                continue;
            };
            let Ok(mut cookie) = Cookie::parse(value, url) else {
                continue;
            };
            if normalize_domain(&mut cookie, url) {
                let _ = self.store.insert(cookie.into_owned(), url);
            }
        }
    }

    pub(crate) fn for_url(&self, url: &Url) -> Self {
        let cookies = self.matching(url).into_iter().cloned();
        let store =
            CookieStore::from_cookies(cookies.map(Ok::<Cookie<'static>, Infallible>), false)
                .unwrap_or_else(|never| match never {});
        Self { store }
    }

    fn matching<'a>(&'a self, url: &Url) -> Vec<&'a Cookie<'static>> {
        let mut cookies = self.store.matches(url);
        cookies.sort_by(|left, right| {
            right
                .path
                .len()
                .cmp(&left.path.len())
                .then_with(|| left.domain.cmp(&right.domain))
                .then_with(|| left.name().cmp(right.name()))
        });
        cookies
    }
}

pub(crate) fn validate(name: &str, value: &str) -> Result<(), Error> {
    const SEPARATORS: &[u8] = b"()<>@,;:\\\"/[]?={} \t";
    if name.is_empty()
        || !name.bytes().all(|byte| {
            byte.is_ascii() && byte > 0x20 && byte < 0x7f && !SEPARATORS.contains(&byte)
        })
    {
        return Err(Error::Name(name.to_string()));
    }
    if !value.bytes().all(|byte| {
        byte == 0x21
            || (0x23..=0x2b).contains(&byte)
            || (0x2d..=0x3a).contains(&byte)
            || (0x3c..=0x5b).contains(&byte)
            || (0x5d..=0x7e).contains(&byte)
    }) {
        return Err(Error::Value);
    }
    Ok(())
}

impl fmt::Debug for Cookies {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Cookies")
            .field("len", &self.len())
            .finish()
    }
}

impl Serialize for Cookies {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut cookies = self.store.iter_unexpired().cloned().collect::<Vec<_>>();
        cookies.sort_by(|left, right| {
            left.domain
                .cmp(&right.domain)
                .then_with(|| left.path.cmp(&right.path))
                .then_with(|| left.name().cmp(right.name()))
        });
        cookies.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Cookies {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let cookies = Vec::<Cookie<'static>>::deserialize(deserializer)?;
        let mut keys = HashSet::with_capacity(cookies.len());
        for cookie in &cookies {
            let Some(domain) = cookie.domain.as_cow() else {
                return Err(D::Error::custom("cookie domain must be resolved"));
            };
            if !valid_snapshot_domain(&cookie.domain) {
                return Err(D::Error::custom(
                    "cookie domain is invalid or a public suffix",
                ));
            }
            if !cookie.path.starts_with('/') {
                return Err(D::Error::custom("cookie path must start with /"));
            }
            let key = (
                domain.into_owned(),
                String::from(&cookie.path),
                cookie.name().to_string(),
            );
            if !keys.insert(key) {
                return Err(D::Error::custom("duplicate cookie identity"));
            }
        }
        let store = CookieStore::from_cookies(cookies.into_iter().map(Ok::<_, Infallible>), false)
            .unwrap_or_else(|never| match never {});
        Ok(Self { store })
    }
}

fn normalize_domain(cookie: &mut Cookie<'_>, url: &Url) -> bool {
    if valid_snapshot_domain(&cookie.domain) {
        return true;
    }
    if matches!(&cookie.domain, CookieDomain::Suffix(_))
        && cookie.domain.host_is_identical(url)
        && let Ok(domain) = CookieDomain::host_only(url)
    {
        cookie.domain = domain;
        return true;
    }
    false
}

fn valid_snapshot_domain(domain: &CookieDomain) -> bool {
    match domain {
        CookieDomain::HostOnly(domain) => canonical_host(domain).as_deref() == Some(domain),
        CookieDomain::Suffix(domain) => {
            matches!(url::Host::parse(domain), Ok(url::Host::Domain(ref value)) if value == domain)
                && psl::domain(domain.as_bytes()).is_some()
        }
        CookieDomain::NotPresent | CookieDomain::Empty => false,
    }
}

fn canonical_host(value: &str) -> Option<String> {
    match url::Host::parse(value).ok()? {
        url::Host::Domain(domain) => Some(domain),
        url::Host::Ipv4(address) => Some(address.to_string()),
        url::Host::Ipv6(address) => Some(format!("[{address}]")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(value: &str) -> Url {
        Url::parse(value).unwrap()
    }

    fn response_headers(values: &[&str]) -> Headers {
        let mut headers = Headers::new();
        for value in values {
            headers.try_append("set-cookie", value).unwrap();
        }
        headers
    }

    #[test]
    fn explicit_cookies_are_host_scoped_with_root_path() {
        let origin = url("https://example.com/list");
        let mut cookies = Cookies::new();
        cookies.insert(&origin, "sid", "one").unwrap();

        assert_eq!(
            cookies.get(&url("https://example.com/detail"), "sid"),
            Some("one")
        );
        assert_eq!(
            cookies.get(&url("https://other.example/detail"), "sid"),
            None
        );
        assert!(cookies.insert(&origin, "bad;name", "one").is_err());
        assert!(cookies.insert(&origin, "sid", "bad;value").is_err());
    }

    #[test]
    fn response_cookies_obey_path_secure_and_expiry() {
        let origin = url("https://example.com/admin/login");
        let mut cookies = Cookies::new();
        cookies.store_response(
            &origin,
            &response_headers(&["admin=1; Path=/admin; Secure", "public=2; Path=/"]),
        );

        assert_eq!(
            cookies.get(&url("https://example.com/admin/home"), "admin"),
            Some("1")
        );
        assert_eq!(
            cookies.get(&url("https://example.com/public"), "admin"),
            None
        );
        assert_eq!(
            cookies.get(&url("http://example.com/admin/home"), "admin"),
            None
        );

        cookies.store_response(&origin, &response_headers(&["public=; Max-Age=0; Path=/"]));
        assert_eq!(cookies.get(&origin, "public"), None);
    }

    #[test]
    fn same_name_cookies_use_longest_path_first_deterministically() {
        let origin = url("https://example.com/admin/page");
        let mut cookies = Cookies::new();
        cookies.store_response(
            &origin,
            &response_headers(&["sid=root; Path=/", "sid=admin; Path=/admin"]),
        );

        assert_eq!(cookies.get(&origin, "sid"), Some("admin"));
        assert_eq!(cookies.get_all(&origin, "sid"), ["admin", "root"]);
        assert_eq!(
            cookies.request_header(&origin).unwrap().unwrap(),
            "sid=admin; sid=root"
        );
    }

    #[test]
    fn empty_cookie_value_is_not_treated_as_deletion() {
        let origin = url("https://example.com/");
        let mut cookies = Cookies::new();
        cookies.store_response(&origin, &response_headers(&["empty=; Path=/"]));

        assert_eq!(cookies.get(&origin, "empty"), Some(""));
    }

    #[test]
    fn expires_controls_cookie_lifetime() {
        let origin = url("https://example.com/");
        let mut cookies = Cookies::new();
        cookies.store_response(
            &origin,
            &response_headers(&[
                "past=gone; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/",
                "future=kept; Expires=Thu, 01 Jan 2099 00:00:00 GMT; Path=/",
            ]),
        );

        assert_eq!(cookies.get(&origin, "past"), None);
        assert_eq!(cookies.get(&origin, "future"), Some("kept"));
    }

    #[test]
    fn snapshot_round_trip_keeps_session_cookies() {
        let origin = url("https://example.com/");
        let mut cookies = Cookies::new();
        cookies.insert(&origin, "sid", "session").unwrap();

        let encoded = serde_json::to_value(&cookies).unwrap();
        assert!(encoded.is_array());
        assert_eq!(encoded.as_array().unwrap().len(), 1);
        let restored = serde_json::from_value::<Cookies>(encoded).unwrap();

        assert_eq!(restored.get(&origin, "sid"), Some("session"));
        assert!(serde_json::from_value::<Cookies>(serde_json::json!({"sid": "old"})).is_err());
    }

    #[test]
    fn filtering_keeps_only_cookies_applicable_to_the_target() {
        let origin = url("https://www.example.com/");
        let target = url("https://api.example.com/v1");
        let mut cookies = Cookies::new();
        cookies.store_response(
            &origin,
            &response_headers(&["host=one; Path=/", "domain=two; Domain=example.com; Path=/"]),
        );

        let filtered = cookies.for_url(&target);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered.get(&target, "domain"), Some("two"));
        assert_eq!(filtered.get(&target, "host"), None);
    }

    #[test]
    fn public_suffix_cookies_cannot_cross_to_an_unrelated_site() {
        let origin = url("https://example.com/");
        let target = url("https://other.com/");
        let mut cookies = Cookies::new();

        cookies.store_response(
            &origin,
            &response_headers(&["super=secret; Domain=com; Path=/"]),
        );

        assert!(cookies.is_empty());
        assert!(cookies.for_url(&target).is_empty());
    }

    #[test]
    fn same_host_public_suffix_domain_becomes_host_only() {
        let origin = url("http://localhost/");
        let mut cookies = Cookies::new();

        cookies.store_response(
            &origin,
            &response_headers(&["sid=one; Domain=localhost; Path=/"]),
        );

        assert_eq!(cookies.get(&origin, "sid"), Some("one"));
        assert_eq!(cookies.get(&url("http://sub.localhost/"), "sid"), None);
    }

    #[test]
    fn snapshot_rejects_a_public_suffix_cookie() {
        let origin = url("https://www.example.com/");
        let mut cookies = Cookies::new();
        cookies.store_response(
            &origin,
            &response_headers(&["shared=one; Domain=example.com; Path=/"]),
        );
        let mut encoded = serde_json::to_value(cookies).unwrap();
        encoded[0]["domain"] = serde_json::json!({"Suffix": "com"});

        let error = serde_json::from_value::<Cookies>(encoded).unwrap_err();

        assert!(error.to_string().contains("public suffix"));
    }

    #[test]
    fn snapshot_rejects_noncanonical_host_only_domains() {
        let origin = url("https://example.com/");
        let mut cookies = Cookies::new();
        cookies.insert(&origin, "sid", "one").unwrap();
        let encoded = serde_json::to_value(cookies).unwrap();

        for domain in ["", "EXAMPLE.COM", "not a host"] {
            let mut malformed = encoded.clone();
            malformed[0]["domain"] = serde_json::json!({"HostOnly": domain});

            assert!(serde_json::from_value::<Cookies>(malformed).is_err());
        }
    }
}
