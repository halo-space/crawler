use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redis::AsyncCommands as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use harbor::downloader::Error;
use harbor::downloader::browser::{Cookie, CookieLoadFuture, CookieSaveFuture, CookieStore};

const DEFAULT_NAMESPACE: &str = "crawler";

/// Redis-backed Browser CookieStore for Workers that share login state.
pub struct RedisCookieStore {
    client: redis::Client,
    namespace: String,
}

#[derive(Deserialize, Serialize)]
struct StoredCookies {
    cookies: Vec<Cookie>,
    saved_at_ms: u64,
    refresh_after_ms: u64,
}

impl RedisCookieStore {
    pub fn new(url: impl Into<String>) -> Result<Self, redis::RedisError> {
        Self::with_namespace(url, DEFAULT_NAMESPACE)
    }

    pub fn with_namespace(
        url: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self, redis::RedisError> {
        let namespace = namespace.into();
        if namespace.trim().is_empty()
            || namespace
                .chars()
                .any(|value| value.is_control() || value.is_whitespace())
        {
            return Err(redis::RedisError::from((
                redis::ErrorKind::InvalidClientConfig,
                "Redis CookieStore namespace must be non-empty and contain no whitespace",
            )));
        }
        Ok(Self {
            client: redis::Client::open(url.into())?,
            namespace,
        })
    }

    fn key(&self, login_key: &str, origin: &str) -> String {
        let mut digest = Sha256::new();
        digest.update(login_key.as_bytes());
        digest.update([0]);
        digest.update(origin.as_bytes());
        format!(
            "{}:browser:cookies:{}",
            self.namespace,
            hex(&digest.finalize())
        )
    }
}

impl CookieStore for RedisCookieStore {
    fn load<'a>(&'a self, login_key: &'a str, origin: &'a str) -> CookieLoadFuture<'a> {
        Box::pin(async move {
            let mut connection = redis::aio::ConnectionManager::new(self.client.clone())
                .await
                .map_err(redis_error)?;
            let encoded: Option<String> = connection
                .get(self.key(login_key, origin))
                .await
                .map_err(redis_error)?;
            let Some(encoded) = encoded else {
                return Ok(None);
            };
            let stored = serde_json::from_str::<StoredCookies>(&encoded).map_err(|error| {
                Error::InvalidConfig(format!("Redis CookieStore data is invalid: {error}"))
            })?;
            let now = now_millis();
            if now.saturating_sub(stored.saved_at_ms) >= stored.refresh_after_ms {
                return Ok(None);
            }
            Ok(Some(stored.cookies))
        })
    }

    fn save<'a>(
        &'a self,
        login_key: &'a str,
        origin: &'a str,
        cookies: Vec<Cookie>,
        ttl: Duration,
        refresh_after: Duration,
    ) -> CookieSaveFuture<'a> {
        Box::pin(async move {
            let encoded = serde_json::to_string(&StoredCookies {
                cookies,
                saved_at_ms: now_millis(),
                refresh_after_ms: refresh_after.as_millis().try_into().map_err(|_| {
                    Error::InvalidConfig("Redis CookieStore refresh_after is too large".to_string())
                })?,
            })
            .map_err(|error| {
                Error::InvalidConfig(format!("Redis CookieStore encode failed: {error}"))
            })?;
            let ttl_seconds = ttl_seconds(ttl)?;
            let mut connection = redis::aio::ConnectionManager::new(self.client.clone())
                .await
                .map_err(redis_error)?;
            connection
                .set_ex::<_, _, ()>(self.key(login_key, origin), encoded, ttl_seconds)
                .await
                .map_err(redis_error)
        })
    }
}

fn ttl_seconds(ttl: Duration) -> Result<u64, Error> {
    let millis = ttl.as_millis();
    if millis == 0 {
        return Err(Error::InvalidConfig(
            "Redis CookieStore ttl must be positive".to_string(),
        ));
    }
    let seconds = millis.div_ceil(1000);
    seconds
        .try_into()
        .map_err(|_| Error::InvalidConfig("Redis CookieStore ttl is too large".to_string()))
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn redis_error(error: redis::RedisError) -> Error {
    Error::InvalidConfig(format!("Redis CookieStore operation failed: {error}"))
}

fn hex(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(TABLE[(byte >> 4) as usize] as char);
        output.push(TABLE[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_namespace_and_ttl() {
        assert!(RedisCookieStore::new("redis://127.0.0.1/").is_ok());
        assert!(RedisCookieStore::with_namespace("redis://127.0.0.1/", "bad namespace").is_err());
        assert!(ttl_seconds(Duration::from_millis(1)).unwrap() == 1);
        assert!(ttl_seconds(Duration::ZERO).is_err());
    }

    #[test]
    fn cookie_key_is_stable_and_separates_origins() {
        let store = RedisCookieStore::new("redis://127.0.0.1/").unwrap();
        assert_eq!(
            store.key("jd", "https://example.com"),
            store.key("jd", "https://example.com")
        );
        assert_ne!(
            store.key("jd", "https://example.com"),
            store.key("jd", "https://other.example")
        );
    }
}
