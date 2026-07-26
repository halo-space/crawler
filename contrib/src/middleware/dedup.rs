use base64::Engine as _;

use spider::middleware::dedup::{Config, Ttl};
use spider::middleware::{BoxFuture, Middleware, Next, Spec};
use spider::net::Request;

use super::connection::Connection;

/// Redis Bloom sizing for each `task_id + node` Dedup bucket.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Options {
    capacity: usize,
    error_rate: f64,
}

/// A Redis Bloom-backed Dedup shared by every Worker using the same Redis database.
pub struct Redis {
    connection: Connection,
    options: Options,
}

impl Options {
    /// Creates Bloom options with a positive capacity and an error rate in `0.0..1.0`.
    pub fn new(capacity: usize, error_rate: f64) -> Result<Self, spider::middleware::Error> {
        if capacity == 0 {
            return Err(invalid("capacity must be greater than zero"));
        }
        if !error_rate.is_finite() || error_rate <= 0.0 || error_rate >= 1.0 {
            return Err(invalid(
                "error_rate must be greater than zero and less than one",
            ));
        }
        Ok(Self {
            capacity,
            error_rate,
        })
    }
}

impl Redis {
    /// Creates a distributed Dedup. The connection is established on its first operation.
    pub fn new(
        url: impl Into<String>,
        options: Options,
    ) -> Result<Self, spider::middleware::Error> {
        Ok(Self {
            connection: Connection::new(url)
                .map_err(|error| invalid(format!("Redis URL is invalid: {error}")))?,
            options,
        })
    }

    async fn contains_or_insert(
        &self,
        fingerprint: &spider::middleware::dedup::Fingerprint,
    ) -> Result<bool, spider::middleware::Error> {
        let mut connection = self.connection.manager().await.map_err(redis_error)?;
        let inserted = redis::cmd("BF.INSERT")
            .arg(key(fingerprint.task_id(), fingerprint.node()))
            .arg("ERROR")
            .arg(self.options.error_rate)
            .arg("CAPACITY")
            .arg(self.options.capacity)
            .arg("ITEMS")
            .arg(fingerprint.value())
            .query_async::<Vec<bool>>(&mut connection)
            .await
            .map_err(redis_error)?;
        match inserted.as_slice() {
            [inserted] => Ok(!inserted),
            values => Err(message(format!(
                "Redis Bloom returned {} results for one fingerprint",
                values.len()
            ))),
        }
    }
}

impl Middleware for Redis {
    fn order(&self, _hook: &str) -> i32 {
        400
    }

    fn before_scheduler<'a>(
        &'a self,
        request: Request,
        spec: &'a Spec,
    ) -> BoxFuture<'a, Next<Request>> {
        Box::pin(async move {
            if request.dont_filter {
                return Ok(Next::Continue(request));
            }
            let config = Config::from_spec(spec)?;
            match config.ttl() {
                Ttl::Disabled => return Ok(Next::Continue(request)),
                Ttl::Finite(_) => {
                    return Err(invalid(
                        "Redis Bloom does not support finite fingerprint ttl; use -1 for permanent",
                    ));
                }
                Ttl::Permanent => {}
            }
            let fingerprint = config
                .fingerprint(&request)?
                .expect("permanent Dedup configuration always produces a fingerprint");
            if self.contains_or_insert(&fingerprint).await? {
                Ok(Next::Skip)
            } else {
                Ok(Next::Continue(request))
            }
        })
    }
}

fn key(task_id: &str, node: &str) -> String {
    let encoder = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    format!("dedup:{}:{}", encoder.encode(task_id), encoder.encode(node))
}

fn redis_error(error: redis::RedisError) -> spider::middleware::Error {
    let detail = error.to_string();
    if detail.to_ascii_lowercase().contains("unknown command")
        && detail.to_ascii_lowercase().contains("bf.insert")
    {
        return message("Redis Bloom module is required for distributed Dedup");
    }
    message(format!("Redis Dedup operation failed: {detail}"))
}

fn invalid(message: impl Into<String>) -> spider::middleware::Error {
    spider::middleware::Error::InvalidConfig {
        name: "dedup".to_string(),
        message: message.into(),
    }
}

fn message(message: impl Into<String>) -> spider::middleware::Error {
    spider::middleware::Error::Message(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn redis() -> Redis {
        Redis::new("redis://127.0.0.1:1", Options::new(100, 0.01).unwrap()).unwrap()
    }

    #[test]
    fn validates_options() {
        assert!(Options::new(0, 0.01).is_err());
        for error_rate in [0.0, 1.0, -0.1, f64::INFINITY, f64::NAN] {
            assert!(Options::new(100, error_rate).is_err());
        }
        let options = Options::new(100, 0.01).unwrap();
        assert_eq!(options.capacity, 100);
        assert_eq!(options.error_rate, 0.01);
    }

    #[test]
    fn encodes_bucket_segments_without_delimiter_collisions() {
        assert_ne!(key("a:b", "c"), key("a", "b:c"));
        assert_eq!(key("task", "detail"), "dedup:dGFzaw:ZGV0YWls");
    }

    #[tokio::test]
    async fn bypasses_redis_for_dont_filter_and_disabled_ttl() {
        let mut bypass = Request::follow("https://example.com").unwrap();
        bypass.dont_filter = true;
        assert!(matches!(
            redis()
                .before_scheduler(bypass, &Spec::new("dedup"))
                .await
                .unwrap(),
            Next::Continue(_)
        ));

        let mut disabled = Request::follow("https://example.com").unwrap();
        disabled.task_id = "task".to_string();
        assert!(matches!(
            redis()
                .before_scheduler(
                    disabled,
                    &Spec::new("dedup").args(serde_json::json!({
                        "key": ["$request.url"],
                        "ttl": 0
                    }))
                )
                .await
                .unwrap(),
            Next::Continue(_)
        ));
    }

    #[tokio::test]
    async fn rejects_finite_ttl_before_connecting() {
        let mut request = Request::follow("https://example.com").unwrap();
        request.task_id = "task".to_string();
        let error = redis()
            .before_scheduler(
                request,
                &Spec::new("dedup").args(serde_json::json!({
                    "key": ["$request.url"],
                    "ttl": 1000
                })),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            spider::middleware::Error::InvalidConfig { name, message }
                if name == "dedup" && message.contains("does not support finite")
        ));
    }
}
