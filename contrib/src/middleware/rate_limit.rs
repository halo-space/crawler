use std::time::Duration;

use base64::Engine as _;
use spider::middleware::{BoxFuture, Middleware, Next, Spec};
use spider::net::Request;

use super::connection::Connection;

const IDLE_TTL_MS: u64 = 60_000;
const MAX_LUA_INTEGER: u128 = 9_007_199_254_740_991;

/// A Redis-backed RateLimit shared by every Worker using the same group.
pub struct Redis {
    connection: Connection,
    script: redis::Script,
}

impl Redis {
    /// Creates a shared RateLimit. The connection is established on its first operation.
    pub fn new(url: impl Into<String>) -> Result<Self, spider::middleware::Error> {
        let connection = Connection::new(url).map_err(redis_error)?;
        let script = redis::Script::new(include_str!("rate_limit/reserve.lua"));
        Ok(Self { connection, script })
    }

    async fn reserve(
        &self,
        group: &str,
        interval: Duration,
    ) -> Result<Duration, spider::middleware::Error> {
        let interval = interval_micros(interval)?;
        let mut connection = self.connection.manager().await.map_err(redis_error)?;
        let (status, delay) = self
            .script
            .key(key(group))
            .arg(interval)
            .arg(IDLE_TTL_MS)
            .invoke_async::<(String, String)>(&mut connection)
            .await
            .map_err(redis_error)?;

        match status.as_str() {
            "OK" => delay
                .parse::<u64>()
                .map(Duration::from_micros)
                .map_err(|_| message("Redis RateLimit returned an invalid delay")),
            "CONFLICT" => Err(invalid("group is already active with a different qps")),
            "RANGE" => Err(invalid("qps exceeds the Redis server time range")),
            "CORRUPT" => Err(message("Redis RateLimit state is invalid")),
            _ => Err(message(format!(
                "Redis RateLimit returned an unknown status: {status}"
            ))),
        }
    }
}

impl Middleware for Redis {
    fn order(&self, _hook: &str) -> i32 {
        200
    }

    fn before_download<'a>(
        &'a self,
        request: Request,
        spec: &'a Spec,
    ) -> BoxFuture<'a, Next<Request>> {
        Box::pin(async move {
            let config = spider::middleware::rate_limit::Config::from_spec(spec)?;
            let delay = self
                .reserve(&config.group(&request)?, config.interval())
                .await?;
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            Ok(Next::Continue(request))
        })
    }
}

fn interval_micros(interval: Duration) -> Result<u64, spider::middleware::Error> {
    let nanos = interval.as_nanos();
    if nanos < 1_000 {
        return Err(invalid("qps exceeds the Redis timer precision"));
    }
    let micros = nanos.div_ceil(1_000);
    if micros == 0 || micros > MAX_LUA_INTEGER {
        return Err(invalid("qps is outside the Redis timer precision range"));
    }
    u64::try_from(micros).map_err(|_| invalid("qps is outside the Redis timer precision range"))
}

fn key(group: &str) -> String {
    let group = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(group.as_bytes());
    format!("rate_limit:{group}")
}

fn redis_error(error: redis::RedisError) -> spider::middleware::Error {
    message(format!("Redis RateLimit operation failed: {error}"))
}

fn invalid(message: impl Into<String>) -> spider::middleware::Error {
    spider::middleware::Error::InvalidConfig {
        name: "rate_limit".to_string(),
        message: message.into(),
    }
}

fn message(message: impl Into<String>) -> spider::middleware::Error {
    spider::middleware::Error::Message(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_sub_microsecond_intervals_and_rounds_larger_values_up() {
        assert!(matches!(
            interval_micros(Duration::from_nanos(999)),
            Err(spider::middleware::Error::InvalidConfig { name, message })
                if name == "rate_limit" && message.contains("timer precision")
        ));
        assert_eq!(interval_micros(Duration::from_nanos(1_000)).unwrap(), 1);
        assert_eq!(interval_micros(Duration::from_nanos(1_001)).unwrap(), 2);
    }

    #[test]
    fn keys_encode_only_the_group() {
        assert_eq!(key("api"), "rate_limit:YXBp");
        assert_eq!(key("api:global"), "rate_limit:YXBpOmdsb2JhbA");
    }
}
