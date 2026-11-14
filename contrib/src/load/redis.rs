use tokio::sync::OnceCell;

use harbor::load::{Error, Load, Message};

/// A Redis list-backed task configuration loader.
pub struct Redis {
    client: redis::Client,
    connection: OnceCell<redis::aio::ConnectionManager>,
    key: String,
}

impl Redis {
    pub fn new(url: impl Into<String>, key: impl Into<String>) -> Result<Self, redis::RedisError> {
        let key = key.into();
        if key.trim().is_empty() || key.chars().any(char::is_control) {
            return Err(redis::RedisError::from((
                redis::ErrorKind::InvalidClientConfig,
                "Redis Load key must be non-empty and contain no control characters",
            )));
        }
        Ok(Self {
            client: redis::Client::open(url.into())?,
            connection: OnceCell::new(),
            key,
        })
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn push(&self, message: &Message) -> Result<(), Error> {
        message.validate()?;
        let encoded = serde_json::to_string(message)
            .map_err(|error| Error::Message(format!("Redis Load encode failed: {error}")))?;
        let mut connection = self.manager().await?;
        let _: i64 = redis::cmd("RPUSH")
            .arg(&self.key)
            .arg(encoded)
            .query_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Ok(())
    }

    async fn manager(&self) -> Result<redis::aio::ConnectionManager, Error> {
        self.connection
            .get_or_try_init(|| async {
                redis::aio::ConnectionManager::new(self.client.clone())
                    .await
                    .map_err(redis_error)
            })
            .await
            .cloned()
    }
}

impl Load for Redis {
    fn load<'a>(
        &'a self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<Message>, Error>> + Send + 'a>,
    > {
        Box::pin(async move {
            let mut connection = self.manager().await?;
            loop {
                let encoded: Option<String> = redis::cmd("LPOP")
                    .arg(&self.key)
                    .query_async(&mut connection)
                    .await
                    .map_err(redis_error)?;
                if let Some(encoded) = encoded {
                    return serde_json::from_str(&encoded)
                        .map(Some)
                        .map_err(|error| Error::Message(format!("Redis Load decode failed: {error}")));
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        })
    }
}

fn redis_error(error: redis::RedisError) -> Error {
    Error::Message(format!("Redis Load operation failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_is_preserved() {
        let load = Redis::new("redis://127.0.0.1/", "task-config").unwrap();
        assert_eq!(load.key(), "task-config");
    }

    #[test]
    fn empty_key_is_rejected() {
        assert!(Redis::new("redis://127.0.0.1/", " ").is_err());
    }
}
