use harbor::{item, payload};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

const DEFAULT_NAMESPACE: &str = "crawler";
const DEFAULT_TOPIC: &str = "items";

/// A Redis-backed Item Store.
///
/// Each submitted Item is serialized as one JSON message and appended to the
/// configured Redis list. The Store only produces queue messages; consuming
/// and acknowledging them belongs to the downstream application.
pub struct Redis {
    client: redis::Client,
    connection: Mutex<Option<redis::aio::ConnectionManager>>,
    namespace: String,
    topic: String,
}

/// The message written to the Redis Item queue.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Record {
    pub id: String,
    pub task_id: String,
    pub trace_id: String,
    pub request_id: String,
    pub node: String,
    pub data: serde_json::Value,
}

impl Redis {
    /// Creates a Redis Item Store with the default `crawler:items` topic.
    pub fn new(url: impl Into<String>) -> Result<Self, redis::RedisError> {
        Self::with_namespace(url, DEFAULT_NAMESPACE)
    }

    /// Creates a Redis Item Store with a caller-provided namespace.
    pub fn with_namespace(
        url: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self, redis::RedisError> {
        let namespace = namespace.into();
        validate_key_part(&namespace, "namespace")?;
        Ok(Self {
            client: redis::Client::open(url.into())?,
            connection: Mutex::new(None),
            namespace,
            topic: DEFAULT_TOPIC.to_string(),
        })
    }

    /// Selects the topic used for Item messages.
    pub fn with_topic(mut self, topic: impl Into<String>) -> Result<Self, redis::RedisError> {
        let topic = topic.into();
        validate_key_part(&topic, "topic")?;
        self.topic = topic;
        Ok(self)
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn topic_key(&self) -> String {
        format!("{}:{}", self.namespace, self.topic)
    }

    async fn manager(&self) -> Result<redis::aio::ConnectionManager, item::Error> {
        self.connection
            .lock()
            .await
            .as_ref()
            .cloned()
            .ok_or_else(|| item::Error::Message("Redis Item Store is not open".to_string()))
    }
}

impl item::Store for Redis {
    async fn open(&self) -> Result<(), item::Error> {
        let mut active = self.connection.lock().await;
        if active.is_some() {
            return Ok(());
        }

        let mut connection = redis::aio::ConnectionManager::new(self.client.clone())
            .await
            .map_err(redis_error)?;
        let _: String = redis::cmd("PING")
            .query_async(&mut connection)
            .await
            .map_err(redis_error)?;
        *active = Some(connection);
        Ok(())
    }

    async fn close(&self) -> Result<(), item::Error> {
        self.connection.lock().await.take();
        Ok(())
    }

    async fn submit(&self, payload: &payload::Payload) -> Result<(), item::Error> {
        payload
            .validate_store()
            .map_err(|message| item::Error::Message(message.to_string()))?;
        if payload.items.is_empty() {
            return Ok(());
        }

        let mut connection = self.manager().await?;
        let mut messages = Vec::with_capacity(payload.items.len());
        for value in &payload.items {
            let record = Record {
                id: value.id().to_string(),
                task_id: payload.task_id.clone(),
                trace_id: payload.trace_id.clone(),
                request_id: payload.id.clone(),
                node: payload.node.clone(),
                data: serde_json::to_value(value.as_ref())?,
            };
            messages.push(serde_json::to_string(&record)?);
        }

        let mut command = redis::cmd("RPUSH");
        command.arg(self.topic_key());
        for message in messages {
            command.arg(message);
        }
        let _: i64 = command
            .query_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Ok(())
    }
}

fn validate_key_part(value: &str, part: &str) -> Result<(), redis::RedisError> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "invalid Redis Item Store key",
            format!("{part} must be non-empty and contain no control characters"),
        )));
    }
    Ok(())
}

fn redis_error(error: redis::RedisError) -> item::Error {
    item::Error::Message(format!("Redis Item Store operation failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_key_uses_only_namespace_and_topic() {
        let store = Redis::with_namespace("redis://127.0.0.1/", "harbor")
            .unwrap()
            .with_topic("items")
            .unwrap();
        assert_eq!(store.topic_key(), "harbor:items");
    }

    #[test]
    fn record_round_trips_as_json() {
        let record = Record {
            id: "item-1".to_string(),
            task_id: "task-1".to_string(),
            trace_id: "trace-1".to_string(),
            request_id: "request-1".to_string(),
            node: "detail".to_string(),
            data: serde_json::json!({"title": "百度词条"}),
        };
        let encoded = serde_json::to_string(&record).unwrap();
        assert_eq!(serde_json::from_str::<Record>(&encoded).unwrap(), record);
    }
}
