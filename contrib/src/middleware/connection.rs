use tokio::sync::OnceCell;

pub(super) struct Connection {
    client: redis::Client,
    manager: OnceCell<redis::aio::ConnectionManager>,
}

impl Connection {
    pub(super) fn new(url: impl Into<String>) -> redis::RedisResult<Self> {
        Ok(Self {
            client: redis::Client::open(url.into())?,
            manager: OnceCell::new(),
        })
    }

    pub(super) async fn manager(&self) -> redis::RedisResult<redis::aio::ConnectionManager> {
        self.manager
            .get_or_try_init(|| redis::aio::ConnectionManager::new(self.client.clone()))
            .await
            .cloned()
    }
}
