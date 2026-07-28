use redis::AsyncCommands as _;
use spider::{scheduler, trace};

use super::Redis;
use super::error::redis as redis_error;

impl Redis {
    pub(super) async fn load_trace(
        &self,
        trace_id: &str,
    ) -> Result<Option<trace::Snapshot>, scheduler::Error> {
        if trace_id.is_empty() {
            return Err(scheduler::Error::Message(
                "trace_id must not be empty".to_string(),
            ));
        }
        let mut connection = self.connection().await?;
        let stored: Option<String> = connection
            .hget(self.keys.traces(), trace_id)
            .await
            .map_err(redis_error)?;
        stored
            .map(|encoded| {
                serde_json::from_str::<trace::Snapshot>(&encoded)
                    .map_err(|error| scheduler::Error::InvalidTrace {
                        id: trace_id.to_string(),
                        message: error.to_string(),
                    })
                    .and_then(|snapshot| {
                        snapshot
                            .validate()
                            .map_err(|message| scheduler::Error::InvalidTrace {
                                id: trace_id.to_string(),
                                message,
                            })?;
                        Ok(snapshot)
                    })
            })
            .transpose()
    }
}
