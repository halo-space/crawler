use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

use crate::trace;

/// One crawler configuration consumed by a Worker.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Message {
    pub task_id: String,
    pub trace_id: String,
    pub snapshot: trace::Snapshot,
}

impl Message {
    pub fn new(
        task_id: impl Into<String>,
        trace_id: impl Into<String>,
        snapshot: trace::Snapshot,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            trace_id: trace_id.into(),
            snapshot,
        }
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.task_id.is_empty() {
            return Err(Error::Message("task_id must not be empty".to_string()));
        }
        if self.trace_id.is_empty() {
            return Err(Error::Message("trace_id must not be empty".to_string()));
        }
        if self.snapshot.task_id != self.task_id {
            return Err(Error::Message(
                "task_id does not match the configuration snapshot".to_string(),
            ));
        }
        self.snapshot.validate().map_err(Error::Message)
    }
}

/// Loads one complete crawler configuration for a Worker.
pub trait Load: Send + Sync {
    fn load<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Message>, Error>> + Send + 'a>>;
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("load error: {0}")]
    Message(String),
}
