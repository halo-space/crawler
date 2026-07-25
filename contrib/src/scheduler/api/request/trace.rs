use std::sync::Arc;

use spider::{scheduler, trace};

use super::super::Api;

impl Api {
    pub(in crate::scheduler::api) async fn load_trace(
        &self,
        trace_id: &str,
    ) -> Result<Option<trace::Snapshot>, scheduler::Error> {
        self.require_open()?;
        if trace_id.is_empty() {
            return Err(scheduler::Error::Message(
                "trace_id must not be empty".to_string(),
            ));
        }
        if let Some(snapshot) = self.cached_trace(trace_id).await {
            return Ok(Some(snapshot.as_ref().clone()));
        }

        validate_id(trace_id)?;
        let snapshot = self
            .client
            .get_segments::<Option<trace::Snapshot>>(&["v1", "worker", "traces", trace_id])
            .await?;
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };
        snapshot
            .validate()
            .map_err(|message| scheduler::Error::InvalidTrace {
                id: trace_id.to_string(),
                message,
            })?;
        self.cache_trace(trace_id.to_string(), snapshot.clone())
            .await?;
        Ok(Some(snapshot))
    }

    pub(super) async fn cached_trace(&self, trace_id: &str) -> Option<Arc<trace::Snapshot>> {
        self.runtime.traces.lock().await.get(trace_id)
    }

    pub(super) async fn cache_trace(
        &self,
        trace_id: String,
        snapshot: trace::Snapshot,
    ) -> Result<Arc<trace::Snapshot>, scheduler::Error> {
        self.runtime
            .traces
            .lock()
            .await
            .insert(trace_id.clone(), snapshot)
            .map_err(|message| scheduler::Error::InvalidTrace {
                id: trace_id,
                message,
            })
    }
}

fn validate_id(value: &str) -> Result<(), scheduler::Error> {
    if value.chars().any(char::is_control) {
        Err(scheduler::Error::Message(
            "trace_id must not contain control characters".to_string(),
        ))
    } else {
        Ok(())
    }
}
