use spider::{net, payload, scheduler, trace};

use super::super::{Api, wire};

impl Api {
    pub(in crate::scheduler::api) async fn initialize(
        &self,
        trace_id: String,
        snapshot: trace::Snapshot,
        requests: Vec<net::Request>,
    ) -> Result<(), scheduler::Error> {
        self.require_open()?;
        if trace_id.is_empty() {
            return Err(scheduler::Error::Message(
                "trace_id must not be empty".to_string(),
            ));
        }
        snapshot
            .validate()
            .map_err(|message| scheduler::Error::InvalidTrace {
                id: trace_id.clone(),
                message,
            })?;
        if requests.iter().any(|request| request.trace_id != trace_id) {
            return Err(scheduler::Error::Message(
                "all initial requests must reference the initialized trace_id".to_string(),
            ));
        }
        if requests
            .iter()
            .any(|request| request.task_id != snapshot.task_id)
        {
            return Err(scheduler::Error::Message(
                "all initial requests must reference the Trace Snapshot task_id".to_string(),
            ));
        }
        let payload = payload::Payload::new().requests(requests);
        payload.validate_push().map_err(scheduler::Error::Message)?;
        let requests = payload.requests;
        let requests = requests
            .into_iter()
            .map(net::request::Snapshot::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(scheduler::Error::Message)?;
        let body = wire::Init {
            trace_id,
            trace: snapshot,
            requests,
        };
        self.client.validate_body(&body)?;
        let digest = wire::canonical_digest(&body)
            .map_err(|error| scheduler::Error::Message(error.to_string()))?;
        let key = format!("init-{digest}");
        self.client
            .post_empty("v1/worker/runs/init", &body, Some(&key))
            .await
    }
}
