mod claim;
mod init;
mod trace;

use spider::{net, payload, scheduler};

use super::{Api, wire};

impl Api {
    pub(super) async fn enqueue(&self, payload: payload::Payload) -> Result<(), scheduler::Error> {
        self.require_open()?;
        payload.validate_push().map_err(scheduler::Error::Message)?;
        if payload.requests.is_empty() {
            return Ok(());
        }
        let context = wire::Context::from_payload(&payload);
        let requests = payload
            .requests
            .into_iter()
            .map(net::request::Snapshot::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(scheduler::Error::Message)?;
        let body = wire::Push { context, requests };
        self.client
            .post_empty("v1/worker/requests/push", &body, None)
            .await
    }
}
