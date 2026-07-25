mod claim;
mod init;
mod trace;

use spider::{net, payload, scheduler};

use super::{Api, wire};

impl Api {
    pub(super) async fn enqueue(&self, payload: payload::Payload) -> Result<(), scheduler::Error> {
        self.require_open()?;
        payload
            .validate_push()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        if payload.requests.is_empty() {
            return Ok(());
        }
        let context = wire::Context::from_payload(&payload);
        validate_new_requests(&payload.requests)?;
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

fn validate_new_requests(requests: &[net::Request]) -> Result<(), scheduler::Error> {
    let mut ids = std::collections::HashSet::with_capacity(requests.len());
    for request in requests {
        if !ids.insert(request.id.as_str()) {
            return Err(scheduler::Error::Message(format!(
                "duplicate Request id in payload: {}",
                request.id
            )));
        }
        if request.id.is_empty() {
            return Err(scheduler::Error::Message(
                "new Request id must not be empty".to_string(),
            ));
        }
        if request.task_id.is_empty() != request.trace_id.is_empty() {
            return Err(scheduler::Error::Message(
                "new Request task_id and trace_id must both be set or both be empty".to_string(),
            ));
        }
        if request.version != 0 {
            return Err(scheduler::Error::Message(
                "new Request version must be 0".to_string(),
            ));
        }
        if request.state != net::State::Pending {
            return Err(scheduler::Error::Message(
                "new Request state must be pending".to_string(),
            ));
        }
        if !request.leased_by.is_empty() || request.lease_time != 0 {
            return Err(scheduler::Error::Message(
                "new Request must not have a lease".to_string(),
            ));
        }
        if !request.failed_workers.is_empty() {
            return Err(scheduler::Error::Message(
                "new Request failed_workers must be empty".to_string(),
            ));
        }
        if request.next_time < 0 {
            return Err(scheduler::Error::Message(
                "new Request next_time must not be negative".to_string(),
            ));
        }
        if request.retry_count != 0 || request.max_retry_count <= 0 {
            return Err(scheduler::Error::Message(
                "new Request requires retry_count 0 and a positive max_retry_count".to_string(),
            ));
        }
        for spec in &request.middlewares {
            spider::middleware::check(spec).map_err(|error| {
                scheduler::Error::Message(format!("new Request has invalid middleware: {error}"))
            })?;
        }
    }
    Ok(())
}
