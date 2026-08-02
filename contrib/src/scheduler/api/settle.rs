use spider::{payload, scheduler};

use super::{Api, wire};

impl Api {
    pub(super) async fn acknowledge(
        &self,
        payload: &payload::Payload,
    ) -> Result<(), scheduler::Error> {
        self.require_open()?;
        payload
            .validate_ack()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        self.client
            .post_empty(
                "v1/worker/requests/ack",
                &wire::Lease::from_payload(payload),
                None,
            )
            .await
    }

    pub(super) async fn return_to_queue(
        &self,
        payload: &payload::Payload,
    ) -> Result<(), scheduler::Error> {
        self.require_open()?;
        payload
            .validate_release()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let identity = wire::Lease::from_payload(payload);
        let key = Self::invocation_key();
        self.client
            .post_empty("v1/worker/requests/release", &identity, Some(&key))
            .await
    }

    pub(super) async fn refresh(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.require_open()?;
        payload
            .validate_refresh_lease()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        self.client
            .post_empty(
                "v1/worker/requests/refresh",
                &wire::Lease::from_payload(payload),
                None,
            )
            .await
    }

    pub(super) async fn succeed(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.require_open()?;
        payload
            .validate_success()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let body = wire::Success {
            identity: wire::Lease::from_payload(payload),
            stats: payload.stats.clone(),
            start_time: payload.start_time.expect("validated success start time"),
            end_time: payload.end_time.expect("validated success end time"),
        };
        self.client
            .post_empty("v1/worker/requests/success", &body, None)
            .await
    }

    pub(super) async fn fail(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.require_open()?;
        payload
            .validate_failure()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let body = wire::Failure {
            identity: wire::Lease::from_payload(payload),
            error: payload.error.clone().expect("validated failure error"),
            stats: payload.stats.clone(),
            start_time: payload.start_time.expect("validated failure start time"),
            end_time: payload.end_time.expect("validated failure end time"),
        };
        self.client
            .post_empty("v1/worker/requests/failure", &body, None)
            .await
    }
}
