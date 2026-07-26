use std::collections::HashMap;

use serde_json::Value;

use crate::{item, net};

pub use net::State;

pub struct Payload {
    pub id: String,
    pub task_id: String,
    pub trace_id: String,
    pub version: i64,
    pub worker_id: String,
    pub node: String,

    pub state: State,
    pub error: Option<String>,
    pub stats: HashMap<String, Value>,
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,

    pub requests: Vec<net::Request>,
    pub items: Vec<Box<dyn item::Item>>,
}

impl Default for Payload {
    fn default() -> Self {
        Self::new()
    }
}

impl Payload {
    pub fn new() -> Self {
        Self {
            id: String::new(),
            task_id: String::new(),
            trace_id: String::new(),
            version: 0,
            worker_id: String::new(),
            node: String::new(),
            state: State::Done,
            error: None,
            stats: HashMap::new(),
            start_time: None,
            end_time: None,
            requests: Vec::new(),
            items: Vec::new(),
        }
    }

    pub fn for_request(request: &net::Request, worker_id: impl Into<String>) -> Self {
        Self {
            id: request.id.clone(),
            task_id: request.task_id.clone(),
            trace_id: request.trace_id.clone(),
            version: request.version,
            worker_id: worker_id.into(),
            node: request.node_key().to_string(),
            state: State::Done,
            error: None,
            stats: HashMap::new(),
            start_time: None,
            end_time: None,
            requests: Vec::new(),
            items: Vec::new(),
        }
    }

    pub fn requests(mut self, requests: Vec<net::Request>) -> Self {
        self.requests = requests;
        self
    }

    pub fn items(mut self, items: Vec<Box<dyn item::Item>>) -> Self {
        self.items = items;
        self
    }

    pub fn failed(mut self, error: impl Into<String>) -> Self {
        self.state = State::Failed;
        self.error = Some(error.into());
        self
    }

    pub fn validate_push(&self) -> Result<(), String> {
        if !self.items.is_empty() || self.has_completion_fields() {
            return Err("request payload contains unrelated fields".to_string());
        }
        if self.requests.iter().any(|request| {
            (!self.task_id.is_empty() && request.task_id != self.task_id)
                || (!self.trace_id.is_empty() && request.trace_id != self.trace_id)
        }) {
            return Err("request payload ownership mismatch".to_string());
        }
        let mut ids = std::collections::HashSet::with_capacity(self.requests.len());
        for request in &self.requests {
            request.validate_initial()?;
            if !ids.insert(request.id.as_str()) {
                return Err(format!("duplicate Request id in payload: {}", request.id));
            }
        }
        Ok(())
    }

    /// Validates the fields consumed by an Item Store.
    pub fn validate_store(&self) -> Result<(), &'static str> {
        if !self.items.is_empty() && self.task_id.is_empty() {
            return Err("item payload requires task id");
        }
        if !self.requests.is_empty() || self.has_completion_fields() {
            return Err("item payload contains unrelated fields");
        }
        if self.items.iter().any(|item| item.id().is_empty()) {
            return Err("every Item requires a non-empty framework Item ID");
        }
        Ok(())
    }

    pub fn validate_ack(&self) -> Result<(), &'static str> {
        self.validate_ownership()?;
        if self.state != State::Processing {
            return Err("ack payload state must be processing");
        }
        self.validate_empty_collections()?;
        if self.error.is_some()
            || !self.stats.is_empty()
            || self.start_time.is_some()
            || self.end_time.is_some()
        {
            return Err("ack payload contains completion fields");
        }
        Ok(())
    }

    pub fn validate_refresh_lease(&self) -> Result<(), &'static str> {
        self.validate_ownership()?;
        if self.state != State::Processing {
            return Err("refresh_lease payload state must be processing");
        }
        self.validate_empty_collections()?;
        if self.error.is_some()
            || !self.stats.is_empty()
            || self.start_time.is_some()
            || self.end_time.is_some()
        {
            return Err("refresh_lease payload contains completion fields");
        }
        Ok(())
    }

    pub fn validate_release(&self) -> Result<(), &'static str> {
        self.validate_ownership()?;
        if self.state != State::Processing {
            return Err("release payload state must be processing");
        }
        self.validate_empty_collections()?;
        if self.error.is_some()
            || !self.stats.is_empty()
            || self.start_time.is_some()
            || self.end_time.is_some()
        {
            return Err("release payload contains completion fields");
        }
        Ok(())
    }

    pub fn validate_success(&self) -> Result<(), &'static str> {
        self.validate_completion()?;
        if self.state != State::Done || self.error.is_some() {
            return Err("success payload requires done state without error");
        }
        Ok(())
    }

    pub fn validate_failure(&self) -> Result<(), &'static str> {
        self.validate_completion()?;
        if self.state != State::Failed || self.error.as_deref().is_none_or(str::is_empty) {
            return Err("failure payload requires failed state and error");
        }
        Ok(())
    }

    pub fn validate_ownership(&self) -> Result<(), &'static str> {
        if self.id.is_empty() || self.worker_id.is_empty() || self.node.is_empty() {
            return Err("payload requires request id, worker id, and node");
        }
        if self.version <= 0 {
            return Err("payload requires a positive request version");
        }
        Ok(())
    }

    fn validate_empty_collections(&self) -> Result<(), &'static str> {
        if !self.requests.is_empty() || !self.items.is_empty() {
            return Err("completion payload must not contain requests or items");
        }
        Ok(())
    }

    fn validate_completion(&self) -> Result<(), &'static str> {
        self.validate_ownership()?;
        self.validate_empty_collections()?;
        let (Some(start_time), Some(end_time)) = (self.start_time, self.end_time) else {
            return Err("completion payload requires start and end time");
        };
        if start_time < 0 || end_time < 0 {
            return Err("completion times must be non-negative");
        }
        if end_time < start_time {
            return Err("completion end time must not precede start time");
        }
        Ok(())
    }

    fn has_completion_fields(&self) -> bool {
        self.error.is_some()
            || !self.stats.is_empty()
            || self.start_time.is_some()
            || self.end_time.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_validation_rejects_contradictory_state_and_error() {
        let mut request = net::Request::follow("https://example.com").unwrap();
        request.version = 1;

        let mut done = Payload::for_request(&request, "worker-1");
        done.start_time = Some(1);
        done.end_time = Some(2);
        done.error = Some("unexpected".to_string());
        assert_eq!(
            done.validate_success(),
            Err("success payload requires done state without error")
        );

        done.error = None;
        done.state = State::Failed;
        assert_eq!(
            done.validate_success(),
            Err("success payload requires done state without error")
        );

        let mut failed = Payload::for_request(&request, "worker-1");
        failed.state = State::Failed;
        failed.start_time = Some(1);
        failed.end_time = Some(2);
        assert_eq!(
            failed.validate_failure(),
            Err("failure payload requires failed state and error")
        );

        failed.state = State::Done;
        failed.error = Some("unexpected".to_string());
        assert_eq!(
            failed.validate_failure(),
            Err("failure payload requires failed state and error")
        );
    }

    #[test]
    fn completion_validation_accepts_valid_terminal_states() {
        let mut request = net::Request::follow("https://example.com").unwrap();
        request.version = 1;
        let mut success = Payload::for_request(&request, "worker-1");
        success.start_time = Some(0);
        success.end_time = Some(0);
        assert!(success.validate_success().is_ok());

        let mut failure = Payload::for_request(&request, "worker-1").failed("boom");
        failure.start_time = Some(1);
        failure.end_time = Some(2);
        assert!(failure.validate_failure().is_ok());
    }

    #[test]
    fn success_validation_rejects_invalid_completion_times() {
        let mut request = net::Request::follow("https://example.com").unwrap();
        request.version = 1;
        let mut payload = Payload::for_request(&request, "worker-1");

        for (start_time, end_time, expected) in [
            (-1, 1, "completion times must be non-negative"),
            (1, -1, "completion times must be non-negative"),
            (2, 1, "completion end time must not precede start time"),
        ] {
            payload.start_time = Some(start_time);
            payload.end_time = Some(end_time);
            assert_eq!(payload.validate_success(), Err(expected));
        }
    }

    #[test]
    fn failure_validation_rejects_invalid_completion_times() {
        let mut request = net::Request::follow("https://example.com").unwrap();
        request.version = 1;
        let mut payload = Payload::for_request(&request, "worker-1").failed("boom");

        for (start_time, end_time, expected) in [
            (-1, 1, "completion times must be non-negative"),
            (1, -1, "completion times must be non-negative"),
            (2, 1, "completion end time must not precede start time"),
        ] {
            payload.start_time = Some(start_time);
            payload.end_time = Some(end_time);
            assert_eq!(payload.validate_failure(), Err(expected));
        }
    }

    #[test]
    fn refresh_lease_validation_accepts_only_processing_identity() {
        let mut request = net::Request::follow("https://example.com").unwrap();
        request.version = 1;
        let mut refresh = Payload::for_request(&request, "worker-1");
        refresh.state = State::Processing;
        assert!(refresh.validate_refresh_lease().is_ok());

        refresh.start_time = Some(1);
        assert_eq!(
            refresh.validate_refresh_lease(),
            Err("refresh_lease payload contains completion fields")
        );
    }

    #[test]
    fn ownership_validation_requires_node_and_claimed_version() {
        let request = net::Request::follow("https://example.com").unwrap();
        let mut payload = Payload::for_request(&request, "worker-1");
        assert_eq!(
            payload.validate_ownership(),
            Err("payload requires a positive request version")
        );

        payload.version = 1;
        payload.node.clear();
        assert_eq!(
            payload.validate_ownership(),
            Err("payload requires request id, worker id, and node")
        );
    }

    #[test]
    fn empty_collections_still_reject_unrelated_fields() {
        let request = net::Request::follow("https://example.com").unwrap();
        let with_item =
            Payload::new().items(vec![Box::new(crate::item::Map::new(Default::default()))]);
        assert_eq!(
            with_item.validate_push(),
            Err("request payload contains unrelated fields".to_string())
        );

        let with_request = Payload::new().requests(vec![request]);
        assert_eq!(
            with_request.validate_store(),
            Err("item payload contains unrelated fields")
        );
    }

    #[test]
    fn push_validation_rejects_duplicate_request_ids() {
        let request = net::Request::follow("https://example.com").unwrap();
        let id = request.id.clone();
        let payload = Payload::new().requests(vec![request.clone(), request]);

        assert_eq!(
            payload.validate_push(),
            Err(format!("duplicate Request id in payload: {id}"))
        );
    }

    #[test]
    fn push_validation_checks_every_request() {
        let valid = net::Request::follow("https://example.com/valid").unwrap();
        let mut invalid = net::Request::follow("https://example.com/invalid").unwrap();
        invalid.version = 1;

        assert_eq!(
            Payload::new()
                .requests(vec![valid, invalid])
                .validate_push(),
            Err("new Request version must be 0".to_string())
        );
    }
}
