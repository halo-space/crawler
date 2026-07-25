use spider::{net, scheduler, stats};

pub(super) fn request(request: &net::Request) -> Result<(), scheduler::Error> {
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
    if request.retry_count != 0
        || !(1..=net::request::MAX_RETRY_COUNT).contains(&request.max_retry_count)
    {
        return Err(scheduler::Error::Message(format!(
            "new Request requires retry_count 0 and max_retry_count between 1 and {}",
            net::request::MAX_RETRY_COUNT
        )));
    }
    for middleware in &request.middlewares {
        spider::middleware::check(middleware).map_err(|error| {
            scheduler::Error::Message(format!("new Request has invalid middleware: {error}"))
        })?;
    }
    Ok(())
}

pub(super) fn worker(worker_id: &str, modes: &[net::Mode]) -> Result<(), scheduler::Error> {
    if worker_id.trim().is_empty() {
        return Err(scheduler::Error::Message(
            "worker_id must not be empty".to_string(),
        ));
    }
    if modes.is_empty() {
        return Err(scheduler::Error::Message(
            "worker modes must not be empty".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn integer(value: &str, id: &str, field: &str) -> Result<i64, scheduler::Error> {
    value
        .parse()
        .map_err(|error| scheduler::Error::InvalidRequest {
            id: id.to_string(),
            message: format!("stored Request has invalid {field}: {error}"),
        })
}

pub(super) fn counter(value: &stats::Counter) -> bool {
    value.total >= 0
        && value.done >= 0
        && value.filter >= 0
        && value.dedup >= 0
        && value.validate >= 0
        && value.download >= 0
}

pub(super) fn snapshot_digest(
    snapshot: &net::request::Snapshot,
    expected: &str,
    id: &str,
) -> Result<(), scheduler::Error> {
    let actual = snapshot
        .digest()
        .map_err(|error| scheduler::Error::InvalidRequest {
            id: id.to_string(),
            message: format!("stored Request Snapshot digest cannot be calculated: {error}"),
        })?;
    if hex(&actual) != expected {
        return Err(scheduler::Error::InvalidRequest {
            id: id.to_string(),
            message: "stored Request Snapshot digest does not match its content".to_string(),
        });
    }
    Ok(())
}

pub(super) fn hex(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(TABLE[(byte >> 4) as usize] as char);
        value.push(TABLE[(byte & 0x0f) as usize] as char);
    }
    value
}
