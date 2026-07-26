use spider::{net, scheduler, stats};

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
