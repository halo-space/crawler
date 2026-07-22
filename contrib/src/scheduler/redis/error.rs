use spider::scheduler;

pub(super) fn redis(error: redis::RedisError) -> scheduler::Error {
    use redis::{ErrorKind, ServerErrorKind};

    match error.kind() {
        ErrorKind::Io
        | ErrorKind::Server(
            ServerErrorKind::BusyLoading
            | ServerErrorKind::TryAgain
            | ServerErrorKind::ClusterDown
            | ServerErrorKind::MasterDown
            | ServerErrorKind::ReadOnly,
        ) => scheduler::Error::Unavailable(error.to_string()),
        _ => scheduler::Error::Message(format!("Redis Scheduler operation failed: {error}")),
    }
}

pub(super) fn message(error: impl std::fmt::Display) -> scheduler::Error {
    scheduler::Error::Message(error.to_string())
}

pub(super) fn status(value: &str, fallback_id: &str) -> scheduler::Error {
    let (code, id) = value
        .split_once(':')
        .filter(|(_, id)| !id.is_empty())
        .unwrap_or((value, fallback_id));
    match code {
        "REQUEST_NOT_FOUND" => scheduler::Error::RequestNotFound(id.to_string()),
        "TASK_ID_MISMATCH" => scheduler::Error::IdentityMismatch {
            id: id.to_string(),
            field: "task_id",
        },
        "TRACE_ID_MISMATCH" => scheduler::Error::IdentityMismatch {
            id: id.to_string(),
            field: "trace_id",
        },
        "NODE_MISMATCH" => scheduler::Error::IdentityMismatch {
            id: id.to_string(),
            field: "node",
        },
        "LEASE_MISMATCH" => scheduler::Error::LeaseMismatch(id.to_string()),
        "LEASE_EXPIRED" => scheduler::Error::LeaseExpired(id.to_string()),
        "NOT_ACKNOWLEDGED" => scheduler::Error::NotAcknowledged(id.to_string()),
        "STATE_MISMATCH" => scheduler::Error::StateMismatch(id.to_string()),
        "VERSION_MISMATCH" => scheduler::Error::VersionMismatch(id.to_string()),
        "TRACE_NOT_FOUND" => scheduler::Error::TraceNotFound(id.to_string()),
        "DUPLICATE" => scheduler::Error::Message(format!("duplicate Request id in payload: {id}")),
        "CONFLICT" => {
            scheduler::Error::Message(format!("Request id conflicts with existing Snapshot: {id}"))
        }
        "TRACE_EXISTS" => scheduler::Error::Message(format!("Trace already exists: {id}")),
        "REQUEST_EXISTS" => {
            scheduler::Error::Message(format!("initial Request id already exists: {id}"))
        }
        "SEQUENCE_OVERFLOW" => {
            scheduler::Error::Message("Redis Scheduler enqueue sequence overflow".to_string())
        }
        "VERSION_OVERFLOW" => scheduler::Error::Message(format!("Request version overflow: {id}")),
        "STATS_OVERFLOW" => scheduler::Error::Message(format!("stats counter overflow: {id}")),
        code => scheduler::Error::Message(format!("Redis Scheduler operation failed: {code}")),
    }
}
