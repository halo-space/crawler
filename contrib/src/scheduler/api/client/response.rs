use futures_util::StreamExt as _;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde_json::Value;
use spider::scheduler;

use super::super::wire;

pub(super) fn decode<T>(bytes: Vec<u8>) -> Result<T, scheduler::Error>
where
    T: DeserializeOwned,
{
    serde_json::from_slice(&bytes).map_err(|error| {
        scheduler::Error::Message(format!(
            "Master returned an invalid success response: {error}"
        ))
    })
}

pub(super) fn empty(bytes: Vec<u8>) -> Result<(), scheduler::Error> {
    if bytes.is_empty() {
        return Ok(());
    }
    match serde_json::from_slice::<Value>(&bytes) {
        Ok(Value::Object(values)) if values.is_empty() => Ok(()),
        Ok(_) => Err(ambiguous("Master returned a non-empty mutation response")),
        Err(error) => Err(ambiguous(&format!(
            "Master returned an invalid mutation response: {error}"
        ))),
    }
}

fn ambiguous(message: &str) -> scheduler::Error {
    scheduler::Error::Unavailable(format!(
        "{message}; mutation outcome is ambiguous and must retain its idempotency key"
    ))
}

pub(super) enum ReadError {
    Transport(String),
    TooLarge(String),
}

pub(super) async fn read(
    response: reqwest::Response,
    max_response_bytes: usize,
) -> Result<Vec<u8>, ReadError> {
    if response
        .content_length()
        .is_some_and(|length| length > max_response_bytes as u64)
    {
        return Err(ReadError::TooLarge(format!(
            "Master response exceeds the configured {max_response_bytes} byte limit"
        )));
    }

    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| ReadError::Transport(error.to_string()))?;
        let next = bytes
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| ReadError::TooLarge("Master response size overflow".to_string()))?;
        if next > max_response_bytes {
            return Err(ReadError::TooLarge(format!(
                "Master response exceeds the configured {max_response_bytes} byte limit"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

pub(in crate::scheduler::api) fn map_error(status: StatusCode, body: &[u8]) -> scheduler::Error {
    let Ok(envelope) = serde_json::from_slice::<wire::ErrorEnvelope>(body) else {
        return scheduler::Error::Message(message(status, body));
    };
    let wire::ErrorBody {
        code,
        id,
        field,
        message,
    } = envelope.error;
    let id = id.filter(|id| !id.is_empty());
    match code.as_str() {
        "identity_mismatch" => match (id, field.as_deref()) {
            (Some(id), Some("task_id")) => scheduler::Error::IdentityMismatch {
                id,
                field: "task_id",
            },
            (Some(id), Some("trace_id")) => scheduler::Error::IdentityMismatch {
                id,
                field: "trace_id",
            },
            (Some(id), Some("node")) => scheduler::Error::IdentityMismatch { id, field: "node" },
            _ => protocol_error(&code, message),
        },
        "lease_mismatch" => match id {
            Some(id) => scheduler::Error::LeaseMismatch(id),
            None => protocol_error(&code, message),
        },
        "lease_expired" => match id {
            Some(id) => scheduler::Error::LeaseExpired(id),
            None => protocol_error(&code, message),
        },
        "not_acknowledged" => match id {
            Some(id) => scheduler::Error::NotAcknowledged(id),
            None => protocol_error(&code, message),
        },
        "state_mismatch" => match id {
            Some(id) => scheduler::Error::StateMismatch(id),
            None => protocol_error(&code, message),
        },
        "version_mismatch" => match id {
            Some(id) => scheduler::Error::VersionMismatch(id),
            None => protocol_error(&code, message),
        },
        "request_not_found" => match id {
            Some(id) => scheduler::Error::RequestNotFound(id),
            None => protocol_error(&code, message),
        },
        "trace_not_found" => match id {
            Some(id) => scheduler::Error::TraceNotFound(id),
            None => protocol_error(&code, message),
        },
        "invalid_trace" => match id {
            Some(id) => scheduler::Error::InvalidTrace { id, message },
            None => protocol_error(&code, message),
        },
        "invalid_request" => match id {
            Some(id) => scheduler::Error::InvalidRequest { id, message },
            None => scheduler::Error::Message(message),
        },
        "unavailable" => scheduler::Error::Unavailable(message),
        _ => scheduler::Error::Message(message),
    }
}

fn protocol_error(code: &str, message: String) -> scheduler::Error {
    scheduler::Error::Message(format!(
        "Master error {code} omitted a required request id: {message}"
    ))
}

pub(super) fn message(status: StatusCode, body: &[u8]) -> String {
    if let Ok(envelope) = serde_json::from_slice::<wire::ErrorEnvelope>(body) {
        return envelope.error.message;
    }
    let detail = std::str::from_utf8(body)
        .ok()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("no response body");
    let detail = detail.chars().take(1024).collect::<String>();
    format!("Master returned HTTP {status}: {detail}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutation_success_requires_an_empty_response() {
        assert!(empty(Vec::new()).is_ok());
        assert!(empty(b"{}".to_vec()).is_ok());
        assert!(matches!(
            empty(b"null".to_vec()),
            Err(scheduler::Error::Unavailable(_))
        ));
        assert!(matches!(
            empty(b"{".to_vec()),
            Err(scheduler::Error::Unavailable(_))
        ));
    }
}
