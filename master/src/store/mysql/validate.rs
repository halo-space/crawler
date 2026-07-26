use crate::Error;
use crate::types;

const MAX_IDENTIFIER_BYTES: usize = 191;

pub(super) fn namespace(value: &str) -> Result<(), Error> {
    if value.trim().is_empty() || value.len() > 128 {
        return Err(Error::Invalid("invalid namespace".to_string()));
    }
    Ok(())
}

pub(super) fn identifier(value: &str, field: &str) -> Result<(), Error> {
    if value.trim().is_empty()
        || value.len() > MAX_IDENTIFIER_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(Error::Invalid(format!(
            "{field} must be non-empty, at most {MAX_IDENTIFIER_BYTES} bytes, and contain no control characters"
        )));
    }
    Ok(())
}

pub(super) fn worker_id(value: &str) -> Result<(), Error> {
    identifier(value, "worker_id")
}

pub(super) fn identity(value: &types::Identity) -> Result<(), Error> {
    if value.version <= 0 {
        return Err(Error::Invalid(
            "request identity requires a positive version".to_string(),
        ));
    }
    identifier(&value.id, "request id")?;
    worker_id(&value.worker_id)?;
    identifier(&value.node, "request node")?;
    identifier(&value.task_id, "task_id")?;
    identifier(&value.trace_id, "trace_id")?;
    Ok(())
}
