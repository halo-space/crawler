use serde::Serialize;

use crate::Error;
use crate::control::{Page, cursor};

mod item;
mod request;
mod task;
mod trace;
mod worker;

#[cfg(test)]
mod tests;

pub(super) fn timed(
    value: Option<&str>,
    namespace: &str,
    endpoint: &str,
    filter: &impl Serialize,
) -> Result<Option<(i64, String)>, Error> {
    value
        .map(
            |value| match cursor::decode(value, namespace, endpoint, filter)? {
                cursor::Key::Timed { time, id } => Ok((time, id)),
                cursor::Key::Id { .. } => Err(invalid_cursor()),
            },
        )
        .transpose()
}

pub(super) fn id(
    value: Option<&str>,
    namespace: &str,
    endpoint: &str,
    filter: &impl Serialize,
) -> Result<Option<String>, Error> {
    value
        .map(
            |value| match cursor::decode(value, namespace, endpoint, filter)? {
                cursor::Key::Id { id } => Ok(id),
                cursor::Key::Timed { .. } => Err(invalid_cursor()),
            },
        )
        .transpose()
}

pub(super) fn page<T>(
    mut items: Vec<T>,
    limit: usize,
    namespace: &str,
    endpoint: &str,
    filter: &impl Serialize,
    key: impl FnOnce(&T) -> cursor::Key,
) -> Result<Page<T>, Error> {
    let next_cursor = if items.len() > limit {
        items.truncate(limit);
        let last = items
            .last()
            .ok_or_else(|| Error::Invalid("invalid empty control page".to_string()))?;
        Some(cursor::encode(namespace, endpoint, filter, key(last))?)
    } else {
        None
    };
    Ok(Page { items, next_cursor })
}

fn invalid_cursor() -> Error {
    Error::Invalid("invalid cursor".to_string())
}
