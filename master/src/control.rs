use serde::Serialize;

use crate::Error;

pub(crate) mod cursor;
pub(crate) mod item;
pub(crate) mod request;
pub(crate) mod task;
pub(crate) mod trace;
pub(crate) mod worker;

pub(crate) const DEFAULT_LIMIT: usize = 50;
pub(crate) const MAX_LIMIT: usize = 200;

#[derive(Debug, Serialize)]
pub(crate) struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

pub(crate) fn limit(value: Option<usize>) -> Result<usize, Error> {
    let value = value.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&value) {
        return Err(Error::Invalid(format!(
            "limit must be between 1 and {MAX_LIMIT}"
        )));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_limit_is_bounded() {
        assert_eq!(limit(None).unwrap(), DEFAULT_LIMIT);
        assert_eq!(limit(Some(MAX_LIMIT)).unwrap(), MAX_LIMIT);
        assert!(limit(Some(0)).is_err());
        assert!(limit(Some(MAX_LIMIT + 1)).is_err());
    }
}
