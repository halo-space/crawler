use std::time::Duration;

use crate::Error;

const DEFAULT_TTL: Duration = Duration::from_secs(2 * 24 * 60 * 60);
const DEFAULT_CLEANUP_LIMIT: usize = 1_000;

#[derive(Clone, Debug)]
pub struct History {
    pub ttl: Duration,
    pub cleanup_limit: usize,
}

impl Default for History {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_TTL,
            cleanup_limit: DEFAULT_CLEANUP_LIMIT,
        }
    }
}

impl History {
    pub fn validate(&self) -> Result<(), Error> {
        if self.ttl.is_zero() {
            return Err(Error::Config("history TTL must be positive".to_string()));
        }
        if self.ttl.as_millis() > i64::MAX as u128 {
            return Err(Error::Config(
                "history TTL exceeds the supported timestamp range".to_string(),
            ));
        }
        if self.cleanup_limit == 0 {
            return Err(Error::Config("cleanup limit must be positive".to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        let history = History::default();

        assert_eq!(history.ttl, DEFAULT_TTL);
        assert_eq!(history.cleanup_limit, DEFAULT_CLEANUP_LIMIT);
        history.validate().unwrap();
    }

    #[test]
    fn rejects_invalid_ttl_and_cleanup_limit() {
        assert!(
            History {
                ttl: Duration::ZERO,
                ..History::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            History {
                ttl: Duration::from_millis(i64::MAX as u64) + Duration::from_millis(1),
                ..History::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            History {
                cleanup_limit: 0,
                ..History::default()
            }
            .validate()
            .is_err()
        );
    }
}
