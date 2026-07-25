use crate::Error;

#[derive(Clone, Debug)]
pub struct Policy {
    pub lease_timeout_ms: i64,
    pub lease_interval_ms: i64,
    pub heartbeat_interval_ms: i64,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            lease_timeout_ms: 30_000,
            lease_interval_ms: 10_000,
            heartbeat_interval_ms: 10_000,
        }
    }
}

impl Policy {
    pub fn validate(&self) -> Result<(), Error> {
        if self.lease_timeout_ms <= 0
            || self.lease_interval_ms <= 0
            || self.heartbeat_interval_ms <= 0
        {
            return Err(Error::Config(
                "lease and heartbeat intervals must be positive".to_string(),
            ));
        }
        if self.lease_interval_ms >= self.lease_timeout_ms {
            return Err(Error::Config(
                "lease interval must be shorter than lease timeout".to_string(),
            ));
        }
        if self.heartbeat_interval_ms >= self.lease_timeout_ms {
            return Err(Error::Config(
                "heartbeat interval must be shorter than lease timeout".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        Policy::default().validate().unwrap();
    }

    #[test]
    fn rejects_non_positive_or_late_intervals() {
        assert!(
            Policy {
                lease_timeout_ms: 0,
                ..Policy::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            Policy {
                lease_interval_ms: 30_000,
                ..Policy::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            Policy {
                heartbeat_interval_ms: 30_000,
                ..Policy::default()
            }
            .validate()
            .is_err()
        );
    }
}
