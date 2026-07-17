use std::time::Duration;

use crate::scheduler;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Lease {
    timeout: Duration,
    interval: Duration,
}

impl Lease {
    pub fn new(timeout: Duration, interval: Duration) -> Result<Self, scheduler::Error> {
        if timeout.is_zero() {
            return Err(scheduler::Error::Message(
                "lease timeout must be greater than zero".to_string(),
            ));
        }
        if interval.is_zero() || interval >= timeout {
            return Err(scheduler::Error::Message(
                "lease interval must be greater than zero and shorter than timeout".to_string(),
            ));
        }
        if timeout.as_millis() > i64::MAX as u128 {
            return Err(scheduler::Error::Message(
                "lease timeout exceeds the supported millisecond range".to_string(),
            ));
        }
        Ok(Self { timeout, interval })
    }

    pub fn timeout(self) -> Duration {
        self.timeout
    }

    pub fn interval(self) -> Duration {
        self.interval
    }

    pub(crate) fn timeout_millis(self) -> i64 {
        self.timeout.as_millis() as i64
    }
}

impl Default for Lease {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            interval: Duration::from_secs(10),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_timeout_and_interval() {
        assert!(Lease::new(Duration::ZERO, Duration::from_secs(1)).is_err());
        assert!(Lease::new(Duration::from_secs(1), Duration::ZERO).is_err());
        assert!(Lease::new(Duration::from_secs(1), Duration::from_secs(1)).is_err());
        assert_eq!(
            Lease::new(Duration::from_secs(2), Duration::from_secs(1)).unwrap(),
            Lease {
                timeout: Duration::from_secs(2),
                interval: Duration::from_secs(1),
            }
        );
    }
}
