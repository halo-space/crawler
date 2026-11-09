use std::time::Duration;

use crate::scheduler;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Lease {
    timeout: Duration,
    interval: Duration,
}

impl Lease {
    pub fn new(timeout: Duration, interval: Duration) -> Result<Self, scheduler::Error> {
        if timeout.is_zero() || !is_whole_millisecond(timeout) {
            return Err(scheduler::Error::Message(
                "lease timeout must be a positive whole number of milliseconds".to_string(),
            ));
        }
        if interval.is_zero() || !is_whole_millisecond(interval) || interval >= timeout {
            return Err(scheduler::Error::Message(
                "lease interval must be a positive whole number of milliseconds shorter than timeout"
                    .to_string(),
            ));
        }
        if timeout.as_millis() > i64::MAX as u128 {
            return Err(scheduler::Error::Message(
                "lease timeout exceeds the supported millisecond range".to_string(),
            ));
        }
        let now = std::time::Instant::now();
        if now.checked_add(timeout).is_none() || now.checked_add(interval).is_none() {
            return Err(scheduler::Error::Message(
                "lease duration exceeds the runtime clock range".to_string(),
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

fn is_whole_millisecond(duration: Duration) -> bool {
    duration.subsec_nanos().is_multiple_of(1_000_000)
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
        assert!(Lease::new(Duration::from_nanos(500_000), Duration::from_nanos(250_000)).is_err());
        assert!(Lease::new(Duration::from_millis(2), Duration::from_micros(1500)).is_err());
        assert_eq!(
            Lease::new(Duration::from_secs(2), Duration::from_secs(1)).unwrap(),
            Lease {
                timeout: Duration::from_secs(2),
                interval: Duration::from_secs(1),
            }
        );
    }

    #[test]
    fn rejects_durations_outside_the_runtime_clock_range() {
        let timeout = Duration::from_millis(i64::MAX as u64);
        if std::time::Instant::now().checked_add(timeout).is_none() {
            assert!(Lease::new(timeout, Duration::from_millis(1)).is_err());
        }
    }
}
