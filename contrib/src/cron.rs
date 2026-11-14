use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::parser::{CronParser, Seconds};
use thiserror::Error;

const DEFAULT_TIMEZONE: &str = "UTC";

/// A standard five-field crontab expression with an explicit IANA timezone.
#[derive(Clone, Debug)]
pub struct Crontab {
    expression: String,
    timezone: Tz,
    schedule: croner::Cron,
}

impl Crontab {
    /// Parses a five-field expression using UTC.
    pub fn parse(expression: impl Into<String>) -> Result<Self, Error> {
        Self::with_timezone(expression, DEFAULT_TIMEZONE)
    }

    /// Parses a five-field expression using an IANA timezone.
    pub fn with_timezone(
        expression: impl Into<String>,
        timezone: impl AsRef<str>,
    ) -> Result<Self, Error> {
        let expression = expression.into();
        let timezone = timezone
            .as_ref()
            .parse::<Tz>()
            .map_err(|_| Error::Timezone(timezone.as_ref().to_string()))?;
        let parser = CronParser::builder().seconds(Seconds::Disallowed).build();
        let schedule = parser
            .parse(&expression)
            .map_err(|error| Error::Expression(error.to_string()))?;
        Ok(Self {
            expression,
            timezone,
            schedule,
        })
    }

    pub fn expression(&self) -> &str {
        &self.expression
    }

    pub fn timezone(&self) -> Tz {
        self.timezone
    }

    /// Returns the next execution time after the supplied UTC instant.
    pub fn next_at(&self, now: DateTime<Utc>) -> Result<DateTime<Tz>, Error> {
        let local_now = now.with_timezone(&self.timezone);
        self.schedule
            .find_next_occurrence(&local_now, false)
            .map_err(|error| Error::Next(error.to_string()))
    }

    /// Returns the next execution time as a Unix timestamp in milliseconds.
    pub fn next_time(&self, now_millis: i64) -> Result<i64, Error> {
        let now =
            DateTime::from_timestamp_millis(now_millis).ok_or(Error::Timestamp(now_millis))?;
        self.next_at(now).map(|next| next.timestamp_millis())
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid crontab expression: {0}")]
    Expression(String),
    #[error("invalid IANA timezone: {0}")]
    Timezone(String),
    #[error("cannot find the next crontab execution time: {0}")]
    Next(String),
    #[error("invalid Unix timestamp in milliseconds: {0}")]
    Timestamp(i64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculates_next_weekday_in_the_configured_timezone() {
        let schedule = Crontab::with_timezone("0 10 * * 1-5", "Asia/Shanghai").unwrap();
        let now = DateTime::parse_from_rfc3339("2026-11-13T10:01:00+08:00")
            .unwrap()
            .with_timezone(&Utc);

        let next = schedule.next_at(now).unwrap();

        assert_eq!(next.to_rfc3339(), "2026-11-16T10:00:00+08:00");
    }

    #[test]
    fn returns_unix_milliseconds_for_task_next_time() {
        let schedule = Crontab::parse("0 0 * * *").unwrap();
        let now = DateTime::parse_from_rfc3339("2026-11-13T23:59:59.500Z")
            .unwrap()
            .timestamp_millis();

        assert_eq!(
            schedule.next_time(now).unwrap(),
            DateTime::parse_from_rfc3339("2026-11-14T00:00:00Z")
                .unwrap()
                .timestamp_millis()
        );
    }

    #[test]
    fn rejects_seconds_and_invalid_timezones() {
        assert!(Crontab::parse("0 0 0 * * *").is_err());
        assert!(Crontab::with_timezone("0 0 * * *", "Mars/Olympus").is_err());
    }
}
