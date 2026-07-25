use std::time::Duration;

use sqlx::{MySql as SqlxMySql, Transaction};

use super::MySql;
use super::request::{DONE, FAILED};
use super::validate::namespace as validate_namespace;
use crate::Error;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Cleanup {
    pub requests: u64,
    pub completions: u64,
    pub operations: u64,
}

impl MySql {
    pub(crate) async fn cleanup(
        &self,
        namespace: &str,
        now: i64,
        retention: Duration,
        limit: usize,
    ) -> Result<Cleanup, Error> {
        validate_namespace(namespace)?;
        let before = before(now, retention)?;
        let limit = u64::try_from(limit)
            .map_err(|_| Error::Invalid("cleanup limit exceeds u64".to_string()))?;
        if limit == 0 {
            return Err(Error::Invalid("cleanup limit must be positive".to_string()));
        }

        let mut tx = self.pool.begin().await?;
        let report = Cleanup {
            requests: requests(&mut tx, namespace, before, limit).await?,
            completions: completions(&mut tx, namespace, before, limit).await?,
            operations: operations(&mut tx, namespace, before, limit).await?,
        };
        tx.commit().await?;
        Ok(report)
    }
}

fn before(now: i64, retention: Duration) -> Result<i64, Error> {
    let retention = i64::try_from(retention.as_millis()).map_err(|_| {
        Error::Invalid("history retention exceeds the supported timestamp range".to_string())
    })?;
    if retention <= 0 {
        return Err(Error::Invalid(
            "history retention must be positive".to_string(),
        ));
    }
    Ok(now.saturating_sub(retention))
}

async fn requests(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    before: i64,
    limit: u64,
) -> Result<u64, Error> {
    Ok(sqlx::query(
        "DELETE FROM requests WHERE namespace = ? AND state IN (?, ?) AND updated_time < ? \
         ORDER BY updated_time ASC, id ASC LIMIT ?",
    )
    .bind(namespace)
    .bind(DONE)
    .bind(FAILED)
    .bind(before)
    .bind(limit)
    .execute(&mut **tx)
    .await?
    .rows_affected())
}

async fn completions(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    before: i64,
    limit: u64,
) -> Result<u64, Error> {
    Ok(sqlx::query(
        "DELETE FROM request_completions WHERE namespace = ? AND created_time < ? \
         ORDER BY created_time ASC, request_id ASC, version ASC LIMIT ?",
    )
    .bind(namespace)
    .bind(before)
    .bind(limit)
    .execute(&mut **tx)
    .await?
    .rows_affected())
}

async fn operations(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    before: i64,
    limit: u64,
) -> Result<u64, Error> {
    Ok(sqlx::query(
        "DELETE FROM operations WHERE namespace = ? AND updated_time < ? \
         ORDER BY updated_time ASC, kind ASC, operation_key ASC LIMIT ?",
    )
    .bind(namespace)
    .bind(before)
    .bind(limit)
    .execute(&mut **tx)
    .await?
    .rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_uses_a_strict_expiry_boundary() {
        assert_eq!(before(10_000, Duration::from_millis(1_000)).unwrap(), 9_000);
    }

    #[test]
    fn retention_rejects_zero() {
        assert!(before(10_000, Duration::ZERO).is_err());
    }

    #[test]
    fn retention_saturates_at_the_timestamp_floor() {
        assert_eq!(
            before(i64::MIN + 10, Duration::from_millis(20)).unwrap(),
            i64::MIN
        );
    }
}
