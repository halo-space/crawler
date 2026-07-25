use sqlx::{MySql as SqlxMySql, Transaction};

use crate::Error;

pub(super) async fn allocate(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    count: usize,
) -> Result<Vec<u64>, Error> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let count = u64::try_from(count)
        .map_err(|_| Error::Invalid("queue sequence count exceeds u64".to_string()))?;
    let current = lock(tx, namespace).await?;
    let end = current
        .checked_add(count)
        .ok_or_else(|| Error::Invalid("queue sequence overflow".to_string()))?;
    let updated = sqlx::query("UPDATE queue_sequences SET value = ? WHERE namespace = ?")
        .bind(end)
        .bind(namespace)
        .execute(&mut **tx)
        .await?;
    if updated.rows_affected() != 1 {
        return Err(Error::Unavailable(
            "failed to advance queue sequence".to_string(),
        ));
    }
    Ok((current + 1..=end).collect())
}

pub(super) async fn lock(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
) -> Result<u64, Error> {
    sqlx::query("INSERT IGNORE INTO queue_sequences (namespace, value) VALUES (?, 0)")
        .bind(namespace)
        .execute(&mut **tx)
        .await?;
    Ok(sqlx::query_scalar::<_, u64>(
        "SELECT value FROM queue_sequences WHERE namespace = ? FOR UPDATE",
    )
    .bind(namespace)
    .fetch_one(&mut **tx)
    .await?)
}

pub(super) async fn next(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
) -> Result<u64, Error> {
    allocate(tx, namespace, 1)
        .await?
        .pop()
        .ok_or_else(|| Error::Unavailable("queue sequence was not allocated".to_string()))
}
