use sqlx::types::Json;
use sqlx::{AssertSqlSafe, MySql as SqlxMySql, Transaction};

use super::MySql;
use super::request::mode_name;
use super::time::now_millis;
use super::validate::{namespace as validate_namespace, worker_id};
use crate::{Error, wire};

pub(super) fn canonical_modes(values: &[spider::net::Mode]) -> Result<Vec<String>, Error> {
    if values.is_empty() {
        return Err(Error::Invalid("worker modes must not be empty".to_string()));
    }
    let mut values = values
        .iter()
        .map(|mode| mode_name(mode).to_string())
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    Ok(values)
}

impl MySql {
    pub(crate) async fn pending(
        &self,
        namespace: &str,
        body: &wire::Worker,
    ) -> Result<bool, Error> {
        validate_namespace(namespace)?;
        worker_id(&body.worker_id)?;
        let modes = canonical_modes(&body.modes)?;
        let mut tx = self.pool.begin().await?;
        self.ensure_worker(&mut tx, namespace, &body.worker_id, &modes)
            .await?;
        let placeholders = std::iter::repeat_n("?", modes.len())
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "SELECT EXISTS(SELECT 1 FROM requests WHERE namespace = ? AND \
             (state = 1 OR (state = 0 \
             AND JSON_CONTAINS(failed_workers, JSON_QUOTE(?)) = 0)) \
             AND mode IN ({placeholders}))"
        );
        // The only dynamic fragment is a count of bind placeholders derived from supported modes.
        let mut request = sqlx::query_scalar::<_, i8>(AssertSqlSafe(query))
            .bind(namespace)
            .bind(&body.worker_id);
        for mode in &modes {
            request = request.bind(mode);
        }
        let pending = request.fetch_one(&mut *tx).await? != 0;
        tx.commit().await?;
        Ok(pending)
    }

    pub(crate) async fn heartbeat(
        &self,
        namespace: &str,
        body: &wire::Heartbeat,
    ) -> Result<(), Error> {
        validate_namespace(namespace)?;
        worker_id(&body.worker_id)?;
        let modes = canonical_modes(&body.modes)?;
        let now = now_millis();
        sqlx::query(
            "INSERT INTO workers (namespace, id, modes, last_heartbeat, created_time, updated_time) \
             VALUES (?, ?, ?, ?, ?, ?) AS new \
             ON DUPLICATE KEY UPDATE modes = new.modes, last_heartbeat = new.last_heartbeat, \
             updated_time = new.updated_time",
        )
        .bind(namespace)
        .bind(&body.worker_id)
        .bind(Json(&modes))
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn ensure_worker(
        &self,
        tx: &mut Transaction<'_, SqlxMySql>,
        namespace: &str,
        id: &str,
        modes: &[String],
    ) -> Result<(), Error> {
        worker_id(id)?;
        let now = now_millis();
        let existing = sqlx::query_as::<_, (Json<Vec<String>>, i64)>(
            "SELECT modes, last_heartbeat FROM workers \
             WHERE namespace = ? AND id = ? FOR UPDATE",
        )
        .bind(namespace)
        .bind(id)
        .fetch_optional(&mut **tx)
        .await?;
        if let Some((Json(current_modes), last_heartbeat)) = existing {
            let stale = now.saturating_sub(last_heartbeat) >= self.heartbeat_interval_ms;
            if current_modes == modes && !stale {
                return Ok(());
            }
            let updated = sqlx::query(
                "UPDATE workers SET modes = ?, last_heartbeat = ?, updated_time = ? \
                 WHERE namespace = ? AND id = ?",
            )
            .bind(Json(modes))
            .bind(if stale { now } else { last_heartbeat })
            .bind(now)
            .bind(namespace)
            .bind(id)
            .execute(&mut **tx)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(Error::Unavailable(format!(
                    "failed to update Worker registration: {id}"
                )));
            }
            return Ok(());
        }

        sqlx::query(
            "INSERT INTO workers (namespace, id, modes, last_heartbeat, created_time, updated_time) \
             VALUES (?, ?, ?, ?, ?, ?) AS new \
             ON DUPLICATE KEY UPDATE modes = new.modes, last_heartbeat = new.last_heartbeat, \
             updated_time = new.updated_time",
        )
        .bind(namespace)
        .bind(id)
        .bind(Json(modes))
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}
