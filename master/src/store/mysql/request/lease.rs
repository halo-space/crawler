use super::super::MySql;
use super::super::operation;
use super::super::time::now_millis;
use super::super::validate::{identity as validate_identity, namespace as validate_namespace};
use super::{load, queue, verify_identity, verify_lease};
use crate::{Error, wire};

impl MySql {
    pub(crate) async fn ack(
        &self,
        namespace: &str,
        identity: &wire::Identity,
    ) -> Result<(), Error> {
        validate_namespace(namespace)?;
        validate_identity(identity)?;
        let mut tx = self.pool.begin().await?;
        let row = load(&mut tx, namespace, &identity.id).await?;
        verify_identity(&row, identity)?;
        verify_lease(&row, self.lease_timeout_ms)?;
        if row.ack_version == Some(identity.version) {
            tx.commit().await?;
            return Ok(());
        }
        sqlx::query(
            "UPDATE requests SET ack_version = ?, updated_time = ? \
             WHERE namespace = ? AND id = ? AND version = ? AND state = 1",
        )
        .bind(identity.version)
        .bind(now_millis())
        .bind(namespace)
        .bind(&identity.id)
        .bind(identity.version)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn release(
        &self,
        namespace: &str,
        key: &str,
        identity: &wire::Identity,
    ) -> Result<(), Error> {
        validate_namespace(namespace)?;
        validate_identity(identity)?;
        let digest = operation::digest(identity)?;
        let mut tx = self.pool.begin().await?;
        if operation::reserve::<()>(&mut tx, namespace, "release", key, &digest)
            .await?
            .is_some()
        {
            tx.commit().await?;
            return Ok(());
        }
        let row = load(&mut tx, namespace, &identity.id).await?;
        verify_identity(&row, identity)?;
        verify_lease(&row, self.lease_timeout_ms)?;
        let sequence = queue::next(&mut tx, namespace).await?;
        let updated = sqlx::query(
            "UPDATE requests SET state = 0, leased_by = '', lease_time = 0, ack_version = NULL, \
             next_time = 0, sequence = ?, updated_time = ? \
             WHERE namespace = ? AND id = ? AND version = ? AND state = 1",
        )
        .bind(sequence)
        .bind(now_millis())
        .bind(namespace)
        .bind(&identity.id)
        .bind(identity.version)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(Error::Unavailable(format!(
                "release lost Request state transition: {}",
                identity.id
            )));
        }
        operation::record(&mut tx, namespace, "release", key, &digest, &()).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn refresh(
        &self,
        namespace: &str,
        identity: &wire::Identity,
    ) -> Result<(), Error> {
        validate_namespace(namespace)?;
        validate_identity(identity)?;
        let mut tx = self.pool.begin().await?;
        let row = load(&mut tx, namespace, &identity.id).await?;
        verify_identity(&row, identity)?;
        verify_lease(&row, self.lease_timeout_ms)?;
        if row.ack_version != Some(identity.version) {
            return Err(Error::NotAcknowledged(identity.id.clone()));
        }
        let now = now_millis();
        let updated = sqlx::query(
            "UPDATE requests SET lease_time = ?, updated_time = ? \
             WHERE namespace = ? AND id = ? AND version = ? AND state = 1 AND ack_version = ?",
        )
        .bind(now)
        .bind(now)
        .bind(namespace)
        .bind(&identity.id)
        .bind(identity.version)
        .bind(identity.version)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(Error::Unavailable(format!(
                "refresh lost Request state transition: {}",
                identity.id
            )));
        }
        tx.commit().await?;
        Ok(())
    }
}
