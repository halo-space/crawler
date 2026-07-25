use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySql as SqlxMySql, Pool};

use crate::{Config, Error};

mod cleanup;
#[cfg(test)]
mod integration;
mod item;
mod observe;
mod operation;
mod request;
mod task;
mod time;
mod trace;
mod validate;
mod worker;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

pub(super) fn duplicate(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(error) if error.is_unique_violation())
}

#[derive(Clone)]
pub(crate) struct MySql {
    pub(super) pool: Pool<SqlxMySql>,
    pub(super) lease_timeout_ms: i64,
    pub(super) worker_timeout_ms: i64,
    pub(super) heartbeat_interval_ms: i64,
    pub(super) recovery_limit: usize,
    pub(super) max_response_bytes: usize,
}

impl MySql {
    pub async fn connect(config: &Config) -> Result<Self, Error> {
        let pool = MySqlPoolOptions::new()
            .max_connections(32)
            .after_connect(|connection, _| {
                Box::pin(async move {
                    sqlx::query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        .execute(&mut *connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(config.database_url())
            .await?;
        verify_server(&pool).await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self {
            pool,
            lease_timeout_ms: config.policy().lease_timeout_ms,
            // A missed heartbeat only shortens a lease when the Worker has been silent for a
            // whole lease window. This is conservative and requires no second policy surface.
            worker_timeout_ms: config.policy().lease_timeout_ms,
            heartbeat_interval_ms: config.policy().heartbeat_interval_ms,
            recovery_limit: config.recovery_limit(),
            max_response_bytes: config.max_api_bytes(),
        })
    }

    #[cfg(test)]
    pub(crate) fn pool(&self) -> &Pool<SqlxMySql> {
        &self.pool
    }

    #[cfg(test)]
    pub(crate) fn disconnected(config: &Config) -> Self {
        let pool = MySqlPoolOptions::new()
            .connect_lazy(config.database_url())
            .expect("test database URL must be valid");
        Self {
            pool,
            lease_timeout_ms: config.policy().lease_timeout_ms,
            worker_timeout_ms: config.policy().lease_timeout_ms,
            heartbeat_interval_ms: config.policy().heartbeat_interval_ms,
            recovery_limit: config.recovery_limit(),
            max_response_bytes: config.max_api_bytes(),
        }
    }
}

async fn verify_server(pool: &Pool<SqlxMySql>) -> Result<(), Error> {
    let version = sqlx::query_scalar::<_, String>("SELECT VERSION()")
        .fetch_one(pool)
        .await?;
    let lowered = version.to_ascii_lowercase();
    let mut parts = version.split(['.', '-']);
    let major = parts
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_default();
    let minor = parts
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_default();
    let patch = parts
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_default();
    if lowered.contains("mariadb") || major < 8 || (major == 8 && minor == 0 && patch < 19) {
        return Err(Error::Config(format!(
            "Master requires MySQL 8.0.19 or later, got {version}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
