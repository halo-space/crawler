use spider::{net, scheduler, trace};
use sqlx::Row as _;

use super::MySql;
use super::error::sqlx as sql_error;

impl MySql {
    pub(super) async fn load_trace(
        &self,
        trace_id: &str,
    ) -> Result<Option<trace::Snapshot>, scheduler::Error> {
        if trace_id.is_empty() {
            return Err(scheduler::Error::Message(
                "trace_id must not be empty".to_string(),
            ));
        }
        let pool = self.pool().await?;
        let stored = sqlx::query("SELECT snapshot FROM traces WHERE id = ?")
            .bind(trace_id)
            .fetch_optional(&pool)
            .await
            .map_err(sql_error)?;
        stored
            .map(|row| {
                let encoded = row
                    .try_get::<serde_json::Value, _>("snapshot")
                    .map_err(sql_error)?;
                let snapshot =
                    serde_json::from_value::<trace::Snapshot>(encoded).map_err(|error| {
                        scheduler::Error::InvalidTrace {
                            id: trace_id.to_string(),
                            message: error.to_string(),
                        }
                    })?;
                snapshot
                    .validate()
                    .map_err(|message| scheduler::Error::InvalidTrace {
                        id: trace_id.to_string(),
                        message,
                    })?;
                Ok(snapshot)
            })
            .transpose()
    }

    pub(super) async fn initialize(
        &self,
        trace_id: String,
        snapshot: trace::Snapshot,
        requests: Vec<net::Request>,
    ) -> Result<(), scheduler::Error> {
        if trace_id.is_empty() {
            return Err(scheduler::Error::Message(
                "trace_id must not be empty".to_string(),
            ));
        }
        snapshot
            .validate()
            .map_err(|message| scheduler::Error::InvalidTrace {
                id: trace_id.clone(),
                message,
            })?;
        if requests.iter().any(|request| request.trace_id != trace_id) {
            return Err(scheduler::Error::Message(
                "all initial requests must reference the initialized trace_id".to_string(),
            ));
        }
        if requests
            .iter()
            .any(|request| request.task_id != snapshot.task_id)
        {
            return Err(scheduler::Error::Message(
                "all initial requests must reference the Trace Snapshot task_id".to_string(),
            ));
        }
        let payload = spider::payload::Payload::new().requests(requests);
        payload.validate_push().map_err(scheduler::Error::Message)?;
        let requests = Self::prepare_requests(payload.requests)?;
        let encoded = serde_json::to_value(&snapshot)
            .map_err(|error| scheduler::Error::Message(error.to_string()))?;

        let pool = self.pool().await?;
        let mut transaction = pool.begin().await.map_err(sql_error)?;
        let result = sqlx::query(
            "INSERT INTO traces \
             (id, task_id, snapshot, created_time, updated_time) \
             VALUES (?, ?, ?, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))",
        )
        .bind(&trace_id)
        .bind(&snapshot.task_id)
        .bind(encoded)
        .execute(&mut *transaction)
        .await;
        if let Err(error) = result {
            if super::error::database_number(&error) == Some(1062) {
                return Err(scheduler::Error::Message(format!(
                    "Trace already exists: {trace_id}"
                )));
            }
            return Err(sql_error(error));
        }

        for request in &requests {
            Self::insert_initial_request(&mut transaction, request).await?;
        }
        transaction.commit().await.map_err(sql_error)
    }
}
