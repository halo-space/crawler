use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use sqlx::mysql::MySqlRow;
use sqlx::types::Json;
use sqlx::{MySql as SqlxMySql, Row, Transaction};

use super::time::now_millis;
use crate::Error;

pub(super) fn digest<T: Serialize>(value: &T) -> Result<String, Error> {
    let mut value = serde_json::to_value(value)?;
    canonicalize(&mut value);
    let bytes = serde_json::to_vec(&value)?;
    Ok(hex(&Sha256::digest(bytes)))
}

pub(super) fn hex(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(TABLE[(byte >> 4) as usize] as char);
        output.push(TABLE[(byte & 0x0f) as usize] as char);
    }
    output
}

pub(super) async fn reserve<T>(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    kind: &str,
    key: &str,
    request_digest: &str,
) -> Result<Option<T>, Error>
where
    T: for<'de> Deserialize<'de>,
{
    validate_key(key)?;
    let now = now_millis();
    let inserted = sqlx::query(
        "INSERT IGNORE INTO operations \
         (namespace, kind, operation_key, request_digest, result, completed, created_time, updated_time) \
         VALUES (?, ?, ?, ?, ?, FALSE, ?, ?)",
    )
    .bind(namespace)
    .bind(kind)
    .bind(key)
    .bind(request_digest)
    .bind(Json(Value::Null))
    .bind(now)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    if inserted.rows_affected() == 1 {
        return Ok(None);
    }

    let row = sqlx::query(
        "SELECT request_digest, result, completed FROM operations \
         WHERE namespace = ? AND kind = ? AND operation_key = ? FOR UPDATE",
    )
    .bind(namespace)
    .bind(kind)
    .bind(key)
    .fetch_optional(&mut **tx)
    .await?;
    decode_result(row, kind, request_digest)
}

fn decode_result<T>(
    row: Option<MySqlRow>,
    kind: &str,
    request_digest: &str,
) -> Result<Option<T>, Error>
where
    T: for<'de> Deserialize<'de>,
{
    let Some(row) = row else {
        return Err(Error::Unavailable(format!(
            "reserved {kind} operation disappeared"
        )));
    };
    if !row.try_get::<bool, _>("completed")? {
        return Err(Error::Unavailable(format!(
            "reserved {kind} operation is incomplete"
        )));
    }
    let existing: String = row.try_get("request_digest")?;
    if existing != request_digest {
        return Err(Error::Conflict(format!(
            "idempotency key reused with a different {kind} body"
        )));
    }
    let result: Json<Value> = row.try_get("result")?;
    Ok(Some(serde_json::from_value(result.0)?))
}

pub(super) async fn record<T: Serialize + ?Sized>(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    kind: &str,
    key: &str,
    request_digest: &str,
    result: &T,
) -> Result<(), Error> {
    let now = now_millis();
    let updated = sqlx::query(
        "UPDATE operations SET result = ?, completed = TRUE, updated_time = ? \
         WHERE namespace = ? AND kind = ? AND operation_key = ? \
         AND request_digest = ? AND completed = FALSE",
    )
    .bind(Json(result))
    .bind(now)
    .bind(namespace)
    .bind(kind)
    .bind(key)
    .bind(request_digest)
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(Error::Unavailable(format!(
            "failed to record reserved {kind} operation"
        )));
    }
    Ok(())
}

fn validate_key(key: &str) -> Result<(), Error> {
    if key.trim().is_empty() || key.len() > 191 || key.chars().any(char::is_control) {
        return Err(Error::Invalid(
            "Idempotency-Key must be non-empty, at most 191 bytes, and contain no control characters"
                .to_string(),
        ));
    }
    Ok(())
}

fn canonicalize(value: &mut Value) {
    match value {
        Value::Array(values) => values.iter_mut().for_each(canonicalize),
        Value::Object(values) => {
            for value in values.values_mut() {
                canonicalize(value);
            }
            let mut fields = std::mem::take(values).into_iter().collect::<Vec<_>>();
            fields.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            values.extend(fields);
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::validate_key;

    #[test]
    fn operation_key_uses_storage_bounds() {
        assert!(validate_key(&"x".repeat(191)).is_ok());
        assert!(validate_key(&"x".repeat(192)).is_err());
        assert!(validate_key("bad\nkey").is_err());
        assert!(validate_key(" ").is_err());
    }
}
