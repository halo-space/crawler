use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::Error;

const PREFIX: &str = "v1";
const MAX_LENGTH: usize = 4096;
const MAX_ID_LENGTH: usize = 191;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub(crate) enum Key {
    Timed { time: i64, id: String },
    Id { id: String },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Token {
    namespace: String,
    endpoint: String,
    filter: String,
    key: Key,
}

pub(crate) fn encode(
    namespace: &str,
    endpoint: &str,
    filter: &impl Serialize,
    key: Key,
) -> Result<String, Error> {
    let token = Token {
        namespace: namespace.to_string(),
        endpoint: endpoint.to_string(),
        filter: digest(&serde_json::to_vec(filter)?),
        key,
    };
    let payload = serde_json::to_vec(&token)?;
    let checksum = digest(&payload);
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
    Ok(format!("{PREFIX}.{payload}.{checksum}"))
}

pub(crate) fn decode(
    value: &str,
    namespace: &str,
    endpoint: &str,
    filter: &impl Serialize,
) -> Result<Key, Error> {
    if value.is_empty() || value.len() > MAX_LENGTH {
        return Err(invalid());
    }
    let mut parts = value.split('.');
    let (Some(prefix), Some(payload), Some(checksum), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(invalid());
    };
    if prefix != PREFIX {
        return Err(invalid());
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| invalid())?;
    if digest(&payload) != checksum {
        return Err(invalid());
    }
    let token: Token = serde_json::from_slice(&payload).map_err(|_| invalid())?;
    let expected_filter = digest(&serde_json::to_vec(filter)?);
    if token.namespace != namespace || token.endpoint != endpoint || token.filter != expected_filter
    {
        return Err(invalid());
    }
    match &token.key {
        Key::Timed { time, id } if *time >= 0 && valid_id(id) => {}
        Key::Id { id } if valid_id(id) => {}
        _ => return Err(invalid()),
    }
    Ok(token.key)
}

fn valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_ID_LENGTH && !value.chars().any(char::is_control)
}

fn digest(value: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(value))
}

fn invalid() -> Error {
    Error::Invalid("invalid cursor".to_string())
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;

    #[derive(Serialize)]
    struct Filter<'a> {
        state: Option<&'a str>,
    }

    #[test]
    fn cursor_round_trip_preserves_its_key() {
        let filter = Filter {
            state: Some("pending"),
        };
        let key = Key::Timed {
            time: 42,
            id: "request-1".to_string(),
        };
        let encoded = encode("crawler", "requests", &filter, key.clone()).unwrap();

        assert_eq!(
            decode(&encoded, "crawler", "requests", &filter).unwrap(),
            key
        );
    }

    #[test]
    fn cursor_is_bound_to_namespace_endpoint_and_filter() {
        let pending = Filter {
            state: Some("pending"),
        };
        let done = Filter {
            state: Some("done"),
        };
        let encoded = encode(
            "crawler",
            "requests",
            &pending,
            Key::Id {
                id: "request-1".to_string(),
            },
        )
        .unwrap();

        assert!(decode(&encoded, "other", "requests", &pending).is_err());
        assert!(decode(&encoded, "crawler", "items", &pending).is_err());
        assert!(decode(&encoded, "crawler", "requests", &done).is_err());
    }

    #[test]
    fn malformed_or_modified_cursor_is_rejected() {
        let filter = Filter { state: None };
        let mut encoded = encode(
            "crawler",
            "workers",
            &filter,
            Key::Id {
                id: "worker-1".to_string(),
            },
        )
        .unwrap();
        encoded.push('0');

        assert!(decode("", "crawler", "workers", &filter).is_err());
        assert!(decode("v1.not_base64.checksum", "crawler", "workers", &filter).is_err());
        assert!(decode(&encoded, "crawler", "workers", &filter).is_err());

        let oversized = encode(
            "crawler",
            "workers",
            &filter,
            Key::Id {
                id: "x".repeat(MAX_ID_LENGTH + 1),
            },
        )
        .unwrap();
        assert!(decode(&oversized, "crawler", "workers", &filter).is_err());
    }
}
