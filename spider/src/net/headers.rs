use std::collections::BTreeMap;
use std::fmt;

use http::header::{AsHeaderName, GetAll, HeaderMap};
pub use http::header::{HeaderName, HeaderValue};
use serde::de::{Error as _, MapAccess, Visitor};
use serde::ser::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid header name: {0}")]
    Name(#[from] http::header::InvalidHeaderName),

    #[error("invalid header value: {0}")]
    Value(#[from] http::header::InvalidHeaderValue),
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct Headers {
    values: HeaderMap<HeaderValue>,
}

impl Headers {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&mut self, name: HeaderName, value: HeaderValue) -> Option<HeaderValue> {
        self.values.insert(name, value)
    }

    pub fn append(&mut self, name: HeaderName, value: HeaderValue) -> bool {
        self.values.append(name, value)
    }

    pub fn try_set(&mut self, name: &str, value: &str) -> Result<(), Error> {
        let name = HeaderName::from_bytes(name.as_bytes())?;
        let value = HeaderValue::from_str(value)?;
        self.set(name, value);
        Ok(())
    }

    pub fn try_append(&mut self, name: &str, value: &str) -> Result<(), Error> {
        let name = HeaderName::from_bytes(name.as_bytes())?;
        let value = HeaderValue::from_str(value)?;
        self.append(name, value);
        Ok(())
    }

    pub fn get<K>(&self, name: K) -> Option<&HeaderValue>
    where
        K: AsHeaderName,
    {
        self.values.get(name)
    }

    pub fn get_all<K>(&self, name: K) -> GetAll<'_, HeaderValue>
    where
        K: AsHeaderName,
    {
        self.values.get_all(name)
    }

    pub fn contains<K>(&self, name: K) -> bool
    where
        K: AsHeaderName,
    {
        self.values.contains_key(name)
    }

    pub fn remove<K>(&mut self, name: K) -> Option<HeaderValue>
    where
        K: AsHeaderName,
    {
        self.values.remove(name)
    }

    pub fn clear(&mut self) {
        self.values.clear();
    }

    /// Returns the number of values, including repeated header names.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn names_len(&self) -> usize {
        self.values.keys_len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn iter(&self) -> http::header::Iter<'_, HeaderValue> {
        self.values.iter()
    }

    pub fn as_map(&self) -> &HeaderMap<HeaderValue> {
        &self.values
    }

    pub fn into_map(self) -> HeaderMap<HeaderValue> {
        self.values
    }

    pub(crate) fn validate_snapshot(&self) -> Result<(), String> {
        for (name, value) in &self.values {
            value.to_str().map_err(|error| {
                format!("Request Snapshot header {name} is not a string: {error}")
            })?;
        }
        Ok(())
    }
}

impl From<HeaderMap<HeaderValue>> for Headers {
    fn from(values: HeaderMap<HeaderValue>) -> Self {
        Self { values }
    }
}

impl fmt::Debug for Headers {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Headers")
            .field("names", &self.names_len())
            .field("values", &self.len())
            .finish()
    }
}

impl Serialize for Headers {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut values = BTreeMap::new();
        for name in self.values.keys() {
            let entries = self
                .values
                .get_all(name)
                .iter()
                .map(|value| value.to_str().map(str::to_owned).map_err(S::Error::custom))
                .collect::<Result<Vec<_>, _>>()?;
            values.insert(name.as_str(), entries);
        }
        values.serialize(serializer)
    }
}

struct HeadersVisitor;

impl<'de> Visitor<'de> for HeadersVisitor {
    type Value = Headers;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a map of header names to non-empty string arrays")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut headers = Headers::new();
        while let Some((raw_name, raw_values)) = map.next_entry::<String, Vec<String>>()? {
            let name = HeaderName::from_bytes(raw_name.as_bytes()).map_err(A::Error::custom)?;
            if headers.contains(&name) {
                return Err(A::Error::custom(format!(
                    "duplicate normalized header name: {raw_name}"
                )));
            }
            if raw_values.is_empty() {
                return Err(A::Error::custom(format!(
                    "header {raw_name} must contain at least one value"
                )));
            }
            for raw_value in raw_values {
                let value = HeaderValue::from_str(&raw_value).map_err(A::Error::custom)?;
                headers.append(name.clone(), value);
            }
        }
        Ok(headers)
    }
}

impl<'de> Deserialize<'de> for Headers {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(HeadersVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_replaces_and_append_preserves_values_case_insensitively() {
        let mut headers = Headers::new();
        headers.try_append("X-Test", "one").unwrap();
        headers.try_append("x-test", "two").unwrap();

        assert_eq!(headers.names_len(), 1);
        assert_eq!(headers.len(), 2);
        assert_eq!(
            headers
                .get_all("X-TEST")
                .iter()
                .map(|value| value.to_str().unwrap())
                .collect::<Vec<_>>(),
            ["one", "two"]
        );

        headers.try_set("x-test", "three").unwrap();
        assert_eq!(headers.len(), 1);
        assert_eq!(headers.get("X-Test").unwrap(), "three");
    }

    #[test]
    fn snapshot_serde_uses_normalized_non_empty_arrays() {
        let mut headers = Headers::new();
        headers.try_append("X-Test", "one").unwrap();
        headers.try_append("x-test", "two").unwrap();

        let encoded = serde_json::to_value(&headers).unwrap();
        assert_eq!(encoded, serde_json::json!({"x-test": ["one", "two"]}));
        assert_eq!(serde_json::from_value::<Headers>(encoded).unwrap(), headers);

        assert!(serde_json::from_value::<Headers>(serde_json::json!({"x": []})).is_err());
        assert!(serde_json::from_value::<Headers>(serde_json::json!({"x": "one"})).is_err());
    }

    #[test]
    fn response_values_may_remain_opaque_but_cannot_enter_a_snapshot() {
        let mut map = HeaderMap::new();
        map.insert(
            HeaderName::from_static("x-opaque"),
            HeaderValue::from_bytes(b"\xFF").unwrap(),
        );
        let headers = Headers::from(map);

        assert_eq!(headers.get("x-opaque").unwrap().as_bytes(), b"\xFF");
        assert!(headers.validate_snapshot().is_err());
        assert!(serde_json::to_value(headers).is_err());
    }

    #[test]
    fn deserialization_rejects_names_that_normalize_to_the_same_key() {
        let encoded = r#"{"Accept":["text/html"],"accept":["application/json"]}"#;
        let error = serde_json::from_str::<Headers>(encoded).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("duplicate normalized header name")
        );
    }
}
