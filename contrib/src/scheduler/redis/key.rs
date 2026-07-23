use base64::Engine as _;

const MAX_NAMESPACE_LEN: usize = 128;

#[derive(Clone, Debug)]
pub(super) struct Keys {
    namespace: String,
}

impl Keys {
    pub(super) fn new(namespace: impl Into<String>) -> Result<Self, spider::scheduler::Error> {
        let namespace = namespace.into();
        if namespace.trim().is_empty() {
            return Err(spider::scheduler::Error::Message(
                "Redis namespace must not be empty".to_string(),
            ));
        }
        if namespace.len() > MAX_NAMESPACE_LEN {
            return Err(spider::scheduler::Error::Message(format!(
                "Redis namespace must not exceed {MAX_NAMESPACE_LEN} bytes"
            )));
        }
        if namespace.chars().any(char::is_control) {
            return Err(spider::scheduler::Error::Message(
                "Redis namespace must not contain control characters".to_string(),
            ));
        }
        Ok(Self { namespace })
    }

    pub(super) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(super) fn prefix(&self) -> String {
        format!("{}:", self.namespace)
    }

    pub(super) fn meta(&self) -> String {
        self.key("meta")
    }

    pub(super) fn traces(&self) -> String {
        self.key("traces")
    }

    pub(super) fn trace_tasks(&self) -> String {
        self.key("trace_tasks")
    }

    pub(super) fn leases(&self) -> String {
        self.key("leases")
    }

    pub(super) fn request(&self, id: &str) -> String {
        self.request_token(&token(id))
    }

    pub(super) fn request_token(&self, token: &str) -> String {
        self.key(&format!("request:{token}"))
    }

    pub(super) fn completion(&self, id: &str, version: i64) -> String {
        format!("{}:completion:{version}", self.request(id))
    }

    pub(super) fn stats(&self, trace_id: &str) -> String {
        self.key(&format!("trace:{}:stats", token(trace_id)))
    }

    pub(super) fn items(&self) -> String {
        self.key("items")
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.namespace)
    }
}

pub(super) fn token(value: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_ids_are_encoded_as_opaque_key_segments() {
        let keys = Keys::new("crawler").unwrap();
        assert_eq!(keys.request("a:b/中"), "crawler:request:YTpiL-S4rQ");
    }

    #[test]
    fn namespace_validation_rejects_ambiguous_values() {
        assert!(Keys::new(" ").is_err());
        assert!(Keys::new("x\n").is_err());
        assert!(Keys::new("x".repeat(MAX_NAMESPACE_LEN + 1)).is_err());
    }
}
