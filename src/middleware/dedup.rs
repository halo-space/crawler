mod config;
mod fingerprint;
mod memory;

pub use config::{Config, Ttl};
pub use fingerprint::Fingerprint;
pub use memory::Memory;

use crate::middleware::Spec;

pub(super) fn check(spec: &Spec) -> Result<(), crate::middleware::Error> {
    if spec
        .hook
        .as_deref()
        .is_some_and(|hook| !matches!(hook, "before_scheduler" | "before_item"))
    {
        return Err(invalid("hook must be before_scheduler or before_item"));
    }
    if spec.skip {
        return Ok(());
    }
    Config::from_spec(spec).map(|_| ())
}

pub(super) fn invalid(message: impl Into<String>) -> crate::middleware::Error {
    crate::middleware::Error::InvalidConfig {
        name: "dedup".to_string(),
        message: message.into(),
    }
}
