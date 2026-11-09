mod config;
mod memory;

pub use config::Config;
pub use memory::Memory;

use crate::middleware::Spec;

pub(super) fn check(spec: &Spec) -> Result<(), crate::middleware::Error> {
    if spec
        .hook
        .as_deref()
        .is_some_and(|hook| hook != "before_download")
    {
        return Err(invalid("hook must be before_download"));
    }
    if spec.skip {
        return Ok(());
    }
    Config::from_spec(spec).map(|_| ())
}

pub(super) fn invalid(message: impl Into<String>) -> crate::middleware::Error {
    crate::middleware::Error::InvalidConfig {
        name: "rate_limit".to_string(),
        message: message.into(),
    }
}
