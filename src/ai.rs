//! Worker-local AI provider runtime.

mod openai;
mod transport;

pub use crate::error::ai::Error;
pub use openai::OpenAI;

#[cfg(test)]
pub(crate) mod server;
