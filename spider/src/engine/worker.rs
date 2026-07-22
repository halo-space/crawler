use crate::net;

const DEFAULT_ID: &str = "worker-1";

#[derive(Clone)]
pub(super) struct Worker {
    pub(super) id: String,
    pub(super) modes: Vec<net::Mode>,
}

impl Default for Worker {
    fn default() -> Self {
        Self {
            id: DEFAULT_ID.to_string(),
            modes: vec![net::Mode::Http],
        }
    }
}

impl Worker {
    pub(super) fn set_id(&mut self, id: impl Into<String>) {
        self.id = id.into();
    }

    pub(super) fn set_modes(&mut self, modes: impl IntoIterator<Item = net::Mode>) {
        self.modes.clear();
        for mode in modes {
            if !self.modes.contains(&mode) {
                self.modes.push(mode);
            }
        }
    }

    pub(super) fn validate(&self) -> Result<(), crate::Error> {
        if self.id.trim().is_empty() {
            return Err(crate::Error::message("Worker id must not be empty"));
        }
        if self.modes.is_empty() {
            return Err(crate::Error::message("Worker modes must not be empty"));
        }
        Ok(())
    }
}
