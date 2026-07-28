use crate::net;

const DEFAULT_ID: &str = "worker-1";

#[derive(Clone)]
pub(super) struct Worker {
    pub(super) id: String,
    pub(super) modes: Vec<net::Mode>,
    explicit_id: bool,
}

impl Default for Worker {
    fn default() -> Self {
        Self {
            id: DEFAULT_ID.to_string(),
            modes: vec![net::Mode::Http],
            explicit_id: false,
        }
    }
}

impl Worker {
    pub(super) fn set_id(&mut self, id: impl Into<String>) {
        self.id = id.into();
        self.explicit_id = true;
    }

    pub(super) fn set_modes(&mut self, modes: impl IntoIterator<Item = net::Mode>) {
        self.modes.clear();
        for mode in modes {
            if !self.modes.contains(&mode) {
                self.modes.push(mode);
            }
        }
    }

    pub(super) fn validate(&self, requires_explicit_id: bool) -> Result<(), crate::Error> {
        if self.id.trim().is_empty() {
            return Err(crate::Error::message("Worker id must not be empty"));
        }
        if requires_explicit_id && !self.explicit_id {
            return Err(crate::Error::message(
                "Scheduler requires an explicit Worker id; call with_worker_id(...) before build",
            ));
        }
        if self.modes.is_empty() {
            return Err(crate::Error::message("Worker modes must not be empty"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_worker_may_use_the_default_identity() {
        assert!(Worker::default().validate(false).is_ok());
    }

    #[test]
    fn distributed_worker_requires_an_explicit_identity() {
        let mut worker = Worker::default();
        assert!(worker.validate(true).is_err());

        worker.set_id("worker-a");
        assert!(worker.validate(true).is_ok());
    }
}
