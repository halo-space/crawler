mod browser;
mod middleware;
mod scheduler;
#[path = "worker.rs"]
mod worker_runtime;

/// Core Harbor APIs exposed through the public `halo` facade.
pub mod harbor {
    pub use ::harbor::*;
}

/// Optional Scheduler, Middleware, and browser integrations.
pub mod contrib {
    pub mod browser {
        pub use crate::browser::*;
    }

    pub mod middleware {
        pub use crate::middleware::*;
    }

    pub mod scheduler {
        pub use crate::scheduler::*;
    }
}

/// Standalone Worker configuration and process runner.
pub mod worker {
    pub use crate::worker_runtime::*;
}
