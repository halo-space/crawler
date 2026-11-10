mod browser;
mod middleware;
mod scheduler;

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
