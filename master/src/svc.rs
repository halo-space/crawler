use std::sync::Arc;

use crate::config::Config;
use crate::store::MySql;

#[derive(Clone)]
pub(crate) struct Context {
    pub(crate) config: Arc<Config>,
    pub(crate) store: MySql,
}

impl Context {
    pub(crate) fn new(config: Config, store: MySql) -> Self {
        Self {
            config: Arc::new(config),
            store,
        }
    }
}
