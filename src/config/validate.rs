mod graph;
mod harbor;
mod item;
mod rules;

use super::{Config, Error};

pub(super) fn check(config: &Config) -> Result<(), Error> {
    harbor::check(&config.spider, &config.graph)?;
    graph::check(&config.graph, config.item.is_some())?;
    rules::check(&config.graph)?;
    item::check(config.item.as_ref(), &config.graph)
}
