use harbor::item::Map;
use harbor::{Error as HarborError, Spider, SpiderFactory, Tx};

#[derive(Clone)]
pub struct RulesWorker {
    tx: Tx,
}

impl Spider for RulesWorker {
    type Item = Map;

    fn name(&self) -> &str {
        "halo-harbor-worker"
    }

    fn tx(&self) -> &Tx {
        &self.tx
    }

    async fn index(&self, _response: harbor::net::Response) -> Result<(), HarborError> {
        Ok(())
    }
}

pub struct RulesWorkerFactory;

impl SpiderFactory for RulesWorkerFactory {
    type Spider = RulesWorker;

    fn build(self, tx: Tx) -> Self::Spider {
        RulesWorker { tx }
    }
}
