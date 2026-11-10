use harbor::item::Map;
use harbor::{Error as HarborError, Spider, SpiderFactory, Tx};

#[derive(Clone)]
pub struct RulesSpider {
    tx: Tx,
}

impl Spider for RulesSpider {
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

pub struct RulesSpiderFactory;

impl SpiderFactory for RulesSpiderFactory {
    type Spider = RulesSpider;

    fn build(self, tx: Tx) -> Self::Spider {
        RulesSpider { tx }
    }
}
