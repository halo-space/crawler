use spider::{engine, net};

#[macros::spider]
struct BasicSpider;

#[macros::spider]
impl BasicSpider {
    fn name(&self) -> &str {
        "basic"
    }

    async fn start_urls(&self) -> Vec<String> {
        vec!["https://example.com/".to_string()]
    }

    async fn index(&self, response: net::Response) -> Result<(), spider::Error> {
        let soup = response.css()?;
        let title = soup
            .find("h1")
            .map_err(|error| spider::Error::Message(error.to_string()))?
            .map(|node| node.text())
            .unwrap_or_else(|| response.url.clone());

        self.tx.item(vec![BasicItem::new(title)]).await?;
        Ok(())
    }
}

#[derive(serde::Deserialize, serde::Serialize, macros::Item)]
#[serde(deny_unknown_fields)]
struct BasicItem {
    title: String,
    #[serde(skip)]
    state: spider::item::State,
}

impl BasicItem {
    fn new(title: String) -> Self {
        Self {
            title,
            state: spider::item::State::default(),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = engine::Engine::new()
        .with_spider(BasicSpider::new())
        .build()
        .with_concurrency(16);

    engine.start().await?;
    Ok(())
}
