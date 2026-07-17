use spider::{Item, engine, net};
use std::any::Any;

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

#[derive(serde::Serialize)]
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

impl Item for BasicItem {
    fn from_values(mut values: spider::item::Values) -> Result<Self, spider::item::Error> {
        let title = values
            .shift_remove("title")
            .and_then(|value| value.as_str().map(str::to_string))
            .ok_or_else(|| spider::item::Error::Message("title must be a string".to_string()))?;
        Ok(Self::new(title))
    }

    fn state(&self) -> &spider::item::State {
        &self.state
    }

    fn state_mut(&mut self) -> &mut spider::item::State {
        &mut self.state
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
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
