use std::path::PathBuf;

use spider::item::{Item, State, Values};
use spider::{config, engine, net};

#[derive(serde::Serialize)]
struct Article {
    title: String,
    content: String,
    url: String,
    page_url: String,
    #[serde(skip)]
    state: State,
}

type ArticleItem = Article;

impl Item for Article {
    fn from_values(mut values: Values) -> Result<Self, spider::item::Error> {
        Ok(Self {
            title: string(&mut values, "title")?,
            content: string(&mut values, "content")?,
            url: string(&mut values, "url")?,
            page_url: string(&mut values, "page_url")?,
            state: State::default(),
        })
    }

    fn state(&self) -> &State {
        &self.state
    }

    fn state_mut(&mut self) -> &mut State {
        &mut self.state
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

fn string(values: &mut Values, name: &str) -> Result<String, spider::item::Error> {
    values
        .shift_remove(name)
        .and_then(|value| value.as_str().map(str::to_string))
        .ok_or_else(|| spider::item::Error::Message(format!("{name} must be a string")))
}

#[macros::spider]
struct Newspaper;

#[macros::spider(item = Article)]
impl Newspaper {
    fn name(&self) -> &str {
        "newspaper-worker"
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        Ok(())
    }

    #[item]
    async fn save(&self, article: ArticleItem) -> Result<(), spider::Error> {
        self.tx.item(vec![article]).await
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rules_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("rules-newspaper.yaml");
    let rules = config::Config::load(rules_path).await?;
    let mut engine = engine::Engine::new()
        .with_rules(rules)
        .with_spider(Newspaper::new())
        .build()
        .with_concurrency(8);

    engine.start().await?;
    println!(
        "completed: {}, failed: {}",
        engine.scheduler().done_len(),
        engine.scheduler().failed_len()
    );
    for error in engine.scheduler().errors() {
        eprintln!("failed request: {error}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn newspaper_rules_are_valid() {
        spider::config::Config::from_yaml(include_str!("../../rules-newspaper.yaml")).unwrap();
    }
}
