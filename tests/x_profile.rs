use std::path::PathBuf;

use halo::harbor::config::Config;
use halo::harbor::downloader::{Downloader, browser::Browser};
use halo::harbor::item::{Jsonl, Map};
use halo::harbor::net::Mode;
use halo::harbor::{Error, Memory, Spider, Tx, engine};

struct XProfileSpider {
    tx: Tx,
}

impl Spider for XProfileSpider {
    type Item = Map;

    fn name(&self) -> &str {
        "x-profile-test"
    }

    fn tx(&self) -> &Tx {
        &self.tx
    }

    async fn index(&self, _response: halo::harbor::net::Response) -> Result<(), Error> {
        Ok(())
    }

    async fn item(&self, item: Map) -> Result<(), Error> {
        self.tx.item(vec![item]).await
    }
}

#[tokio::test]
#[ignore = "requires a compatible visible ChromeDriver and the public X page"]
async fn x_profile_rules_fetch_posts_and_items() {
    let webdriver_url = std::env::var("WEBDRIVER_URL")
        .expect("WEBDRIVER_URL must point to a compatible ChromeDriver");
    let rules_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/rules-x-profile.yaml");
    let config = Config::load(rules_path).await.unwrap();
    let output_dir = std::env::temp_dir().join(format!("harbor-x-profile-{}", std::process::id()));
    let browser = Browser::new()
        .with_webdriver_url(webdriver_url)
        .with_headless(false);
    let downloader = Downloader::new().with_browser(browser);

    let mut engine = engine::Builder::new()
        .with_scheduler(Memory::new().with_modes([Mode::Browser]))
        .with_downloader(downloader)
        .with_rules(config)
        .with_spider(|tx| XProfileSpider { tx })
        .with_store(Jsonl::with_dir(&output_dir))
        .build()
        .with_concurrency(2);

    engine.start_until_idle().await.unwrap();
    assert_eq!(
        engine.scheduler().failed_len(),
        0,
        "scheduler errors: {:?}",
        engine.scheduler().errors()
    );

    let trace_id = engine.scheduler().trace_ids().into_iter().next().unwrap();
    let stats = engine.scheduler().trace_stats(&trace_id);
    assert_eq!(stats["profile"].done, 3);
    assert!(stats["post"].done > 0, "post stats: {stats:?}");
    assert!(stats["items"].done > 0, "item stats: {stats:?}");

    tokio::fs::remove_dir_all(output_dir).await.unwrap();
}
