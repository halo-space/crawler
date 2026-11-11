use halo::harbor::config::Config;
use halo::harbor::downloader::{Downloader, browser::Browser};
use halo::harbor::item::{Jsonl, Map};
use halo::harbor::net::Mode;
use halo::harbor::{Error, Memory, Spider, Tx, engine};

const RULES: &str = r#"
spider:
  name: x_caiziboshi_posts
  allowed_domains: [x.com]
  start:
    - node: profile
      url: "https://x.com/caiziboshi"
      download_mode: browser
      wait_selector: "article"
      timeout: 60000
      scroll:
        max_rounds: 3
        wait_ms: 500
        stable_rounds: 2
      vals:
        account: "caiziboshi"

graph:
  nodes:
    profile:
      fields:
        post_hrefs:
          default: []
          extractors:
            - kind: css
              expr: "article a[href*='/status/']:not([href*='/photo/'])::attr(href)"
      binds:
        post_urls:
          kind: pipeline
          from: "$fields.post_hrefs"
          transforms:
            - kind: url_join
              base_url: "$response.url"

    post:
      fields:
        text:
          default: ""
          extractors:
            - kind: css
              expr: "article:first-of-type [data-testid='tweetText']::text"
        created_time:
          default: ""
          extractors:
            - kind: css
              expr: "article:first-of-type time::attr(datetime)"
        images:
          kind: image
          default: []
          download: false
          extractors:
            - kind: css
              expr: "article:first-of-type div[data-testid='tweetPhoto'] img::attr(src)"
        videos:
          kind: video
          default: []
          download: false
          extractors:
            - kind: css
              expr: "article:first-of-type video::attr(src)"

  edges:
    - from: profile
      kind: request
      request:
        node: post
        url:
          from: "$binds.post_urls"
        download_mode: browser
        wait_selector: "article"
        timeout: 60000
        vals:
          account:
            from: "$vals.account"

    - from: post
      kind: item
      vals:
        account:
          from: "$vals.account"
        post_url:
          from: "$response.url"
        text:
          from: "$fields.text"
        created_time:
          from: "$fields.created_time"
        images:
          from: "$fields.images"
        videos:
          from: "$fields.videos"

item:
  schema:
    fields:
      account: {type: string}
      post_url: {type: string}
      text: {type: string}
      created_time: {type: string}
      images:
        type: array
        fields:
          name: {type: string}
          url: {type: string}
          src: {type: string}
          width: {type: int}
          height: {type: int}
          size: {type: int}
          ext: {type: string}
          alt: {type: string}
      videos:
        type: array
        fields:
          name: {type: string}
          url: {type: string}
          src: {type: string}
          width: {type: int}
          height: {type: int}
          size: {type: int}
          ext: {type: string}
          alt: {type: string}
"#;

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
    let config = Config::from_yaml(RULES).unwrap();
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
    assert_eq!(stats["profile"].done, 1);
    assert!(stats["post"].done > 0, "post stats: {stats:?}");
    assert!(stats["items"].done > 0, "item stats: {stats:?}");

    tokio::fs::remove_dir_all(output_dir).await.unwrap();
}
