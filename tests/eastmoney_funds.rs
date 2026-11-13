use std::collections::HashMap;
use std::path::PathBuf;

use halo::harbor::config::Config;
use halo::harbor::downloader::{Download, Downloader, browser::Browser, http::Http};
use halo::harbor::item::{Jsonl, Map};
use halo::harbor::net::{Body, Method, Mode, Request};
use halo::harbor::selector;
use halo::harbor::{Error, Memory, Spider, Tx, engine};
use serde_json::Value;

struct EastMoneySpider {
    tx: Tx,
}

impl Spider for EastMoneySpider {
    type Item = Map;

    fn name(&self) -> &str {
        "eastmoney-fund-daily-test"
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
#[ignore = "requires the public EastMoney service"]
async fn eastmoney_fund_rules_fetch_ranking_and_overview() {
    let rules_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/rules-eastmoney-funds-daily.yaml");
    let rules = Config::load(rules_path).await.unwrap();
    let mut requests = rules
        .initial_requests("eastmoney-test", "eastmoney-trace", HashMap::new())
        .unwrap();
    let ranking_request = requests.remove(0);

    let http = Http::new();
    http.open().await.unwrap();

    let ranking_response = http.fetch(ranking_request).await.unwrap();
    assert_eq!(ranking_response.status.0, 200);
    assert!(ranking_response.text().unwrap().contains("rankData"));

    let codes = selector::regex::select(&ranking_response, r"([0-9]{6}),").unwrap();
    assert!(
        !codes.is_empty(),
        "ranking response did not contain fund codes"
    );

    let overview_url = format!("https://fundf10.eastmoney.com/jbgk_{}.html", codes[0]);
    let overview_response = http
        .fetch(Request::follow(overview_url).unwrap())
        .await
        .unwrap();
    assert_eq!(overview_response.status.0, 200);
    assert!(overview_response.text().unwrap().contains("基金全称"));

    http.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the public EastMoney service and a local Chrome/Chromium"]
async fn eastmoney_fund_rules_run_full_graph() {
    let rules_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/rules-eastmoney-funds-daily.yaml");
    let rules = tokio::fs::read_to_string(rules_path).await.unwrap();
    let rules = rules
        .replace("pn=50", "pn=1")
        .replace(
            "expr: \"([0-9]{6}),\"",
            "expr: \"^.*?([0-9]{6}),\"",
        )
        .replace(
            "        wait_selector: \"table.info\"\n",
            "        timeout: 15000\n        wait_selector: \"table.info\"\n",
        )
        .replace(
            "        wait_selector: \"#cctable table\"\n",
            "        timeout: 15000\n        wait_selector: \"#cctable table\"\n",
        )
        .replace(
            "        wait_selector: \"#hypztable table\"\n",
            "        timeout: 15000\n        wait_selector: \"#hypztable table\"\n",
        )
        .replace(
            "      when:\n        from: \"$fields.fund_codes\"\n        op: present\n",
            "      when:\n        all:\n          - from: \"$fields.fund_codes\"\n            op: present\n          - from: \"$vals.page\"\n            op: lte\n            value: 1\n",
        );
    let config = Config::from_yaml(&rules).unwrap();
    let output_dir =
        std::env::temp_dir().join(format!("harbor-eastmoney-full-{}", std::process::id()));
    let webdriver_url = std::env::var("WEBDRIVER_URL")
        .expect("WEBDRIVER_URL must point to a ChromeDriver for the Browser graph test");
    let browser = Browser::new().with_webdriver_url(webdriver_url);
    let downloader = Downloader::new().with_browser(browser);

    let mut engine = engine::Engine::new()
        .with_scheduler(Memory::new().with_modes([Mode::Http, Mode::Browser]))
        .with_downloader(downloader)
        .with_rules(config)
        .with_spider(|tx| EastMoneySpider { tx })
        .with_store(Jsonl::with_dir(&output_dir))
        .build()
        .with_concurrency(4);

    engine.start_until_idle().await.unwrap();

    assert_eq!(
        engine.scheduler().failed_len(),
        0,
        "{:?}",
        engine.scheduler().errors()
    );
    let trace_id = engine.scheduler().trace_ids().into_iter().next().unwrap();
    let stats = engine.scheduler().trace_stats(&trace_id);

    let ranking = stats
        .get("ranking")
        .unwrap_or_else(|| panic!("ranking stats missing: {stats:?}"));
    assert!(
        ranking.total >= 2,
        "pagination did not produce a second ranking Request: {stats:?}"
    );
    for node in ["ranking", "overview", "holdings", "industry"] {
        let counter = stats
            .get(node)
            .unwrap_or_else(|| panic!("node {node} stats missing: {stats:?}"));
        assert!(
            counter.total > 0,
            "node {node} produced no Request: {stats:?}"
        );
        assert_eq!(
            counter.done, counter.total,
            "node {node} did not finish: {stats:?}"
        );
    }
    let items = stats
        .get("items")
        .unwrap_or_else(|| panic!("items stats missing: {stats:?}"));
    assert!(
        items.done >= 8,
        "ranking and detail nodes did not all produce Items: {stats:?}"
    );

    tokio::fs::remove_dir_all(output_dir).await.unwrap();
}

#[tokio::test]
#[ignore = "requires the public httpbin service"]
async fn real_http_keyword_post_round_trip() {
    let request = Request::follow("https://httpbin.org/post")
        .unwrap()
        .method(Method::Post)
        .header("Content-Type", "application/json")
        .unwrap()
        .body(Body::Json(serde_json::json!({"query": "Rust"})));
    let response = Http::new().fetch(request).await.unwrap();
    assert_eq!(response.status.0, 200);

    let document: Value = response.json().unwrap();
    assert_eq!(document["json"]["query"], "Rust");
    println!("real POST response: {}", document["json"]);
}
