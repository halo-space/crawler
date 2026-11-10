use std::collections::HashMap;
use std::path::PathBuf;

use halo::harbor::config::Config;
use halo::harbor::downloader::{Download, http::Http};
use halo::harbor::net::Request;
use halo::harbor::selector;

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
