use std::collections::HashSet;

use serde_json::Value;
use spider::downloader::{Download, http::Http};
use spider::{net, selector};

const EASTMONEY_URL: &str = "https://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&secids=1.601398,1.600036,1.600030,1.601318,0.300059,1.600705,1";

#[tokio::test]
#[ignore = "requires the public EastMoney service"]
async fn eastmoney_json_selector_smoke() {
    let mut request = net::Request::follow(EASTMONEY_URL)
        .unwrap()
        .max_body_bytes(1024 * 1024);
    request.timeout = Some(10_000);
    let response = Http::new().fetch(request).await.unwrap();

    assert!((200..300).contains(&response.status.0));
    assert!(
        response
            .headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
    );

    let document: Value = response.json().unwrap();
    assert_eq!(document["rc"], Value::from(0));

    let rows = selector::json::select(&document, "$.data.diff[*]").unwrap();
    assert!(!rows.is_empty());
    assert_eq!(document["data"]["total"], Value::from(rows.len()));
    assert!(rows.iter().all(|row| {
        row["f12"].as_str().is_some_and(|value| !value.is_empty())
            && row["f14"].as_str().is_some_and(|value| !value.is_empty())
    }));

    let codes = selector::json::select(&document, "$.data.diff[*].f12")
        .unwrap()
        .into_iter()
        .filter_map(Value::as_str)
        .collect::<HashSet<_>>();
    let expected = HashSet::from(["601398", "600036", "600030", "601318", "300059", "600705"]);
    assert!(expected.is_subset(&codes));
}
