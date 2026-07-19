use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use serde_json::Value;
use spider::item::Item;
use spider::middleware::{BoxFuture, Middleware, Next, Spec};
use spider::scheduler::{Init, Scheduler};
use spider::{downloader, engine, net, payload};

#[macros::spider]
struct RulesSpider;

#[macros::spider]
impl RulesSpider {
    fn name(&self) -> &str {
        "rules"
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        Ok(())
    }
}

#[derive(serde::Serialize)]
struct RulesItem {
    title: String,
    #[serde(skip)]
    state: spider::item::State,
}

impl Item for RulesItem {
    fn from_values(mut values: spider::item::Values) -> Result<Self, spider::item::Error> {
        let title = values
            .shift_remove("title")
            .and_then(|value| value.as_str().map(str::to_string))
            .ok_or_else(|| spider::item::Error::Message("title must be a string".to_string()))?;
        Ok(Self {
            title,
            state: spider::item::State::default(),
        })
    }

    fn state(&self) -> &spider::item::State {
        &self.state
    }

    fn state_mut(&mut self) -> &mut spider::item::State {
        &mut self.state
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[macros::spider]
struct TypedRulesSpider {
    called: Arc<AtomicBool>,
}

#[macros::spider(item = RulesItem)]
impl TypedRulesSpider {
    fn name(&self) -> &str {
        "typed-rules"
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        Ok(())
    }

    #[item]
    async fn publish(&self, item: RulesItem) -> Result<(), spider::Error> {
        assert_eq!(item.title, "First Book");
        assert!(item.schema().is_some());
        self.called.store(true, Ordering::SeqCst);
        self.tx.item(vec![item]).await
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PayloadRecord {
    id: String,
    task_id: String,
    trace_id: String,
    version: i64,
    worker_id: String,
    node: String,
    state: payload::State,
    has_start_time: bool,
    has_end_time: bool,
}

impl PayloadRecord {
    fn from_payload(payload: &payload::Payload) -> Self {
        Self {
            id: payload.id.clone(),
            task_id: payload.task_id.clone(),
            trace_id: payload.trace_id.clone(),
            version: payload.version,
            worker_id: payload.worker_id.clone(),
            node: payload.node.clone(),
            state: payload.state,
            has_start_time: payload.start_time.is_some(),
            has_end_time: payload.end_time.is_some(),
        }
    }
}

struct LifecycleMiddleware {
    calls: Arc<Mutex<Vec<&'static str>>>,
}

impl Middleware for LifecycleMiddleware {
    fn before_scheduler<'a>(
        &'a self,
        request: net::Request,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, Next<net::Request>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("before_scheduler");
            Ok(Next::Continue(request))
        })
    }

    fn before_download<'a>(
        &'a self,
        request: net::Request,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, Next<net::Request>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("before_download");
            Ok(Next::Continue(request))
        })
    }

    fn after_download<'a>(
        &'a self,
        response: net::Response,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, Next<net::Response>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("after_download");
            Ok(Next::Continue(response))
        })
    }

    fn before_parse<'a>(
        &'a self,
        response: net::Response,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, Next<net::Response>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("before_parse");
            Ok(Next::Continue(response))
        })
    }

    fn before_item<'a>(
        &'a self,
        item: Box<dyn Item>,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, Next<Box<dyn Item>>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("before_item");
            Ok(Next::Continue(item))
        })
    }

    fn error_parse<'a>(
        &'a self,
        _response: &'a net::Response,
        _error: &'a str,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("error_parse");
            Ok(())
        })
    }

    fn error_item<'a>(
        &'a self,
        _item: &'a dyn Item,
        _error: &'a str,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("error_item");
            Ok(())
        })
    }
}

struct TestDownload;

impl downloader::Download for TestDownload {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        if request.url.contains("/fail") {
            return Err(downloader::Error::UnsupportedMode("fail".to_string()));
        }

        let url = request.url.clone();

        Ok(net::Response {
            vals: request.vals.clone(),
            kwargs: request.kwargs.clone(),
            middlewares: request.middlewares.clone(),
            request,
            url,
            status: net::StatusCode(200),
            reason: Some("OK".to_string()),
            version: net::HttpVersion::Http11,
            redirects: Vec::new(),
            headers: net::Headers::new(),
            cookies: net::Cookies::new(),
            body: Bytes::new(),
        })
    }
}

#[tokio::test]
async fn rules_node_without_emissions_succeeds() {
    let config = spider::config::Config::from_yaml(
        r#"
spider:
  name: rules-test
  start:
    - node: index
      url: https://example.com/
      method: GET

graph:
  nodes:
    index:
      parse: {}
      bind: {}
  edges: []
"#,
    )
    .unwrap();

    let mut engine = engine::Builder::new()
        .with_rules(config)
        .with_spider(RulesSpider::new())
        .with_downloader(TestDownload)
        .build();

    engine.start().await.unwrap();

    assert_eq!(engine.scheduler().trace_len(), 1);
    assert_eq!(engine.scheduler().done_len(), 1);
    assert_eq!(engine.scheduler().failed_len(), 0);
}

struct RulesDownload;

impl downloader::Download for RulesDownload {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        let body = if request.url.ends_with("/list") {
            r#"<a class="detail" href="/detail/1">One</a><a class="detail" href="/detail/2">Two</a>"#
        } else if request.url.ends_with("/detail/1") {
            "<h1> First Book </h1>"
        } else {
            "<h1> Second Book </h1>"
        };
        let url = request.url.clone();
        Ok(net::Response {
            vals: request.vals.clone(),
            kwargs: request.kwargs.clone(),
            middlewares: request.middlewares.clone(),
            request,
            url,
            status: net::StatusCode(200),
            reason: Some("OK".to_string()),
            version: net::HttpVersion::Http11,
            redirects: Vec::new(),
            headers: net::Headers::new(),
            cookies: net::Cookies::new(),
            body: Bytes::from(body),
        })
    }
}

#[tokio::test]
async fn rules_mode_expands_list_requests_and_submits_items() {
    let config = spider::config::Config::from_yaml(
        r#"
spider:
  name: rules-books
  start: [{node: list, url: https://example.com/list, method: GET}]

graph:
  nodes:
    list:
      parse:
        fields:
          links:
            required: true
            extractors:
              - {kind: css, expr: "a.detail::attr(href)"}
      bind:
        urls:
          kind: pipeline
          from: $fields.links
          transforms:
            - {kind: url_join, base_url: $response.url}
    detail:
      parse:
        fields:
          title:
            required: true
            extractors:
              - {kind: css, expr: "h1::text"}
      bind:
        title:
          kind: pipeline
          from: $fields.title
          transforms:
            - {kind: trim}
  edges:
    - from: list
      kind: request
      when: $bind.urls != null
      request:
        node: detail
        url: {from: $bind.urls}
        method: GET
    - from: detail
      kind: item
      vals:
        title: {from: $bind.title}
        url: {from: $response.url}

item:
  schema:
    fields:
      title:
        type: string
        rules:
          - required
          - {min: 1}
      url:
        type: string
        rules:
          - required
          - url
"#,
    )
    .unwrap();
    let mut engine = engine::Builder::new()
        .with_rules(config)
        .with_spider(RulesSpider::new())
        .with_downloader(RulesDownload)
        .build();

    engine.start().await.unwrap();

    assert_eq!(engine.scheduler().done_len(), 3);
    assert_eq!(engine.scheduler().failed_len(), 0);
}

#[tokio::test]
async fn rules_builds_the_spider_item_type_and_calls_the_configured_function() {
    let config = spider::config::Config::from_yaml(
        r#"
spider:
  name: typed-rules
  start: [{node: detail, url: https://example.com/detail/1}]
graph:
  nodes:
    detail:
      parse:
        fields:
          title:
            required: true
            extractors:
              - {kind: css, expr: "h1::text"}
      bind:
        title:
          kind: pipeline
          from: $fields.title
          transforms:
            - {kind: trim}
  edges:
    - from: detail
      kind: item
      fn: publish
      vals: {}
item:
  schema:
    fields:
      title: {type: string, rules: [required]}
"#,
    )
    .unwrap();
    let called = Arc::new(AtomicBool::new(false));
    let dir = std::env::temp_dir().join(format!("crawler-typed-{}", uuid::Uuid::now_v7()));
    let mut engine = engine::Builder::new()
        .with_scheduler(spider::Memory::new("worker-1").with_dir(&dir))
        .with_rules(config)
        .with_spider(TypedRulesSpider::new(called.clone()))
        .with_downloader(RulesDownload)
        .build();

    engine.start().await.unwrap();

    assert!(called.load(Ordering::SeqCst));
    assert_eq!(engine.scheduler().done_len(), 1);
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[test]
#[should_panic(expected = "Rules item function is not registered")]
fn rules_builder_rejects_an_unregistered_item_function() {
    let config = spider::config::Config::from_yaml(
        r#"
spider:
  name: typed-rules
  start: [{node: detail, url: https://example.com/detail/1}]
graph:
  nodes:
    detail: {}
  edges:
    - from: detail
      kind: item
      fn: missing
      vals: {}
item:
  schema:
    fields:
      title: {type: string}
"#,
    )
    .unwrap();

    let _engine = engine::Builder::new()
        .with_rules(config)
        .with_spider(TypedRulesSpider::new(Arc::new(AtomicBool::new(false))))
        .build();
}

struct MediaDownload;

impl downloader::Download for MediaDownload {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        Ok(response(
            request,
            r#"<img src="../cover.JPG#preview" width="640" height="480" alt="Cover">"#,
        ))
    }
}

#[tokio::test]
async fn rules_media_field_is_normalized_before_validation_and_submit() {
    let config = spider::config::Config::from_yaml(
        r#"
spider:
  name: rules-media
  start: [{node: index, url: https://example.com/books/1, method: GET}]
graph:
  nodes:
    index:
      parse:
        fields:
          images:
            extractors:
              - {kind: css, expr: "img"}
      bind: {}
  edges:
    - from: index
      kind: item
      vals:
        images: {from: $fields.images}
item:
  fields:
    images:
      kind: image
  schema:
    fields:
      images:
        type: array
        fields:
          name: {type: string}
          url: {type: string, rules: [required, url]}
          src: {type: string, rules: [required]}
          width: {type: int}
          height: {type: int}
          size: {type: int}
          ext: {type: string}
          alt: {type: string}
"#,
    )
    .unwrap();
    let dir = std::env::temp_dir().join(format!("crawler-media-{}", uuid::Uuid::now_v7()));
    let mut engine = engine::Builder::new()
        .with_scheduler(spider::Memory::new("worker-1").with_dir(&dir))
        .with_rules(config)
        .with_spider(RulesSpider::new())
        .with_downloader(MediaDownload)
        .build();

    engine.start().await.unwrap();

    let item_dir = dir
        .join("data")
        .join("items")
        .join("output")
        .join("rules-media");
    let mut files = tokio::fs::read_dir(item_dir).await.unwrap();
    let path = files.next_entry().await.unwrap().unwrap().path();
    let line = tokio::fs::read_to_string(path).await.unwrap();
    let item: Value = serde_json::from_str(line.trim()).unwrap();
    let media = &item["images"][0];
    assert_eq!(media["name"], "");
    assert_eq!(media["url"], "https://example.com/cover.JPG");
    assert_eq!(media["src"], "../cover.JPG#preview");
    assert_eq!(media["width"], 640);
    assert_eq!(media["height"], 480);
    assert_eq!(media["size"], 0);
    assert_eq!(media["ext"], "jpg");
    assert_eq!(media["alt"], "Cover");
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn rules_required_field_failure_fails_only_that_request() {
    let config = spider::config::Config::from_yaml(
        r#"
spider:
  name: rules-fail
  start: [{node: index, url: https://example.com/list, method: GET}]
graph:
  nodes:
    index:
      parse:
        fields:
          missing:
            required: true
            extractors:
              - {kind: css, expr: "h2::text"}
      bind: {}
  edges: []
"#,
    )
    .unwrap();
    let mut engine = engine::Builder::new()
        .with_rules(config)
        .with_spider(RulesSpider::new())
        .with_downloader(RulesDownload)
        .build();

    engine.start().await.unwrap();

    assert_eq!(engine.scheduler().done_len(), 0);
    assert_eq!(engine.scheduler().failed_len(), 1);
}

struct PagingDownload;

impl downloader::Download for PagingDownload {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        let body = if request.url.ends_with("/page/1") {
            r#"<a class="next" href="/page/2">Next</a>"#
        } else {
            ""
        };
        Ok(response(request, body))
    }
}

#[tokio::test]
async fn rules_pagination_self_loop_stops_when_next_link_is_absent() {
    let config = spider::config::Config::from_yaml(
        r#"
spider:
  name: rules-pages
  start: [{node: page, url: https://example.com/page/1, method: GET}]
graph:
  nodes:
    page:
      parse:
        fields:
          next:
            extractors:
              - {kind: css, expr: "a.next::attr(href)"}
      bind:
        next_url:
          kind: pipeline
          from: $fields.next
          transforms:
            - {kind: url_join, base_url: $response.url}
  edges:
    - from: page
      kind: request
      when: $fields.next != null
      request:
        node: page
        url: {from: $bind.next_url}
        method: GET
"#,
    )
    .unwrap();
    let mut engine = engine::Builder::new()
        .with_rules(config)
        .with_spider(RulesSpider::new())
        .with_downloader(PagingDownload)
        .build();

    engine.start().await.unwrap();

    assert_eq!(engine.scheduler().done_len(), 2);
    assert_eq!(engine.scheduler().failed_len(), 0);
}

struct IsolatedRulesDownload;

impl downloader::Download for IsolatedRulesDownload {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        let body = if request.url.ends_with("/good") {
            "<h1>Good</h1>"
        } else {
            ""
        };
        Ok(response(request, body))
    }
}

#[tokio::test]
async fn rules_error_fails_only_the_invalid_request() {
    let config = spider::config::Config::from_yaml(
        r#"
spider:
  name: rules-isolation
  start:
    - {node: index, url: https://example.com/good, method: GET}
    - {node: index, url: https://example.com/bad, method: GET}
graph:
  nodes:
    index:
      parse:
        fields:
          title:
            required: true
            extractors:
              - {kind: css, expr: "h1::text"}
      bind: {}
  edges: []
"#,
    )
    .unwrap();
    let mut engine = engine::Builder::new()
        .with_rules(config)
        .with_spider(RulesSpider::new())
        .with_downloader(IsolatedRulesDownload)
        .build()
        .with_concurrency(2);

    engine.start().await.unwrap();

    assert_eq!(engine.scheduler().done_len(), 1);
    assert_eq!(engine.scheduler().failed_len(), 1);
}

struct ConcurrentRulesDownload {
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

impl downloader::Download for ConcurrentRulesDownload {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(response(request, ""))
    }
}

#[tokio::test]
async fn rules_requests_execute_concurrently() {
    let config = spider::config::Config::from_yaml(
        r#"
spider:
  name: rules-concurrent
  start:
    - {node: index, url: https://example.com/1, method: GET}
    - {node: index, url: https://example.com/2, method: GET}
    - {node: index, url: https://example.com/3, method: GET}
    - {node: index, url: https://example.com/4, method: GET}
graph:
  nodes:
    index:
      parse: {}
      bind: {}
  edges: []
"#,
    )
    .unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let mut engine = engine::Builder::new()
        .with_rules(config)
        .with_spider(RulesSpider::new())
        .with_downloader(ConcurrentRulesDownload {
            active: active.clone(),
            max_active: max_active.clone(),
        })
        .build()
        .with_concurrency(4);

    engine.start().await.unwrap();

    assert_eq!(engine.scheduler().done_len(), 4);
    assert_eq!(engine.scheduler().failed_len(), 0);
    assert!(max_active.load(Ordering::SeqCst) > 1);
}

fn response(request: net::Request, body: &str) -> net::Response {
    let url = request.url.clone();
    net::Response {
        vals: request.vals.clone(),
        kwargs: request.kwargs.clone(),
        middlewares: request.middlewares.clone(),
        request,
        url,
        status: net::StatusCode(200),
        reason: Some("OK".to_string()),
        version: net::HttpVersion::Http11,
        redirects: Vec::new(),
        headers: net::Headers::new(),
        cookies: net::Cookies::new(),
        body: Bytes::copy_from_slice(body.as_bytes()),
    }
}

struct LifecycleDownload {
    calls: Arc<Mutex<Vec<&'static str>>>,
}

impl downloader::Download for LifecycleDownload {
    async fn open(&self) -> Result<(), downloader::Error> {
        self.calls.lock().unwrap().push("downloader.open");
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        self.calls.lock().unwrap().push("downloader.close");
        Ok(())
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        self.calls.lock().unwrap().push("downloader.fetch");

        let url = request.url.clone();

        Ok(net::Response {
            vals: request.vals.clone(),
            kwargs: request.kwargs.clone(),
            middlewares: request.middlewares.clone(),
            request,
            url,
            status: net::StatusCode(200),
            reason: Some("OK".to_string()),
            version: net::HttpVersion::Http11,
            redirects: Vec::new(),
            headers: net::Headers::new(),
            cookies: net::Cookies::new(),
            body: Bytes::new(),
        })
    }
}

struct FailingOpenDownload {
    calls: Arc<Mutex<Vec<&'static str>>>,
}

struct FailingCloseDownload {
    calls: Arc<Mutex<Vec<&'static str>>>,
}

impl downloader::Download for FailingCloseDownload {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        self.calls.lock().unwrap().push("downloader.close");
        Err(downloader::Error::UnsupportedMode(
            "downloader close".to_string(),
        ))
    }

    async fn fetch(&self, _request: net::Request) -> Result<net::Response, downloader::Error> {
        Err(downloader::Error::UnsupportedMode("fetch".to_string()))
    }
}

impl downloader::Download for FailingOpenDownload {
    async fn open(&self) -> Result<(), downloader::Error> {
        self.calls.lock().unwrap().push("downloader.open");
        Err(downloader::Error::UnsupportedMode("open".to_string()))
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        self.calls.lock().unwrap().push("downloader.close");
        Ok(())
    }

    async fn fetch(&self, _request: net::Request) -> Result<net::Response, downloader::Error> {
        Err(downloader::Error::UnsupportedMode("fetch".to_string()))
    }
}

struct SlowDownload {
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

impl downloader::Download for SlowDownload {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);

        let url = request.url.clone();
        Ok(net::Response {
            vals: request.vals.clone(),
            kwargs: request.kwargs.clone(),
            middlewares: request.middlewares.clone(),
            request,
            url,
            status: net::StatusCode(200),
            reason: Some("OK".to_string()),
            version: net::HttpVersion::Http11,
            redirects: Vec::new(),
            headers: net::Headers::new(),
            cookies: net::Cookies::new(),
            body: Bytes::new(),
        })
    }
}

struct LifecycleScheduler {
    inner: spider::Memory,
    calls: Arc<Mutex<Vec<&'static str>>>,
    fail_push: bool,
    reject_refresh: bool,
    block_refresh: bool,
    transient_refreshes: AtomicUsize,
    completion_delay: std::time::Duration,
    lease_refreshes: AtomicUsize,
    completions: AtomicUsize,
}

impl LifecycleScheduler {
    fn test_lease() -> spider::scheduler::Lease {
        spider::scheduler::Lease::new(
            std::time::Duration::from_millis(100),
            std::time::Duration::from_millis(5),
        )
        .unwrap()
    }

    fn new(calls: Arc<Mutex<Vec<&'static str>>>) -> Self {
        Self {
            inner: spider::Memory::new("worker-1").with_lease(Self::test_lease()),
            calls,
            fail_push: false,
            reject_refresh: false,
            block_refresh: false,
            transient_refreshes: AtomicUsize::new(0),
            completion_delay: std::time::Duration::ZERO,
            lease_refreshes: AtomicUsize::new(0),
            completions: AtomicUsize::new(0),
        }
    }

    fn fail_push(calls: Arc<Mutex<Vec<&'static str>>>) -> Self {
        Self {
            inner: spider::Memory::new("worker-1").with_lease(Self::test_lease()),
            calls,
            fail_push: true,
            reject_refresh: false,
            block_refresh: false,
            transient_refreshes: AtomicUsize::new(0),
            completion_delay: std::time::Duration::ZERO,
            lease_refreshes: AtomicUsize::new(0),
            completions: AtomicUsize::new(0),
        }
    }

    fn reject_refresh(mut self) -> Self {
        self.reject_refresh = true;
        self
    }

    fn block_refresh(mut self) -> Self {
        self.block_refresh = true;
        self
    }

    fn transient_refreshes(mut self, count: usize) -> Self {
        self.transient_refreshes = AtomicUsize::new(count);
        self
    }

    fn delay_completion(mut self, delay: std::time::Duration) -> Self {
        self.completion_delay = delay;
        self
    }
}

impl Scheduler for LifecycleScheduler {
    fn lease(&self) -> Option<spider::scheduler::Lease> {
        self.inner.lease()
    }

    async fn open(&self) -> Result<(), spider::scheduler::Error> {
        self.calls.lock().unwrap().push("scheduler.open");
        self.inner.open().await
    }

    async fn close(&self) -> Result<(), spider::scheduler::Error> {
        self.calls.lock().unwrap().push("scheduler.close");
        self.inner.close().await
    }

    async fn push(&self, payload: payload::Payload) -> Result<(), spider::scheduler::Error> {
        if self.fail_push {
            self.calls.lock().unwrap().push("scheduler.push");
            return Err(spider::scheduler::Error::Message("push".to_string()));
        }

        self.inner.push(payload).await
    }

    async fn push_items(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.push_items(payload).await
    }

    async fn trace(
        &self,
        trace_id: &str,
    ) -> Result<Option<spider::trace::Snapshot>, spider::scheduler::Error> {
        self.inner.trace(trace_id).await
    }

    async fn next_requests(
        &self,
        limit: usize,
    ) -> Result<Vec<net::Request>, spider::scheduler::Error> {
        self.inner.next_requests(limit).await
    }

    async fn has_pending_requests(&self) -> Result<bool, spider::scheduler::Error> {
        self.inner.has_pending_requests().await
    }

    async fn ack(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.ack(payload).await
    }

    async fn release(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.release(payload).await
    }

    async fn refresh_lease(
        &self,
        payload: &payload::Payload,
    ) -> Result<(), spider::scheduler::Error> {
        self.lease_refreshes.fetch_add(1, Ordering::SeqCst);
        if self.reject_refresh {
            return Err(spider::scheduler::Error::LeaseMismatch(payload.id.clone()));
        }
        if self
            .transient_refreshes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(spider::scheduler::Error::Unavailable(
                "transient lease refresh".to_string(),
            ));
        }
        if self.block_refresh {
            std::future::pending::<()>().await;
        }
        self.inner.refresh_lease(payload).await
    }

    async fn success(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.completions.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.completion_delay).await;
        self.inner.success(payload).await
    }

    async fn failure(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.completions.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.completion_delay).await;
        self.inner.failure(payload).await
    }
}

impl Init for LifecycleScheduler {
    fn initializes_run(&self) -> bool {
        self.inner.initializes_run()
    }

    async fn init(
        &self,
        trace_id: String,
        snapshot: spider::trace::Snapshot,
        requests: Vec<net::Request>,
    ) -> Result<(), spider::scheduler::Error> {
        self.inner.init(trace_id, snapshot, requests).await
    }
}

struct FailingCloseScheduler {
    inner: spider::Memory,
    calls: Arc<Mutex<Vec<&'static str>>>,
}

struct FlakyScheduler {
    inner: spider::Memory,
    fail_next: AtomicBool,
    fail_after_commit: AtomicBool,
    claim_calls: AtomicUsize,
    delay_second_claim: bool,
}

impl Scheduler for FlakyScheduler {
    fn lease(&self) -> Option<spider::scheduler::Lease> {
        self.inner.lease()
    }

    async fn open(&self) -> Result<(), spider::scheduler::Error> {
        self.inner.open().await
    }

    async fn close(&self) -> Result<(), spider::scheduler::Error> {
        self.inner.close().await
    }

    async fn push(&self, payload: payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.push(payload).await
    }

    async fn push_items(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.push_items(payload).await
    }

    async fn trace(
        &self,
        trace_id: &str,
    ) -> Result<Option<spider::trace::Snapshot>, spider::scheduler::Error> {
        self.inner.trace(trace_id).await
    }

    async fn next_requests(
        &self,
        limit: usize,
    ) -> Result<Vec<net::Request>, spider::scheduler::Error> {
        let call = self.claim_calls.fetch_add(1, Ordering::SeqCst);
        if self.delay_second_claim && call == 1 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        self.inner.next_requests(limit).await
    }

    async fn has_pending_requests(&self) -> Result<bool, spider::scheduler::Error> {
        self.inner.has_pending_requests().await
    }

    async fn ack(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.ack(payload).await
    }

    async fn release(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.release(payload).await
    }

    async fn refresh_lease(
        &self,
        payload: &payload::Payload,
    ) -> Result<(), spider::scheduler::Error> {
        self.inner.refresh_lease(payload).await
    }

    async fn success(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(spider::scheduler::Error::Unavailable(
                "success failed".to_string(),
            ));
        }
        self.inner.success(payload).await?;
        if self.fail_after_commit.swap(false, Ordering::SeqCst) {
            return Err(spider::scheduler::Error::Message(
                "success response failed after commit".to_string(),
            ));
        }
        Ok(())
    }

    async fn failure(&self, _payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        Err(spider::scheduler::Error::Message(
            "failure failed".to_string(),
        ))
    }
}

impl Init for FlakyScheduler {
    fn initializes_run(&self) -> bool {
        self.inner.initializes_run()
    }

    async fn init(
        &self,
        trace_id: String,
        snapshot: spider::trace::Snapshot,
        requests: Vec<net::Request>,
    ) -> Result<(), spider::scheduler::Error> {
        self.inner.init(trace_id, snapshot, requests).await
    }
}

type RequestRecords = Arc<Mutex<Vec<(PayloadRecord, Vec<net::Request>)>>>;

struct RecordingScheduler {
    inner: spider::Memory,
    requests: RequestRecords,
    records: Arc<Mutex<Vec<PayloadRecord>>>,
    items: Arc<Mutex<Vec<PayloadRecord>>>,
    reject_emitted: bool,
    reject_items: bool,
    claim_sync: Option<Arc<ClaimSync>>,
}

impl RecordingScheduler {
    fn new(
        requests: RequestRecords,
        records: Arc<Mutex<Vec<PayloadRecord>>>,
        items: Arc<Mutex<Vec<PayloadRecord>>>,
    ) -> Self {
        Self {
            inner: spider::Memory::new("worker-1"),
            requests,
            records,
            items,
            reject_emitted: false,
            reject_items: false,
            claim_sync: None,
        }
    }

    fn reject_emitted(mut self) -> Self {
        self.reject_emitted = true;
        self
    }

    fn reject_items(mut self) -> Self {
        self.reject_items = true;
        self
    }

    fn with_stale_claim(mut self, sync: Arc<ClaimSync>) -> Self {
        self.claim_sync = Some(sync);
        self
    }
}

impl Scheduler for RecordingScheduler {
    fn lease(&self) -> Option<spider::scheduler::Lease> {
        self.inner.lease()
    }

    async fn open(&self) -> Result<(), spider::scheduler::Error> {
        self.inner.open().await
    }

    async fn close(&self) -> Result<(), spider::scheduler::Error> {
        self.inner.close().await
    }

    async fn push(&self, payload: payload::Payload) -> Result<(), spider::scheduler::Error> {
        let emitted = payload
            .requests
            .iter()
            .any(|request| request.url.ends_with("/emitted"));
        let reject = self.reject_emitted
            && payload
                .requests
                .iter()
                .any(|request| request.url.ends_with("/emitted"));
        self.requests.lock().unwrap().push((
            PayloadRecord::from_payload(&payload),
            payload.requests.clone(),
        ));
        if reject {
            return Err(spider::scheduler::Error::Message(
                "emitted request push".to_string(),
            ));
        }

        let result = self.inner.push(payload).await;
        if result.is_ok()
            && emitted
            && let Some(sync) = &self.claim_sync
        {
            sync.pushed.add_permits(1);
        }
        result
    }

    async fn push_items(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.items
            .lock()
            .unwrap()
            .push(PayloadRecord::from_payload(payload));
        if self.reject_items {
            return Err(spider::scheduler::Error::Message("item push".to_string()));
        }
        self.inner.push_items(payload).await
    }

    async fn trace(
        &self,
        trace_id: &str,
    ) -> Result<Option<spider::trace::Snapshot>, spider::scheduler::Error> {
        self.inner.trace(trace_id).await
    }

    async fn next_requests(
        &self,
        limit: usize,
    ) -> Result<Vec<net::Request>, spider::scheduler::Error> {
        self.inner.next_requests(limit).await
    }

    async fn has_pending_requests(&self) -> Result<bool, spider::scheduler::Error> {
        let pending = self.inner.has_pending_requests().await?;
        if !pending
            && let Some(sync) = &self.claim_sync
            && sync.armed.swap(false, Ordering::SeqCst)
        {
            sync.empty.add_permits(1);
            ClaimSync::acquire(&sync.pushed).await;
            ClaimSync::acquire(&sync.producer_done).await;
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        Ok(pending)
    }

    async fn ack(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.ack(payload).await
    }

    async fn release(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.release(payload).await
    }

    async fn refresh_lease(
        &self,
        payload: &payload::Payload,
    ) -> Result<(), spider::scheduler::Error> {
        self.inner.refresh_lease(payload).await
    }

    async fn success(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.records
            .lock()
            .unwrap()
            .push(PayloadRecord::from_payload(payload));
        self.inner.success(payload).await
    }

    async fn failure(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.records
            .lock()
            .unwrap()
            .push(PayloadRecord::from_payload(payload));
        self.inner.failure(payload).await
    }
}

impl Init for RecordingScheduler {
    fn initializes_run(&self) -> bool {
        self.inner.initializes_run()
    }

    async fn init(
        &self,
        trace_id: String,
        snapshot: spider::trace::Snapshot,
        requests: Vec<net::Request>,
    ) -> Result<(), spider::scheduler::Error> {
        self.inner.init(trace_id, snapshot, requests).await
    }
}

impl Scheduler for FailingCloseScheduler {
    fn lease(&self) -> Option<spider::scheduler::Lease> {
        self.inner.lease()
    }

    async fn open(&self) -> Result<(), spider::scheduler::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), spider::scheduler::Error> {
        self.calls.lock().unwrap().push("scheduler.close");
        Err(spider::scheduler::Error::Message(
            "scheduler close".to_string(),
        ))
    }

    async fn push(&self, payload: payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.push(payload).await
    }

    async fn push_items(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.push_items(payload).await
    }

    async fn trace(
        &self,
        trace_id: &str,
    ) -> Result<Option<spider::trace::Snapshot>, spider::scheduler::Error> {
        self.inner.trace(trace_id).await
    }

    async fn next_requests(
        &self,
        limit: usize,
    ) -> Result<Vec<net::Request>, spider::scheduler::Error> {
        self.inner.next_requests(limit).await
    }

    async fn has_pending_requests(&self) -> Result<bool, spider::scheduler::Error> {
        self.inner.has_pending_requests().await
    }

    async fn ack(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.ack(payload).await
    }

    async fn release(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.release(payload).await
    }

    async fn refresh_lease(
        &self,
        payload: &payload::Payload,
    ) -> Result<(), spider::scheduler::Error> {
        self.inner.refresh_lease(payload).await
    }

    async fn success(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.success(payload).await
    }

    async fn failure(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.failure(payload).await
    }
}

impl Init for FailingCloseScheduler {
    fn initializes_run(&self) -> bool {
        self.inner.initializes_run()
    }

    async fn init(
        &self,
        trace_id: String,
        snapshot: spider::trace::Snapshot,
        requests: Vec<net::Request>,
    ) -> Result<(), spider::scheduler::Error> {
        self.inner.init(trace_id, snapshot, requests).await
    }
}

#[macros::spider]
struct TestSpider;

#[macros::spider]
impl TestSpider {
    fn name(&self) -> &str {
        "test"
    }

    async fn index(&self, response: net::Response) -> Result<(), spider::Error> {
        if !response.url.contains("/ok") {
            let next = net::Request::follow("https://example.com/detail")
                .map_err(|error| spider::Error::Message(error.to_string()))?
                .node(Self::detail);
            self.tx.request(vec![next]).await?;
        }

        self.tx.item(vec![TestItem::new()]).await?;
        Ok(())
    }

    async fn detail(&self, _response: net::Response) -> Result<(), spider::Error> {
        self.tx.item(vec![TestItem::new()]).await?;
        Ok(())
    }
}

#[macros::spider]
struct StartSpider;

#[macros::spider]
impl StartSpider {
    fn name(&self) -> &str {
        "start"
    }

    async fn start_urls(&self) -> Vec<String> {
        vec!["https://example.com/ok".to_string()]
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        self.tx.item(vec![TestItem::new()]).await?;
        Ok(())
    }
}

#[macros::spider]
struct EmptySpider;

#[macros::spider]
impl EmptySpider {
    fn name(&self) -> &str {
        "empty"
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        Ok(())
    }
}

#[macros::spider]
struct MiddlewareSpider;

#[macros::spider]
impl MiddlewareSpider {
    fn name(&self) -> &str {
        "middleware"
    }

    async fn start(&self) -> Result<(), spider::Error> {
        let mut request = net::Request::follow("https://example.com/middleware")
            .map_err(|error| spider::Error::Message(error.to_string()))?;
        request.middlewares = vec![lifecycle_spec("before_scheduler")];
        request.middlewares.push(lifecycle_spec("before_download"));
        request.middlewares.push(lifecycle_spec("after_download"));
        request.middlewares.push(lifecycle_spec("before_parse"));
        self.tx.request(vec![request]).await
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        let item = TestItem::new().with_middlewares(vec![lifecycle_spec("before_item")]);
        self.tx.item(vec![item]).await
    }
}

fn lifecycle_spec(hook: &str) -> Spec {
    Spec {
        hook: Some(hook.to_string()),
        name: "lifecycle".to_string(),
        ..Spec::default()
    }
}

#[macros::spider]
struct StartItemSpider;

#[macros::spider]
impl StartItemSpider {
    fn name(&self) -> &str {
        "start_item"
    }

    async fn start(&self) -> Result<(), spider::Error> {
        self.tx.item(vec![TestItem::new()]).await
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        Ok(())
    }
}

#[macros::spider]
struct StartFailSpider;

#[macros::spider]
impl StartFailSpider {
    fn name(&self) -> &str {
        "start_fail"
    }

    async fn start(&self) -> Result<(), spider::Error> {
        Err(spider::Error::Message("start".to_string()))
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        Ok(())
    }
}

#[macros::spider]
struct PanicStartSpider;

#[macros::spider]
impl PanicStartSpider {
    fn name(&self) -> &str {
        "panic-start"
    }

    async fn start(&self) -> Result<(), spider::Error> {
        panic!("startup task panic")
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        Ok(())
    }
}

#[macros::spider]
struct StartEmitFailSpider {
    calls: Arc<AtomicUsize>,
}

#[macros::spider]
impl StartEmitFailSpider {
    fn name(&self) -> &str {
        "start-emit-fail"
    }

    async fn start(&self) -> Result<(), spider::Error> {
        let request = net::Request::follow("https://example.com/accepted")
            .map_err(|error| spider::Error::Message(error.to_string()))?;
        self.tx.request(vec![request]).await?;
        Err(spider::Error::Message("start after output".to_string()))
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct Args {
    start_url: String,
}

#[macros::spider]
struct ArgsSpider {
    args: Args,
}

#[macros::spider]
struct EventSpider;

#[macros::spider]
impl EventSpider {
    fn name(&self) -> &str {
        "events"
    }

    async fn index(&self, response: net::Response) -> Result<(), spider::Error> {
        if response.url.ends_with("/emit") || response.url.ends_with("/push-fail") {
            let emitted = net::Request::follow("https://example.com/emitted")
                .map_err(|error| spider::Error::Message(error.to_string()))?;
            self.tx.request(vec![emitted]).await?;
        }
        if response.url.ends_with("/emit") || response.url.ends_with("/item-fail") {
            let mut item = TestItem::new();
            if response.url.ends_with("/item-fail") {
                item = item.with_middlewares(vec![lifecycle_spec("error_item")]);
            }
            self.tx.item(vec![item]).await?;
        }

        Ok(())
    }
}

#[macros::spider]
struct LateEventSpider;

#[macros::spider]
impl LateEventSpider {
    fn name(&self) -> &str {
        "late-event"
    }

    async fn start(&self) -> Result<(), spider::Error> {
        let request = net::Request::follow("https://example.com/source")
            .map_err(|error| spider::Error::Message(error.to_string()))?;
        self.tx.request(vec![request]).await?;
        Ok(())
    }

    async fn index(&self, response: net::Response) -> Result<(), spider::Error> {
        if response.url.ends_with("/source") {
            let tx = self.tx.clone();
            let _task = tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                let emitted = net::Request::follow("https://example.com/emitted").unwrap();
                tx.request(vec![emitted]).await.unwrap();
                tx.item(vec![TestItem::new()]).await.unwrap();
            });
        }
        Ok(())
    }
}

struct ClaimSync {
    armed: AtomicBool,
    empty: tokio::sync::Semaphore,
    pushed: tokio::sync::Semaphore,
    producer_done: tokio::sync::Semaphore,
}

impl ClaimSync {
    fn new() -> Self {
        Self {
            armed: AtomicBool::new(true),
            empty: tokio::sync::Semaphore::new(0),
            pushed: tokio::sync::Semaphore::new(0),
            producer_done: tokio::sync::Semaphore::new(0),
        }
    }

    async fn acquire(semaphore: &tokio::sync::Semaphore) {
        semaphore.acquire().await.unwrap().forget();
    }
}

#[macros::spider]
struct StaleClaimSpider {
    sync: Arc<ClaimSync>,
}

#[macros::spider]
impl StaleClaimSpider {
    fn name(&self) -> &str {
        "stale-claim"
    }

    async fn start(&self) -> Result<(), spider::Error> {
        let request = net::Request::follow("https://example.com/source")
            .map_err(|error| spider::Error::Message(error.to_string()))?;
        self.tx.request(vec![request]).await
    }

    async fn index(&self, response: net::Response) -> Result<(), spider::Error> {
        if response.url.ends_with("/source") {
            let tx = self.tx.clone();
            let sync = self.sync.clone();
            tokio::spawn(async move {
                ClaimSync::acquire(&sync.empty).await;
                let request = net::Request::follow("https://example.com/emitted").unwrap();
                tx.request(vec![request]).await.unwrap();
                sync.producer_done.add_permits(1);
            });
        }
        Ok(())
    }
}

#[macros::spider]
impl ArgsSpider {
    fn name(&self) -> &str {
        "args"
    }

    async fn start_urls(&self) -> Vec<String> {
        vec![self.args.start_url.clone()]
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        self.tx.item(vec![TestItem::new()]).await?;
        Ok(())
    }
}

#[derive(serde::Serialize)]
struct TestItem {
    #[serde(skip)]
    state: spider::item::State,
    #[serde(skip)]
    middlewares: Vec<Spec>,
}

impl TestItem {
    fn new() -> Self {
        Self {
            state: spider::item::State::default(),
            middlewares: Vec::new(),
        }
    }

    fn with_middlewares(mut self, middlewares: Vec<Spec>) -> Self {
        self.middlewares = middlewares;
        self
    }
}

impl Item for TestItem {
    fn from_values(_values: spider::item::Values) -> Result<Self, spider::item::Error> {
        Ok(Self::new())
    }

    fn state(&self) -> &spider::item::State {
        &self.state
    }

    fn state_mut(&mut self) -> &mut spider::item::State {
        &mut self.state
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn middlewares(&self) -> &[spider::middleware::Spec] {
        &self.middlewares
    }
}

#[tokio::test]
async fn engine_open_opens_scheduler_then_downloader() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let spider = TestSpider::new();
    let scheduler = LifecycleScheduler::new(calls.clone());
    let downloader = LifecycleDownload {
        calls: calls.clone(),
    };

    let engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(downloader)
        .with_spider(spider)
        .build();

    engine.open().await.unwrap();

    assert_eq!(
        calls.lock().unwrap().as_slice(),
        ["scheduler.open", "downloader.open"]
    );
}

#[tokio::test]
async fn engine_close_closes_downloader_then_scheduler() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let spider = TestSpider::new();
    let scheduler = LifecycleScheduler::new(calls.clone());
    let downloader = LifecycleDownload {
        calls: calls.clone(),
    };

    let engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(downloader)
        .with_spider(spider)
        .build();

    engine.close().await.unwrap();

    assert_eq!(
        calls.lock().unwrap().as_slice(),
        ["downloader.close", "scheduler.close"]
    );
}

#[tokio::test]
async fn engine_close_attempts_every_component_when_cleanup_fails() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let scheduler = FailingCloseScheduler {
        inner: spider::Memory::new("worker-1"),
        calls: calls.clone(),
    };
    let downloader = FailingCloseDownload {
        calls: calls.clone(),
    };
    let engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(downloader)
        .with_spider(EmptySpider::new())
        .build();

    engine.close().await.unwrap_err();
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        ["downloader.close", "scheduler.close"]
    );
}

#[tokio::test]
async fn engine_start_opens_runs_and_closes_resources() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let spider = StartSpider::new();
    let scheduler = LifecycleScheduler::new(calls.clone());
    let downloader = LifecycleDownload {
        calls: calls.clone(),
    };

    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(downloader)
        .with_spider(spider)
        .build();

    engine.start().await.unwrap();

    assert_eq!(
        calls.lock().unwrap().as_slice(),
        [
            "scheduler.open",
            "downloader.open",
            "downloader.fetch",
            "downloader.close",
            "scheduler.close"
        ]
    );
}

#[tokio::test]
async fn engine_runs_request_response_and_item_middleware_hooks_in_order() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let middleware = LifecycleMiddleware {
        calls: calls.clone(),
    };
    let mut engine = engine::Builder::new()
        .with_scheduler(spider::Memory::new("worker-1"))
        .with_downloader(TestDownload)
        .with_spider(MiddlewareSpider::new())
        .with_middleware("lifecycle", middleware)
        .build();

    engine.start().await.unwrap();

    assert_eq!(
        calls.lock().unwrap().as_slice(),
        [
            "before_scheduler",
            "before_download",
            "after_download",
            "before_parse",
            "before_item"
        ]
    );
}

#[tokio::test]
async fn engine_start_closes_resources_when_spider_start_fails() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let spider = StartFailSpider::new();
    let scheduler = LifecycleScheduler::new(calls.clone());
    let downloader = LifecycleDownload {
        calls: calls.clone(),
    };

    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(downloader)
        .with_spider(spider)
        .build();

    let error = engine.start().await.unwrap_err();

    assert!(error.to_string().contains("start"));
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        [
            "scheduler.open",
            "downloader.open",
            "downloader.close",
            "scheduler.close"
        ]
    );
}

#[tokio::test]
async fn engine_reports_start_task_panic_without_hanging() {
    let mut engine = engine::Builder::new()
        .with_scheduler(spider::Memory::new("worker-1"))
        .with_downloader(TestDownload)
        .with_spider(PanicStartSpider::new())
        .build();

    let error = tokio::time::timeout(std::time::Duration::from_secs(1), engine.start())
        .await
        .expect("engine must clear a panicked start task")
        .unwrap_err();

    assert!(error.to_string().contains("startup task panic"));
}

#[tokio::test]
async fn spider_start_error_drains_work_that_was_already_accepted() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut engine = engine::Builder::new()
        .with_scheduler(spider::Memory::new("worker-1"))
        .with_downloader(TestDownload)
        .with_spider(StartEmitFailSpider::new(calls.clone()))
        .build();

    let error = engine.start().await.unwrap_err();

    assert!(error.to_string().contains("start after output"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.scheduler().done_len(), 1);
    assert_eq!(engine.scheduler().processing_len(), 0);
}

#[tokio::test]
async fn engine_start_closes_resources_when_event_push_fails() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let spider = StartSpider::new();
    let scheduler = LifecycleScheduler::fail_push(calls.clone());
    let downloader = LifecycleDownload {
        calls: calls.clone(),
    };

    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(downloader)
        .with_spider(spider)
        .build();

    let error = engine.start().await.unwrap_err();

    assert!(error.to_string().contains("push"));
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        [
            "scheduler.open",
            "downloader.open",
            "scheduler.push",
            "downloader.close",
            "scheduler.close"
        ]
    );
}

#[tokio::test]
async fn engine_start_closes_resources_when_downloader_open_fails() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let spider = EmptySpider::new();
    let scheduler = LifecycleScheduler::new(calls.clone());
    let downloader = FailingOpenDownload {
        calls: calls.clone(),
    };

    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(downloader)
        .with_spider(spider)
        .build();

    let error = engine.start().await.unwrap_err();

    assert!(error.to_string().contains("open"));
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        ["scheduler.open", "downloader.open", "scheduler.close"]
    );
}

#[tokio::test]
async fn executor_completes_current_request() {
    let scheduler = spider::Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();

    scheduler
        .push(payload::Payload::for_request(&request, "worker-1").requests(vec![request]))
        .await
        .unwrap();

    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(TestDownload)
        .with_spider(EmptySpider::new())
        .build();

    engine.start().await.unwrap();

    let scheduler = engine.scheduler();
    assert_eq!(scheduler.done_len(), 1);
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.processing_len(), 0);
}

#[tokio::test]
async fn engine_starts_until_scheduler_is_empty() {
    let spider = StartSpider::new();
    let scheduler = spider::Memory::new("worker-1");

    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(TestDownload)
        .with_spider(spider)
        .build();

    engine.start().await.unwrap();

    let scheduler = engine.scheduler();
    assert_eq!(scheduler.done_len(), 1);
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.processing_len(), 0);
}

#[tokio::test]
async fn engine_waits_for_output_sent_after_the_request_task_finishes() {
    let pushed_requests = Arc::new(Mutex::new(Vec::new()));
    let records = Arc::new(Mutex::new(Vec::new()));
    let pushed_items = Arc::new(Mutex::new(Vec::new()));
    let scheduler = RecordingScheduler::new(pushed_requests.clone(), records, pushed_items.clone());
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(TestDownload)
        .with_spider(LateEventSpider::new())
        .build()
        .with_concurrency(1);

    tokio::time::timeout(std::time::Duration::from_secs(2), engine.start())
        .await
        .expect("engine must drain late output")
        .unwrap();

    assert_eq!(engine.scheduler().inner.done_len(), 2);
    assert_eq!(engine.scheduler().inner.queued_len(), 0);
    assert_eq!(engine.scheduler().inner.processing_len(), 0);

    let pushed_requests = pushed_requests.lock().unwrap();
    let (_, emitted) = pushed_requests
        .iter()
        .find(|(_, requests)| {
            requests
                .iter()
                .any(|request| request.url.ends_with("/emitted"))
        })
        .unwrap();
    assert_eq!(emitted[0].task_id, "late-event");
    assert!(emitted[0].trace_id.starts_with("trace_late-event_"));
    let trace_id = emitted[0].trace_id.clone();
    let items = pushed_items.lock().unwrap();
    assert_eq!(items[0].task_id, "late-event");
    assert_eq!(items[0].trace_id, trace_id);
    drop(items);
    let stats = engine.scheduler().inner.trace_stats(&trace_id);
    assert!(!stats.contains_key("items"));
}

#[tokio::test]
async fn stale_empty_claim_is_rechecked_after_detached_output() {
    let sync = Arc::new(ClaimSync::new());
    let scheduler = RecordingScheduler::new(
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(Mutex::new(Vec::new())),
    )
    .with_stale_claim(sync.clone());
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(TestDownload)
        .with_spider(StaleClaimSpider::new(sync))
        .build()
        .with_concurrency(2);

    tokio::time::timeout(std::time::Duration::from_secs(2), engine.start())
        .await
        .expect("engine must recheck an empty claim after detached output")
        .unwrap();

    assert_eq!(engine.scheduler().inner.done_len(), 2);
    assert_eq!(engine.scheduler().inner.queued_len(), 0);
    assert_eq!(engine.scheduler().inner.processing_len(), 0);
}

#[tokio::test]
async fn detached_rules_output_keeps_only_the_trace_identity() {
    let config = spider::config::Config::from_yaml(
        r#"
spider:
  name: rules-late-event
  start: [{node: index, url: https://example.com/source}]
graph:
  nodes:
    index: {}
  edges: []
"#,
    )
    .unwrap();
    let pushed_requests = Arc::new(Mutex::new(Vec::new()));
    let records = Arc::new(Mutex::new(Vec::new()));
    let pushed_items = Arc::new(Mutex::new(Vec::new()));
    let scheduler = RecordingScheduler::new(pushed_requests.clone(), records, pushed_items.clone());
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_rules(config)
        .with_spider(LateEventSpider::new())
        .with_downloader(TestDownload)
        .build()
        .with_concurrency(1);

    tokio::time::timeout(std::time::Duration::from_secs(2), engine.start())
        .await
        .expect("engine must drain detached Rules output")
        .unwrap();

    let pushed_requests = pushed_requests.lock().unwrap();
    let (_, emitted) = pushed_requests
        .iter()
        .find(|(_, requests)| {
            requests
                .iter()
                .any(|request| request.url.ends_with("/emitted"))
        })
        .unwrap();
    assert_eq!(emitted[0].task_id, "rules-late-event");
    assert!(!emitted[0].trace_id.is_empty());
    let trace_id = emitted[0].trace_id.clone();
    drop(pushed_requests);

    let items = pushed_items.lock().unwrap();
    assert_eq!(items[0].task_id, "rules-late-event");
    assert_eq!(items[0].trace_id, trace_id);
    assert!(items[0].id.is_empty());
    assert!(items[0].worker_id.is_empty());
    assert!(items[0].node.is_empty());
}

#[tokio::test]
async fn engine_sends_start_items_to_scheduler() {
    let spider = StartItemSpider::new();
    let scheduler = spider::Memory::new("worker-1");

    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(TestDownload)
        .with_spider(spider)
        .build();

    engine.start().await.unwrap();

    let scheduler = engine.scheduler();
    assert_eq!(scheduler.done_len(), 0);
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.processing_len(), 0);
}

#[tokio::test]
async fn builder_uses_memory_scheduler_and_local_item_by_default() {
    let mut engine = engine::Builder::new()
        .with_spider(EmptySpider::new())
        .build();

    engine.start().await.unwrap();

    let scheduler = engine.scheduler();
    assert_eq!(scheduler.done_len(), 0);
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.processing_len(), 0);
    assert_eq!(engine.scheduler().dir(), Some(std::path::Path::new(".")));
}

#[tokio::test]
async fn spider_macro_keeps_user_business_fields_and_injects_tx() {
    let spider = ArgsSpider::new(Args {
        start_url: "https://example.com/ok".to_string(),
    });
    let scheduler = spider::Memory::new("worker-1");

    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(TestDownload)
        .with_spider(spider)
        .build();

    engine.start().await.unwrap();

    let scheduler = engine.scheduler();
    assert_eq!(scheduler.done_len(), 1);
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.processing_len(), 0);
}

#[tokio::test]
async fn engine_records_failed_request_and_continues_other_requests() {
    let spider = TestSpider::new();
    let scheduler = spider::Memory::new("worker-1");
    let failed = net::Request::follow("https://example.com/fail").unwrap();
    let ok = net::Request::follow("https://example.com/ok").unwrap();

    scheduler
        .push(payload::Payload::for_request(&failed, "worker-1").requests(vec![failed, ok]))
        .await
        .unwrap();

    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(TestDownload)
        .with_spider(spider)
        .build()
        .with_concurrency(2);

    engine.start().await.unwrap();

    let scheduler = engine.scheduler();
    assert_eq!(scheduler.done_len(), 1);
    assert_eq!(scheduler.failed_len(), 1);
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.processing_len(), 0);
}

#[tokio::test]
async fn engine_concurrency_controls_concurrent_requests() {
    let spider = EmptySpider::new();
    let scheduler = spider::Memory::new("worker-1");
    let requests = vec![
        net::Request::follow("https://example.com/1").unwrap(),
        net::Request::follow("https://example.com/2").unwrap(),
    ];

    scheduler
        .push(payload::Payload::new().requests(requests))
        .await
        .unwrap();

    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let downloader = SlowDownload {
        active: active.clone(),
        max_active: max_active.clone(),
    };

    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(downloader)
        .with_spider(spider)
        .build()
        .with_concurrency(2);

    engine.start().await.unwrap();

    let scheduler = engine.scheduler();
    assert_eq!(scheduler.done_len(), 2);
    assert_eq!(max_active.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn engine_refreshes_the_lease_while_a_request_is_running() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let scheduler = LifecycleScheduler::new(calls);
    scheduler
        .push(payload::Payload::new().requests(vec![
            net::Request::follow("https://example.com/slow").unwrap(),
        ]))
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(SlowDownload {
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        })
        .with_spider(EmptySpider::new())
        .build()
        .with_concurrency(1);

    engine.start().await.unwrap();

    assert!(engine.scheduler().lease_refreshes.load(Ordering::SeqCst) > 0);
    assert_eq!(engine.scheduler().inner.done_len(), 1);
}

#[tokio::test]
async fn slow_lease_refresh_does_not_pause_request_execution() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let scheduler = LifecycleScheduler::new(calls).block_refresh();
    scheduler
        .push(payload::Payload::new().requests(vec![
            net::Request::follow("https://example.com/slow").unwrap(),
        ]))
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(SlowDownload {
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        })
        .with_spider(EmptySpider::new())
        .build()
        .with_concurrency(1);

    tokio::time::timeout(std::time::Duration::from_millis(500), engine.start())
        .await
        .expect("request execution must continue while lease refresh is pending")
        .unwrap();

    assert!(engine.scheduler().lease_refreshes.load(Ordering::SeqCst) > 0);
    assert_eq!(engine.scheduler().inner.done_len(), 1);
}

#[tokio::test]
async fn transient_refresh_errors_are_retried_before_the_lease_expires() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let scheduler = LifecycleScheduler::new(calls).transient_refreshes(2);
    scheduler
        .push(payload::Payload::new().requests(vec![
            net::Request::follow("https://example.com/slow").unwrap(),
        ]))
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(SlowDownload {
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        })
        .with_spider(EmptySpider::new())
        .build()
        .with_concurrency(1);

    engine.start().await.unwrap();

    assert!(engine.scheduler().lease_refreshes.load(Ordering::SeqCst) >= 3);
    assert_eq!(engine.scheduler().completions.load(Ordering::SeqCst), 1);
    assert_eq!(engine.scheduler().inner.done_len(), 1);
}

#[tokio::test]
async fn lease_is_refreshed_until_completion_finishes() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let scheduler =
        LifecycleScheduler::new(calls).delay_completion(std::time::Duration::from_millis(50));
    scheduler
        .push(payload::Payload::new().requests(vec![
            net::Request::follow("https://example.com/completion").unwrap(),
        ]))
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(TestDownload)
        .with_spider(EmptySpider::new())
        .build()
        .with_concurrency(1);

    engine.start().await.unwrap();

    assert!(engine.scheduler().lease_refreshes.load(Ordering::SeqCst) > 1);
    assert_eq!(engine.scheduler().inner.done_len(), 1);
}

#[tokio::test]
async fn engine_does_not_settle_after_lease_refresh_is_rejected() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let scheduler = LifecycleScheduler::new(calls).reject_refresh();
    scheduler
        .push(payload::Payload::new().requests(vec![
            net::Request::follow("https://example.com/slow").unwrap(),
        ]))
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(SlowDownload {
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        })
        .with_spider(EmptySpider::new())
        .build()
        .with_concurrency(1);

    let error = engine.start().await.unwrap_err();

    assert!(error.to_string().contains("not leased by worker"));
    assert_eq!(engine.scheduler().lease_refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(engine.scheduler().completions.load(Ordering::SeqCst), 0);
    assert_eq!(engine.scheduler().inner.processing_len(), 0);
    assert_eq!(engine.scheduler().inner.failed_len(), 1);
}

#[tokio::test]
async fn tx_events_keep_current_request_context_and_inherit_trace_fields() {
    let pushed_requests = Arc::new(Mutex::new(Vec::new()));
    let records = Arc::new(Mutex::new(Vec::new()));
    let pushed_items = Arc::new(Mutex::new(Vec::new()));
    let scheduler = RecordingScheduler::new(
        pushed_requests.clone(),
        records.clone(),
        pushed_items.clone(),
    );
    scheduler
        .inner
        .init(
            "trace-7".to_string(),
            spider::trace::Snapshot::code("task-7"),
            Vec::new(),
        )
        .await
        .unwrap();
    let mut request = net::Request::follow("https://example.com/emit").unwrap();
    request.id = "source-7".to_string();
    request.task_id = "task-7".to_string();
    request.trace_id = "trace-7".to_string();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();

    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(TestDownload)
        .with_spider(EventSpider::new())
        .build()
        .with_concurrency(2);

    engine.start().await.unwrap();

    let pushed_requests = pushed_requests.lock().unwrap();
    let (payload, requests) = pushed_requests
        .iter()
        .find(|(_, requests)| {
            requests
                .iter()
                .any(|request| request.url.ends_with("/emitted"))
        })
        .unwrap();
    let payload = payload.clone();
    assert_eq!(
        payload,
        PayloadRecord {
            id: "source-7".to_string(),
            task_id: "task-7".to_string(),
            trace_id: "trace-7".to_string(),
            version: 1,
            worker_id: "worker-1".to_string(),
            node: "index".to_string(),
            state: payload::State::Done,
            has_start_time: false,
            has_end_time: false,
        }
    );
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].task_id, "task-7");
    assert_eq!(requests[0].trace_id, "trace-7");
    drop(pushed_requests);

    assert_eq!(pushed_items.lock().unwrap().as_slice(), [payload]);

    let records = records.lock().unwrap();
    let current = records
        .iter()
        .find(|payload| payload.id == "source-7")
        .unwrap();
    assert_eq!(current.task_id, "task-7");
    assert_eq!(current.trace_id, "trace-7");
    assert_eq!(current.version, 1);
    assert_eq!(current.worker_id, "worker-1");
    assert_eq!(current.node, "index");
    assert_eq!(current.state, payload::State::Done);
    assert!(current.has_start_time);
    assert!(current.has_end_time);
}

#[tokio::test]
async fn rejected_requests_only_fail_current_request_and_keep_consuming() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let records = Arc::new(Mutex::new(Vec::new()));
    let scheduler =
        RecordingScheduler::new(requests, records.clone(), Arc::new(Mutex::new(Vec::new())))
            .reject_emitted();
    let failed = net::Request::follow("https://example.com/push-fail").unwrap();
    let failed_id = failed.id.clone();
    let ok = net::Request::follow("https://example.com/ok").unwrap();
    let ok_id = ok.id.clone();
    scheduler
        .push(payload::Payload::new().requests(vec![failed, ok]))
        .await
        .unwrap();

    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(TestDownload)
        .with_spider(EventSpider::new())
        .build()
        .with_concurrency(2);

    engine.start().await.unwrap();

    let records = records.lock().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(
        records
            .iter()
            .find(|payload| payload.id == failed_id)
            .unwrap()
            .state,
        payload::State::Failed
    );
    assert_eq!(
        records
            .iter()
            .find(|payload| payload.id == ok_id)
            .unwrap()
            .state,
        payload::State::Done
    );
}

#[tokio::test]
async fn rejected_items_only_fail_current_request_and_keep_consuming() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let records = Arc::new(Mutex::new(Vec::new()));
    let pushed_items = Arc::new(Mutex::new(Vec::new()));
    let scheduler =
        RecordingScheduler::new(requests, records.clone(), pushed_items.clone()).reject_items();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut failed = net::Request::follow("https://example.com/item-fail").unwrap();
    failed.middlewares = vec![lifecycle_spec("error_parse")];
    let failed_id = failed.id.clone();
    let ok = net::Request::follow("https://example.com/ok").unwrap();
    let ok_id = ok.id.clone();
    scheduler
        .push(payload::Payload::new().requests(vec![failed, ok]))
        .await
        .unwrap();

    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(TestDownload)
        .with_spider(EventSpider::new())
        .with_middleware(
            "lifecycle",
            LifecycleMiddleware {
                calls: calls.clone(),
            },
        )
        .build()
        .with_concurrency(2);

    engine.start().await.unwrap();

    assert_eq!(pushed_items.lock().unwrap().len(), 1);
    assert_eq!(calls.lock().unwrap().as_slice(), ["error_item"]);
    let records = records.lock().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(
        records
            .iter()
            .find(|payload| payload.id == failed_id)
            .unwrap()
            .state,
        payload::State::Failed
    );
    assert_eq!(
        records
            .iter()
            .find(|payload| payload.id == ok_id)
            .unwrap()
            .state,
        payload::State::Done
    );
}

#[tokio::test]
async fn independent_items_run_concurrently() {
    let scheduler = spider::Memory::new("worker-1");
    let requests = vec![
        net::Request::follow("https://example.com/item-fail").unwrap(),
        net::Request::follow("https://example.com/item-fail").unwrap(),
    ];
    scheduler
        .push(payload::Payload::new().requests(requests))
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(TestDownload)
        .with_spider(EventSpider::new())
        .build()
        .with_concurrency(2);

    engine.start().await.unwrap();

    assert_eq!(engine.scheduler().done_len(), 2);
}

#[tokio::test]
async fn engine_retries_transient_success_error_without_reexecution() {
    let scheduler = FlakyScheduler {
        inner: spider::Memory::new("worker-1"),
        fail_next: AtomicBool::new(true),
        fail_after_commit: AtomicBool::new(false),
        claim_calls: AtomicUsize::new(0),
        delay_second_claim: false,
    };
    scheduler
        .push(payload::Payload::new().requests(vec![
            net::Request::follow("https://example.com/one").unwrap(),
            net::Request::follow("https://example.com/two").unwrap(),
        ]))
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(SlowDownload {
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        })
        .with_spider(EmptySpider::new())
        .build()
        .with_concurrency(2);

    let completion = tokio::time::timeout(std::time::Duration::from_secs(1), engine.start())
        .await
        .expect("engine must not wait forever after success fails");

    completion.unwrap();
    assert_eq!(engine.scheduler().inner.done_len(), 2);
    assert_eq!(engine.scheduler().inner.processing_len(), 0);
}

#[tokio::test]
async fn claim_returning_after_a_request_error_still_executes_its_request() {
    let scheduler = FlakyScheduler {
        inner: spider::Memory::new("worker-1"),
        fail_next: AtomicBool::new(false),
        fail_after_commit: AtomicBool::new(true),
        claim_calls: AtomicUsize::new(0),
        delay_second_claim: true,
    };
    scheduler
        .push(payload::Payload::new().requests(vec![
            net::Request::follow("https://example.com/one").unwrap(),
            net::Request::follow("https://example.com/two").unwrap(),
        ]))
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(TestDownload)
        .with_spider(EmptySpider::new())
        .build()
        .with_concurrency(2)
        .with_limit(1);

    let error = tokio::time::timeout(std::time::Duration::from_secs(1), engine.start())
        .await
        .expect("engine must drain the late claim")
        .unwrap_err();

    assert!(error.to_string().contains("after commit"));
    assert_eq!(engine.scheduler().inner.done_len(), 2);
    assert_eq!(engine.scheduler().inner.processing_len(), 0);
    assert_eq!(engine.scheduler().inner.queued_len(), 0);
}
