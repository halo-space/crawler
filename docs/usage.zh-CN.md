# Harbor 使用手册

[项目 README](../README.zh-CN.md) | 简体中文 | [English](usage.md)

本文只介绍 Harbor 的使用方法：如何创建爬虫、如何编写 Rules、如何抓取列表和详情、如何保存 Item。

## 1. 准备项目

Harbor 使用 Rust 2024 edition。开发环境需要 Rust 1.97 或更高版本。

工作区内运行示例：

```bash
# 运行代码模式示例。
cargo run -p examples --bin basic

# 运行 Rules 模式示例。
cargo run -p examples --bin rules
```

自己的应用需要依赖 Harbor、过程宏和 Tokio：

```toml
[dependencies]
# Harbor 公共入口提供 Engine、Request、Response、Item、Rules 和 contrib 扩展。
halo = { package = "halo-harbor-contrib", path = "../contrib" }

# macros 提供 #[macros::harbor] 和 #[derive(macros::Item)]。
macros = { package = "halo-harbor-macros", path = "../macros" }

# Tokio 用于运行异步 Spider。
tokio = { version = "1", features = ["macros", "rt", "fs", "signal"] }

# Item 和 Rules 配置使用 Serde。
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.10"
```

## 2. 代码模式

代码模式适合需要在 Rust 中自由编写解析逻辑的爬虫。

### 2.1 最小代码

```rust
use halo::harbor::{engine, net};

// #[macros::harbor] 为 Spider 注入框架需要的运行能力。
#[macros::harbor]
struct ExampleSpider;

#[macros::harbor]
impl ExampleSpider {
    // 返回当前爬虫的名称。
    fn name(&self) -> &str {
        "example"
    }

    // 返回起始 URL。每个 URL 会生成一条初始 Request。
    async fn start_urls(&self) -> Vec<String> {
        vec!["https://example.com".to_string()]
    }

    // index 是默认的页面处理入口。
    async fn index(&self, response: net::Response) -> Result<(), halo::harbor::Error> {
        // 解析当前页面的 CSS 文档。
        let soup = response.css()?;

        // 读取页面标题；没有标题时使用当前 URL。
        let title = soup
            .find("h1")
            .map_err(|error| halo::harbor::Error::Message(error.to_string()))?
            .map(|node| node.text())
            .unwrap_or_else(|| response.url.clone());

        // 批量提交 Item。这里提交一个 Item，因此仍使用 Vec。
        self.tx.item(vec![ExampleItem::new(title)]).await?;
        Ok(())
    }
}

#[derive(serde::Deserialize, serde::Serialize, macros::Item)]
#[serde(deny_unknown_fields)]
struct ExampleItem {
    // 业务字段会写入最终 JSONL。
    title: String,

    // state 是框架字段，不写入业务数据。
    #[serde(skip)]
    state: halo::harbor::item::State,
}

impl ExampleItem {
    // 创建一条 Item。
    fn new(title: String) -> Self {
        Self {
            title,
            state: halo::harbor::item::State::default(),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 默认使用内存调度器、HTTP Downloader 和本地 JSONL Store。
    let mut engine = engine::Engine::new()
        .with_spider(ExampleSpider::new())
        .build()
        .with_concurrency(8);

    // 启动爬虫。
    engine.start().await?;
    Ok(())
}
```

默认 Item 文件写入：

```text
./data/items/output/<task_id>/<yyyy-mm-dd-HH>.jsonl
```

每行只保存框架 Item ID 和业务数据：

```json
{"id":"...","data":{"title":"示例标题"}}
```

### 2.2 提交下一条 Request

```rust
use halo::harbor::net::Request;

// 在当前页面解析详情链接。
let detail_url = "https://example.com/detail/1";

// Request::follow 要求绝对 URL。
let request = Request::follow(detail_url)?;

// 批量提交下一跳 Request。
self.tx.request(vec![request]).await?;
```

### 2.3 读取 Response

```rust
// CSS 解析返回页面节点。
let soup = response.css()?;

// JSON 解析保留数字、布尔值、对象和数组的原始类型。
let document: serde_json::Value = response.json()?;

// 使用 JSONPath 读取 JSON 字段。
let values = halo::harbor::selector::json::select(&document, "$.data.items[*].id")?;

// 读取响应文本。
let text = response.text()?;
```

## 3. Rules 模式

Rules 模式用 YAML 配置起始请求、节点、字段、绑定、下一跳和 Item Schema。

Rules 顶层只有三个业务块：

```yaml
# 爬虫和起始 Request。
spider: {}

# 页面节点和节点之间的 Request / Item 流转。
graph: {}

# 最终 Item 字段类型和校验规则。
item: {}
```

### 3.1 启动 Rules

```rust
use std::path::PathBuf;

use halo::harbor::{config, engine, net};

#[derive(serde::Deserialize, serde::Serialize, macros::Item)]
#[serde(deny_unknown_fields)]
struct Article {
    // 这些字段由 Rules 的 edge.vals 映射。
    title: String,
    content: String,
    url: String,

    // state 是框架字段，不写入最终 Item。
    #[serde(skip)]
    state: halo::harbor::item::State,
}

#[macros::harbor(item = Article)]
struct ArticleSpider;

#[macros::harbor(item = Article)]
impl ArticleSpider {
    // Rust Spider 名称。
    fn name(&self) -> &str {
        "article"
    }

    // Rules 每条 Request 仍会经过这个入口。
    async fn index(&self, _response: net::Response) -> Result<(), halo::harbor::Error> {
        Ok(())
    }

    // Rules 的 Item edge 可以通过 fn: save 调用这个函数。
    #[item]
    async fn save(&self, item: Article) -> Result<(), halo::harbor::Error> {
        // Item 仍通过统一的 Tx 提交。
        self.tx.item(vec![item]).await
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 读取并校验 YAML Rules。
    let path = PathBuf::from("rules.yaml");
    let rules = config::Config::load(path).await?;

    // Rules 需要关联一个 Rust Spider 类型。
    let mut engine = engine::Engine::new()
        .with_rules(rules)
        .with_spider(ArticleSpider::new())
        .build()
        .with_concurrency(8);

    // 启动 Rules 爬虫。
    engine.start().await?;
    Ok(())
}
```

## 4. Spider 配置

```yaml
spider:
  # Rules 任务名称。
  name: article

  # 可选存储主题。未配置时默认使用 name。
  topic: article

  # 可选配置版本。
  version: "1"

  # 默认 Request 优先级。
  priority: 0

  # 允许抓取的域名。
  allowed_domains:
    - example.com

  # 可选业务时区。
  timezone: Asia/Shanghai

  # 起始 Request。node 必须存在于 graph.nodes。
  start:
    - node: index
      url: "https://example.com"
      method: GET
      timeout: 30000
      vals:
        source: homepage
```

`spider.topic` 与 `spider.name` 同级，是当前 Spider 的统一存储主题。框架会把它写入初始
Request 的 `vals.topic`，后续 Request 自动继承；Item Store 从 Item Payload 的
`Payload.vals.topic` 读取它进行路由。Payload 只携带这个存储路由值，不会复制其他 Request `vals`。
`start` 和 Request edge 不能覆盖这个值。

`start` 中可以配置完整 Request：

```yaml
spider:
  start:
    - node: index
      url: "https://example.com/api/list"
      method: POST
      download_mode: http
      timeout: 30000
      max_body_bytes: 8388608
      headers:
        Accept: application/json
        Content-Type: application/json
      cookies:
        region: cn
      body:
        kind: json
        data:
          page: 1
      vals:
        page: 1
```

## 5. Graph 节点

```yaml
graph:
  nodes:
    index:
      # 当前节点的说明文字，可选。
      label: 首页

      # 当前节点自己的域名限制，可选。
      allowed_domains:
        - example.com

      # 从当前 Response 提取字段。
      fields: {}

      # 根据字段计算派生值。
      binds: {}

      # 当前节点产出的 Item Middleware。
      middlewares: []
```

节点名称由用户决定，例如 `index`、`list`、`detail`。起始 Request 和 Request edge 的 `node` 必须引用已有节点。

## 6. Fields 字段提取

### 6.1 CSS

```yaml
fields:
  title:
    # 所有 extractor 都为空时，该字段报错。
    required: true
    extractors:
      - kind: css
        expr: "h1.title::text"

  cover:
    # 媒体字段使用 crawler 处理类型。
    kind: image
    extractors:
      - kind: css
        expr: "img.cover::attr(src)"
```

CSS 表达式支持：

```yaml
# 提取文本。
expr: "h1::text"

# 提取属性。
expr: "a.detail::attr(href)"

# 不加输出后缀时保留节点对象。
expr: "article.content"
```

CSS Healing 需要显式开启：

```yaml
fields:
  title:
    extractors:
      - kind: css
        expr: "h1.title::text"
        args:
          healing:
            # 取值范围为 0.0 到 1.0。
            min: 0.8
```

### 6.2 Regex

```yaml
fields:
  article_id:
    extractors:
      - kind: regex
        # 优先返回第一个捕获组。
        expr: "article_id=(\\w+)"
```

### 6.3 JSONPath

```yaml
fields:
  titles:
    extractors:
      - kind: json
        # JSONPath 直接保留原始 JSON 类型。
        expr: "$.data.items[*].title"

  total:
    extractors:
      - kind: json
        expr: "$.data.total"
```

### 6.4 多个 extractor 兜底

```yaml
fields:
  title:
    required: true
    extractors:
      # 第一个 extractor 有非空结果时停止。
      - kind: css
        expr: "h1.title::text"

      # 第一个结果为空时尝试第二个 extractor。
      - kind: css
        expr: "meta[property='og:title']::attr(content)"
```

可选字段没有结果时使用 `default`；省略时结果为 `null`：

```yaml
fields:
  summary:
    required: false
    default: ""
    extractors:
      - kind: css
        expr: ".summary::text"
```

## 7. Binds 派生字段

### 7.1 Pipeline

```yaml
binds:
  detail_urls:
    # 从 fields 取值，再按顺序执行转换。
    kind: pipeline
    from: "$fields.detail_links"
    transforms:
      - kind: url_join
        base_url: "$response.url"
      - kind: url_normalize
```

支持的常用 transform：

- `trim`
- `normalize_space`
- `lowercase`
- `uppercase`
- `url_join`
- `url_normalize`
- `add`：整数加法

页码或偏移量可以通过 `vals` 传递：

```yaml
binds:
  page:
    # 读取当前 Request 的 page。
    kind: pipeline
    from: "$vals.page"
    transforms:
      # 计算下一页。
      - kind: add
        value: 1
```

### 7.2 Template

```yaml
binds:
  detail_url:
    # 模板变量必须在 vars 中声明。
    kind: template
    template: "https://example.com/detail/{id}"
    vars:
      id:
        from: "$fields.article_id"
```

## 8. Edges 下一跳

### 8.1 列表页生成详情页

```yaml
graph:
  edges:
    - from: list
      # request 表示生成下一条 Request。
      kind: request
      request:
        node: detail
        url:
          from: "$binds.detail_urls"

    - from: detail
      # item 表示生成最终 Item。
      kind: item
      fn: save
      vals:
        title:
          from: "$binds.title"
        url:
          from: "$response.url"
```

`request.url` 是数组时会批量生成多条 Request；`request.vals` 的数组按位置对齐，标量会广播到每条 Request。

### 8.2 条件 Edge

```yaml
edges:
  - from: list
    kind: request
    when:
      # 只有 has_more 为 true 时才生成下一条 Request。
      from: "$fields.has_more"
      op: eq
      value: true
    request:
      node: list
      url:
        from: "$binds.next_url"
```

支持的 `when.op`：

- `present`
- `absent`
- `eq`
- `ne`
- `gt`
- `gte`
- `lt`
- `lte`

缺少运行期值时条件不成立。`false` 和 `0` 仍然属于存在值。

### 8.3 分页

分页不需要单独的 `pagination` 配置。通常使用当前节点指向自己的 Request edge：

```yaml
spider:
  start:
    - node: list
      url: "https://example.com/list?page=1"
      vals:
        # 当前 Request 的页码。
        page: 1

graph:
  nodes:
    list:
      fields:
        has_more:
          extractors:
            - kind: json
              expr: "$.has_more"
      binds:
        page:
          kind: pipeline
          from: "$vals.page"
          transforms:
            - kind: add
              value: 1
        page_url:
          kind: template
          template: "https://example.com/list?page={page}"
          vars:
            page:
              from: "$binds.page"
  edges:
    - from: list
      kind: request
      when:
        # 没有更多数据时正常停止。
        from: "$fields.has_more"
        op: eq
        value: true
      request:
        # 自环表示下一页仍然使用 list 节点。
        node: list
        url:
          from: "$binds.page_url"
        vals:
          # 下一条 Request 继续使用 page 这个名字。
          page:
            from: "$binds.page"
```

如果响应直接返回下一页 URL：

```yaml
fields:
  next_url:
    required: false
    extractors:
      - kind: json
        expr: "$.next"

edges:
  - from: list
    kind: request
    when:
      from: "$fields.next_url"
      op: present
    request:
      node: list
      url:
        from: "$fields.next_url"
```

如果 HTML 页面通过 CSS 选择器提供下一页链接，HTTP 和 Browser 都使用同一套字段与 Request
edge 规则。HTTP 不需要执行滚动，只需提取链接并生成下一条 Request：

```yaml
graph:
  nodes:
    list:
      fields:
        next_href:
          required: false
          extractors:
            - kind: css
              expr: "a.next::attr(href)"
  edges:
    - from: list
      kind: request
      when:
        from: "$fields.next_href"
        op: present
      request:
        node: list
        url:
          from: "$fields.next_href"
```

`scroll` 不是通用分页配置。它只属于 Browser Downloader，用于触发页面滚动并等待动态 DOM
继续增长；接口分页、下一页链接和页码分页仍然通过字段、绑定、条件和 Request edge 完成。

## 9. Item Schema

`item.schema` 描述最终 Item 的字段类型和校验规则：

```yaml
item:
  schema:
    fields:
      title:
        type: string
        rules:
          - required
          - min: 1

      url:
        type: string
        rules:
          - required
          - url

      images:
        type: array
```

`node.fields.<name>.kind` 和 `item.schema.fields.<name>.type` 是两个维度：

- `kind` 表示爬虫如何处理字段：`text`、`image`、`video`、`audio`
- `type` 表示字段数据类型，例如 `string`、`array`

## 10. 媒体字段

媒体字段仍然按照普通字段提取，只需要声明媒体类型：

```yaml
graph:
  nodes:
    detail:
      fields:
        images:
          # 图片字段使用 image。
          kind: image
          # true 表示在 Item 阶段下载附件。
          download: true
          extractors:
            - kind: css
              expr: "img.cover::attr(src)"

item:
  schema:
    fields:
      images:
        # 图片、视频、音频统一以数组保存。
        type: array
```

媒体对象包含：

```json
{
  "name": "",
  "url": "https://cdn.example.com/image.jpg",
  "src": "https://origin.example.com/image.jpg",
  "width": 0,
  "height": 0,
  "size": 0,
  "ext": "jpg",
  "alt": ""
}
```

第一版只保存媒体信息，不下载文件。需要下载时，在 `spider.attachment` 开启配置；`src` 始终保留原始地址。

## 11. Middleware

Middleware 配置在具体对象上：

```yaml
middlewares:
  - hook: before_scheduler
    name: dedup
    key: detail_url
    args:
      key:
        - "$request.url"
      ttl: 604800000
```

字段说明：

- `hook`：使用哪个生命周期阶段
- `name`：Middleware 名称
- `key`：当前 Middleware 实例名称
- `order`：可选执行顺序
- `skip`：`true` 表示跳过；省略表示启用
- `args`：当前 Middleware 的参数

Request Middleware 放在 `spider.start[*]` 或 `edge.request`：

```yaml
request:
  node: detail
  url:
    from: "$binds.detail_url"
  middlewares:
    - hook: before_scheduler
      name: dedup
      key: detail_url
      args:
        key: ["$request.url"]
        ttl: 604800000
```

Item Middleware 放在节点下：

```yaml
graph:
  nodes:
    detail:
      middlewares:
        - hook: before_item
          name: download
          skip: true
```

常用内置 Middleware：

```yaml
# 重试。
- hook: error_download
  name: retry
  key: default
  args:
    count: 3
    backoff: [1000, 3000, 5000]

# 限速。
- hook: before_download
  name: rate_limit
  key: api
  args:
    group: example_api
    qps: 2

# 请求去重。
- hook: before_scheduler
  name: dedup
  key: request_url
  args:
    key: ["$request.url"]
    ttl: 604800000
```

## 12. HTTP 和 Browser

### 12.1 HTTP

HTTP 是默认下载模式：

```yaml
spider:
  start:
    - node: index
      url: "https://example.com"
      download_mode: http
      method: GET
      timeout: 30000
```

### 12.2 Browser

Browser 适合需要 JavaScript 渲染的页面：

```yaml
spider:
  start:
    - node: index
      url: "https://example.com"
      download_mode: browser
      method: GET
      timeout: 60000
      wait_selector: "main.content"
```

Browser 第一版支持 GET 页面导航、渲染后的 DOM、Cookie Session 复用、CSS 等待和页面滚动：

```yaml
spider:
  start:
    - node: list
      url: "https://example.com/list"
      download_mode: browser
      scroll:
        max_rounds: 10
        wait_ms: 500
        stable_rounds: 2
```

`max_rounds` 是最多滚动次数，`wait_ms` 是每次滚动后的等待时间，`stable_rounds` 是页面高度
连续不增长后停止所需的次数。未配置 `scroll` 时只导航一次。`scroll` 只能与
`download_mode: browser` 一起使用；HTTP 配置了 `scroll` 会直接校验失败。Browser 第一版仍不支持
在 Rules 中配置点击、表单操作或多 Tab 流程。

## 13. Item 输出目录

默认使用 JSONL Store：

```rust
use halo::harbor::{engine, item};

// 修改 Item 输出根目录。
let store = item::Jsonl::with_dir("./crawler-data");

// 将 Store 装配到 Engine。
let mut engine = engine::Engine::new()
    .with_spider(ExampleSpider::new())
    .with_store(store)
    .build();

// 启动爬虫。
engine.start().await?;
```

输出文件位于：

```text
./crawler-data/data/items/output/<task_id>/<yyyy-mm-dd-HH>.jsonl
```

如果要把 Item 写入 Redis 队列，可以替换 Store：

```rust
use halo::contrib::store::redis::Redis;
use halo::harbor::engine;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // "items" 是使用方传入的 Redis List Key，框架不会自动拼接业务名称。
    let store = Redis::new("redis://127.0.0.1:6379", "items")?;

    let mut engine = engine::Engine::new()
        .with_spider(ExampleSpider::new())
        .with_store(store)
        .build();

    engine.start().await?;
    Ok(())
}
```

Redis Store 只负责把 Item 写入使用方指定的 Redis Key，业务消费者通过 Redis 的 `BRPOP` 等命令读取；Request
调度仍由 Scheduler 负责。

## 14. 选择 Scheduler

默认使用内存 Scheduler：

```rust
// 不配置 Scheduler 时，Engine 使用内存 Scheduler。
let engine = halo::harbor::engine::Engine::new()
    .with_spider(ExampleSpider::new())
    .build();
```

Redis、MySQL 和 Master API Scheduler 通过替换 Scheduler 装配；Spider、Rules 和 Item 配置不需要改变。

```rust
use halo::contrib::scheduler::redis::Redis;
use halo::harbor::{engine, net};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建 Redis Scheduler。
    let scheduler = Redis::new("redis://127.0.0.1:6379")?
        .with_namespace("harbor")?
        .with_worker_id("worker-01")?
        .with_worker_host("crawler-01")?
        .with_worker_version("0.1.0")?
        .with_modes([net::Mode::Http])?;

    // 只替换 Scheduler，Spider 代码不变。
    let mut engine = engine::Engine::new()
        .with_scheduler(scheduler)
        .with_spider(ExampleSpider::new())
        .build();

    engine.start().await?;
    Ok(())
}
```

## 15. 常见配置问题

- `spider.start[*].node` 必须存在于 `graph.nodes`。
- Request edge 必须配置 `request.node` 和 `request.url`。
- Item edge 不能配置 `request`。
- 媒体字段必须在 `item.schema.fields` 中声明为 `type: array`。
- `headers.Cookie` 不允许配置，Cookie 使用 `cookies`。
- `when` 使用结构化条件，不使用字符串表达式。
- `page`、`offset`、`cursor` 等都是用户字段，不是框架保留字段。
- URL 数组为空时不会生成下一条 Request，也不表示当前 Request 失败。
- 没有下一页时让 `when` 返回 false，当前列表 Request 正常结束。
