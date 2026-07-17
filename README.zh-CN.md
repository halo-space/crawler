# crawler

简体中文 | [English](README.md)

`crawler` 是一个使用 Rust 编写的单进程爬虫运行时。当前 v2 提供内存调度器、HTTP 下载器、
中间件生命周期、异步 Spider 处理器、规则模式、CSS Healing、AI Selector 以及本地 JSONL 数据输出。

完整的当前功能、运行模型与扩展边界见[架构与功能说明](docs/architecture.zh-CN.md)。

## 运行环境

- Rust 1.97.0
- Cargo

克隆仓库后可直接检查整个 workspace：

```bash
cargo check --workspace --all-targets
```

## 代码模式

代码模式使用 `#[spider]` 宏定义 Spider。宏负责注入框架内部的事件发送器，业务代码只需要声明
Spider 字段和处理函数。

完整可编译示例位于 [examples/src/bin/basic.rs](examples/src/bin/basic.rs)，其中包含 Spider、Item 和
Engine 的完整定义。运行命令：

```bash
cargo run -p examples --bin basic
```

默认 Memory Scheduler 每次启动都以唯一的 `Spider.name` 创建一份运行种子：新的 `trace_id`、
不可变 Trace Snapshot 和初始 Request。远程 Scheduler 可以声明只消费已有运行种子；此时 Engine
不会在 Worker 本地创建 Trace，也不会调用 `Spider.start()`。代码 Request 只持久化稳定 node，
Worker 通过本地 Spider node 注册表恢复处理函数，不保存 Rust 函数指针。

在处理函数中通过 `self.tx.request(...)` 提交的新请求会进入同一个 Scheduler 队列；通过
`self.tx.item(...)` 提交的数据经过 Item 中间件后由 Scheduler 提交。

Rules 模式中，extractor 表达式直接决定结果数量：零个匹配为 `null`，一个匹配为标量或节点对象，
多个匹配为数组，不再提供额外的 `select`。媒体字段通过
`item.fields.<name>.kind = image | video | audio` 声明，crawler 在 validator 处理
`item.schema` 前将其规范化为固定 media object 数组。

代码模式通过 `response.css()?` 获得原生 `scrape_core::Soup`，业务代码继续直接使用 Soup 和
Tag。确定性 CSS Healing 需要显式开启，只扫描当前文档，不保存历史节点指纹，也不会调用 AI。
Rules 模式使用同一实现：

```yaml
args:
  healing:
    min: 0.8
```

AI 是与 CSS 并列的独立 extractor。它通过 OpenAI-compatible Chat Completion API 发送当前
Response 文本和 `expr` 提示词，并将模型内容直接解析为 JSON：

```yaml
- kind: ai
  expr: "提取文章信息，只返回合法 JSON。"
  args:
    base_url: "https://api.example.com/v1"
    api_key: "env:OPENAI_API_KEY"
    model_name: "..."
```

AI 不生成 CSS，也不会被 CSS Healing 自动调用。Rules 快照只保存环境变量引用，Worker 在执行
Selector 时才读取真正的 API Key。

## 规则模式

规则模式从 YAML 加载任务入口、请求图、解析规则、数据绑定和 Item Schema，但运行时仍装载一份
Rust Spider。多份 Rules 可以共享同一套 Spider 业务代码和同一种 Item 类型；YAML 负责表达每个
任务不同的抓取规则。Rules 任务名不要求等于 `Spider::name()`。

```rust
use spider::{config, engine};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let rules = config::Config::load("examples/rules-newspaper.yaml").await?;
let mut engine = engine::Engine::new()
    .with_rules(rules)
    .with_spider(Newspaper::new())
    .build()
    .with_concurrency(8);

engine.start().await?;
# Ok(())
# }
```

完整配置位于 [examples/rules-newspaper.yaml](examples/rules-newspaper.yaml)，运行命令：

```bash
cargo run -p examples --bin rules
```

`spider.start[*]` 和 request edge 使用同一套完整 Request Spec；Request 在入队前已经拥有 node、
URL、transport、priority 和 vals。URL 数组展开会在模板渲染前写入从 `1` 开始的保留
`vals.idx`。Rules 根据 `#[spider(item = Article)]` 关联的 Rust 类型调用 `Item::from_values`，注入
SchemaKey，再调用默认 `item` 或 edge 的 `fn` 业务函数。规则模式与代码模式共用 Engine、
Scheduler、Downloader、Middleware、Request、Response、Item 和 Payload，不维护第二套运行时模型。
每个具体 Item 必须持有一份不参与序列化的 `item::State`，并实现强制的
`from_values / state / state_mut` 合同。edge `fn` 引用的额外 Item 函数必须使用 `#[item]` 显式注册；
本地 Rules 装配发现函数不存在时，会在运行时初始化前直接 panic。

## Item 与输出

框架在 `Tx.item(...)` 提交数据时为没有 ID 的 Item 生成 UUID v7。这个 ID 只标识当前数据实例，
不参与业务去重。内置去重只作用于进入 Scheduler 前的 Request；Item 业务去重应由下游或自定义
Scheduler 实现处理。

默认 Memory Scheduler 将每条数据保存为一行 JSON：

```text
./data/items/output/<task_id>/<yyyy-mm-dd-HH>.jsonl
```

普通 JSONL 只包含业务字段。最终提交失败时，本地快照会额外保存 Item ID 和业务数据，便于恢复和
排查。失败快照位于 `./data/items/snapshots/`。Item 提交采用 at-least-once 语义；替换
Scheduler 时由新实现完整实现相同的 Request 与 Item 提交合同。

Request 进入本地执行槽后先通过 `Scheduler::ack` 确认执行权，长任务运行期间通过
`Scheduler::refresh_lease` 独立续租，提交最终结果期间也保持续租，最后分别调用 `Scheduler::success` 或
`Scheduler::failure`。租约超时和续租间隔由具体 Scheduler 自己提供；Memory 默认超时 30 秒、
每 10 秒续租。已确认执行失败时会保留有序且不重复的失败 Worker；未 ack 的领取过期不消费尝试次数。
Memory 只从进程内不可变映射读取 Trace Snapshot。Engine 直接跟踪克隆出来的 Tx 生产者，不再通过固定空闲等待窗口猜测是否仍有输出。
当前 Request 内直接 await 的 Tx 调用可以使用完整 Request 上下文；移动到独立任务中的 Tx clone
只保留 `task_id / trace_id`，不会继承 Request 执行权、租约身份、node、version 或 stats。
Request 并发数、单次领取上限和 Event 容量是三个独立且只在启动时加载的配置，分别通过
`with_concurrency`、`with_limit` 和 `with_event_limit` 设置。

Dedup 只处理配置 key 生成的 Request 指纹。默认精确存储使用 `HashMap + 过期时间堆`；TTL 省略或
`-1` 表示进程生命周期内永久保留，`0` 表示不保留。同源 redirect 会把中间响应的 Cookie 带到
下一跳；跨源 follow 和 redirect 均不继承 Request headers/cookies。

## 当前范围

v2 当前聚焦于：

- 单进程运行
- Memory Scheduler
- HTTP 下载
- CSS 和正则表达式
- 确定性 CSS Healing 和显式 AI Selector
- 请求校验、重试、限速和去重中间件
- 代码模式与 YAML 规则模式
- 本地 JSONL Item 输出

Browser 下载器、分布式/API Scheduler 和 Master 控制面不在当前 v2 范围内。

后端或 API 在保存配置前，可以通过 `Config::validate()` 校验完整规则，也可以通过
`middleware::check(&spec)` 单独校验一条中间件配置。

## 开发验证

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
```
