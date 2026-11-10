# Harbor

简体中文 | [English](README.md)

`Harbor` 是一个同时支持代码模式与 YAML Rules 模式的 Rust 爬虫运行时。同一套 Engine 合同可以运行在
本地内存调度、Redis 持久化调度与 MySQL 9 持久化调度中。

## 核心能力

- 代码模式与 Rules 模式共用 Spider、Request、Response、Item、Middleware 和 Engine。
- Memory、Redis、MySQL Scheduler 与 Worker 侧 API Scheduler 适配器使用同一套调度合同。
- HTTP 下载支持重试、响应体上限、重定向、Cookie、Proxy/TLS 和确定性字符集解码。
- Browser Downloader 使用 `thirtyfour` 驱动 Chrome/Chromium，支持渲染 DOM、Session Cookie 复用和 CSS 等待。
- 内置 CSS Selector、确定性 CSS Healing、RFC 9535 JSONPath，以及显式的 OpenAI-compatible JSON 对象提取。
- Item 使用独立 Store 合同，默认实现为本地 JSONL。
- 可选 `fastrace` 运行期追踪为每条 Request 建立独立链路，不改变业务 `trace_id` 或爬虫控制流。

## 快速开始

运行环境：Rust 1.97.0 或更高版本。

运行完整的代码模式示例：

```bash
cargo run -p examples --bin basic
```

运行 Rules 示例：

```bash
cargo run -p examples --bin rules
```

示例中还包含一个真实基金数据流程：`examples/rules-eastmoney-funds-daily.yaml` 抓取带分页的
基金排行以及每日详情，`examples/rules-eastmoney-funds-history.yaml` 用于首次执行的历史净值抓取。
每天一次或只执行一次由外部 Scheduler/Master 创建 Task，Rules DSL 只描述 Request 图。

`halo-harbor-worker` 是通用 Worker 库。应用通过 `Builder::with_scheduler(...)` 注入具体
Scheduler；Memory、Redis、MySQL 和 Master API 都是可替换的 Scheduler 实现，不由 Worker
通过环境变量选择。注册、心跳、领取、恢复和结算都由注入的 Scheduler 自己负责。

最小 Engine 装配方式：

```rust
let mut engine = halo::harbor::engine::Engine::new()
    .with_spider(BasicSpider::new())
    .build();

engine.start().await?;
```

`start()` 在队列为空时仍保持 Worker 运行，收到 SIGINT/SIGTERM 后再排空并关闭。
默认运行时使用 Memory Scheduler，并把 Item 以 JSONL 写入 `./data/items/`。Redis/MySQL Scheduler
只替换调度依赖，Item 持久化仍可独立替换。

## Workspace

| Crate | 职责 |
| --- | --- |
| `halo-harbor` | 核心运行时、Engine、Scheduler 合同、HTTP 下载、Rules、Selector 与 Item Store |
| `halo-harbor-macros` | Spider、处理函数与 Item 派生宏 |
| `halo-harbor-contrib` | Redis/MySQL Scheduler、Worker 侧 API Scheduler 适配器与分布式中间件实现 |
| `halo-harbor-worker` | 通用 Worker 库，运行共享 Engine，并接收外部注入的 Scheduler、Downloader、Spider、Middleware 和 Store |
| `examples` | 可直接运行的代码模式与 Rules 模式示例 |

## 文档

- [使用手册](docs/usage.zh-CN.md)
- [架构与当前能力](docs/architecture.zh-CN.md)
- [代码模式示例](examples/src/bin/basic.rs)
- [Rules 示例](examples/src/bin/rules.rs)与[配置文件](examples/rules-newspaper.yaml)

## 开发验证

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo test --workspace --all-targets --features runtime-tracing
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
```

当前 workspace 版本为 `0.1.0`，使用 MIT License。
