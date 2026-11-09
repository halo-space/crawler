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

运行独立 Worker 服务，默认使用 Memory Scheduler：

```bash
HARBOR_WORKER_ID=worker-01 \
HARBOR_WORKER_LIMIT=5 \
cargo run -p halo-harbor-worker
```

Worker 通过 `HARBOR_SCHEDULER` 选择 `memory`、`redis`、`mysql` 或 `master_api`。
Redis 和 MySQL 分别使用 `HARBOR_REDIS_URL`、`HARBOR_MYSQL_DSN`；Master API 使用
`HARBOR_MASTER_URL`、`HARBOR_TOKEN`。注册、心跳、领取、恢复和结算都由选中的 Scheduler
自己负责。`HARBOR_WORKER_LIMIT` 是初始领取上限，支持控制面的 Scheduler 可以在心跳时更新它。

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
| `halo-harbor-worker` | 独立 Worker 服务，装配选中的 Scheduler，运行共享 Engine，并通过 Worker 自己的 Store 写入 Item |
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
