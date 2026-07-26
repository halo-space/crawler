# crawler

简体中文 | [English](README.md)

`crawler` 是一个同时支持代码模式与 YAML Rules 模式的 Rust 爬虫运行时。同一套 Engine 合同可以运行在
本地内存调度、Redis 持久化调度，以及通过 Master API 连接远程 Worker 的分布式形态中。

## 核心能力

- 代码模式与 Rules 模式共用 Spider、Request、Response、Item、Middleware 和 Engine。
- Memory、Redis 与 HTTP API Scheduler 实现同一套调度合同。
- HTTP 下载支持重试、响应体上限、重定向、Cookie、Proxy/TLS 和确定性字符集解码。
- 内置 CSS Selector、确定性 CSS Healing、RFC 9535 JSONPath，以及显式的 OpenAI-compatible JSON 对象提取。
- Item 使用独立 Store 合同，默认实现为本地 JSONL。
- `master` crate 使用 Axum/MySQL，为远程 Worker 提供 Task 派发和控制面 API。

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

最小 Engine 装配方式：

```rust
let mut engine = spider::engine::Engine::new()
    .with_spider(BasicSpider::new())
    .build();

engine.start().await?;
```

默认运行时使用 Memory Scheduler，并把 Item 以 JSONL 写入 `./data/items/`。Redis 和 API Scheduler
只替换调度依赖，Item 持久化仍可独立替换。

## Workspace

| Crate | 职责 |
| --- | --- |
| `spider` | 核心运行时、Engine、Scheduler 合同、HTTP 下载、Rules、Selector 与 Item Store |
| `macros` | Spider、处理函数与 Item 派生宏 |
| `contrib` | Redis/API Scheduler 与分布式中间件实现 |
| `master` | API Scheduler 使用的 Axum/MySQL 控制面 |
| `examples` | 可直接运行的代码模式与 Rules 模式示例 |

## 文档

- [使用手册](docs/usage.zh-CN.md)
- [架构与当前能力](docs/architecture.zh-CN.md)
- [代码模式示例](examples/src/bin/basic.rs)
- [Rules 示例](examples/src/bin/rules.rs)与[配置文件](examples/rules-newspaper.yaml)
- [Master 配置模板](master/etc/master-api.yaml)

## 开发验证

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
```

当前 workspace 版本为 `0.1.0`，使用 MIT License。
