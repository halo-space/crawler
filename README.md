# crawler

[简体中文](README.zh-CN.md) | English

`crawler` is a Rust crawler runtime for code-driven and YAML Rules-driven workloads. The same Engine
contract supports local execution, durable Redis scheduling, and remote Workers connected through the
Master API.

## Highlights

- Code mode and YAML Rules mode share the same Spider, Request, Response, Item, Middleware, and Engine.
- Memory, Redis, and HTTP API Scheduler implementations use one scheduling contract.
- HTTP downloading includes retries, bounded bodies, redirects, cookies, proxy/TLS settings, and
  deterministic response decoding.
- CSS selection, deterministic CSS healing, and explicit OpenAI-compatible JSON extraction are built in.
- Items use an independent Store contract; JSONL is the default local implementation.
- The Axum/MySQL `master` crate provides Task dispatch and control-plane APIs for remote Workers.

## Quick Start

Requirements: Rust 1.97.0 or later.

Run the complete code-mode example:

```bash
cargo run -p examples --bin basic
```

Run the Rules example:

```bash
cargo run -p examples --bin rules
```

The smallest Engine assembly is:

```rust
let mut engine = spider::engine::Engine::new()
    .with_spider(BasicSpider::new())
    .build();

engine.start().await?;
```

The default runtime uses the in-memory Scheduler and writes Items as JSONL below `./data/items/`.
Redis and API Schedulers replace only the scheduling dependency; Item persistence remains independently
replaceable.

## Workspace

| Crate | Responsibility |
| --- | --- |
| `spider` | Core runtime, Engine, Scheduler contracts, HTTP downloader, Rules, selectors, and Item Store |
| `macros` | Spider, handler, and Item derive macros |
| `contrib` | Redis/API Schedulers and distributed middleware implementations |
| `master` | Axum/MySQL control plane used by the API Scheduler |
| `examples` | Executable code-mode and Rules-mode examples |

## Documentation

- [Usage guide](docs/usage.md)
- [Architecture and current capabilities](docs/architecture.md)
- [Code-mode example](examples/src/bin/basic.rs)
- [Rules example](examples/src/bin/rules.rs) and [Rules configuration](examples/rules-newspaper.yaml)
- [Master configuration template](master/etc/master-api.yaml)

## Development

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
```

The workspace is currently at version `0.1.0` and uses the MIT license.
