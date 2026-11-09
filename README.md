# Harbor

[简体中文](README.zh-CN.md) | English

`Harbor` is a Rust crawler runtime for code-driven and YAML Rules-driven workloads. The same Engine
contract supports local execution and durable Redis or MySQL 9 scheduling.

## Highlights

- Code mode and YAML Rules mode share the same Spider, Request, Response, Item, Middleware, and Engine.
- Memory, Redis, and MySQL Scheduler implementations, plus the Worker-side API Scheduler adapter, use one
  scheduling contract.
- HTTP downloading includes retries, bounded bodies, redirects, cookies, proxy/TLS settings, and
  deterministic response decoding.
- Browser downloading uses `thirtyfour` with Chrome/Chromium, rendered DOM responses, Session cookie
  reuse, and optional CSS waits.
- CSS selection, deterministic CSS healing, RFC 9535 JSONPath, and explicit OpenAI-compatible JSON-object extraction are built in.
- Items use an independent Store contract; JSONL is the default local implementation.
- Optional `fastrace` instrumentation traces each Request independently without changing business
  `trace_id` or crawler control flow.

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

Run the standalone Worker service with the default in-memory Scheduler:

```bash
HARBOR_WORKER_ID=worker-01 \
HARBOR_WORKER_LIMIT=5 \
cargo run -p halo-harbor-worker
```

The Worker assembles one shared Engine and can select `memory`, `redis`, `mysql`, or `master_api`
with `HARBOR_SCHEDULER`. Redis and MySQL use `HARBOR_REDIS_URL` or `HARBOR_MYSQL_DSN`; the
Master API backend uses `HARBOR_MASTER_URL` and `HARBOR_TOKEN`. Each Scheduler owns its own
registration, heartbeat, claiming, recovery, and settlement behavior. `HARBOR_WORKER_LIMIT` is
the initial claim limit; a control-plane Scheduler may update it through its heartbeat.

The smallest Engine assembly is:

```rust
let mut engine = halo::harbor::engine::Engine::new()
    .with_spider(BasicSpider::new())
    .build();

engine.start().await?;
```

`start()` keeps the Worker alive while the queue is empty and shuts down cleanly on SIGINT/SIGTERM.
The default runtime uses the in-memory Scheduler and writes Items as JSONL below `./data/items/`.
Redis or MySQL replaces only the scheduling dependency; Item persistence remains independently
replaceable.

## Workspace

| Crate | Responsibility |
| --- | --- |
| `halo-harbor` | Core runtime, Engine, Scheduler contracts, HTTP downloader, Rules, selectors, and Item Store |
| `halo-harbor-macros` | Spider, handler, and Item derive macros |
| `halo-harbor-contrib` | Redis/MySQL Schedulers, the Worker-side API Scheduler adapter, and distributed middleware implementations |
| `halo-harbor-worker` | Standalone Worker service that assembles a selected Scheduler, runs the shared Engine, and writes Items through a Worker-owned Store |
| `examples` | Executable code-mode and Rules-mode examples |

## Documentation

- [Usage guide](docs/usage.md)
- [Architecture and current capabilities](docs/architecture.md)
- [Code-mode example](examples/src/bin/basic.rs)
- [Rules example](examples/src/bin/rules.rs) and [Rules configuration](examples/rules-newspaper.yaml)

## Development

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo test --workspace --all-targets --features runtime-tracing
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
```

The workspace is currently at version `0.1.0` and uses the MIT license.
