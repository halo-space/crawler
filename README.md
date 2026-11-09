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

Run the standalone Worker service after configuring its Master API connection:

```bash
HARBOR_MASTER_URL=https://master.example.com/ \
HARBOR_TOKEN=... \
HARBOR_WORKER_ID=worker-01 \
HARBOR_WORKER_LIMIT=5 \
cargo run -p halo-harbor-worker
```

The Worker receives Requests from the Master API and restores the Rules graph from each Trace
Snapshot. It does not read the Master database directly. `HARBOR_WORKER_LIMIT` is the initial single-claim batch size; the Master heartbeat can change the runtime value for later claims.

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
| `harbor` | Core runtime, Engine, Scheduler contracts, HTTP downloader, Rules, selectors, and Item Store |
| `macros` | Spider, handler, and Item derive macros |
| `contrib` | Redis/MySQL Schedulers, the Worker-side API Scheduler adapter, and distributed middleware implementations |
| `halo-harbor-worker` | Standalone Worker service that connects to Master API, runs the shared Engine, and writes Items through a Worker-owned Store |
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
