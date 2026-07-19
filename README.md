# crawler

[简体中文](README.zh-CN.md) | English

`crawler` is a single-process Rust crawler runtime. The v2 runtime uses the in-memory Scheduler,
the HTTP downloader, middleware hooks, async Spider handlers, CSS healing, explicit AI selectors,
and local JSONL item output.

See the [architecture and feature overview](docs/architecture.md) for the complete runtime model,
current capabilities, and extension boundaries.

## Quick Start

The complete code-mode example is in [examples/src/bin/basic.rs](examples/src/bin/basic.rs). It
defines a Spider with the `#[spider]` macro, extracts an Item, and runs an Engine:

```bash
cargo run -p examples --bin basic
```

The macro supplies the internal event sender, so application code only defines Spider fields and
handlers. Build an Engine with `engine::Engine::new().with_spider(...).build()`, then call
`engine.start().await`.

For the default Memory Scheduler, each start creates one run seed from `Spider.name`: a new
`trace_id`, an immutable Trace Snapshot, and the initial Requests. A remote Scheduler can declare
that it consumes an existing run; in that mode Engine neither creates a local Trace nor calls
`Spider.start()`. Persisted code Requests contain only the stable node name, and the Worker resolves
that node through its local Spider registry.

In rules mode, extractor expressions determine result cardinality directly: zero matches produce
`null`, one match produces a scalar or element object, and multiple matches produce an array.
There is no separate `select` option. Media fields are declared with
`item.fields.<name>.kind = image | video | audio`; crawler normalizes them into fixed media objects
before validator processes `item.schema`.

Code mode parses HTML through `response.css()?` and uses the native `scrape_core::Soup` and `Tag`
APIs. Deterministic CSS healing is opt-in and scans the current document only; it does not persist
node fingerprints or invoke AI. Rules mode enables the same implementation with:

```yaml
args:
  healing:
    min: 0.8
```

AI is an independent extractor that sends the current Response text and `expr` prompt through an
OpenAI-compatible Chat Completion endpoint and parses the model content as JSON:

```yaml
- kind: ai
  expr: "Extract the article and return valid JSON only."
  args:
    base_url: "https://api.example.com/v1"
    api_key: "env:OPENAI_API_KEY"
    model_name: "..."
```

AI does not generate CSS and is not invoked by CSS healing. Rules persist only the environment
variable reference; the Worker resolves the actual API key when the selector runs.

## Rules Mode

Rules loads task seeds, a request graph, extraction, bindings, and Item Schema from YAML, while the
runtime still loads a Rust Spider. Multiple Rules configurations can share the same Spider business
code and concrete Item type; a Rules task name does not have to equal `Spider::name()`.

```rust
let rules = config::Config::load("examples/rules-newspaper.yaml").await?;
let mut engine = engine::Engine::new()
    .with_rules(rules)
    .with_spider(Newspaper::new())
    .build();
```

`spider.start[*]` and request edges use one complete Request Spec. Requests have their node, URL,
transport, priority, and vals before entering Tx or Scheduler. URL-array expansion writes a reserved
one-based `vals.idx` before transport templates render. Rules constructs the Spider's Rust Item via
`Item::from_values`, assigns its SchemaKey, and calls the default `item` function or the edge's `fn`.
Every concrete Item owns one non-serialized `item::State` and implements the mandatory
`from_values / state / state_mut` contract. Additional Item functions referenced by edge `fn` are
marked with `#[item]`; local Rules assembly panics before runtime initialization when a referenced
function is not registered.
The complete executable example is in [examples/src/bin/rules.rs](examples/src/bin/rules.rs).

The default Memory Scheduler writes one JSON value per line below
`./data/items/output/<task_id>/<yyyy-mm-dd-HH>.jsonl`. Item submission uses
`Scheduler::push_items`; Requests emitted by `self.tx.request(...)` enter the same Scheduler queue.
The current Request is acknowledged before execution, has its lease refreshed through
`Scheduler::refresh_lease` while it is running and while completion is being submitted, and then
completed through `Scheduler::success` or `Scheduler::failure`. Each Scheduler owns its lease timeout
and refresh interval; Memory defaults to a 30-second timeout and a 10-second interval. Acknowledged
lease failures preserve ordered, duplicate-free failed Worker history; unacknowledged claim expiry
does not consume an attempt. Memory reads Trace Snapshots only from its immutable in-process map.
Engine tracks cloned Tx producers directly, so delayed output is drained without a fixed idle timeout.
Its internal coordinator is one Kameo Actor; Request and output I/O remains in independent Tokio tasks.
An awaited Tx call can use the current Request context. A Tx clone moved into a detached task retains
only `task_id / trace_id`; it never retains Request ownership, lease identity, node, version, or stats.
Request concurrency, the per-call claim limit, and Event capacity are independent startup-frozen
settings exposed by `with_concurrency`, `with_limit`, and `with_event_limit`.
Event capacity is released when the Actor starts handling an Event, while the Tx call continues waiting
for the corresponding Scheduler and Middleware work to finish.

Dedup is Request-only and uses explicitly configured keys. The default exact store uses a `HashMap`
plus an expiry heap; omitted TTL or `-1` lasts for the process lifetime, while `0` retains nothing.
Same-origin redirects carry intermediate response cookies into the next hop. Cross-origin follow and
redirect handling never inherit Request headers or cookies.

## Development

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
```

The current release scope is single-process Memory scheduling, HTTP crawling, deterministic CSS
healing, and explicit AI selectors. Browser rendering, distributed/API schedulers, and Master
control-plane features are outside v2.

Backend and API integrations can validate a complete rules document with `Config::validate()` or
validate one middleware declaration with `middleware::check(&spec)` before saving it.
