# crawler

[简体中文](README.zh-CN.md) | English

`crawler` is a single-process Rust crawler runtime. The v3 runtime uses the in-memory Scheduler, the
HTTP downloader, middleware hooks, async Spider handlers, CSS healing, explicit AI selectors, and
local JSONL item output. It also includes Task/Trace run seeds, capability-aware Scheduler claims,
and deterministic response charset decoding.

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

Rules startup runs every generated initial Request through `before_scheduler`, then atomically stores
the Trace Snapshot with the accepted Requests. A run whose initial Requests are all filtered remains
valid: its Trace Snapshot is stored and the Engine exits normally.

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
code and concrete Item type; a Rules `spider.name` becomes the local `task_id` and does not have to
equal the deployed Rust `Spider::name()`.

```rust
let rules = config::Config::load("examples/rules-newspaper.yaml").await?;
let mut engine = engine::Engine::new()
    .with_rules(rules)
    .with_spider(Newspaper::new())
    .build();
```

`spider.start[*]` and request edges use one complete Request Spec. Requests have their node, URL,
transport, priority, and vals before entering Tx or Scheduler. URL-array expansion writes a reserved
one-based `vals.idx` before transport templates render. Rules composes parse/bind fields first; only a
non-empty Item-edge `vals` value overrides the same field. It then constructs the Spider's Rust Item via
the derived `Item::from_values`, assigns its SchemaKey, and calls the default `item` function or the edge's `fn`.
For every Rules Request, the shared Executor first calls the Rust Spider's `index(response.clone())`
business entry and then interprets the DSL node selected by `Request.node`; this is one Executor path,
not a Code/Rules Executor swap.
Every concrete Item derives `serde::Serialize`, `serde::Deserialize`, and `macros::Item`, rejects unknown
serde fields, and explicitly owns one `#[serde(skip)] item::State`. The Item derive generates
`from_values / state / state_mut`; business code does not write those adapter methods. Additional Item
functions referenced by edge `fn` are marked with `#[item]`; local Rules assembly panics before runtime
initialization when a referenced function is not registered.
The complete executable example is in [examples/src/bin/rules.rs](examples/src/bin/rules.rs).

The default Memory Scheduler writes one JSON value per line below
`./data/items/output/<task_id>/<yyyy-mm-dd-HH>.jsonl`. Item submission uses
`Scheduler::push_items`; Requests emitted by `self.tx.request(...)` enter the same Scheduler queue.
The current Request is acknowledged before execution, has its lease refreshed through
`Scheduler::refresh_lease` while it is running and while completion is being submitted, and then
completed through `Scheduler::success` or `Scheduler::failure`. Each Scheduler owns its lease timeout
and refresh interval; Memory defaults to a 30-second timeout and a 10-second interval. Definitive
ownership loss before the final Payload exists stops execution without settlement. Once that Payload
exists, `success / failure` is authoritative: a concurrent refresh error cannot cancel settlement,
and transient settlement errors retry the same Payload without re-executing the Request.
Failures after acknowledgment preserve ordered, duplicate-free failed Worker history; claim expiry
before acknowledgment does not consume an attempt. Memory reads Trace Snapshots only from its
immutable in-process map.
Engine tracks cloned Tx producers directly, so delayed output is drained without a fixed idle timeout.
Its internal coordinator is one Kameo Actor; Request and output I/O remains in independent Tokio tasks.
An awaited Tx call can use the current Request context. A Tx clone moved into a detached task retains
only `task_id / trace_id`; it never retains Request ownership, lease identity, node, version, or stats.
Request concurrency, the per-call claim limit, and Event capacity are independent startup-frozen
settings exposed by `with_concurrency`, `with_claim_limit`, and `with_event_limit`.
Event capacity is released when the Actor starts handling an Event, while the Tx call continues waiting
for the corresponding Scheduler and Middleware work to finish.

Dedup is Request-only and uses explicitly configured keys. It records fingerprints when
`before_scheduler` observes a Request; a later `Scheduler::push` or run-seed `init` failure does not
roll them back.
The SHA-256 input is a structured tuple of `task_id`, middleware key, rule name, and ordered values,
so namespace parts cannot collide. URL normalization stably sorts query pairs by key while preserving
the original order of repeated keys. All active rules are checked and inserted atomically; omitted TTL
or `-1` lasts for the process lifetime, while a rule with TTL `0` neither checks nor stores a
fingerprint.

The HTTP downloader applies proxy and TLS settings per Request. It reuses clients only for the same
proxy URL and `accept_invalid_certs` setting, lazily removes clients after 90 idle seconds, and clears
the pool on `Http::close()`. Direct requests, different proxy credentials, and different TLS behavior
therefore never share a client entry.
Same-origin redirects carry intermediate response cookies into the next hop. Cross-origin follow and
redirect handling never inherit Request headers or cookies.

The v3 response-text contract keeps `Response.body` as the downloaded bytes and centralizes character
decoding in `Response::text()`. Encoding precedence is BOM, a valid `Content-Type` charset, an HTML
meta declaration within the first 1024 bytes for HTML or a missing MIME type, then UTF-8. Malformed
sequences use Unicode replacement semantics; the runtime performs no statistical charset guessing.
`Response::json<T>()` uses the same decoded text path.

## Development

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
```

The v3 Scheduler contract scopes `next_requests` and pending-work checks to the current Worker's
supported download modes; filtering must be atomic with claim. A real Browser Downloader and mixed
HTTP/browser end-to-end execution remain v5 work. The optional API/Redis/MySQL Schedulers and Master
control plane remain v4 work and do not depend on Browser implementation.
`Memory::new(worker_id)` defaults to HTTP-only claims; `Memory::with_modes(...)` replaces that
capability set and rejects an empty set.

Media normalization does not download files. Item attachment downloading is not implemented and has
no assigned release; a separate future OpenSpec must define it.

Backend and API integrations can validate a complete rules document with `Config::validate()` or
validate one middleware declaration with `middleware::check(&spec)` before saving it.
