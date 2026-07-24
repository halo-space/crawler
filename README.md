# crawler

[简体中文](README.zh-CN.md) | English

`crawler` is a Rust crawler runtime. Its default single-process topology uses the in-memory
Scheduler, HTTP downloader, middleware hooks, async Spider handlers, CSS healing, explicit AI
selectors, and local JSONL item output. `contrib` also provides a Redis 7 standalone Scheduler for
durable multi-Worker queues while preserving the same Engine contract. The runtime includes
Task/Trace run seeds, capability-aware Scheduler claims, and deterministic response charset decoding.

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

## Redis Scheduler

`contrib::scheduler::redis::Redis` is a complete implementation of the core `Scheduler` and `Init`
contracts. It is assembled exactly like Memory; Engine has no Redis-specific path:

```rust
use contrib::scheduler::redis::Redis;
use spider::{engine, net};

let scheduler = Redis::new("redis://127.0.0.1:6379")?
    .with_namespace("crawler")?;

let mut engine = engine::Engine::new()
    .with_worker_id("worker-01")
    .with_modes([net::Mode::Http])
    .with_scheduler(scheduler)
    .with_spider(BasicSpider::new())
    .build();

engine.start().await?;
```

All Redis keys are isolated by the selected namespace; normal `close()` releases client resources
without deleting queued work. Redis stores Trace Snapshots, Request state, mode-scoped processing
leases, settlements, statistics, and Items. Each `processing:<mode>` ZSET is the only active Request
index and uses `lease_time` as its score; the Request Hash remains the authoritative state. Each
accepted non-empty Item `Payload` becomes one Redis Stream entry and remains at-least-once; it does
not perform business Item deduplication.

Redis bounds recurring claim maintenance: one `next_requests` call recovers at most 64 expired
leases per mode, inspects at most 128 processing records across both modes, and promotes and
inspects at most 128 delayed Requests
for each requested mode. Missing per-Request records found while recovering, promoting, inspecting,
or selecting work have their dangling indexes removed. A valid processing Hash repairs a stale score
or misplaced mode projection without consuming a retry; an invalid Hash or queue record is removed
from active indexes and moved to terminal failure with a completion record. Neither blocks later valid
Requests. Before returning a claim, the Worker recalculates the immutable Request Snapshot digest;
a mismatch follows the same recovery path and is never executed. A verified Snapshot retry limit
cannot be overridden by the mutable Hash, and a failed recovery cannot withhold valid Requests from
the same claim. A corrupt shared index type instead
fails the claim explicitly before state mutation. The current key layout does not migrate an older
Redis namespace; deployment starts with a new namespace.

This release supports one Redis 7+ standalone primary only. It does not support Redis Cluster: the
multi-key Lua transitions rely on single-instance atomicity, so Cluster will be a separate Scheduler
design. For durable use, enable AOF (`appendonly yes`) and set `maxmemory-policy noeviction`.
`appendfsync` is an operator choice between stronger durability and write throughput/latency (for
example, `always` versus `everysec`). Monitor Stream growth and apply an explicit retention policy;
the Scheduler does not trim Item output automatically.

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
OpenAI-compatible Chat Completion endpoint. Its result is strictly one JSON object:

```yaml
- kind: ai
  expr: 'Extract the article as {"title":"...","content":"..."}.'
```

Provider configuration is Worker-local and is constructed once, then reused by code and Rules
selection through `response.ai(expr).await`:

```rust
let client = selector::ai::Client::from_env(
    "https://api.example.com/v1",
    "OPENAI_API_KEY",
    "model-name",
)?;
let mut engine = engine::Engine::new()
    .with_ai(client)
    .with_rules(rules)
    .with_spider(Newspaper::new())
    .build();
```

`Client::from_env` reads the secret while the Worker is assembled; neither provider settings nor
credentials enter Rules or Trace snapshots. `base_url` must be an absolute HTTP(S) endpoint without
embedded credentials, a query, or a fragment. Every Worker that can claim AI work from the same task
pool must use an equivalent provider endpoint and model. AI does not generate CSS and is not invoked
by CSS healing. The request also sets `response_format=json_object`; arrays, scalars, Markdown, and
prose are rejected after the response is received. The Response body buffer is limited to 1 MiB
before character-set decoding. The complete UTF-8 prompt, including `expr`, the fixed constraint,
and decoded content, is independently limited to 1 MiB. The provider HTTP body is limited to 4 MiB
after HTTP content decoding and before `async-openai` buffers it. Each provider attempt has a
60-second timeout; retries remain controlled by the existing `error_parse` policy.

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
Request and bind templates parse their source once; braces introduced by a resolved value remain data
and are never interpreted as another template expression.
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
Memory serializes and writes Items one at a time under one append lock, and rolls the entire append back
if any Item fails. Failure snapshots are written to a temporary file and atomically renamed only after
the complete snapshot is flushed.
`Scheduler::push` treats the same Request ID and initial Snapshot as an idempotent replay, atomically
adds missing Requests, and rejects the whole collection when any existing Snapshot conflicts.
For a directly awaited `Tx.request` call inside a Request execution, framework-created child IDs are
derived from the parent Request ID, the canonical initial child specification, and that specification's
occurrence within the current parse attempt. Replaying the same output after a parse or queue retry
therefore reuses its ID, while intentional identical children in one attempt remain distinct. An ID set
with `Request::with_id` remains authoritative. This is Request-output replay protection, not business
Dedup; Items and detached Tx output retain their at-least-once semantics.
The current Request is acknowledged before execution, has its lease refreshed through
`Scheduler::refresh_lease` while it is running and while completion is being submitted, and then
completed through `Scheduler::success` or `Scheduler::failure`. Each Scheduler owns its lease timeout
and refresh interval; Memory defaults to a 30-second timeout and a 10-second interval. Definitive
ownership loss before the final Payload exists stops execution without settlement. Once that Payload
exists, `success / failure` is authoritative: a concurrent refresh error cannot cancel settlement,
and transient settlement errors retry the same Payload without re-executing the Request.
For every Request returned by a successful `next_requests` call, Engine derives its initial local lease
deadline from a monotonic instant recorded immediately before that attempt, so Scheduler and network
latency consume part of the conservative lease budget. It never compares a Worker's wall clock with
Scheduler server time; a successful refresh starts the next local deadline from its own monotonic start.
Failures after acknowledgment preserve ordered, duplicate-free failed Worker history; claim expiry
before acknowledgment does not consume an attempt. Memory reads Trace Snapshots only from its
immutable in-process map.
Engine tracks cloned Tx producers directly, so delayed output is drained without a fixed idle timeout.
Its internal coordinator is one Kameo Actor; Request and output I/O remains in independent Tokio tasks.
An awaited Tx call can use the current Request context. A Tx clone moved into a detached task retains
only `task_id / trace_id`; it never retains Request ownership, lease identity, node, version, or stats.
Every emitted Request must match that Tx `task_id / trace_id` before any `before_scheduler` Middleware
runs, and no such Middleware may rewrite `id`, `task_id`, or `trace_id`.
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
proxy URL and `accept_invalid_certs` setting, retains at most 64 idle clients by default, lazily
expires them after 90 idle seconds, and clears the pool on `Http::close()`. The oldest idle client is
evicted under pressure; active clients are never evicted and may temporarily exceed the idle bound.
`Http::with_max_idle_clients` replaces this startup-time Worker setting.
Direct requests, distinct proxy credentials, and different TLS behavior never share a client entry.

One Worker accepts at most 64 MiB of decoded response body by default. `Http::with_max_body_bytes`
can replace that ceiling, while code or Rules `Request.max_body_bytes` may only select a positive limit
less than or equal to it; a value above the Worker ceiling fails before network I/O. Bodies are consumed as
decoded chunks without preallocating the limit, but the successful result remains one bounded
`Response.body: Bytes`; this contract does not expose a public stream or file sink. `Request.timeout`
is one budget for connection, every redirect, and the final body. A redirect never resets it, while
each new download retry receives a fresh full budget.
Only `301`, `302`, `303`, `307`, and `308` are followed. When a redirect changes a body-bearing Request
to GET, body-related headers are removed together with the discarded body; an original GET body keeps
its headers.

`Headers` wraps the standard `http::HeaderMap`, preserving case-insensitive names, raw response
values, and repeated fields. `set` replaces a name and `append` adds another value; Rules input stays
single-valued. Request Snapshots normalize names and serialize every name to a value array, while
non-string Request values are rejected at the Snapshot boundary.

Each Request carries a serializable CookieStore snapshot. Every response cookie is applied with
standard Domain, Path, Secure, Max-Age, and Expires rules before parsing, and descendant Requests
carry the resulting lineage through Scheduler recovery. Parallel siblings keep independent snapshots.
On a cross-origin follow or redirect, source headers are removed and the store is reduced to cookies
applicable to the target URL, so unrelated credentials cannot enter the target Request Snapshot.
Cross-site public-suffix Domain attributes are rejected; an identical public-suffix Request host is
normalized to HostOnly. A raw `Cookie` header is never a second session source, so code and Rules
must use the Request cookie API.

Each `rate_limit` group fixes its interval while active. Reusing the group with a conflicting QPS is
invalid configuration; an inactive group is removed lazily only after its next permitted instant has
passed.

The v3 response-text contract keeps `Response.body` as the Downloader-delivered bytes after HTTP
content decoding and before character transcoding, and centralizes character decoding in
`Response::text()`. Encoding precedence is BOM, a valid `Content-Type` charset, an HTML
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

The Scheduler contract receives the Engine-owned Worker ID and supported download modes for
`next_requests` and pending-work checks; filtering must be atomic with claim. A real Browser Downloader and mixed
HTTP/browser end-to-end execution remain v5 work. Redis is the available v4 persistent Scheduler;
API and MySQL Schedulers, the Master control plane, Item replay, and runtime tracing remain separate
v4 work and do not depend on Browser implementation. AI provider configuration is already
Worker-local: one reusable Client is injected through `Engine::with_ai`, while Rules retain only the
prompt. Redis is covered by the shared Scheduler conformance suite.
Engine defaults to `worker-1` and HTTP mode. `with_worker_id(...)` and `with_modes(...)` replace
those frozen startup values; an empty Worker ID or mode set is rejected before execution.

Media normalization does not download files. Item attachment downloading is planned as an
independent v5 change alongside, but not dependent on, the Browser Downloader.

Backend and API integrations can validate a complete rules document with `Config::validate()` or
validate one middleware declaration with `middleware::check(&spec)` before saving it.
