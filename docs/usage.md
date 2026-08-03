# Crawler Usage Guide

[Project README](../README.md) | [简体中文](usage.zh-CN.md) | English

`crawler` is a Rust crawler runtime. Its default single-process topology uses the in-memory
Scheduler, HTTP downloader, middleware hooks, async Spider handlers, CSS healing, explicit AI
selectors, and local JSONL item output. `contrib` provides Redis/MySQL Schedulers and distributed
middleware while preserving the same Engine contract. The runtime includes Task/Trace run seeds,
capability-aware Scheduler claims, and deterministic response charset decoding.

See the [architecture and feature overview](architecture.md) for the complete runtime model,
current capabilities, and extension boundaries.

## Quick Start

The complete code-mode example is in [examples/src/bin/basic.rs](../examples/src/bin/basic.rs). It
defines a Spider with the `#[spider]` macro, extracts an Item, and runs an Engine:

```bash
cargo run -p examples --bin basic
```

The macro supplies the internal event sender, so application code only defines Spider fields and
handlers. Build an Engine with `engine::Engine::new().with_spider(...).build()`, then call
`engine.start().await`.

For code mode with the default Memory Scheduler, each start creates one run seed from
`Spider.name`: a new `trace_id`, an immutable Trace Snapshot, and the initial Requests. A remote
Scheduler can declare that it consumes an existing run; in that code-mode path Engine neither
creates a local Trace nor calls `Spider.start()`. Persisted code Requests contain only the stable
node name, and the Worker resolves that node through its local Spider registry.
`Request::follow()` only constructs a Request specification, so its `task_id / trace_id` remain empty
until a run seed or current Tx context binds them. `Scheduler::init` and `Scheduler::push` reject an
unbound Request; queued Snapshots, claims, retries, and recovery always carry both identities and a
real Trace Snapshot. Code mode uses a Trace Snapshot whose `dsl` is empty rather than omitting Trace.

Rules startup statically validates the configuration, freezes the Trace Snapshot, materializes the
initial Requests, and passes both directly to atomic `Scheduler::init`. Publishing a run seed never
executes Worker Middleware, `before_scheduler`, or Dedup. Requests emitted later through `Tx` still
use the normal admission path before `Scheduler::push`.

## Redis Scheduler

`contrib::scheduler::redis::Redis` is a complete implementation of the core `Scheduler` and `Init`
contracts. It is assembled exactly like Memory; Engine has no Redis-specific path:

```rust
use contrib::scheduler::redis::Redis;
use spider::{engine, net};

let scheduler = Redis::new("redis://127.0.0.1:6379")?
    .with_namespace("crawler")?
    .with_worker_id("worker-01")?
    .with_worker_host("crawler-node-01")?
    .with_worker_version("1.0.0")?
    .with_modes([net::Mode::Http])?;

let mut engine = engine::Engine::new()
    .with_scheduler(scheduler)
    .with_spider(BasicSpider::new())
    .build();

engine.start().await?; // Runs until SIGINT or SIGTERM, then drains accepted work.
```

Worker ID, host, version, and modes are Scheduler configuration. `open(concurrency)` registers them,
starts the heartbeat, and rejects a duplicate ID that is still online. Heartbeat failure pauses only
future claims and retries until recovery; already claimed Requests continue normally. Normal `close()`
waits for the heartbeat task to stop before attempting the explicit offline update, so no later heartbeat
can follow that update. A failed update is logged and the registration then expires through heartbeat
timeout. If `close()` is cancelled while waiting for heartbeat shutdown or the offline response, the
connection, registration token, and stop state remain available; another `close()` continues shutdown.
All Redis keys are isolated by the selected namespace,
and closing never deletes queued work. Redis stores Trace Snapshots, Request state, mode-scoped processing
leases, settlements, and statistics. Each `processing:<mode>` ZSET is the only active
execution-lease projection and uses `lease_time` as its score; the Request Hash remains the
authoritative state. Item persistence is a separate `item::Store` dependency and never enters the
Redis Scheduler. Each Engine run binds exactly one Store at startup: use `.with_store(store)` to replace
the default JSONL implementation. Store routing, Store registries, per-Task Store selection, and
`persister_id` are not part of the runtime contract.
Repeated `open` calls in one lifecycle are idempotent only with the same concurrency; a different value
is rejected until `close()` starts a new lifecycle.

Worker and namespace settings remain frozen from the first registration attempt until registration is
confirmed or `close()` explicitly abandons it. This keeps cancellation retries bound to the same Worker
identity and namespace.

Redis bounds recurring claim maintenance: one `next_requests` call recovers at most 64 expired
leases per mode, inspects at most 128 processing records across both modes, and promotes and
inspects at most 128 delayed Requests
for each requested mode. Missing per-Request records found while recovering, promoting, inspecting,
or selecting work have their dangling indexes removed. A valid processing Hash repairs a stale score
or misplaced mode projection without consuming a retry; an invalid Hash or queue record is removed
from active indexes and moved to terminal failure with a completion record. Neither blocks later valid
Requests. Before returning a claim, the Worker recalculates the immutable Request Snapshot hash;
a mismatch follows the same recovery path and is never executed. A verified Snapshot retry limit
cannot be overridden by the mutable Hash, and a failed recovery cannot withhold valid Requests from
the same claim. A corrupt shared index type instead fails the claim explicitly before state mutation.
A Request Snapshot must set `max_retry_count` within `1..=128`; the immutable Snapshot value controls
recovery and the mutable Hash cannot expand it. The current key layout does not migrate an older Redis
namespace; deployment starts with a new namespace.

Failed-Worker eligibility is projected into a mode-scoped `pending_exclusions` ZSET. Ready selection
checks at most 128 excluded members per call and may return a temporary empty claim while it advances
a per-mode cursor containing the latest inspected ready-event revision and excluded ready member.
Every transition into a ready queue publishes an event to `ready_events:<mode>`. Before resuming after
the saved member, claim inspects later events for that mode. Only a new ready Request that sorts before
the saved member and is eligible for the current Worker resets the scan; lower-priority and cross-mode
ready writes preserve its progress. Selection never bypasses an unresolved higher-priority prefix or
mode. Pending checks compare the queued count with the Worker's indexed exclusions, so they do not
scan an entire backlog inside one Redis Lua invocation.

This release supports one Redis 7+ standalone primary only. It does not support Redis Cluster: the
multi-key Lua transitions rely on single-instance atomicity, so Cluster will be a separate Scheduler
design. For durable use, enable AOF (`appendonly yes`) and set `maxmemory-policy noeviction`.
`appendfsync` is an operator choice between stronger durability and write throughput/latency (for
example, `always` versus `everysec`).

## MySQL Scheduler

`contrib::scheduler::mysql::MySql` targets MySQL 9 and completely implements the same `Scheduler`
and `Init` contracts as Memory and Redis. Isolation comes directly from the database selected by the
DSN; there is no additional namespace:

```rust
use contrib::scheduler::mysql::MySql;
use spider::{engine, net};

let scheduler = MySql::new("mysql://crawler:secret@127.0.0.1:3306/crawler")?
    .with_worker_id("worker-01")?
    .with_worker_host("crawler-node-01")?
    .with_worker_version("1.0.0")?
    .with_modes([net::Mode::Http])?;

let mut engine = engine::Engine::new()
    .with_scheduler(scheduler)
    .with_spider(BasicSpider::new())
    .build();

engine.start().await?;
```

Operators explicitly install [contrib/sql/mysql/schema.sql](../contrib/sql/mysql/schema.sql).
`MySql::open()` never executes DDL. It creates the pool, validates the seven required tables, all
operational column types and nullability, NO PAD `utf8mb4_0900_bin` identity/contract text, `InnoDB`,
and full-column atomicity-critical unique keys, then registers the Worker and starts its heartbeat.
Connections use `READ COMMITTED`. Claim scans priority/FIFO candidates with a keyset cursor and locks
each authoritative Request with `FOR UPDATE OF r SKIP LOCKED`. A locked prefix is skipped and scanning
continues until the requested limit is filled or no eligible candidate remains. Claim, lease recovery,
ack/release, lease refresh, success/failure settlement, statistics merging, and retry enqueueing use
database transactions.

`requests.snapshot` stores the immutable JSON Snapshot and `snapshot_hash` stores its 32-byte
SHA-256. Every transition back into `queues` receives a new auto-increment `sequence`, preserving the
same priority-then-FIFO semantics as Redis. `created_time / updated_time` are `DATETIME(3)` for
operations and inspection; runtime timestamps such as `lease_time`, `next_time`, and
`start_time / end_time` remain integer milliseconds across Scheduler implementations.

The MySQL Scheduler connects directly to its DSN database and does not pass through Master. Where
Workers cannot receive database credentials, use the API Scheduler below and implement the same
transaction semantics in the separate Master project. Both are replaceable Scheduler assemblies;
Items still go to the independently configured Worker Store.

## API Scheduler

The API Scheduler owns the same Worker configuration and lifecycle while using its existing HTTP
transport, namespace, and authentication:

```rust
use contrib::scheduler::api::Api;
use spider::net;

let scheduler = Api::new("https://master.example.com", api_key)?
    .with_namespace("crawler")?
    .with_worker_id("worker-01")?
    .with_worker_host("crawler-node-01")?
    .with_worker_version("1.0.0")?
    .with_modes([net::Mode::Http])?;
```

During `open`, API reads `/v1/worker/policy`, registers through `/v1/worker/register`, and uses the
server heartbeat interval. Heartbeat and explicit offline use `/v1/worker/heartbeat` and
`/v1/worker/offline`. Worker-management responses use `{code, message, data}`; `200` succeeds and
registration code `100` reports an online ID conflict. The server-generated registration token is
stored and returned on heartbeat/offline requests, but it does not participate in Request claim,
lease, or settlement identity.
The policy lease timeout and refresh interval must exactly match the Scheduler's configured lease.
The API Scheduler adds no configurable byte limit to request bodies sent to the Master or response
bodies returned by it.
An unfinished registration key or confirmed token freezes API Scheduler configuration until the
lifecycle is completed or explicitly closed.

Claim and release use operation-scoped `Idempotency-Key` recovery. A transport error, timeout, or
successful response that cannot be decoded leaves the result uncertain, so the next claim with the
same parameters or release with the same lease identity reuses the retained key. A different claim is
rejected while an earlier claim remains unresolved. A successful result or deterministic failure
clears the key. Claims are serialized, queued claims recheck Worker liveness, claim replay keeps the
first operation's monotonic start, and unresolved release keys expire with their lease. API `close()`
stops and waits for the existing heartbeat task before sending the offline update. If that wait is
cancelled, another `close()` continues waiting for the same task.

For a batch claim without embedded Trace Snapshots, API recovery reads each cold `trace_id` once and
loads different Traces concurrently. Trace read, missing, decode, validation, and task/node binding
errors call `release` only; they do not call `ack` or `failure`, and do not consume a Request retry.
Only a damaged Request Snapshot or execution state uses `ack + failure`. Recovery and its per-Request
settlement share a lease handoff deadline, and successfully restored Requests keep the server claim
order. Release operations are independent, so one failed release does not prevent the remaining claims
from being returned.

## Selectors

In rules mode, extractor expressions determine result cardinality directly: zero matches produce
`null`, one match produces a scalar or element object, and multiple matches produce an array.
There is no separate `select` option. Media fields are declared with
`item.fields.<name>.kind = image | video | audio`; crawler normalizes them into fixed media objects
before validator processes `item.schema`.

Code mode parses HTML through `response.css()?` and uses the native `scrape_core::Soup` and `Tag`
APIs. Healing is a CSS-only, opt-in capability that scans the current HTML document; it does not
persist node fingerprints, apply to JSONPath, or invoke AI. Rules mode enables the same CSS
implementation with:

```yaml
args:
  healing:
    min: 0.8
```

The Rules executor deserializes a JSON response once and reuses that document across JSON fields.
Code mode obtains the full `serde_json::Value` through `Response::json<T>()`, then queries it with
RFC 9535 JSONPath. Selected values are returned by reference without stringifying numbers, booleans,
objects, or arrays:

```rust
let document: serde_json::Value = response.json()?;
let stock_codes = spider::selector::json::select(&document, "$.data.diff[*].f12")?;
```

Rules mode uses the same selector directly. This is a complete configuration for the EastMoney test
endpoint:

```yaml
spider:
  name: eastmoney
  start:
    - node: index
      url: "https://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&secids=1.601398,1.600036,1.600030,1.601318,0.300059,1.600705,1"

graph:
  nodes:
    index:
      parse:
        fields:
          quotes:
            required: true
            extractors:
              - kind: json
                expr: $.data.diff[*]
          stock_codes:
            extractors:
              - kind: json
                expr: $.data.diff[*].f12
          total:
            extractors:
              - kind: json
                expr: $.data.total
  edges:
    - from: index
      kind: item
      fn: save
      vals:
        quotes: {from: $fields.quotes}
        stock_codes: {from: $fields.stock_codes}
        total: {from: $fields.total}

item:
  schema:
    fields:
      quotes: {type: array}
      stock_codes: {type: array}
      total: {type: int}
```

`quotes`, `stock_codes`, and `total` are application-defined output field names, not JSON extractor
keywords. Later Rules expressions can reference them as `$fields.quotes`, `$fields.stock_codes`, and
`$fields.total`. Only `kind: json` and the RFC 9535 JSONPath `expr` are fixed parts of the extractor.

The Item edge passes the parsed values to the Rust Spider Item handler registered as `save`. When
`fn` is omitted, the framework calls the handler named `item`. `item.schema` remains the per-Rules
field-validation contract.

For example, `$.data.diff[*].f12` selects security codes from the EastMoney quote response. A valid miss
continues to the next extractor. One match remains its JSON value and multiple matches become an
array under the existing Rules cardinality contract. Invalid JSON and invalid JSONPath expressions
are errors. There is no generic Healing stage: JSON selection has no Healing or AI fallback.

AI is an independent extractor that sends the current Response text and `expr` prompt through an
OpenAI-compatible Chat Completion endpoint. Its result is strictly one JSON object:

```yaml
- kind: ai
  expr: 'Extract the article as {"title":"...","content":"..."}.'
```

Provider configuration is Worker-local and is constructed once, then reused by code and Rules
selection through `response.ai(expr).await`:

```rust
use spider::{ai, engine};

let openai = ai::OpenAI::new(
    "https://api.example.com/v1",
    api_key,
    "model-name",
)?;
let mut engine = engine::Engine::new()
    .with_ai(openai)
    .with_rules(rules)
    .with_spider(Newspaper::new())
    .build();
```

The application obtains `base_url`, `api_key`, and `model_name` from its chosen configuration or
secret source before constructing `ai::OpenAI`; crawler does not read those values from the
environment. Provider settings and credentials never enter Rules or Trace snapshots. `base_url`
must be an absolute HTTP(S) endpoint without embedded credentials, a query, or a fragment. Every
Worker that can claim AI work from the same task pool must use an equivalent provider endpoint and
model. AI does not generate CSS and is not invoked by CSS healing. The request also sets
`response_format=json_object`; arrays, scalars, Markdown, and prose are rejected after the response
is received. The Response body buffer is limited to 1 MiB
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
The complete executable example is in [examples/src/bin/rules.rs](../examples/src/bin/rules.rs).

## Item Storage

Engine uses `item::Jsonl` as its default Item Store. It writes one record per line below
`./data/items/output/<task_id>/<yyyy-mm-dd-HH>.jsonl`, using the stable shape
`{"id":"...","data":{...}}`. `self.tx.request(...)` submits Requests to the Scheduler, while
`self.tx.item(...)` submits an Item-only `Payload` through `Store::submit(&Payload)`. The two
dependencies are independent: `.with_scheduler(...)` replaces Request scheduling and
`.with_store(...)` selects the single Item Store for this Engine run and replaces Item persistence. The
selection is fixed when `build()` completes; the framework does not route a later Item to another Store
or fan one submission out to multiple Stores. There is no `persister_id` field to configure.

`Jsonl::with_dir(path)` changes the output root. It serializes a complete Payload before taking the
hourly file's append lock, then writes and flushes the complete byte sequence. A write or flush error
is logged and returned without a transaction or file rollback; bytes already written may remain. Its
open-file cache is bounded; additional busy paths use uncached handles rather than growing the cache.
Jsonl owns submission-failure snapshots below
`<dir>/data/items/snapshots/`; it publishes each snapshot through a flushed temporary file and atomic
rename, removes it after a later successful retry of the same immutable Payload projection, and leaves an exhausted
snapshot for manual handling. Equal immutable Payload projections may share this recovery snapshot,
but every `submit` call still reaches output and no durable replay index or Item deduplication is created.
Item submission is at-least-once. Business Item deduplication is downstream business logic outside
the Store contract.
Every Store implementation validates the complete Payload with `Payload::validate_store()` before
backend mutation. After Store retries are exhausted, `error_item` is a best-effort notification for
each Item: callback failures are logged, while the original Store error remains the Request failure.

## Request Execution

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
For every Request returned by one logical `next_requests` claim, Engine derives its initial local lease
deadline from a monotonic instant recorded before the first attempt and retained across transient
retries, so Scheduler, network, and retry latency consume part of the conservative lease budget. It
never compares a Worker's wall clock with Scheduler server time; a successful refresh starts the next
local deadline from its own monotonic start.
Failures after acknowledgment preserve ordered, duplicate-free failed Worker history; claim expiry
before acknowledgment does not consume an attempt. Memory reads Trace Snapshots only from its
immutable in-process map.
Engine tracks cloned Tx producers directly, so delayed output is drained without a fixed idle timeout.
Its internal coordinator is one Kameo Actor; Request and output I/O remains in independent Tokio tasks.
An empty claim waits one second and retries by default; `with_idle_interval(duration)` replaces this
positive startup-frozen interval. Empty queues never stop the Worker. `Runtime::start()` listens for
SIGINT/SIGTERM, stops new claims, lets an active claim return, drains accepted Request and Tx work
without an internal timeout, and then closes its components.
An awaited Tx call can use the current Request context. A Tx clone moved into a detached task retains
only `task_id / trace_id`; it never retains Request ownership, lease identity, node, version, or stats.
Every emitted Request must match that Tx `task_id / trace_id` before any `before_scheduler` Middleware
runs, and no such Middleware may rewrite `id`, `task_id`, or `trace_id`.
Request concurrency, the per-call claim limit, and Event capacity are independent startup-frozen
settings exposed by `with_concurrency`, `with_claim_limit`, and `with_event_limit`.
Event capacity is released when the Actor starts handling an Event, while the Tx call continues waiting
for the corresponding Scheduler and Middleware work to finish.

## Deduplication

Dedup is Request-only. One effective `dedup` Spec defines one rule with flat
`args.key / normalize / ttl`; the old nested `args.rules` shape is invalid. Membership is scoped by
`task_id + node`, while SHA-256 hashes only the canonical JSON representation of the explicitly
configured ordered values. `trace_id`, Middleware name, `Spec.key`, Rules names, and an implicit URL
do not participate. Object keys are sorted recursively; array order, configured path order, JSON
types, and duplicate values remain significant. `before_scheduler` may not rewrite the node.

The default `middleware::dedup::Memory` implementation records one exact membership atomically.
Omitted TTL or `-1` lasts for the process lifetime, `0` bypasses lookup and storage, and a positive
integer expires after that many milliseconds. Runtime Tx Request output is observed before
`Scheduler::push`; Rules run seeds bypass admission and therefore never consume Dedup membership.
`contrib::middleware::dedup::Redis` uses RedisBloom with explicit `capacity / error_rate` options,
accepts Bloom false positives, has no configurable namespace, and rejects positive finite TTL rather
than emulating member expiry with Sets or rotating buckets. The first Worker that creates a
`task_id + node` filter fixes its actual options; every Worker sharing that bucket must use equivalent
options.

## HTTP Downloader

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

### Headers

`Headers` wraps the standard `http::HeaderMap`, preserving case-insensitive names, raw response
values, and repeated fields. `set` replaces a name and `append` adds another value; Rules input stays
single-valued. Request Snapshots normalize names and serialize every name to a value array, while
non-string Request values are rejected at the Snapshot boundary.

### Cookies

Each Request carries a serializable CookieStore snapshot. Every response cookie is applied with
standard Domain, Path, Secure, Max-Age, and Expires rules before parsing, and descendant Requests
carry the resulting lineage through Scheduler recovery. Parallel siblings keep independent snapshots.
On a cross-origin follow or redirect, source headers are removed and the store is reduced to cookies
applicable to the target URL, so unrelated credentials cannot enter the target Request Snapshot.
Cross-site public-suffix Domain attributes are rejected; an identical public-suffix Request host is
normalized to HostOnly. A raw `Cookie` header is never a second session source, so code and Rules
must use the Request cookie API.

## Rate Limiting

Each `rate_limit` group fixes its interval while active. Reusing the group with a conflicting QPS is
invalid configuration; an inactive group is removed lazily only after its next permitted instant has
passed. The default `middleware::rate_limit::Memory` is Worker-local.
`contrib::middleware::rate_limit::Redis` uses Redis server time to reserve one shared schedule per
group across Workers; it derives the key only from the explicit group or, when omitted, the Request
URL host, never from `task_id` or Worker identity.

## Response Text

The v3 response-text contract keeps `Response.body` as the Downloader-delivered bytes after HTTP
content decoding and before character transcoding, and centralizes character decoding in
`Response::text()`. Encoding precedence is BOM, a valid `Content-Type` charset, an HTML
meta declaration within the first 1024 bytes for HTML or a missing MIME type, then UTF-8. Malformed
sequences use Unicode replacement semantics; the runtime performs no statistical charset guessing.
`Response::json<T>()` uses the same decoded text path.

## Runtime Tracing

Runtime tracing observes the timing and outcome of one claimed Request from acknowledgement through
download, parsing, Tx output, and final settlement. It is optional and disabled by default. It does
not change Scheduler, Downloader, Spider, Item Store, or business `trace_id` semantics.

### Enable the Dependencies

The executable must enable `spider/runtime-tracing` and depend directly on `fastrace` so that it can
install a Reporter:

```toml
[dependencies]
fastrace = { version = "0.7.18", default-features = false, features = ["enable"] }
spider = { path = "../spider", features = ["runtime-tracing"] }
```

When using the API Scheduler, enable `contrib/runtime-tracing` instead. It also enables
`spider/runtime-tracing` and allows the API Scheduler to propagate tracing context to a trusted
Master:

```toml
[dependencies]
contrib = { path = "../contrib", features = ["runtime-tracing"] }
fastrace = { version = "0.7.18", default-features = false, features = ["enable"] }
```

The Cargo feature alone does not sample Requests. The executable must also install one process-wide
Reporter and call `with_tracing` for the Engine run. When the feature is disabled, no spans are
generated even if the program calls `Tracing::all()`; Engine continues through the non-tracing path.

### View Traces Locally

`ConsoleReporter` writes complete `SpanRecord` values to the process standard error stream (`stderr`),
so they are visible when the program runs in a local terminal:

```rust
use fastrace::collector::{Config, ConsoleReporter};
use spider::{engine, trace};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The Reporter is process-wide and must be installed before Engine creates spans.
    fastrace::set_reporter(ConsoleReporter, Config::default());

    let mut engine = engine::Engine::new()
        .with_spider(BasicSpider::new())
        .build()
        .with_tracing(trace::Tracing::all());

    // Preserve the result so completed spans are flushed even when Engine fails.
    let result = engine.start().await;
    fastrace::flush();
    result?;
    Ok(())
}
```

`Config::default()` has a maximum background report interval of one second. A long-running Worker
reports periodically, but the executable should still call `fastrace::flush()` before a normal exit
to deliver remaining records immediately. Configure a different interval explicitly when needed:

```rust
use std::time::Duration;

fastrace::set_reporter(
    ConsoleReporter,
    Config::default().report_interval(Duration::from_millis(500)),
);
```

Console output can also be redirected to a file. Because ConsoleReporter uses `stderr`, regular error
logs enter the same file:

```bash
cargo run --bin <your-bin> 2> runtime-traces.log
```

The fastrace ConsoleReporter and the codebase's `tracing::warn!` or `tracing::error!` calls are separate
pipelines. The application's `tracing` Subscriber still controls the latter; `RUST_LOG` does not
filter ConsoleReporter spans. In production, replace ConsoleReporter with a remote implementation of
`fastrace::collector::Reporter`; Engine configuration stays the same, while the application chooses
the concrete backend and companion crate rather than crawler fixing a Jaeger, Datadog, or
OpenTelemetry integration.

### Full Tracing and Sampling

Trace every Request during local debugging:

```rust
let tracing = trace::Tracing::all();
```

Production deployments will normally sample by ratio:

```rust
let tracing = trace::Tracing::sample(0.1)?; // approximately 10%

let mut engine = engine::Engine::new()
    .with_spider(BasicSpider::new())
    .build()
    .with_tracing(tracing);
```

`Tracing::sample(ratio)` accepts only finite values in `0.0..=1.0`. `0.0` disables sampling, `1.0`
is equivalent to `Tracing::all()`, and NaN, infinity, or an out-of-range value returns an error.
`Tracing::default()` also disables sampling. Configuration is frozen when that Engine run starts and
is not hot-reloaded.

Sampling is not randomized again for every claim. The decision is a stable function of Request ID,
Request version, and Worker ID, so another execution of the same Request version on the same Worker
keeps the same decision. A Request executed by a different Worker can have a different sampling
result. Internal download, parse, and Scheduler operation retries do not resample the root trace.

### Trace Structure

Every sampled claimed Request creates one independent `crawler.request` root span. Engine does not
create a root around the whole Worker, preventing a long-running process from accumulating one
unbounded trace. Multiple Requests in one business Trace therefore have different fastrace TraceIds;
query the root span's `crawler.trace_id` property to aggregate one business run.

The root span records these bounded properties:

| Property | Meaning |
| --- | --- |
| `crawler.task_id` | Task identity |
| `crawler.trace_id` | crawler business-run identity |
| `crawler.request_id` | current Request identity |
| `crawler.node` | current node |
| `crawler.version` | Request execution version |
| `crawler.worker_id` | current Worker identity |
| `crawler.mode` | `http` or `browser` |

Depending on the executed path, the root can contain these spans:

| Span | Scope |
| --- | --- |
| `scheduler.ack` | Confirm execution ownership before Downloader starts |
| `crawler.execute` | Main Middleware, download, and parse lifecycle |
| `middleware.before_download`, `middleware.after_download`, `middleware.before_parse` | Corresponding Middleware stage |
| `downloader.fetch` | One download attempt |
| `executor.parse` | One complete parse attempt |
| `middleware.error_download`, `middleware.error_parse` | Terminal download or parse error callback |
| `output.requests`, `output.items` | Request or Item output emitted through Spider Tx |
| `middleware.before_scheduler`, `scheduler.push` | Admission and enqueue of new Requests |
| `middleware.before_item`, `item_store.submit` | Item admission and persistence |
| `middleware.error_item` | Terminal Item submission error callback |
| `scheduler.refresh_lease` | Execution-lease refresh for long work |
| `scheduler.success / failure / release` | Success, failure, or unstarted-release settlement |

Stages that do not run produce no placeholder span. For example, a path without retries has only one
download or parse span, and a path without Item output has no `output.items`. The Request root begins
after Scheduler has claimed the Request; it does not cover idle Engine polling or `next_requests`.

Each operation span uses `span.status_code=ok|error`; failures record only a bounded `error.type`
classification. Retry spans record one-based `retry.attempt`, Tx output records `output.count`, and a
download records `http.request.method` plus `http.response.status_code` after a successful response.
The Reporter record also contains TraceId, SpanId, parent relationship, start time, and duration.

### Context and Data Boundaries

A fastrace TraceId is runtime telemetry only. It never replaces crawler's business `trace_id` and is
never persisted in a Request, Trace Snapshot, Payload, Item, failure snapshot, or Rules DSL. Tx
Request/Item Events privately carry the current span context in process so asynchronous output stays
in the Request trace that produced it. This context is not a public or persisted contract.

In distributed operation, W3C `traceparent` is injected only when a Worker uses
`contrib::scheduler::api::Api` to call its trusted Master. The framework does not send that header to
crawl targets, redirect targets, AI providers, or Redis. An API Scheduler call outside an active
Request context does not synthesize a `traceparent`. Transport retries for one API operation keep the
same header. Whether Master accepts and exports the context belongs to the separate Master project.

Spans exclude response content, Request bodies, Item content, AI prompts, API keys, cookies, proxy
credentials, complete URLs, and raw error text. A business identifier is recorded directly only when
it is no longer than 128 bytes and contains no control character; otherwise it becomes a stable
`sha256:<hex>` token. Directly recorded identifiers are still visible to the Reporter, so credentials
or tokens must not be used as Task, Trace, Request, node, or Worker IDs. This boundary applies to
fastrace spans only; application logs need their own redaction policy.

### Troubleshooting Missing Local Output

Check these conditions in order:

1. The executable was compiled with `spider/runtime-tracing` or `contrib/runtime-tracing`.
2. `fastrace::set_reporter(...)` ran before Engine startup. Spans created before Reporter setup are ignored.
3. The Runtime uses `with_tracing(Tracing::all())` or a nonzero `Tracing::sample(...)` ratio.
4. Scheduler actually claimed and executed a Request; an empty queue and idle polling produce no `crawler.request`.
5. The executable called `fastrace::flush()` before exit and the terminal or container captures `stderr`.

Engine continues normally when no Reporter is installed, a Request is not sampled, or the compile-time
feature is disabled. Reporting failures do not participate in Request success, failure, or retry logic.

## Development

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo test --workspace --all-targets --features runtime-tracing
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --features runtime-tracing -- -D warnings
cargo doc --workspace --no-deps
```

Each Scheduler owns its Worker identity and supported download modes; Engine supplies only frozen
concurrency to `open(concurrency)` and a batch size to `next_requests(limit)`. Capability filtering
must remain atomic with claim. Redis and MySQL are the available
persistent Scheduler implementations and are covered by the shared Scheduler conformance suite.
The Worker-side `contrib::scheduler::api::Api` adapter is also implemented. Its corresponding Master
service is not part of this workspace. This repository maintains only the Worker-side adapter and its
client-side HTTP contract; a separate project owns the Master service, server-side protocol, database,
Cron, Control API, frontend, and their design.
Optional `fastrace` runtime tracing is implemented; a real Browser Downloader and mixed HTTP/browser
end-to-end execution remain v5 work. AI provider configuration is already Worker-local: one reusable
`ai::OpenAI` provider is injected through `Engine::with_ai`, while Rules retain only the prompt.
Memory uses the internal identity `local`, defaults to HTTP mode, and can replace its capability set
with `Memory::with_modes(...)`; it does not register or heartbeat. Redis, MySQL, and API require stable Worker
ID, host, and version values on the Scheduler, while `with_modes(...)` freezes their download
capabilities. Missing metadata or an empty mode set is rejected during Scheduler configuration/open.

Media normalization does not download files. Item attachment downloading is planned as an
independent v5 change alongside, but not dependent on, the Browser Downloader.

Backend and API integrations can validate a complete rules document with `Config::validate()` or
validate one middleware declaration with `middleware::check(&spec)` before saving it.
