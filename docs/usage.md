# Crawler Usage Guide

[Project README](../README.md) | [简体中文](usage.zh-CN.md) | English

`crawler` is a Rust crawler runtime. Its default single-process topology uses the in-memory
Scheduler, HTTP downloader, middleware hooks, async Spider handlers, CSS healing, explicit AI
selectors, and local JSONL item output. `contrib` also provides Redis and Worker-side HTTP API
Schedulers for durable multi-Worker queues while preserving the same Engine contract. The `master`
crate is an Axum/MySQL control plane, not a Scheduler. The runtime includes Task/Trace run seeds,
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
leases, settlements, and statistics. Each `processing:<mode>` ZSET is the only active
execution-lease projection and uses `lease_time` as its score; the Request Hash remains the
authoritative state. Item persistence is a separate `item::Store` dependency and never enters the
Redis Scheduler.

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

## Selectors

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
are errors; JSON selection has no Healing or AI fallback.

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

## API Scheduler and Master

`contrib::scheduler::api::Api` is the Scheduler that a remote Worker passes to
`Engine::with_scheduler(...)`. It translates the existing Scheduler and Init contracts into the
Master Worker API; it is not a thin MySQL client. `Api::initializes_run()` is `false`, so a remote
code Worker consumes runs already dispatched by Master rather than creating a local Trace or calling
`Spider.start()`. It still needs its local Rust Spider to resolve stable code nodes and run business
handlers.

```rust
use contrib::scheduler::api::Api;
use spider::{engine, net};

let scheduler = Api::new(master_url, worker_token)?
    .with_namespace("crawler")?;

let mut engine = engine::Engine::new()
    .with_worker_id("worker-01")
    .with_modes([net::Mode::Http])
    .with_scheduler(scheduler)
    .with_spider(Newspaper::new())
    .build();

engine.start().await?;
```

`master` is a separate Axum service backed by private MySQL 8.0.19+ storage. Configure its database
URL, namespace, Worker token, control token, and runtime limits in the strict YAML template at
`master/etc/master-api.yaml`, then start it explicitly with that file:

```bash
cargo run -p master -- --config master/etc/master-api.yaml
```

Master serves plain HTTP. Production deployments must terminate TLS in a trusted reverse proxy or
load balancer before traffic reaches this listener; bearer tokens must never cross an untrusted
network in plaintext.

The checked-in template leaves both tokens empty and will not start until deployment supplies distinct
credentials. Runtime capacities and retention use integers with fixed units:

```yaml
api:
  max_size: 67108864
history:
  ttl: 172800
  cleanup_limit: 1000
```

`api.max_size` is a byte count and `history.ttl` is a second count. Both fields accept only positive
YAML integers; strings, including values such as `"64MiB"`, `"48h"`, or `"67108864"`, are rejected.

The control plane is split by responsibility: `master/src/handler/` contains Axum extraction and
route handlers, `master/src/logic/` contains resource operations, `master/src/svc.rs` owns the
shared service context, and `master/src/types/` contains the unified Worker/control DTOs. Handlers
do not call MySQL directly. `master/src/config/` loads and validates runtime YAML, while
`master/src/store/mysql/` remains the private persistence implementation.

Both sides default to a 64 MiB API message limit. Set a Worker's receive capacity with
`Api::with_max_response_bytes(...)` before opening it, and set Master's request/response limit with
YAML `api.max_size` as an integer byte count or use `master::Config::with_api(...)`. Master accepts
values from 1 KiB through 4 GiB minus one byte. Startup rejects a
Master limit larger than the Worker's configured capacity, so 64 MiB is a default rather than a
fixed system-wide limit. The Worker serializes every outbound JSON message into the same bounded
capacity before network I/O and reuses those immutable bytes across transport retries. Master checks
the applicable bearer credential and namespace before it reads or parses a request body, so an
unauthorized malformed or oversized body is rejected as unauthorized rather than consuming the JSON
body path.

The Worker and control tokens are distinct, valid HTTP bearer credentials. A Worker token may use
only the Scheduler Worker API. A control token publishes Task definitions and reads the control
plane through `/v1/control/tasks`, `/v1/control/traces`, `/v1/control/requests`,
`/v1/control/workers`, and `/v1/control/items`; list responses
use bounded keyset pagination, and large snapshots or Item data appear only in detail responses.
Workers never receive direct MySQL access, and Master itself cannot be passed to
`Engine::with_scheduler(...)`.

Master uses private MySQL connections at `READ COMMITTED`. Namespace and identity columns use
binary `utf8mb4_0900_bin` collations, so identifiers and idempotency keys remain byte-sensitive.

Master stores a Task as either Rules DSL or serialized code seeds, never Rust handlers. Its Cron
creates a fresh Trace and initial Requests for up to the configured dispatch limit of due Tasks,
recovers expired or offline-Worker leases through the normal retry state machine, and does not
start, stop, or supervise Workers. Task publication performs static validation only; Cron materializes
the persisted Rules or code seed and queues it directly, without running a Worker's
`before_scheduler`, Middleware, or Dedup during seed dispatch. A deterministically invalid stored Task
is quarantined as `failed` with its error exposed through the control API; it does not block later due
Tasks, and publishing a corrected definition makes it schedulable again. Claim validates candidates
in 128-row storage pages before assigning one lease timestamp to the accepted result. One call
quarantines at most 128 invalid candidates and then yields; later claims continue cleanup. Valid-only
scans may cross pages until the requested count or response capacity is reached. A response includes
a Trace Snapshot at most
once per `trace_id`. A Worker fetches and caches an omitted Trace independently, so a transportable Request is
not rejected merely because its Trace would exceed the combined response limit. Unresolved local Init/Item operation
keys have a fixed five-minute lifetime from creation. Master requires `history.ttl` of at least
`max(lease timeout, 5m30s)` so persistent operation and completion replay records outlive that window.
The configured retention values then drive bounded cleanup of terminal Request, completion, and
operation history; Item, Trace, Task, and trace-stat retention remain separate work. A remote Worker
is a finite Engine lifecycle: it registers immediately on first use, only rewrites its modes when they
change, refreshes a stale heartbeat while it claims work, exits after its compatible work is drained,
and stops its local heartbeat on Scheduler close. Browser downloading, a direct
MySQL Scheduler, and `fasttrace` runtime tracing are not part of this topology.

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
`.with_store(...)` replaces Item persistence.

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

## Development

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
```

The Scheduler contract receives the Engine-owned Worker ID and supported download modes for
`next_requests` and pending-work checks; filtering must be atomic with claim. Redis and the
Worker-side API Scheduler are available persistent Scheduler implementations. Master is the separate
Axum/MySQL control plane behind the API Scheduler, not another Scheduler implementation. A direct
MySQL Scheduler and `fasttrace` runtime tracing remain separate work; a real Browser
Downloader and mixed HTTP/browser end-to-end execution remain v5 work. AI provider configuration is
already Worker-local: one reusable `ai::OpenAI` provider is injected through `Engine::with_ai`, while
Rules retain only the prompt. Redis is covered by the shared Scheduler conformance suite.
Engine defaults to `worker-1` and HTTP mode. `with_worker_id(...)` and `with_modes(...)` replace
those frozen startup values; an empty Worker ID or mode set is rejected before execution.

Media normalization does not download files. Item attachment downloading is planned as an
independent v5 change alongside, but not dependent on, the Browser Downloader.

Backend and API integrations can validate a complete rules document with `Config::validate()` or
validate one middleware declaration with `middleware::check(&spec)` before saving it.
