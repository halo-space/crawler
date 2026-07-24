# crawler Architecture and Feature Overview

This document describes the features implemented by the current source tree, the core runtime model, and the extension boundaries. It is a standalone architecture overview.

## 1. Positioning and Current Scope

`crawler` is a Rust 2024 workspace. Its default runnable topology is a single-process asynchronous crawler runtime with the in-memory Scheduler and HTTP Downloader. `contrib` also provides a Redis 7 standalone Scheduler for durable multi-Worker queues. Code mode and YAML Rules mode share the same Engine, Request, Response, Item, Middleware, Scheduler, and Payload objects. Rules mode is not a second runtime.

The workspace contains four crates:

| Crate | Responsibility |
| --- | --- |
| `spider` | Core runtime, public data objects, extension contracts, default Memory Scheduler, and HTTP Downloader |
| `macros` | The `#[spider]` procedural macro, including code-mode construction, node registration, and dispatch bindings |
| `contrib` | Replaceable external Scheduler implementations; Redis is implemented, while API and MySQL remain future work |
| `examples` | Runnable code-mode and Rules-mode examples |

### 1.1 Capability Status

| Capability | Status | Notes |
| --- | --- | --- |
| Code mode | Implemented | `#[spider]`, asynchronous handlers, `Tx.request`, and `Tx.item` |
| YAML Rules mode | Implemented | Validation, request graph, extraction, binding, transforms, Item Schema, and downstream Requests |
| Memory Scheduler | Implemented | Priority/FIFO queue, delayed Requests, leases, lease refresh, retry, terminal states, Traces, and statistics |
| HTTP Downloader | Implemented | Bounded decoded body, whole-fetch timeout, structured headers/cookies, redirects, proxy/TLS isolation, and bounded idle clients |
| Response charset decoding | Implemented | Deterministic BOM/header/HTML-meta/UTF-8 selection in `Response::text()` while preserving post-content-decoding bytes |
| Browser Downloader | Not implemented | The current stub returns `UnsupportedMode("browser")`; implementation is planned for v5 |
| CSS Selector | Implemented | `Response::css()` returns the native `scrape_core::Soup` |
| CSS Healing | Implemented | Deterministic whole-document candidate scoring after an exact CSS miss; opt-in only |
| Regex and JSON | Implemented | Regex selection and code-mode `Response::json<T>()` |
| AI Selector | Implemented | Explicit OpenAI-compatible JSON-object extraction, independent of CSS Healing |
| AI runtime configuration | Implemented | One reusable Worker-local Client is injected with `Engine::with_ai`; provider configuration does not enter Rules or Trace snapshots |
| Middleware | Implemented | Lifecycle Registry plus built-in validate, dedup, rate limit, and retry capabilities |
| Item output | Implemented | Schema validation, media normalization, JSONL output, and submission-failure snapshots; attachment download is planned for v5 |
| Capability-aware claiming | Implemented | Memory claims and checks pending work only within the current Worker's configured Request modes |
| Redis Scheduler | Implemented | Redis 7+ standalone only; namespaced complete Scheduler/Init contract, Lua-atomic transitions, and Redis Stream Item output |
| API/MySQL Schedulers | Planned | Separate v4 `contrib` implementations; each must implement the complete Scheduler contract |
| Master control plane | Planned | A v4 capability, not part of the core Scheduler itself |
| Runtime tracing | Planned | v4 will use `fasttrace`; this is separate from the business `trace_id` |

XPath has been removed from the roadmap. CSS is the sole HTML selector path; the project does not maintain a partial XPath implementation.

## 2. Core Design Principles

1. **One runtime:** Code mode and Rules mode use one Executor. Trace Snapshot data selects the code handler or Rules interpretation path; only run-seed initialization differs.
2. **Scheduler as the distribution boundary:** Switching away from Memory changes only `.with_scheduler(...)` at assembly time. A replacement must implement the complete scheduling semantics, not merely wrap a storage client.
3. **Immediate output:** Requests and Items emitted by parsing enter the Engine through `Tx` immediately. They are not held until a Trace or request graph finishes.
4. **Identity is separate from execution ownership:** Request ID survives retry and recovery; `version`, `leased_by`, and `lease_time` describe one execution right.
5. **Immutable run snapshots:** Each Trace has one immutable Trace Snapshot. Rules snapshots contain the complete DSL; code snapshots never persist Rust handlers.
6. **Explicit recovery semantics:** Lease refresh, release, success, and failure use separate methods. There is no overloaded `finish` operation.
7. **Single-purpose modules:** The Actor coordinates, startup-frozen Worker state owns identity and capabilities, a Request task owns one execution right, an Executor parses, and a Scheduler implements scheduling and submission contracts.

## 3. System Overview

```mermaid
flowchart LR
    C["Code mode<br/>#[spider] + handlers"]
    R["Rules mode<br/>YAML Config + graph"]
    X["Shared Execute contract"]
    T["Tx.request / Tx.item"]
    A["Kameo Engine Actor<br/>single coordinator"]
    S["Scheduler contract"]
    M["Memory Scheduler"]
    Z["Redis 7 Scheduler"]
    W["Request Worker"]
    MW["Middleware Registry"]
    D["Downloader"]
    H["HTTP"]
    E["Unified Executor"]
    O["Request / Item output tasks"]
    P["JSONL Item output"]
    I["Redis Stream Item output"]
    F["Item failure snapshots"]

    C --> X
    R --> X
    X --> A
    A --> S
    S --> M
    S --> Z
    A --> W
    W --> MW
    W --> D
    D --> H
    W --> E
    E --> T
    T --> A
    A --> O
    O --> S
    M --> P
    Z --> I
    O -. on submission failure .-> F
```

The Engine uses one private Kameo Actor as its message-driven coordinator, but it does not force the Scheduler, Downloader, Executor, or AI selector into Actor types. Components retain method-based contracts. The Actor directly owns runtime state and dependencies; Request and output work still runs in independent Tokio tasks so no long I/O blocks message handling.

## 4. Identity, Tasks, and Run Seeds

### 4.1 Identity Hierarchy

```mermaid
flowchart LR
    S["Rust Spider::name()<br/>deployed business implementation"]
    C["Code local run<br/>task_id = Spider::name()"]
    Y["Rules local run<br/>task_id = config.spider.name"]
    T["Task.id / task_id<br/>task definition"]
    S --> C
    S -. shared business code .-> Y
    C --> T
    Y --> T
    T --> R["Trace.id<br/>one dispatched run"]
    R --> Q1["Request.id"]
    R --> Q2["Request.id"]
    R --> Q3["Request.id"]
```

- Rust `Spider::name()` identifies the deployed business implementation. There is no duplicate `spider.id`.
- Rules `config.spider.name` identifies that Rules task and becomes its local `task_id`; it is not required to equal Rust `Spider::name()`.
- `Task.id` identifies a task definition. A persistent control plane may create several parameterized or scheduled Tasks from one deployed Spider.
- `Trace.id` identifies one Task run. Each periodic dispatch should create a new Trace.
- `Request.id` identifies one logical Request and remains unchanged across lease recovery and queue retry. For current-Request `Tx.request` output, the framework derives child IDs from the parent ID, the canonical initial child specification, and its occurrence in that parse attempt. Replaying the same output therefore reuses the ID; `Request::with_id` preserves an application-owned ID.
- Local Memory has no persistent Task table. A code run uses Rust `Spider::name()` as `task_id`; a Rules run uses `config.spider.name`.
- Item ID is not part of this hierarchy. `Tx.item` generates a UUID v7 when an Item has no ID. That ID identifies a data instance and does not provide business deduplication.

### 4.2 Run Seed

The logical input of one run is:

```text
task_id + trace_id + immutable Trace Snapshot + initial Requests
```

A Trace Snapshot holds run-level configuration shared by Requests: `task_id`, parameters, optional attachment configuration, persistence target, and priority. It has no schema version, Task revision, or derived Request-mode collection. A Rules Snapshot additionally contains the complete DSL, including its optional non-empty `spider.version` and valid IANA `spider.timezone`; a code Snapshot has an empty `dsl`.

A Request Snapshot stores its stable `node` and executable request fields. It never stores handlers, function pointers, closures, or process-local objects:

- A Rules Request is restored by obtaining the Trace DSL through `trace_id` and validating its node against that DSL.
- A code Request still contains only a node name after restoration. The current Worker resolves that node through its local `#[spider]` registry.
- Claiming, retry, and recovery preserve the original Request `task_id / trace_id`; a Worker must not generate or overwrite them.

### 4.3 Current Startup Paths

```mermaid
flowchart TB
    A["Code Engine starts"] --> B{"Scheduler.initializes_run()"}
    B -- "true: Memory" --> C["Generate local task_id / trace_id"]
    C --> D["Initialize Code Trace Snapshot"]
    D --> E["Call Spider.start()"]
    E --> F["Tx submits initial Requests"]
    B -- "false: remote default" --> G["Do not create a local Trace<br/>do not call Spider.start()"]
    G --> H["Claim existing Requests"]
    F --> H

    I["Rules Engine starts"] --> J["Validate and freeze the full DSL"]
    J --> L["Run before_scheduler<br/>for every initial Request"]
    L --> K["Atomically initialize Trace Snapshot<br/>and accepted Requests"]
    K --> H
```

`scheduler::Init::initializes_run()` currently controls code-mode local initialization. Memory returns `true`. A remote Scheduler defaults to `false`, so a code Worker can consume a run already published by an external task source without creating a local Trace or calling `Spider.start()`.

Rules mode treats the loaded YAML as the definition of this run. Before claiming starts, every generated initial Request passes through the same `before_scheduler` admission path used by Tx output, then `rules::Init` atomically stores the Trace Snapshot and accepted Requests. If every Request is filtered, the empty run still stores its Trace Snapshot and finishes normally. A future externally dispatched Rules worker must preserve this snapshot contract instead of introducing a second identity model in the Worker.

## 5. Engine Actor

The outer `Runtime::start()` order is fixed:

```text
validate runtime limits
-> open Scheduler / Downloader / local snapshot directory
-> before_spider
-> initialize or attach to a run
-> spawn and drain the Engine Actor
-> after_spider
-> close Downloader / Scheduler
```

`Engine` in `engine/actor.rs` is the sole coordinator. It is a real Kameo Actor and owns:

- Executor startup, poll, and producer-idle observation task handles;
- at most one active Scheduler claim;
- the set of active Request tasks;
- the set of active Tx output tasks;
- Tx Event capacity, producer activity, and the first terminal error;
- shared Scheduler, Downloader, Executor, Middleware Registry, and optional Item snapshot store references.

Startup, claim, Request, output, poll, and producer-idle completions return as separate Actor messages. Every spawned task catches panic and reports completion. An accepted output failure normally returns to its waiting `Tx` caller; if that caller has been cancelled, the output completion reports the undelivered error to the Engine instead of silently succeeding. Kameo's mailbox is unbounded for internal messages; the explicit Event capacity controls only external `Tx` output and therefore remains independent from internal completion traffic.

### 5.1 Three Independent Limits

| Setting | Default | Meaning |
| --- | ---: | --- |
| `with_concurrency(n)` | `16` | Maximum number of active Request tasks |
| `with_claim_limit(n)` | concurrency | Maximum Requests requested by one `next_requests(limit, worker_id, modes)` call |
| `with_event_limit(n)` | `32` | Maximum accepted Events whose Actor handler has not started |

The values are validated and frozen when the Engine starts. They are not hot-reloaded and do not replace one another. The actual claim size is:

```text
min(claim_limit, request_concurrency - active_request_tasks)
```

An Event permit is acquired before `Tx` sends an Event and released when the Engine Actor starts its handler. The handler registers an output task and delegates the reply; `Tx` still waits for Scheduler and Middleware processing to finish. Event capacity therefore bounds Events waiting to start, while the Actor's output task set separately prevents early shutdown during processing.

### 5.2 Idle Detection and Exit

An empty `next_requests(limit, worker_id, modes)` result means only that the current claim returned no work. It does not terminate the Engine. The Actor exits only when all of the following are true:

- the Scheduler confirms there are no queued or processing Requests within the current Worker capability scope;
- no startup, claim, poll, or producer-idle observation task is active;
- no Request task is active;
- no output task is active;
- no Event permit is active;
- no tracked Tx producer can still emit an Event.

An empty claim result is valid only for the work state observed by that claim. If a Request,
output Event, or Tx producer changes work while the claim is in flight, the Actor treats the
result as stale and claims again before it can terminate.

This prevents early termination while a list page is producing detail Requests, an Item Event arrives late, or a handler has cloned its Tx for delayed output.

## 6. Scheduler Contract and Memory Implementation

### 6.1 Scheduler Methods

| Method | Single responsibility |
| --- | --- |
| `dir` | Return an optional Worker-local directory used for framework Item failure snapshots |
| `lease` | Return optional lease timeout and refresh interval settings |
| `open / close` | Open and close Scheduler-owned resources |
| `push` | Consume only `Payload.requests`; skip identical replays, atomically insert missing Requests, and reject a conflicting collection |
| `push_items` | Consume only `Payload.items` and submit Items |
| `trace` | Read an immutable Trace Snapshot by `trace_id` |
| `next_requests(limit, worker_id, modes)` | Atomically claim and restore at most `limit` Requests for the supplied Worker identity and modes |
| `has_pending_requests(worker_id, modes)` | Report whether the supplied Worker capability scope still has queued or processing Requests |
| `ack` | Confirm that the Engine accepted a claimed execution right |
| `release` | Voluntarily return execution ownership without consuming a queue retry |
| `refresh_lease` | Extend an acknowledged execution lease |
| `success` | Apply successful settlement and statistics only |
| `failure` | Apply failed settlement, statistics, and queue-level retry only |

`scheduler::Init` adds:

- `initializes_run()`, which declares whether this Engine creates a local run;
- `init(trace_id, snapshot, requests)`, which atomically stores a Trace Snapshot and its accepted initial Requests; an empty collection is valid after admission filtering.

`Payload` is the single transport envelope shared by these methods; the design does not add parallel Batch or Receipt structures. It carries Request execution identity, state, error, timing, statistics, and the `requests / items` output collections. Every Scheduler method rejects fields unrelated to its own semantics: `push` accepts Requests only, `push_items` accepts Items only, and settlement Payloads require both collections to be empty.

For `has_pending_requests`, `modes` defines the capability scope. A processing Request with a matching mode remains pending for every Worker with that capability, regardless of its current `leased_by` value. The `worker_id` identifies and validates the caller; it does not narrow the processing set to leases owned by that Worker. This conservative rule prevents a compatible Worker from exiting before lease recovery or before an in-flight Request can emit more compatible work.

Every `contrib` Scheduler must fully implement these state and identity semantics. The Engine must not contain Redis-, MySQL-, or API-specific branches.

### 6.2 Memory State Model

```mermaid
stateDiagram-v2
    [*] --> Pending: init / push
    Pending --> Processing: next_requests + lease + version
    Processing --> Processing: ack / refresh_lease
    Processing --> Done: success
    Processing --> Pending: failure with retry remaining
    Processing --> Failed: failure with retry exhausted
    Processing --> Pending: release
    Processing --> Pending: expired lease recovery
    Done --> [*]
    Failed --> [*]
```

Memory atomically maintains its queues, known Request IDs, processing records, acknowledgements, completions, Trace Snapshots, and Trace statistics behind one mutex:

- duplicate Request IDs within one Payload are rejected; replaying an existing ID with the same initial Request Snapshot is a no-op, while a different Snapshot conflicts and rejects the whole collection;
- a collection containing matching existing Snapshots and new Requests atomically inserts only the missing Requests;
- Memory keeps a SHA-256 digest of each canonical initial Request Snapshot for replay comparison; this is Scheduler identity protection, not URL/business deduplication;
- ID uniqueness prevents the same Request object from being enqueued twice; URL and business-field deduplication remain Dedup Middleware responsibilities;
- the ready queue uses higher `priority` first and FIFO within one priority; a delayed queue holds future `next_time` values;
- `Memory::new()` owns only process-local Scheduler state; Engine supplies a non-empty Worker ID and mode set for every claim and pending check;
- claim selects the highest-priority FIFO Request supported by the supplied mode set without changing incompatible queue entries, and pending checks use the same Worker scope;
- claiming changes a Request to `processing`, records `leased_by / lease_time`, and advances `version`;
- the default lease timeout is 30 seconds and the refresh interval is 10 seconds; `Memory::with_lease(...)` can replace them with positive whole-millisecond durations representable by the runtime clock;
- `ack` is idempotent for the same valid identity and records only execution confirmation; `refresh_lease` updates the acknowledged lease timestamp;
- an unacknowledged expired claim consumes no retry and records no failed Worker, while an acknowledged expiry appends the current Worker and consumes one queue attempt;
- recovery and retry return a Request to pending without changing `version`; the next successful claim creates the next execution generation;
- ordered duplicate-free `Request.failed_workers` is preserved and validated by the strict Request Snapshot contract;
- repeated `success / failure` with the same identity and terminal state is idempotent, while mismatched task, trace, node, worker, version, or state is rejected;
- `failure` preserves Request ID while advancing queue retry count, then requeues or enters failed when retries are exhausted.
- restoration, version/retry overflow, and queue-conversion failures produce explicit terminal diagnostics with the original Request ID instead of silently dropping work.

Memory is an unregistered process-local Scheduler. Engine owns the Worker identity and frozen mode
capabilities, then supplies them to each claim; Memory does not discover or select among a fleet of Workers;
registration, heartbeat, and cross-Worker eligibility belong to the concrete v4 contrib Scheduler or API control plane that needs them. It reads
Trace Snapshots from an immutable in-process map and has no remote cache, transport retry, or
temporary Trace-storage failure path. It does not restore its Request queue after process exit, and
the current implementation does not write local Request files under `data/requests/`.

### 6.3 Redis Implementation and Operations

`contrib::scheduler::redis::Redis` is a complete persistent implementation of the same `Scheduler`
and `Init` contract. It is not a Redis client wrapped by Engine: Redis itself owns immutable Trace
Snapshots, canonical Request replay identity, capability-scoped queue ordering, leases, acknowledgements,
release, refresh, settlement, retry, terminal records, statistics, and Item submission. The Engine only
switches its assembly dependency:

```rust
let scheduler = contrib::scheduler::redis::Redis::new("redis://127.0.0.1:6379")?
    .with_namespace("crawler")?;

let engine = spider::engine::Engine::new()
    .with_scheduler(scheduler)
    .with_spider(MySpider::new())
    .build();
```

`Redis::new` validates the connection URL and `with_namespace` validates the key namespace. Every
Redis key is scoped below that namespace, and `close()` drops only local client resources. It never
deletes persisted work; a new Scheduler instance using the same URL and namespace can continue it.
Redis returns `initializes_run() == false`, so a code-mode Worker consumes externally initialized
runs; explicit Rules `init` remains atomic and supported.

All state transitions that need shared atomicity execute in Redis Lua scripts. Claim atomically
recovers expired leases, selects compatible work in global priority/FIFO order, increments the
execution version, and establishes ownership using Redis server time. Init and Request replay also
remain all-or-nothing: a conflicting Request Snapshot rejects the whole collection, while an exact
replay is a no-op. Transient connection and availability failures remain `Scheduler::Unavailable`,
rather than being reclassified as ownership loss.

Active ownership has one mode-scoped projection: `processing:<mode>` is a ZSET whose member is the
opaque Request token and whose score is `lease_time`. It supports both capability-scoped pending
checks and expiration scans; there is no separate global lease index. The Request Hash is the source
of truth. A valid processing Hash repairs a stale score or wrong-mode projection without changing
retry state, while an invalid Hash is quarantined. Every transition clears both known mode
projections before publishing its single current membership when it changes active ownership.

Recurring maintenance is deliberately bounded under backlog: one `next_requests` invocation recovers
at most 64 expired leases from each mode and inspects at most 128 processing records across both
modes. The per-mode recovery share prevents equal Redis timestamps in one backlog from starving the
other mode. It promotes
and inspects at most 128 delayed Requests for each requested mode. Additional due entries remain
indexed for later claims rather than being dropped.
When recovery, promotion, or ready-queue selection finds a missing record, it removes the dangling
index entry. A stale processing projection backed by a valid Hash is repaired; malformed Request or
queue state is quarantined by removing its active entries, recording a terminal failure and
completion, then continuing with later valid Requests. This does not hide shared-index corruption:
an invalid Redis type for a shared index rejects the claim before it mutates state. Ready-queue
cleanup is likewise bounded to 128 discarded invalid entries before the claim yields to a later
invocation. Claim returns the persisted digest with the immutable Request Snapshot; Rust recalculates
the canonical digest before overlaying mutable execution fields. A mismatch follows token-scoped
recovery and is never returned as executable work. When that digest is valid, its immutable retry
limit controls recovery and repairs a mismatched mutable Hash value. Recovery failure for one damaged
record does not withhold valid Requests already claimed in the same atomic operation; the damaged
record remains processing for normal lease-timeout recovery. The current internal key layout does not migrate
older Redis namespaces.

`push_items` serializes the full collection before mutation and appends one Redis Stream entry for
each accepted non-empty Item Payload. The entry preserves the Payload identity, framework Item IDs, business
Item JSON, and available Trace metadata. Submission is at-least-once: retrying the same Payload
creates another complete Stream entry, and Redis does not provide business Item deduplication. Item
Stream retention and replay are independent concerns; this Scheduler does not trim the Stream.

The implementation targets one Redis 7+ standalone primary. It intentionally does not support
Redis Cluster, because its namespace spans several keys and its Lua transitions rely on
single-instance atomicity. Cluster is a future separate Scheduler design, not a connection flag.
Durable deployments must enable AOF (`appendonly yes`) and set `maxmemory-policy noeviction`.
`appendfsync` is deliberately an operator choice: `always` trades throughput and latency for a
smaller persistence window, while `everysec` commonly offers higher throughput with up to roughly
one second of acknowledged-write exposure. Operators must also monitor Redis capacity and choose an
explicit Item Stream retention policy.

## 7. Complete Request Lifecycle

```mermaid
sequenceDiagram
    participant A as Engine Actor
    participant S as Scheduler
    participant W as Request Worker
    participant M as Middleware
    participant D as Downloader
    participant E as Executor
    participant T as Tx / Event

    A->>S: next_requests(n, worker_id, modes)
    S-->>A: Requests in processing with lease
    A->>W: run(request)
    W->>S: ack(payload)
    par lease maintenance
        W->>S: refresh_lease(payload) periodically
    and download and parse
        W->>M: before_download(request)
        M->>D: fetch(request)
        D-->>M: Response
        M->>M: after_download / before_parse
        M->>E: parse(request, response)
        E->>T: request([...]) / item([...])
        T->>A: Event with completion reply
        A->>S: push(payload) / push_items(&payload)
        A-->>T: handled
    end
    alt execution succeeded
        W->>S: success(payload)
    else execution failed
        W->>S: failure(payload)
    end
```

The important semantics are:

- `ack` happens before Downloader execution. A failed ack prevents the download.
- `release` returns unfinished ownership voluntarily; it is not failure and does not increment retry count.
- Lease maintenance covers download, parsing, and retries of final `success / failure` settlement.
- Immediately before each `next_requests` attempt, Engine records a monotonic `claim_started` and
  retains it only for a successful call. For every Request returned by that call, that instant plus
  the Scheduler timeout is its initial local lease deadline. Scheduler and network latency therefore
  consume part of this conservative budget; Engine never compares a Worker's wall clock with Scheduler
  server time. A successful refresh begins the next local deadline from its own monotonic start.
- Before a final Payload exists, explicit ownership loss, lease expiry, or another terminal refresh error cancels execution and prevents settlement. Transient refresh errors retry within the current lease deadline while execution continues.
- After execution produces an immutable final Payload, `success / failure` is authoritative. A concurrent refresh error stops further refreshes but cannot cancel settlement; transient settlement errors retry the same Payload without executing the Request again.
- Middleware Retry is a local Worker retry for downloading, parsing, or Item submission.
- Scheduler `failure` is the only queue-level Request retry. The Worker submits it only after local execution retries are exhausted.
- `Tx.request` and `Tx.item` wait for actual Event handling, so parsing cannot report success before the Scheduler accepts its output.
- exhausted Item submission returns into the current parser call and causes the current Request to settle as failed.
- current-Request `Tx.request` output uses a fresh occurrence allocator for each parse attempt. The canonical output includes scheduling intent such as `next_time` and uses a time-stable Cookie view. Parse and queue retries reproduce the same IDs for the same canonical output, while identical outputs within one attempt remain distinct. Detached Tx output has no parent Request identity and retains at-least-once delivery.
- every emitted Request is checked as one collection against its Tx `task_id / trace_id` before any `before_scheduler` Middleware runs; a Middleware is not allowed to rewrite `id`, `task_id`, or `trace_id`. Because the replay-stable ID is derived before this hook, any hook that changes the remaining Request specification must be deterministic for the same input; time-, random-, or external-state-dependent changes intentionally surface as a Snapshot conflict on replay.
- `Response::follow` always inherits vals and Trace identity. A same-origin target clones the updated Headers and CookieStore; a cross-origin target drops source headers and retains only cookies applicable to the target URL.
- the HTTP Downloader validates every redirect target against `allowed_domains` before sending it. A disallowed redirect is a normal filter, not a download retry or `error_download`; an allowed cross-origin redirect drops source headers and retains only target-applicable cookies.
- every redirect applies all intermediate `Set-Cookie` values before the next hop. On a cross-origin hop, source headers are removed and the CookieStore is reduced to target-applicable cookies before sending.

Proxy and TLS are Request-level download settings. `Http` pools clients by the complete proxy URL
(including credentials) and `tls.accept_invalid_certs`; direct requests use a separate no-proxy key.
An entry can serve concurrent matching Requests, becomes idle after its last handle is released, and
is removed lazily after 90 idle seconds. The pool retains at most 64 idle Clients by default and evicts
the oldest idle entry under pressure. Active entries are never evicted and may temporarily exceed the
idle capacity; `Http::with_max_idle_clients` replaces the positive startup-frozen bound. `Http::close()`
clears the pool and invalidates concurrent cold-build insertion, so a client constructed before close
cannot be reattached afterward. Redirects for one Request retain that Request's selected client; proxy
or TLS configuration is never mutated as global Downloader state.

### 7.1 HTTP Transport Bounds and State

`Http` enforces a Worker-level decoded-body ceiling of 64 MiB by default.
`Http::with_max_body_bytes` replaces this positive startup setting. A code or Rules Request may set a
positive `max_body_bytes` no greater than the Worker ceiling; a value above that ceiling fails before
network I/O instead of being clamped. The Downloader consumes decoded chunks and counts their actual
bytes without preallocating the ceiling. An exact-limit body succeeds, while the first byte over the
limit terminates the stream with an explicit error. `Content-Length` may reject an obviously oversized
unencoded response early, but never overrides the actual decoded count.

`Request.timeout` is one monotonic budget for a complete `Http::fetch`: connection establishment, every
allowed redirect, and the final decoded body stream share one deadline. Redirects do not reset it. A
Middleware or Scheduler download retry invokes a new fetch and receives a fresh full budget; parse
retry reuses the existing Response and performs no download. The completed body remains the existing
bounded `Response.body: Bytes`; no public streaming Response, file sink, or attachment API is added.

The Downloader follows only `301`, `302`, `303`, `307`, and `308`. A redirect that changes a
body-bearing Request to GET discards the body and removes `Content-Length`, `Content-Type`,
`Content-Encoding`, `Content-Language`, `Content-Location`, and `Transfer-Encoding` on subsequent hops.
This cleanup also applies when the previous body value was empty but carried body metadata. An original GET or HEAD does not lose
those headers merely because of its method. Other 3xx responses, including `304`, are returned without
following `Location`.

`Headers` is a wrapper over `http::HeaderMap<HeaderValue>`. Names use standard case-insensitive identity,
Response values retain their raw bytes and received multiplicity, `set` replaces every value for a name,
and `append` preserves existing values. Rules Request headers remain a single-value map and therefore
apply through `set`. Request Snapshot serializes normalized names to non-empty arrays of string values;
a non-string Request value makes Snapshot creation fail. Response is not serialized and may retain raw
non-string values.

`Cookies` wraps `cookie_store::CookieStore`. A Request carries its complete lineage snapshot, the
Downloader selects only values applicable to each actual URL, and every response `Set-Cookie` updates
the store before business parsing. Domain, Path, Secure, Max-Age, and Expires behavior comes from the
standard store. `Response::follow` and Rules edge construction copy the updated store into descendants,
including Request Snapshot and cross-Worker recovery. Already queued siblings are immutable copies and
do not observe later mutations. Cross-origin construction removes source headers and reduces the copied
store to target-applicable cookies, preventing unrelated credentials from entering the new Snapshot;
cross-site public-suffix Domain attributes are rejected, while an identical public-suffix Request host
is normalized to HostOnly. Raw `Cookie` headers are removed before transport so CookieStore remains
the only session source. Memory replay comparison includes stored Cookie records through a dedicated
stable view. A `Max-Age` cookie keeps its raw relative attribute while omitting the absolute expiry
derived from the current receive time, so Request identity does not change when the same response is
replayed; explicit `Expires` and session state remain part of the view. The public `Cookies`
serializer, lookup, and transport still ignore expired records. Request Snapshot
serialization preserves those records so a later cross-Worker restore retains the same replay
identity. There is no Trace-wide live or distributed CookieStore.

### 7.2 Response Character Decoding

The v3 response charset contract keeps `Response.body` as the Downloader-delivered payload bytes
after HTTP content decoding and before character transcoding. `Response::text()` is the single decoding
boundary for CSS, Regex, AI, and JSON consumers. It selects the first recognized encoding in this order:

```text
recognized BOM
-> valid Content-Type charset
-> HTML meta in the first 1024 bytes when MIME is text/html or absent
-> UTF-8
```

The BOM is removed from returned text. Empty, malformed, or unknown charset labels fall through to
the next eligible source. Once selected, malformed byte sequences produce `U+FFFD`; they do not
trigger another encoding choice. No statistical detector or site-specific guess participates.
HTML meta follows web prescan rules: UTF-16 labels select UTF-8 and `x-user-defined` selects
Windows-1252; those adjustments do not alter BOM or HTTP-header selections.
`Response::json<T>()` decodes through `Response::text()` before deserialization. Deterministic local
HTTP fixtures provide required regression coverage; live Internet access is not a CI prerequisite.

Each Request execution owns one shared `stats::Delta`, accumulating `total / done / filter / dedup / validate / download` counters by node or `items`. A directly awaited Tx call uses the current task-local Request context and updates that delta. A Tx clone moved to a detached task retains only Trace identity; its output neither changes the settled Request nor delays settlement. The Worker attaches the delta snapshot to the final Payload, and the Scheduler merges it into Trace statistics only on the first `success / failure` settlement; idempotent replay does not double-count it.

## 8. Two Execution Modes

### 8.1 Code Mode

The `#[spider]` macro lets application code declare only Spider fields and asynchronous methods:

- `name` is the unique Spider name;
- `start_urls / start` emit initial Requests;
- `index` is the default node, and additional handler methods register stable nodes;
- handlers use `self.tx.request(Vec<Request>)` for downstream Requests;
- handlers use `self.tx.item(Vec<Item>)` for Items.

The macro generates input checks, construction, node registration, and handler call bindings only. The Engine state machine, Scheduler, Downloader, and Middleware do not live in the procedural macro.

### 8.2 Rules Mode

Rules loads task settings, a request graph, field extraction, bindings, transforms, complete downstream
Request Specs, and Item Schema from YAML. A Rules runtime still requires a Rust Spider through
`with_spider(...)`; the Spider provides shared business code and the concrete Item type. `with_rules(config)`
only creates one local Rules Trace seed. Omitting it creates code-mode work or consumes runs already present
in a remote Scheduler. The Rules task name is not required to equal `Spider::name()`.

`Config::validate()` performs complete static validation before execution, including start/target nodes,
Request transport, template context, reserved `idx`, and one-Item-edge-per-node constraints. Backends and APIs
can call the same validator before storing a configuration.

The unified Executor runs a Rules Request in this fixed order:

```text
Trace Snapshot.dsl = Some(config)
-> Rust Spider.index(response.clone())
-> Request.node
-> graph.nodes[node]
-> post-download field extraction
-> bind / transform / template
-> complete Request Specs and/or typed Rust Item construction
-> shared Tx and Scheduler paths
```

The Rust `index` call is the shared business-code entry for every Rules Request. Only after it
returns successfully does the same Executor interpret the declarative node. Code Requests instead
resolve their stable node directly through the Rust Spider registry.

`spider.start[*]` and request edges use the same complete Request Spec. Graph nodes contain parse, bind, and
domain policy only; they never fill transport fields after a Request has been created. URL-array expansion is
stable, writes a reserved one-based `vals.idx` before transport templates render, and resets the index for each
new expansion. Request and bind templates share one parser. Rendering walks the parsed source once, so braces
inside a resolved dynamic value remain literal data instead of becoming a second template expression.

Rules does not compile into a second Request or Item model. Parse/bind provides each base field and only a
non-empty Item-edge `vals` result overrides it; null, empty strings, arrays, and objects preserve the base while
zero and false remain valid overrides. Rules then builds the Spider's associated Rust Item through the derived
`Item::from_values`, assigns the current `SchemaKey`, and invokes the default `item` function or the edge's
configured `fn`. The business function submits with `Tx.item`, so Middleware and persistence remain shared.

Field extractors run in declaration order. An empty result falls through to the next extractor. The final result is collapsed by match count:

- zero matches: `null`, or `default`; a required field fails;
- one match: a scalar or element object;
- multiple matches: an array.

CSS expressions support `::text`, `::attr(name)`, and element output. An element object contains `html`, `text`, and `attrs`.

## 9. Selectors and Data Extraction

### 9.1 CSS and Healing

Code mode obtains the native Soup through:

```rust
let soup = response.css()?;
let nodes = soup.select("article h2")?;
```

Application code uses `Tag::text()`, `Tag::outer_html()`, and `Tag::get(...)` directly. The framework does not add another node wrapper around Soup.

The shared Healing entry point is `selector::css::select(&soup, expr, &config)`. Its flow is fixed:

```text
compile valid CSS
-> exact selection
-> return exact matches immediately
-> on an exact miss with explicit healing, scan every DOM element
-> score only constraints declared by the CSS AST
-> return all highest-scoring nodes at or above min in DOM order
```

Scoring covers tags, IDs, classes, attributes, combinator relationships, and supported static pseudo-classes. Extra candidate attributes are not penalized. The default `min` is `0.8`, and the valid range is `0.0..=1.0`.
Recursive relationship scoring memoizes each `(DOM node, selector compound)` state for one selection.
Best-ancestor and best-earlier-sibling states reuse the immediately related node, so deep descendant
and wide sibling chains require linear relation states rather than repeated full-chain scans. No state
is persisted across documents.

Healing accepts exactly the syntax that `scrape-core 0.2.9` can compile. In particular, that parser currently rejects `:is()`, `:where()`, and `:has()` before Healing starts.

Healing stores no historical fingerprints, does not rewrite or persist a repaired selector, does not cross selector types, and never calls AI. Invalid CSS returns a CSS error. A score below `min` returns an empty set so Rules can continue to the next extractor for that field.

### 9.2 Regex, JSON, and AI

- Regex returns every capture. It uses capture group one when present, otherwise the full match.
- `Response::json<T>()` follows the shared response-text decoding contract before deserialization.
- AI is an explicit selector alongside CSS and Regex. A Rules AI extractor contains only `kind: ai` and its non-empty `expr`; the prompt describes the expected object fields, while provider configuration is not part of the DSL.
- The Worker constructs one `selector::ai::Client` through `Client::new` or `Client::from_env` and injects it through `Engine::with_ai`. The Client owns one reusable `async-openai` client for an OpenAI-compatible Chat Completion endpoint.
- The unified Executor attaches the shared Client to each Response immediately before parsing. Code handlers and Rules both call `response.ai(expr).await`; Response clones share the same Client, which is crate-private, non-serializable, and omitted from Response Debug output.
- `Client::from_env` resolves the API key during Worker assembly. Provider settings and credentials never enter Rules, Trace Snapshot, Request, Payload, Scheduler, or Item data. `base_url` must be an absolute HTTP(S) base endpoint without user information, a query, or a fragment. Client Debug may identify that validated endpoint and model, but never the key; provider failures omit request URLs and raw response content.
- Local Rules assembly rejects an AI extractor without a configured Client. A remotely restored Rules Trace fails explicitly when it reaches AI on a Worker without a Client; this is not converted into a Scheduler capability or fallback.
- Every call requests `response_format=json_object` and appends the same mandatory object-only output instruction. Runtime parsing rejects arrays, scalars, Markdown, and prose. Response content above 1 MiB is rejected before decoding; a provider attempt times out after 60 seconds and does not add a retry layer outside `error_parse`.
- The Client clears `async-openai` organization/project defaults, validates the Authorization header during construction, suppresses dependency logging of raw error bodies, and maps provider failures to bounded classifications so response content cannot enter Scheduler error storage.
- AI availability is not part of capability-aware claiming. Every Worker that can claim Requests from the same task pool whose Rules can reach AI must use an equivalent provider endpoint and model; credentials remain Worker-local.
- AI does not generate CSS, and CSS Healing never delegates candidates to AI.

### 9.3 Media Fields

Rules `image / video / audio` values are processing types, not validator types. After extraction and before Item Schema validation, the framework normalizes URLs, strings, or HTML element objects into arrays whose entries contain:

```text
name, url, src, width, height, size, ext, alt
```

Relative URLs are resolved against the current Response URL, and duplicate normalized `url` values are removed within the field. The original `src` remains metadata and is not the deduplication key. Plain text fields are not media-normalized.

## 10. Middleware Lifecycle

The currently connected hooks are:

```text
before_spider
before_scheduler
before_download
after_download
before_parse
before_item
error_download
error_parse
error_item
after_spider
```

`Middleware::Next<T>` contains only `Continue(T)` and `Skip`. `Skip` is normal filtering and does not invoke the corresponding error hook. The Registry merges default Specs with object-local Specs, then orders them by `order` and declaration sequence.

Request middleware may change only fields owned by its stage. `before_scheduler` must preserve
`id / task_id / trace_id`; after a Request is claimed, `before_download` must also preserve `node` so
execution and lease settlement cannot refer to different work. `before_download` may still change
transport fields such as URL and headers.

Built-in implementations:

| Middleware | Hook area | Semantics |
| --- | --- | --- |
| `validate` | Default normal hooks | Validate Request/Response invariants and validate Items through validator using `SchemaKey` |
| `dedup` | `before_scheduler` | Build Request fingerprints from configured fields; omitted or `-1` TTL never expires |
| `rate_limit` | `before_download` | Limit downloads by group and QPS |
| `retry` | Error configuration | Provide Worker-local retry policies for download, parse, and Item submission |

`Builder::with_middleware(name, value)` registers a capability; it does not attach it to every object. Request, Response, Item, and Spider lifecycle Specs opt into capabilities explicitly. Only validate Specs for the normal stages are Registry defaults.

Default validation requires a text Request body declaration to contain string `data`. A custom
Downloader Response must have an absolute HTTP(S) URL with a host and a valid HTTP status code;
`after_download` validates that structure, while `before_parse` retains the separate policy of skipping
non-success responses.

Default Dedup handles only Request fingerprints built from explicitly configured keys; it never deduplicates Items or adds an implicit URL key. Both Rules initial Requests and Tx output pass through `before_scheduler`. Fingerprints are observed and inserted there, so a later `Scheduler::push` or run-seed `init` failure does not roll them back. SHA-256 hashes a structured tuple of `task_id`, Middleware key, rule name, and ordered values rather than an ad hoc concatenated namespace. URL normalization stably sorts query pairs only by key, preserving the original order of repeated keys. All active rules are checked and inserted under one lock after every finite TTL deadline has been validated, so a TTL error cannot partially mutate the store. A rule with `ttl: 0` neither checks nor stores its fingerprint; omitted TTL or `-1` remains for the process lifetime without capacity eviction. The exact in-memory store uses a `HashMap` plus an expiry heap and lazily removes only expired heap-head entries.

Each RateLimit group fixes one interval while it is active. A later Spec using the same group with a
different QPS fails immediately as invalid configuration rather than waiting behind the group's delay.
The group can be removed lazily only when no caller retains it and its next permitted instant has
passed; cleanup needs no background task or hot-reload behavior.

## 11. Items, Validation, and Local Persistence

The Item path is fixed:

```text
Tx.item
-> generate UUID v7 when ID is empty
-> before_item
-> Scheduler.push_items(&Payload)
-> success, or retry according to error_item policy
-> on exhaustion call error_item and fail the current Request
```

Every concrete Item owns one explicit `#[serde(skip)] item::State`, rejects unknown serde fields, and derives
`serde::Serialize`, `serde::Deserialize`, and `macros::Item`. The Item derive generates `state / state_mut` and
the mandatory `Item::from_values` Rules construction boundary; code-mode Items continue using normal Rust constructors.
Additional Rules Item functions are explicitly marked with `#[item]`, and local Builder assembly rejects a missing
function before runtime initialization. The Schema Store computes a stable SHA-256 key from the canonicalized schema, caches a
`validator::Validator`, and validates the serialized Item. Item ID is outside the schema and does not participate
in business deduplication.

Memory uses the current working directory by default. `Memory::with_dir(path)` changes the root of both normal output and failure snapshots.

Normal Item output:

```text
<dir>/data/items/output/<task_id>/<yyyy-mm-dd-HH>.jsonl
```

Each Item is one JSON line containing only its business serialization. Concurrent writes to the same hourly file are serialized. A Payload is serialized and written one Item at a time without materializing the whole collection; every complete append is flushed immediately. Any serialization, write, or flush failure attempts to truncate the file back to its pre-Payload length, and `close()` flushes all open files again.

`version / timezone` are Trace-level runtime metadata. A persistent Scheduler may read the
corresponding Trace Snapshot through `payload.trace_id` during `push_items` and denormalize
them into its own Item record. The column names are `config_version` and `timezone`; plain
`version` remains reserved for Request execution ownership. These fields are not automatically
injected into business Item JSON or copied into every Request Snapshot, and the default Memory
JSONL does not store them.

Submission-failure snapshots:

```text
<dir>/data/items/snapshots/<task_id>/<yyyy-mm-dd-HH>/<uuid-v7>.json
```

- the first `push_items` failure attempts to create a snapshot before continuing configured retries;
- a snapshot is streamed to a uniquely named temporary file and atomically renamed only after the complete document is flushed; failure removes the temporary file and publishes no partial snapshot;
- snapshot-write failure does not prevent Scheduler retries;
- a later successful retry removes the snapshot, while cleanup failure does not change Item success;
- an exhausted snapshot remains for manual handling; automatic replay is not currently implemented;
- another Scheduler enables framework-local snapshots only when its `dir()` returns a local path.

Item submission is at-least-once. Business Item deduplication belongs downstream or in a custom Scheduler's Item submission implementation.

## 12. Source Responsibilities

| Path | Single responsibility |
| --- | --- |
| `spider/src/engine/actor.rs` | Own Engine Actor state and the shared transition/termination decision |
| `spider/src/engine/actor/start.rs` | Start Executor work and handle its completion message |
| `spider/src/engine/actor/claim.rs` | Claim Scheduler work and handle its completion message |
| `spider/src/engine/actor/request.rs` | Register one Request task and handle its completion message |
| `spider/src/engine/actor/output.rs` | Accept Tx Events, delegate replies, and track output completion |
| `spider/src/engine/actor/wait.rs` | Schedule poll and producer-idle notifications |
| `spider/src/engine/actor/task.rs` | Own task handles and convert task panic into Engine errors |
| `spider/src/engine/builder.rs` | Assemble components and own the Schema Store used by all execution modes |
| `spider/src/engine/runtime.rs` | Component lifecycle, startup settings, and Actor assembly |
| `spider/src/engine/worker.rs` | Own the startup-frozen Worker ID and download-mode capabilities |
| `spider/src/engine/request/task.rs` | Ack, lease maintenance, and final settlement for one Request |
| `spider/src/engine/admission.rs` | Apply `before_scheduler` to Request output before it enters the Scheduler |
| `spider/src/engine/request.rs` | Download, Middleware, Worker-local retry, and parse lifecycle for one claimed Request |
| `spider/src/engine/event/request.rs` | Handle Request output emitted by Tx |
| `spider/src/engine/event/item.rs` | Handle Items, submission retries, and failure snapshots |
| `spider/src/spider/tx/identity.rs` | Derive replay-stable IDs for current-Request output |
| `spider/src/engine/executor.rs` | Select Code/Rules from Trace Snapshot and invoke the shared Spider |
| `spider/src/engine/code.rs` | Code-mode local run-seed initialization |
| `spider/src/engine/rules.rs` | Rules-mode assembly and run-seed initialization |
| `spider/src/engine/rules/executor.rs` | Coordinate one Rules node execution and emit its outputs |
| `spider/src/engine/rules/executor/field.rs` | Extract declared fields from the current Response |
| `spider/src/engine/rules/executor/value.rs` | Resolve typed value references from the current Rules context |
| `spider/src/engine/rules/executor/bind.rs` | Evaluate ordered bind pipelines, transforms, and templates |
| `spider/src/engine/rules/executor/condition.rs` | Evaluate one edge condition |
| `spider/src/engine/rules/executor/build.rs` | Construct Request and Item values from an enabled edge |
| `spider/src/middleware/registry.rs` | Register and resolve Middleware implementations and Specs only |
| `spider/src/downloader/http.rs` | Execute HTTP requests, redirects, headers, cookies, and response conversion |
| `spider/src/downloader/http/pool.rs` | Key, reuse, expire, and close proxy/TLS-specific HTTP clients |
| `spider/src/scheduler/contract.rs` | Public Scheduler contract |
| `spider/src/scheduler/init.rs` | Run-seed initialization contract |
| `spider/src/scheduler/memory.rs` | Public Memory implementation and submodule composition |
| `spider/src/net/request/digest.rs` | Canonically hash initial Request Snapshots for replay comparison; shared by Memory and Redis |
| `spider/src/scheduler/memory/claim.rs` | Coordinate one capability-aware claim from queued Snapshot to processing Request |
| `spider/src/scheduler/memory/queue.rs` | Ready/delayed queue ordering |
| `spider/src/scheduler/memory/reclaim.rs` | Recover expired acknowledged and unacknowledged leases |
| `spider/src/scheduler/memory/restore.rs` | Restore claimed Request Snapshots and handle restoration retries |
| `spider/src/scheduler/memory/settle.rs` | Identity checks, settlement, and queue retry |
| `spider/src/scheduler/memory/state.rs` | Memory runtime state structures |
| `spider/src/scheduler/memory/validate.rs` | Validate new Requests and their Trace ownership |
| `spider/src/selector/css/healing.rs` | Healing configuration and orchestration |
| `spider/src/selector/css/healing/reference.rs` | CSS AST to scoring reference model |
| `spider/src/selector/css/healing/score.rs` | DOM candidate traversal, relationships, and scoring |
| `spider/src/selector/ai.rs` | Validate and own the reusable Worker-local Client, then perform one JSON extraction |
| `macros/src/spider/model.rs` | Parse the user Spider model and methods |
| `macros/src/spider/check.rs` | Validate macro input constraints |
| `macros/src/spider/bind.rs` | Generate node registration and handler bindings |
| `contrib/src/scheduler/redis/scheduler.rs` | Redis public type, lifecycle, and Scheduler/Init contract wiring |
| `contrib/src/scheduler/redis/request.rs` | Redis Trace/Request storage, claim, restore, and lease recovery |
| `contrib/src/scheduler/redis/settle.rs` | Redis acknowledgement, release, refresh, success, and failure transitions |
| `contrib/src/scheduler/redis/item.rs` | Redis Stream Item submission and Trace metadata projection |
| `contrib/src/scheduler/redis/{key,model,script,validate,error}.rs` | Key isolation, stored records, Lua loading, boundary validation, and error mapping |
| `contrib/src/scheduler/{api,mysql}.rs` | Future external Scheduler boundaries |

Names rely on module context. For example, `request::State`, `memory::State`, and `registry::Bind` do not repeat their module names. Files likewise avoid mixing unrelated parsing, storage, and control-plane responsibilities.

## 13. Extension Boundaries and Roadmap

### Release Boundaries

- v3: capability-scoped atomic Scheduler claiming; deterministic response charset decoding and broader fixture-backed page regression coverage. These contracts are implemented.
- v4: the shared backend-neutral Scheduler conformance suite, the Redis 7 standalone Scheduler, and Engine-level Worker-local AI Client injection are implemented. API and MySQL Schedulers, the Master control plane, auditable Item snapshot replay, and `fasttrace` runtime tracing remain separate work. These implementations depend on the core Scheduler contract, not on Browser delivery.
- v5: a real Browser Downloader plus mixed HTTP/browser end-to-end Engine acceptance, and a separate Item attachment downloader. Attachment downloading and Browser downloading are independent deliverables. Capability-aware claim semantics remain the v3 contract.

These capabilities must preserve the existing core contracts:

- replacing the Scheduler must not change the business shape of Engine, Spider, Downloader, Middleware, Request, Response, or Item;
- Redis, API, and MySQL implementations must provide atomic claims, leases, lease refresh, version validation, retry, terminal states, Trace reads, and Item submission themselves; every lease-backed implementation must recover expired leases even while the Worker is online, while offline-Worker recovery can only trigger that transition earlier;
- Worker capability filtering must be atomic with the Scheduler claim, rather than claiming an incompatible Request and dropping it in the Downloader;
- Browser must implement the existing `Download` contract and produce the same `Response` model;
- `fasttrace` span context is operational telemetry and must not replace business `task_id / trace_id`.

### Explicit Non-Goals Today

- persisting or automatically updating historical CSS Healing fingerprints;
- invoking AI automatically from Healing;
- providing a partial XPath implementation;
- batching an entire Trace's Items at Engine shutdown;
- using Item ID for business deduplication;
- supporting Redis Cluster through the standalone Redis Scheduler; Cluster needs a separate Scheduler design;
- downloading Item attachments; that capability is unimplemented and assigned to an independent v5 change;
- making the core `spider` crate depend on `contrib` or a control-plane implementation.

## 14. Architecture Invariants

Implementation and extensions should continue to satisfy these checks:

1. A Request has at most one valid execution right at a time; settlement by an old version or Worker is rejected.
2. Request retry preserves `id / task_id / trace_id / node / version`, advances retry state, and lets the next successful claim advance the execution generation.
3. Neither code nor Rules serializes Rust handlers. A code Worker invokes its local registry by stable node only.
4. `Tx.request / Tx.item` output is handled immediately, and the Engine cannot exit while any producer can still emit work.
5. `success`, `failure`, `release`, and `refresh_lease` each express exactly one state transition semantic.
6. CSS Healing and AI remain independent and explicit selection capabilities.
7. Character decoding never mutates `Response.body`, and every response text consumer uses the same deterministic decoding path.
8. Planned components are never presented as implemented merely because placeholder files or configuration fields exist.

## 15. Related Documentation

- [Architecture document index](./架构设计文档.md)
- [中文架构总览](./architecture.zh-CN.md)
