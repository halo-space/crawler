# crawler

简体中文 | [English](README.md)

`crawler` 是一个使用 Rust 编写的爬虫运行时。默认单进程形态提供内存调度器、HTTP 下载器、中间件
生命周期、异步 Spider 处理器、规则模式、CSS Healing、AI Selector 和本地 JSONL 数据输出；`contrib`
还提供 Redis 7 单实例调度器，用同一套 Engine 合同支持持久化的多 Worker 队列。运行时已完成
Task/Trace 运行种子、按 Worker 能力领取 Request，以及确定性的响应字符集解码。

完整的当前功能、运行模型与扩展边界见[架构与功能说明](docs/architecture.zh-CN.md)。

## 运行环境

- Rust 1.97.0
- Cargo

克隆仓库后可直接检查整个 workspace：

```bash
cargo check --workspace --all-targets
```

## 代码模式

代码模式使用 `#[spider]` 宏定义 Spider。宏负责注入框架内部的事件发送器，业务代码只需要声明
Spider 字段和处理函数。

完整可编译示例位于 [examples/src/bin/basic.rs](examples/src/bin/basic.rs)，其中包含 Spider、Item 和
Engine 的完整定义。运行命令：

```bash
cargo run -p examples --bin basic
```

默认 Memory Scheduler 每次启动都以唯一的 `Spider.name` 创建一份运行种子：新的 `trace_id`、
不可变 Trace Snapshot 和初始 Request。远程 Scheduler 可以声明只消费已有运行种子；此时 Engine
不会在 Worker 本地创建 Trace，也不会调用 `Spider.start()`。代码 Request 只持久化稳定 node，
Worker 通过本地 Spider node 注册表恢复处理函数，不保存 Rust 函数指针。

Rules 启动时，每条初始 Request 都先经过 `before_scheduler`，再把 Trace Snapshot 与准入后的
Requests 原子写入 Scheduler。所有初始 Request 都被过滤仍是一轮有效运行：Trace Snapshot 会被
保存，Engine 正常结束。

## Redis Scheduler

`contrib::scheduler::redis::Redis` 完整实现核心 `Scheduler` 和 `Init` 合同。它与 Memory 使用相同的
装配方式，Engine 不包含 Redis 特例：

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

所有 Redis key 都按 namespace 隔离；正常 `close()` 只释放客户端资源，不会删除排队数据。Redis 保存
Trace Snapshot、Request 状态、按能力领取的租约、结算、统计和 Item。每个已接受的非空 Item `Payload`
都写成一条 Redis Stream entry，保持 at-least-once 语义，不做业务 Item 去重。

Redis 会限制每次领取的重复维护工作：一次 `next_requests` 最多回收并巡检 128 条租约；对传入的每个
mode，最多提升并巡检 128 条延迟 Request。在回收、延迟提升、巡检或选择任务时发现单条 Request 记录缺失，
会清理对应的悬挂索引；租约或队列元数据损坏则会移出活动索引，写入完成记录并转为失败终态。两者都不会
阻塞后续正常 Request。共享索引的 Redis 类型损坏则会在状态写入前明确使本次领取失败。

本版本只支持 Redis 7+ 单实例 primary，不支持 Redis Cluster。多 key Lua 状态转换依赖单实例原子性，
Cluster 将作为独立 Scheduler 设计。需要可恢复持久化时，必须启用 AOF（`appendonly yes`）并配置
`maxmemory-policy noeviction`。`appendfsync` 是运维侧在更强持久性与写入吞吐/延迟之间的选择
（例如 `always` 与 `everysec`）。应监控 Stream 增长并配置明确的外部保留策略；Scheduler 不会自动
trim Item 输出。

在处理函数中通过 `self.tx.request(...)` 提交的新请求会进入同一个 Scheduler 队列；通过
`self.tx.item(...)` 提交的数据经过 Item 中间件后由 Scheduler 提交。

Rules 模式中，extractor 表达式直接决定结果数量：零个匹配为 `null`，一个匹配为标量或节点对象，
多个匹配为数组，不再提供额外的 `select`。媒体字段通过
`item.fields.<name>.kind = image | video | audio` 声明，crawler 在 validator 处理
`item.schema` 前将其规范化为固定 media object 数组。

代码模式通过 `response.css()?` 获得原生 `scrape_core::Soup`，业务代码继续直接使用 Soup 和
Tag。确定性 CSS Healing 需要显式开启，只扫描当前文档，不保存历史节点指纹，也不会调用 AI。
Rules 模式使用同一实现：

```yaml
args:
  healing:
    min: 0.8
```

AI 是与 CSS 并列的独立 extractor。它通过 OpenAI-compatible Chat Completion API 发送当前
Response 文本和 `expr` 提示词，并将模型内容直接解析为 JSON：

```yaml
- kind: ai
  expr: "提取文章信息，只返回合法 JSON。"
  args:
    base_url: "https://api.example.com/v1"
    api_key: "env:OPENAI_API_KEY"
    model_name: "..."
```

AI 不生成 CSS，也不会被 CSS Healing 自动调用。Rules 快照只保存环境变量引用，Worker 在执行
Selector 时才读取真正的 API Key。`base_url` 必须是绝对 HTTP(S) 端点，不能内嵌凭据，也不能包含 query 或 fragment。

## 规则模式

规则模式从 YAML 加载任务入口、请求图、解析规则、数据绑定和 Item Schema，但运行时仍装载一份
Rust Spider。多份 Rules 可以共享同一套 Spider 业务代码和同一种 Item 类型；YAML 负责表达每个
任务不同的抓取规则。Rules 的 `spider.name` 在本地作为 `task_id`，不要求等于部署侧 Rust
`Spider::name()`。

```rust
use spider::{config, engine};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let rules = config::Config::load("examples/rules-newspaper.yaml").await?;
let mut engine = engine::Engine::new()
    .with_rules(rules)
    .with_spider(Newspaper::new())
    .build()
    .with_concurrency(8);

engine.start().await?;
# Ok(())
# }
```

完整配置位于 [examples/rules-newspaper.yaml](examples/rules-newspaper.yaml)，运行命令：

```bash
cargo run -p examples --bin rules
```

`spider.start[*]` 和 request edge 使用同一套完整 Request Spec；Request 在入队前已经拥有 node、
URL、transport、priority 和 vals。URL 数组展开会在模板渲染前写入从 `1` 开始的保留
`vals.idx`。Rules 先形成 parse/bind 基础字段，Item edge 的 `vals` 只有非空值才覆盖同名字段；随后
根据 `#[spider(item = Article)]` 关联的 Rust 类型调用 derive 生成的 `Item::from_values`，注入
SchemaKey，再调用默认 `item` 或 edge 的 `fn` 业务函数。规则模式与代码模式共用 Engine、
Scheduler、Downloader、Middleware、Request、Response、Item 和 Payload，不维护第二套运行时模型。
Request 与 bind 模板只解析一次源字符串；动态值中带入的花括号只作为数据，不会再次解释为模板表达式。
每条 Rules Request 都由统一 Executor 先调用 Rust Spider 的 `index(response.clone())` 业务入口，
然后再根据 `Request.node` 解释对应 DSL node；这里不存在 Code/Rules Executor 替换。
每个具体 Item 同时 derive `serde::Serialize`、`serde::Deserialize` 和 `macros::Item`，启用 serde
未知字段拒绝，并显式持有一份 `#[serde(skip)] item::State`。Item derive 自动生成
`from_values / state / state_mut`，业务代码不再手写这些适配函数。edge `fn` 引用的额外 Item 函数
必须使用 `#[item]` 显式注册；本地 Rules 装配发现函数不存在时，会在运行时初始化前直接 panic。

## Item 与输出

框架在 `Tx.item(...)` 提交数据时为没有 ID 的 Item 生成 UUID v7。这个 ID 只标识当前数据实例，
不参与业务去重。内置去重只作用于进入 Scheduler 前的 Request；Item 业务去重应由下游或自定义
Scheduler 实现处理。

默认 Memory Scheduler 将每条数据保存为一行 JSON：

```text
./data/items/output/<task_id>/<yyyy-mm-dd-HH>.jsonl
```

普通 JSONL 只包含业务字段。最终提交失败时，本地快照会额外保存 Item ID 和业务数据，便于恢复和
排查。失败快照位于 `./data/items/snapshots/`。Item 提交采用 at-least-once 语义；替换
Scheduler 时由新实现完整实现相同的 Request 与 Item 提交合同。
Memory 在同一次追加锁内逐条序列化并写入 Item；任一 Item 失败会回滚本次完整追加。失败快照先写入
临时文件，完整 flush 后再原子 rename，不会发布半截 JSON。
`Scheduler::push` 对相同 Request ID 与初始 Snapshot 的重放执行幂等 no-op，只原子补写缺失
Request；任一已有 ID 的 Snapshot 冲突时整批失败。
当前 Request 执行中直接等待的 `Tx.request` 会使用父 Request ID、规范化后的子 Request 初始规格，
以及该规格在当前 parse attempt 中的出现次数，为框架生成的子 Request 分配稳定 ID。因此 parse retry
或队列重试重放相同输出时复用同一 ID；同一次执行中有意产生的多个相同 Request 仍拥有不同 ID。
通过 `Request::with_id` 设置的 ID 始终以业务值为准。这是 Request 输出重放保护，不是业务 Dedup；
Item 与 detached Tx 输出仍保持 at-least-once 语义。

Request 进入本地执行槽后先通过 `Scheduler::ack` 确认执行权，长任务运行期间通过
`Scheduler::refresh_lease` 独立续租，提交最终结果期间也保持续租，最后分别调用 `Scheduler::success` 或
`Scheduler::failure`。租约超时和续租间隔由具体 Scheduler 自己提供；Memory 默认超时 30 秒、
每 10 秒续租。最终 Payload 生成前明确丢失执行权时，Engine 终止执行且不提交结算；Payload 生成后
以 `success / failure` 为最终依据，并发续租错误不能取消结算，临时结算错误只重试同一个 Payload，
不会重新执行 Request。已确认执行失败时会保留有序且不重复的失败 Worker；未 ack 的领取过期不消费尝试次数。
每条由成功 `next_requests` 调用返回的 Request，都以该调用前立即记录的单调时刻计算首个本地租约
截止点，因此 Scheduler 处理与网络耗时会占用这份保守的租约预算。Engine 不会把 Worker 墙钟与
Scheduler 服务端时间比较；一次成功续租会从自身的单调起点开始计算下一个本地截止点。
Memory 只从进程内不可变映射读取 Trace Snapshot。Engine 直接跟踪克隆出来的 Tx 生产者，不再通过固定空闲等待窗口猜测是否仍有输出。
Engine 内部由一个 Kameo Actor 统一协调，Request 和输出 I/O 仍在独立 Tokio 任务中执行。
当前 Request 内直接 await 的 Tx 调用可以使用完整 Request 上下文；移动到独立任务中的 Tx clone
只保留 `task_id / trace_id`，不会继承 Request 执行权、租约身份、node、version 或 stats。
任何输出 Request 都必须在执行 `before_scheduler` Middleware 前匹配该 Tx 的 `task_id / trace_id`，
且这些 Middleware 不得改写 `id / task_id / trace_id`。
Request 并发数、单次领取上限和 Event 容量是三个独立且只在启动时加载的配置，分别通过
`with_concurrency`、`with_claim_limit` 和 `with_event_limit` 设置。
Actor 开始处理 Event 时即释放 Event 容量，但 Tx 调用仍会等待对应 Scheduler 与 Middleware 工作完成。

Dedup 只处理配置 key 生成的 Request 指纹。它在 `before_scheduler` 观察到 Request 时写入指纹，
后续 `Scheduler::push` 或运行种子 `init` 失败都不会回滚。SHA-256 输入使用 `task_id`、
Middleware key、rule name 和有序字段值组成的结构化元组，namespace 不会因字符串拼接发生碰撞。
URL 归一化只按 query key 稳定排序，同名 key 的原始顺序不变。所有启用规则在同一临界区内检查并
原子写入；TTL 省略或
`-1` 表示进程生命周期内永久保留，某条规则配置 `0` 时既不查询也不保存该规则的指纹。

HTTP Downloader 按 Request 应用 proxy 和 TLS 配置。只有 proxy URL 与
`accept_invalid_certs` 完全相同的请求才复用 Client；默认最多保留 64 个空闲 Client，
空闲 90 秒后惰性过期。容量压力下淘汰最早空闲的 Client，正在使用的 Client 不会被淘汰，
因此可以暂时超过该上限。`Http::with_max_idle_clients` 在 Worker 启动时替换默认值，
`Http::close()` 清空整个池。直连请求、不同代理凭据和不同 TLS 行为不会共用同一 Client 条目。

每个 Worker 默认最多接收 64 MiB 解码后响应体。`Http::with_max_body_bytes` 可以替换这个硬上限；
代码或 Rules 中的 `Request.max_body_bytes` 只能选择小于或等于 Worker 上限的正数值，超过时在网络 I/O
前失败。Downloader 按解码后 chunk 读取且不预分配上限大小，但成功结果仍是一个有界的
`Response.body: Bytes`；本合同不增加公开 stream 或文件写入 API。`Request.timeout` 覆盖连接、
所有 redirect 和最终 body 读取的一次完整下载；redirect 不重置超时，每次新的下载重试从零开始新预算。
只跟随 `301`、`302`、`303`、`307` 和 `308`。redirect 将带 body 的 Request 改为 GET 时，会随被丢弃
的 body 一并删除相关 headers；原始 GET 即使带 body 也保留其 headers。

`Headers` 直接包装标准 `http::HeaderMap`，保留大小写不敏感的名称、响应原始值和重复字段。
`set` 替换该名称的所有值，`append` 追加新值；Rules 输入仍是单值 map。Request Snapshot
把规范化名称序列化为值数组，不可表示为字符串的 Request header 值在 Snapshot 边界被拒绝。

每条 Request 都携带可序列化的 CookieStore 快照。Downloader 在解析前按标准 Domain、Path、Secure、
Max-Age 和 Expires 规则应用所有响应 Cookie，后代 Request 通过 Snapshot 和 Scheduler 恢复继承这条谱系。
已入队的并行兄弟各自保留独立快照，后续 Cookie 变更不会反向修改它。跨源 follow 或 redirect 会移除
源站 headers，并把 CookieStore 缩减为目标 URL 可用的 Cookie，无关凭据不会进入目标 Request Snapshot。
跨站 Public Suffix Domain 会被拒绝；与当前 Request host 完全相同的 Public Suffix 会规范化为
HostOnly。原始 `Cookie` header 不作为第二套会话来源，代码与 Rules 必须使用 Request cookie API。

每个 `rate_limit` group 在活跃期间固定一个 interval，同组使用冲突 QPS 会返回非法配置错误。
只有当该 group 无持有者且下一次允许时间已过，后续查找才会惰性清理它。

v3 的响应文本合同保持 `Response.body` 为 HTTP 内容解码后、字符转码前的 Downloader 交付 bytes，
并把字符解码统一放在 `Response::text()`。编码优先级固定为 BOM、合法的 `Content-Type` charset、HTML 或缺失 MIME 时前
1024 bytes 内的 HTML meta，最后回退 UTF-8。非法字节使用 Unicode replacement 语义，运行时不做
统计字符集猜测；`Response::json<T>()` 复用同一条文本解码路径。

## 当前范围

当前 v3 运行时包含：

- 单进程运行
- Memory Scheduler
- HTTP 下载
- CSS 和正则表达式
- 确定性 CSS Healing 和显式 AI Selector
- 请求校验、重试、限速和去重中间件
- 代码模式与 YAML 规则模式
- 本地 JSONL Item 输出

Scheduler 合同由 Engine 向 `next_requests` 与待处理判断传入 Worker ID 和支持的下载模式，能力筛选
必须和领取原子完成。真实 Browser Downloader 与 HTTP/browser 混合端到端执行属于 v5；可选的
API、MySQL Scheduler、Master 控制面、Item 回放和运行期链路追踪仍属于后续 v4 工作，且不依赖
Browser 实现。Redis 已通过共享 Scheduler 一致性测试。
Engine 默认使用 `worker-1` 和 HTTP 模式；`with_worker_id(...)` 与 `with_modes(...)` 可替换这些启动时冻结的值，
空 Worker ID 或空 mode 集合会在执行前被拒绝。

媒体对象规范化不会下载文件。Item 附件下载尚未实现，也没有分配版本，后续必须由独立 OpenSpec
确定合同。

后端或 API 在保存配置前，可以通过 `Config::validate()` 校验完整规则，也可以通过
`middleware::check(&spec)` 单独校验一条中间件配置。

## 开发验证

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
```
