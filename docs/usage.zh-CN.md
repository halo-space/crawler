# Crawler 使用手册

[项目 README](../README.zh-CN.md) | 简体中文 | [English](usage.md)

`crawler` 是一个使用 Rust 编写的爬虫运行时。默认单进程形态提供内存调度器、HTTP 下载器、中间件
生命周期、异步 Spider 处理器、规则模式、CSS Healing、AI Selector 和本地 JSONL 数据输出；`contrib`
提供 Redis Scheduler 和分布式中间件，并保持同一套 Engine 合同。运行时已完成 Task/Trace 运行种子、
按 Worker 能力领取 Request，以及确定性的响应字符集解码。

完整的当前功能、运行模型与扩展边界见[架构与功能说明](architecture.zh-CN.md)。

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

完整可编译示例位于 [examples/src/bin/basic.rs](../examples/src/bin/basic.rs)，其中包含 Spider、Item 和
Engine 的完整定义。运行命令：

```bash
cargo run -p examples --bin basic
```

代码模式使用默认 Memory Scheduler 时，每次启动都以唯一的 `Spider.name` 创建一份运行种子：新的
`trace_id`、不可变 Trace Snapshot 和初始 Request。远程 Scheduler 可以声明只消费已有运行种子；在
这条代码模式路径中，Engine 不会在 Worker 本地创建 Trace，也不会调用 `Spider.start()`。代码 Request
只持久化稳定 node，Worker 通过本地 Spider node 注册表恢复处理函数，不保存 Rust 函数指针。
`Request::follow()` 只负责构造 Request 规格，因此 `task_id / trace_id` 可以暂时为空，直到运行种子或
当前 Tx 上下文完成绑定。`Scheduler::init` 与 `Scheduler::push` 一律拒绝未绑定 Request；进入队列后的
Snapshot、领取、重试和恢复始终同时携带两者，并关联真实 Trace Snapshot。代码模式使用 `dsl` 为空的
Trace Snapshot，不以缺少 Trace 表示代码模式。

Rules 启动时先静态校验配置、冻结 Trace Snapshot、生成初始 Requests，再把两者直接交给
`Scheduler::init` 原子发布。运行种子发布不执行 Worker Middleware、`before_scheduler` 或 Dedup；
后续通过 `Tx` 产生的 Request 仍在 `Scheduler::push` 前经过正常准入链路。

## Redis Scheduler

`contrib::scheduler::redis::Redis` 完整实现核心 `Scheduler` 和 `Init` 合同。它与 Memory 使用相同的
装配方式，Engine 不包含 Redis 特例：

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

engine.start().await?; // 持续运行到 SIGINT/SIGTERM，再排空已接收工作。
```

Worker ID、host、version 和 modes 都是 Scheduler 配置。`open(concurrency)` 完成注册并启动心跳；同一个
仍在线的 Worker ID 会拒绝重复注册。心跳失败只暂停后续领取，并持续重试直至恢复；已领取 Request 不受
影响。正常 `close()` 会先等待心跳任务结束，再尝试显式离线，保证离线写入后不会继续发送心跳；更新
失败时记录日志，随后由心跳超时释放注册。若 `close()` 在等待心跳或离线响应时被取消，连接、注册 token
和停止状态仍保留，再次调用 `close()` 会继续完成关闭。所有
Redis key 都按 namespace 隔离，关闭不会删除
排队数据。Redis 保存
Trace Snapshot、Request 状态、按 mode 分域的 processing 租约、结算和统计。每个
`processing:<mode>` ZSET 是唯一的活动执行权/租约投影，score 使用 `lease_time`；Request Hash 仍是状态
事实来源。Item 持久化属于独立的 `item::Store` 依赖，不进入 Redis Scheduler。一次 Engine 运行在启动时只绑定
一个 Store；使用 `.with_store(store)` 替换默认的 JSONL 实现。框架不提供 Store 路由、Store 注册表、
按 Task 选择 Store，也没有 `persister_id` 这一运行时字段。
同一生命周期内，只有并发数相同的重复 `open` 才是幂等操作；不同并发数会被拒绝，必须先 `close()`
再开始新的生命周期。

从第一次注册尝试开始，到注册确认或 `close()` 显式放弃为止，Worker 与 namespace 配置保持冻结，保证
取消后的重试仍绑定同一个 Worker 身份和 namespace。

Redis 会限制每次领取的重复维护工作：一次 `next_requests` 每个 mode 最多回收 64 条过期租约，并在两个 mode 合计巡检
128 条 processing 记录；对传入的每个
mode，最多提升并巡检 128 条延迟 Request。在回收、延迟提升、巡检或选择任务时发现单条 Request 记录缺失，
会清理对应的悬挂索引；合法 processing Hash 的 score 或 mode 投影不一致会原地修复，不消费重试次数；
Request Hash 或队列记录本身非法时才会移出活动索引，写入完成记录并转为失败终态。这些记录都不会阻塞
后续正常 Request。领取返回前，Worker 会重新计算不可变 Request Snapshot 的摘要；摘要不一致会进入同一
恢复路径，不会执行该 Request。已通过摘要验证的 Snapshot 重试上限不能被可变 Hash 覆盖；单条恢复失败也
不能扣留同批合法 Request。共享索引的 Redis 类型损坏则会在状态写入前明确使本次领取失败。Request
Snapshot 的 `max_retry_count` 必须位于 `1..=128`；恢复以不可变 Snapshot 为准，可变 Hash 不能扩大该值。
当前 key 布局不迁移旧 Redis namespace，部署时使用新的 namespace。

失败 Worker 的领取资格投影到按 mode 分域的 `pending_exclusions` ZSET。单次领取最多检查 128 个排除成员，
必要时会推进按 mode 保存的“最新已检查 ready-event revision + 最后一个排除的 ready member”游标并临时
返回空集合。每次进入 ready 队列都会向 `ready_events:<mode>` 写入事件；恢复游标前只检查该 mode 的后续
事件。仅当新的 ready Request 排在游标 member 之前且当前 Worker 可以领取时才重置扫描；低优先级或其他
mode 的 ready 写入不会丢失已有进度。选择不会越过尚未确认的高优先级前缀或 mode。Pending 判断直接比较
排队数量和当前 Worker 的排除索引，不会在一次 Redis Lua 调用中扫描完整积压。

本版本只支持 Redis 7+ 单实例 primary，不支持 Redis Cluster。多 key Lua 状态转换依赖单实例原子性，
Cluster 将作为独立 Scheduler 设计。需要可恢复持久化时，必须启用 AOF（`appendonly yes`）并配置
`maxmemory-policy noeviction`。`appendfsync` 是运维侧在更强持久性与写入吞吐/延迟之间的选择
（例如 `always` 与 `everysec`）。

API Scheduler 使用已有 HTTP transport、namespace 和认证，同时持有完全相同的 Worker 配置与生命周期：

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

`open` 先读取 `/v1/worker/policy`，再通过 `/v1/worker/register` 注册，并采用服务端返回的心跳间隔；
心跳和显式离线分别调用 `/v1/worker/heartbeat` 与 `/v1/worker/offline`。Worker 管理响应固定使用
`{code, message, data}`，`200` 表示成功，注册时 `100` 表示在线 ID 冲突。服务端生成的注册 token 会在
心跳和离线请求中原样返回，但不参与 Request 领取、租约或结算身份校验。
policy 中的租约超时和续租间隔必须与 Scheduler 本地配置完全一致；`max_request_bytes` 必须为正数，
Worker 会在发送前用它校验发给 Master 的单个请求体。该字段不限制 Master 返回的响应体。
只要 API Scheduler 仍保留未完成注册 key 或已确认 token，其配置就保持冻结，直到生命周期完成或被
显式关闭。

批量领取省略 Trace Snapshot 时，API 恢复会对同一批相同 `trace_id` 只读取一次，并发读取不同
Trace。Trace 读取、缺失、解码、校验和 task/node 绑定错误只调用 `release`，不调用 `ack` 或
`failure`，也不消耗 Request 重试；只有 Request Snapshot 或执行状态本身损坏才使用
`ack + failure`。恢复及每条 Request 的恢复结算共用 lease handoff deadline，成功恢复的 Request
保持服务端领取顺序。逐条 release 相互独立，单条失败不会阻止剩余领取结果归还。

## Selector

在处理函数中通过 `self.tx.request(...)` 提交的新请求会进入同一个 Scheduler 队列；通过
`self.tx.item(...)` 提交的数据经过 Item 中间件后交给独立的 Item Store。

Rules 模式中，extractor 表达式直接决定结果数量：零个匹配为 `null`，一个匹配为标量或节点对象，
多个匹配为数组，不再提供额外的 `select`。媒体字段通过
`item.fields.<name>.kind = image | video | audio` 声明，crawler 在 validator 处理
`item.schema` 前将其规范化为固定 media object 数组。

代码模式通过 `response.css()?` 获得原生 `scrape_core::Soup`，业务代码继续直接使用 Soup 和
Tag。Healing 只是 CSS 专属且需要显式开启的能力，只扫描当前 HTML 文档，不保存历史节点指纹，
不作用于 JSONPath，也不会调用 AI。Rules 模式使用同一套 CSS 实现：

```yaml
args:
  healing:
    min: 0.8
```

Rules Executor 会把一份 JSON 响应反序列化一次，并在多个 JSON 字段间复用这份文档。代码模式通过
`Response::json<T>()` 取得完整的 `serde_json::Value`，再使用 RFC 9535 JSONPath 查询。选择结果
直接引用原始值，不会把数字、布尔、对象或数组转成字符串：

```rust
let document: serde_json::Value = response.json()?;
let stock_codes = spider::selector::json::select(&document, "$.data.diff[*].f12")?;
```

Rules 模式直接复用同一个 selector。下面是一份使用东方财富测试接口的完整配置：

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

`quotes`、`stock_codes` 和 `total` 都是用户自定义的输出字段名，不是 JSON extractor 的固定关键字；
后续可以通过 `$fields.quotes`、`$fields.stock_codes` 和 `$fields.total` 引用。固定配置只有
`kind: json` 和 RFC 9535 JSONPath `expr`。

Item edge 会把解析结果交给 Rust Spider 注册的 `save` Item 处理函数；如果省略 `fn`，框架默认调用
名为 `item` 的处理函数。`item.schema` 继续负责不同 Rules 配置自己的字段校验。

例如 `$.data.diff[*].f12` 会从东方财富行情响应中选择证券代码。合法路径未命中时继续当前字段的下一个 extractor；
一个匹配保留原始 JSON 值，多个匹配按现有 Rules 数量合同折叠为数组。正文非法或 JSONPath 非法都
直接报错。框架不存在通用 Healing 阶段；JSON 选择没有 Healing，也不会触发 AI fallback。

AI 是与 CSS 并列的独立 extractor。它通过 OpenAI-compatible Chat Completion API 发送当前
Response 文本和 `expr` 提示词，结果严格限定为一个 JSON 对象：

```yaml
- kind: ai
  expr: '按 {"title":"xx","content":"xx"} 提取文章。'
```

provider 配置属于 Worker 本地运行配置，只构造一次并由代码模式和 Rules 模式通过
`response.ai(expr).await` 复用：

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

业务层负责从环境变量、配置中心或其他密钥来源取得 `base_url`、`api_key` 和 `model_name`，再构造
`ai::OpenAI`；crawler 不负责读取这些值。provider 配置和密钥不会进入 Rules 或 Trace Snapshot。
`base_url` 必须是绝对 HTTP(S) 端点，不能内嵌凭据，也不能包含 query 或 fragment。凡是能从同一个
任务池领取 AI 工作的 Worker，都必须配置等价的 provider endpoint 和 model。AI 不生成 CSS，也
不会被 CSS Healing 自动调用。请求会同时设置
`response_format=json_object`；数组、标量、Markdown 和说明文字在返回后仍会被拒绝。Response
正文缓冲区在字符集解码前限制为 1 MiB；包含 `expr`、固定约束和解码后正文的完整 UTF-8 prompt
另行限制为 1 MiB。provider HTTP body 在 HTTP 内容解码后、`async-openai` 完整缓冲前限制为
4 MiB。每次 provider 调用超时为 60 秒；重试继续只使用已有 `error_parse` 策略。

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

完整配置位于 [examples/rules-newspaper.yaml](../examples/rules-newspaper.yaml)，运行命令：

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
不参与业务去重。内置去重只作用于进入 Scheduler 前的 Request；Item 业务去重属于下游业务逻辑，
不属于 Store 合同。

Engine 默认使用 `item::Jsonl` 作为 Item Store，并将每条数据保存为一行 JSON：

```text
./data/items/output/<task_id>/<yyyy-mm-dd-HH>.jsonl
```

每行格式固定为 `{"id":"...","data":{...}}`，框架 Item ID 与业务数据分开保存。
`self.tx.request(...)` 把 Request 提交给 Scheduler；`self.tx.item(...)` 则把只包含 Item 的 `Payload`
交给 `Store::submit(&Payload)`。两者可以独立替换：`.with_scheduler(...)` 只替换 Request 调度，
`.with_store(...)` 为本次 Engine 运行选择唯一的 Item Store，并替换 Item 持久化。Store 选择在
`build()` 完成时固定；框架不会把后续 Item 路由到其他 Store，也不会把一次提交广播到多个 Store。
不需要、也不能配置 `persister_id`。

`Jsonl::with_dir(path)` 可修改输出根目录。Jsonl 会先序列化完整 Payload，再持有小时文件的追加锁写入并
flush 完整字节序列；写入或 flush 失败时记录并返回错误，不执行事务或文件回滚，底层已经写入的字节可能
保留。打开文件缓存有固定上限，所有缓存槽都在使用时，额外路径使用不进入缓存的临时句柄。提交失败快照由
Jsonl 自己管理，位于
`<dir>/data/items/snapshots/`：先完整写入临时文件并 flush，再原子 rename；同一个不可变 Payload 投影后续重试成功后
删除，重试耗尽则保留供人工处理。完全相同的不可变 Payload 投影可以共用这份恢复快照，但每次 `submit`
仍会执行输出，不会形成持久化回放索引或 Item 去重。Item 提交采用 at-least-once 语义，业务 Item 去重属于
Store 合同之外的下游业务逻辑。
每个 Store 实现都必须在修改后端前调用 `Payload::validate_store()` 校验完整 Payload。Store 重试耗尽后，
`error_item` 只作为逐个 Item 的 best-effort 通知：回调失败记录日志，当前 Request 仍保留原始 Store 错误。

## Request 执行

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
一次领取为空后默认等待一秒再试；`with_idle_interval(duration)` 可替换这个正数且只在启动时加载的
间隔。空队列不会结束 Worker。`Runtime::start()` 监听 SIGINT/SIGTERM，收到信号后停止新领取，允许已经
开始的领取返回，不设置内部超时地排空已接受 Request 和 Tx 工作，最后才关闭各组件。
当前 Request 内直接 await 的 Tx 调用可以使用完整 Request 上下文；移动到独立任务中的 Tx clone
只保留 `task_id / trace_id`，不会继承 Request 执行权、租约身份、node、version 或 stats。
任何输出 Request 都必须在执行 `before_scheduler` Middleware 前匹配该 Tx 的 `task_id / trace_id`，
且这些 Middleware 不得改写 `id / task_id / trace_id`。
Request 并发数、单次领取上限和 Event 容量是三个独立且只在启动时加载的配置，分别通过
`with_concurrency`、`with_claim_limit` 和 `with_event_limit` 设置。
Actor 开始处理 Event 时即释放 Event 容量，但 Tx 调用仍会等待对应 Scheduler 与 Middleware 工作完成。

## Dedup

Dedup 只处理 Request。每一条生效的 `dedup` Spec 就是一条规则，参数固定为扁平的
`args.key / normalize / ttl`，旧 `args.rules` 配置非法。成员桶固定为 `task_id + node`，SHA-256
只计算配置明确选择的有序值的规范 JSON；`trace_id`、Middleware name、`Spec.key`、Rules 名称和
隐式 URL 都不参与。对象 key 递归排序，数组顺序、配置路径顺序、JSON 类型和重复值保持有效；
`before_scheduler` 不允许改写 node。

默认 `middleware::dedup::Memory` 原子检查并写入一条精确成员。TTL 省略或 `-1` 表示进程生命周期内
永久保留，`0` 表示跳过查询和写入，正整数按毫秒过期。运行期 `Tx` 输出在 `Scheduler::push` 前执行
Dedup；Rules 运行种子不经过准入，因此不会提前消耗去重状态。
`contrib::middleware::dedup::Redis` 使用 RedisBloom，注册时显式提供 `capacity / error_rate`，接受
Bloom 误判，不增加可配置 namespace；正数有限 TTL 会被拒绝，不使用 Set 或时间桶模拟成员过期。
第一个创建 `task_id + node` filter 的 Worker 会固定其实际 Options，共享该桶的所有 Worker 必须使用
等价 Options。

## HTTP Downloader

HTTP Downloader 按 Request 应用 proxy 和 TLS 配置。只有 proxy URL 与
`accept_invalid_certs` 完全相同的请求才复用 Client；默认最多保留 64 个空闲 Client，
空闲 90 秒后惰性过期。容量压力下淘汰最早空闲的 Client，正在使用的 Client 不会被淘汰，
因此可以暂时超过该上限。`Http::with_max_idle_clients` 在 Worker 启动时替换默认值，
`Http::close()` 清空整个池。直连请求、不同代理凭据和不同 TLS 行为不会共用同一 Client 条目。

每个 Worker 默认最多接收 64 MiB 解码后响应体。`Http::with_max_body_bytes` 可以替换这个默认上限；
代码或 Rules 中的 `Request.max_body_bytes` 只能选择小于或等于 Worker 上限的正数值，超过时在网络 I/O
前失败。Downloader 按解码后 chunk 读取且不预分配上限大小，但成功结果仍是一个有界的
`Response.body: Bytes`；本合同不增加公开 stream 或文件写入 API。`Request.timeout` 覆盖连接、
所有 redirect 和最终 body 读取的一次完整下载；redirect 不重置超时，每次新的下载重试从零开始新预算。
只跟随 `301`、`302`、`303`、`307` 和 `308`。redirect 将带 body 的 Request 改为 GET 时，会随被丢弃
的 body 一并删除相关 headers；原始 GET 即使带 body 也保留其 headers。

### Headers

`Headers` 直接包装标准 `http::HeaderMap`，保留大小写不敏感的名称、响应原始值和重复字段。
`set` 替换该名称的所有值，`append` 追加新值；Rules 输入仍是单值 map。Request Snapshot
把规范化名称序列化为值数组，不可表示为字符串的 Request header 值在 Snapshot 边界被拒绝。

### Cookies

每条 Request 都携带可序列化的 CookieStore 快照。Downloader 在解析前按标准 Domain、Path、Secure、
Max-Age 和 Expires 规则应用所有响应 Cookie，后代 Request 通过 Snapshot 和 Scheduler 恢复继承这条谱系。
已入队的并行兄弟各自保留独立快照，后续 Cookie 变更不会反向修改它。跨源 follow 或 redirect 会移除
源站 headers，并把 CookieStore 缩减为目标 URL 可用的 Cookie，无关凭据不会进入目标 Request Snapshot。
跨站 Public Suffix Domain 会被拒绝；与当前 Request host 完全相同的 Public Suffix 会规范化为
HostOnly。原始 `Cookie` header 不作为第二套会话来源，代码与 Rules 必须使用 Request cookie API。

## Rate Limit

每个 `rate_limit` group 在活跃期间固定一个 interval，同组使用冲突 QPS 会返回非法配置错误。
只有当该 group 无持有者且下一次允许时间已过，后续查找才会惰性清理它。默认
`middleware::rate_limit::Memory` 只在当前 Worker 内限速；
`contrib::middleware::rate_limit::Redis` 使用 Redis 服务端时间，让多个 Worker 共享同一个 group 的
总 QPS。Redis key 只来自显式 group；未配置时使用 Request URL host，不加入 `task_id` 或 Worker 身份。

## Response 文本

v3 的响应文本合同保持 `Response.body` 为 HTTP 内容解码后、字符转码前的 Downloader 交付 bytes，
并把字符解码统一放在 `Response::text()`。编码优先级固定为 BOM、合法的 `Content-Type` charset、HTML 或缺失 MIME 时前
1024 bytes 内的 HTML meta，最后回退 UTF-8。非法字节使用 Unicode replacement 语义，运行时不做
统计字符集猜测；`Response::json<T>()` 复用同一条文本解码路径。

## 运行期追踪

运行期追踪用于观察一条已领取 Request 从确认执行权、下载、解析、Tx 输出到最终结算的耗时和结果。
它是可选能力，默认关闭，不改变 Scheduler、Downloader、Spider、Item Store 或业务 `trace_id` 的语义。

### 开启依赖

可执行程序需要开启 `spider/runtime-tracing`，并直接依赖 `fastrace` 来安装 Reporter：

```toml
[dependencies]
fastrace = { version = "0.7.18", default-features = false, features = ["enable"] }
spider = { path = "../spider", features = ["runtime-tracing"] }
```

使用 API Scheduler 时，改为开启 `contrib/runtime-tracing`；该 feature 会同时开启
`spider/runtime-tracing`，并允许 API Scheduler 向可信 Master 传播追踪上下文：

```toml
[dependencies]
contrib = { path = "../contrib", features = ["runtime-tracing"] }
fastrace = { version = "0.7.18", default-features = false, features = ["enable"] }
```

只在 Cargo 中开启 feature 还不会采样 Request。可执行程序还必须安装进程级 Reporter，并对本次
Engine 运行调用 `with_tracing`。未启用 feature 时，追踪代码不会生成 span；即使调用
`Tracing::all()`，Engine 仍按无追踪方式运行。

### 在本地终端查看

`ConsoleReporter` 把完整的 `SpanRecord` 输出到当前进程的标准错误流 `stderr`，因此直接在终端运行
程序就能看到本地追踪结果：

```rust
use fastrace::collector::{Config, ConsoleReporter};
use spider::{engine, trace};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Reporter 是进程级依赖，应在 Engine 产生任何 span 之前安装一次。
    fastrace::set_reporter(ConsoleReporter, Config::default());

    let mut engine = engine::Engine::new()
        .with_spider(BasicSpider::new())
        .build()
        .with_tracing(trace::Tracing::all());

    // 先保存结果，保证 Engine 失败时也会刷新已经完成的 span。
    let result = engine.start().await;
    fastrace::flush();
    result?;
    Ok(())
}
```

`Config::default()` 的最大后台上报间隔为 1 秒。长时间运行的 Worker 会周期输出，进程正常退出前仍应
调用一次 `fastrace::flush()`，立即把剩余记录交给 Reporter。需要调整周期时可以显式配置：

```rust
use std::time::Duration;

fastrace::set_reporter(
    ConsoleReporter,
    Config::default().report_interval(Duration::from_millis(500)),
);
```

也可以把输出重定向到文件；由于 `ConsoleReporter` 使用 `stderr`，普通错误日志也会进入同一个文件：

```bash
cargo run --bin <your-bin> 2> runtime-traces.log
```

fastrace 的 ConsoleReporter 与代码中的 `tracing::warn!`、`tracing::error!` 是两条独立管线。后者仍由
应用自己的 `tracing` Subscriber 决定是否和如何输出；`RUST_LOG` 不控制 ConsoleReporter 的 span。生产
环境可以把 ConsoleReporter 替换成实现 `fastrace::collector::Reporter` 的远端 Reporter，Engine 配置
无需变化；具体后端和配套 crate 由应用选择，crawler 不固定 Jaeger、Datadog 或 OpenTelemetry 实现。

### 全量与采样

本地调试可以追踪全部 Request：

```rust
let tracing = trace::Tracing::all();
```

生产环境通常按比例采样：

```rust
let tracing = trace::Tracing::sample(0.1)?; // 约 10%

let mut engine = engine::Engine::new()
    .with_spider(BasicSpider::new())
    .build()
    .with_tracing(tracing);
```

`Tracing::sample(ratio)` 只接受有限的 `0.0..=1.0` 数值；`0.0` 表示不采样，`1.0` 等价于
`Tracing::all()`，NaN、无穷大和越界值会返回错误。`Tracing::default()` 同样关闭采样。配置在本次
Engine 启动后冻结，不做热更新。

采样不是每次领取时重新随机决定。框架根据 Request ID、Request version 和 Worker ID 计算稳定结果，
因此同一 Request version 被同一 Worker 再次执行时会保持一致；Request 被另一个 Worker 执行时，
采样结果可以不同。下载、解析或 Scheduler 操作的内部重试不会重新进行根链路采样。

### Trace 结构

每条被采样且已领取的 Request 都建立一条独立的 `crawler.request` 根 span。Engine 不建立覆盖整个
Worker 的总根 span，避免常驻进程生成一条无限增长的 trace。同一业务 Trace 下的多条 Request 因此会
拥有不同的 fastrace TraceId；需要按一次业务运行聚合时，应查询根 span 的
`crawler.trace_id` 属性。

根 span 固定记录以下有界属性：

| 属性 | 含义 |
| --- | --- |
| `crawler.task_id` | Task 身份 |
| `crawler.trace_id` | crawler 的业务运行身份 |
| `crawler.request_id` | 当前 Request 身份 |
| `crawler.node` | 当前 node |
| `crawler.version` | Request 执行版本 |
| `crawler.worker_id` | 当前 Worker 身份 |
| `crawler.mode` | `http` 或 `browser` |

按实际执行路径，根 span 下可能出现：

| Span | 覆盖范围 |
| --- | --- |
| `scheduler.ack` | Downloader 开始前确认当前执行权 |
| `crawler.execute` | 当前 Request 的 Middleware、下载和解析主流程 |
| `middleware.before_download`、`middleware.after_download`、`middleware.before_parse` | 对应 Middleware 阶段 |
| `downloader.fetch` | 一次下载尝试 |
| `executor.parse` | 一次完整解析尝试 |
| `middleware.error_download`、`middleware.error_parse` | 下载或解析终态错误回调 |
| `output.requests`、`output.items` | Spider Tx 产生的 Request 或 Item 输出 |
| `middleware.before_scheduler`、`scheduler.push` | 新 Request 的准入与入队 |
| `middleware.before_item`、`item_store.submit` | Item 准入与持久化 |
| `middleware.error_item` | Item 提交终态错误回调 |
| `scheduler.refresh_lease` | 长任务执行权刷新 |
| `scheduler.success / failure / release` | 成功、失败或未执行归还结算 |

未发生的阶段不会产生占位 span。例如没有重试时只出现一次下载或解析 span，没有 Item 输出时不会
出现 `output.items`。当前 Request 根 span 从 Scheduler 已经领取 Request 后开始，不覆盖 Engine 的空闲
轮询或 `next_requests` 调用。

每个操作 span 通过 `span.status_code=ok|error` 表示结果；错误只记录有界的 `error.type` 分类。重试
span 从 1 开始记录 `retry.attempt`，Tx 输出记录 `output.count`，下载记录
`http.request.method` 和成功响应的 `http.response.status_code`。Reporter 自身还会提供 TraceId、SpanId、
父子关系、开始时间和耗时。

### 上下文与数据边界

fastrace TraceId 只属于运行期观测，永远不会替代 crawler 的业务 `trace_id`，也不会写入 Request、
Trace Snapshot、Payload、Item、失败快照或 Rules DSL。Tx 的 Request/Item Event 会在进程内私下携带
当前 span context，使异步输出仍属于产生它的当前 Request 链路；这个字段不是公共或持久化合同。

分布式场景只在 Worker 使用 `contrib::scheduler::api::Api` 调用可信 Master 时注入 W3C
`traceparent`。框架不会把该 header 发送给爬取目标、redirect 目标或 AI provider，也不会向 Redis
传播追踪上下文。同一次 API 操作的传输重试使用同一个 `traceparent`。API Scheduler 不在某条
Request 的活动上下文中时，不会凭空生成该 header；Master 是否继续接收和导出上下文由独立 Master
项目决定。

span 不记录响应正文、Request body、Item 内容、AI prompt、API key、Cookie、代理凭据、完整 URL 或
原始错误文本。业务标识不超过 128 bytes 且不含控制字符时按原值记录，否则记录稳定的
`sha256:<hex>` 标记。业务标识本身仍对 Reporter 可见，因此不要把密钥或令牌用作 Task、Trace、
Request、node 或 Worker ID。该数据边界只约束 fastrace span；应用自己的普通日志仍需独立遵守脱敏规则。

### 没有本地输出时

依次检查以下条件：

1. 可执行程序是否编译开启了 `spider/runtime-tracing` 或 `contrib/runtime-tracing`。
2. `fastrace::set_reporter(...)` 是否在 Engine 启动前执行。Reporter 安装前产生的 span 会被忽略。
3. 当前 Runtime 是否调用了 `with_tracing(Tracing::all())` 或非零比例的 `Tracing::sample(...)`。
4. Scheduler 是否真的领取并执行了 Request；空队列和仅有轮询不会产生 `crawler.request`。
5. 进程退出前是否调用了 `fastrace::flush()`，以及终端或容器是否收集了 `stderr`。

Reporter 未安装、当前 Request 未被采样或编译期 feature 未开启时，Engine 都会继续正常执行；追踪
上报失败不参与 Request 的成功、失败或重试判断。

## 当前范围

当前核心运行时包含：

- 单进程运行
- Memory Scheduler
- HTTP 下载
- CSS 和正则表达式
- 确定性 CSS Healing 和显式 AI Selector
- 请求校验、重试、限速和去重中间件
- 代码模式与 YAML 规则模式
- 本地 JSONL Item 输出

每个 Scheduler 自己持有 Worker 身份和支持的下载模式；Engine 只向 `open(concurrency)` 传入冻结的
并发数，并向 `next_requests(limit)` 传入批次上限。能力筛选必须和领取原子完成。Redis 是当前可用的
持久化 Scheduler 实现，并已通过共享 Scheduler 一致性测试。
Worker 侧 `contrib::scheduler::api::Api` 适配器也已经实现；对应 Master 服务不属于这个 workspace。
本仓库只维护 Master 功能和协议设计，服务端、数据库、Cron、Control API 与前端由独立项目负责。
可选 `fastrace` 运行期链路追踪已经实现；真实 Browser Downloader 与 HTTP/browser 混合端到端执行
属于 v5。AI provider 配置已经收口为 Worker 本地配置：通过 `Engine::with_ai` 注入一个可复用的
`ai::OpenAI` provider，Rules 只保留提示词。
Memory 使用内部身份 `local`，默认支持 HTTP，可通过 `Memory::with_modes(...)` 替换能力集合；它不注册、
也不发送心跳。Redis 和 API 要求在 Scheduler 上配置稳定的 Worker ID、host 和 version，
`with_modes(...)` 冻结其下载能力。缺少元数据或 mode 集合为空会在 Scheduler 配置/open 时被拒绝。

媒体对象规范化不会下载文件。Item 附件下载规划为独立的 v5 变更；它与 Browser Downloader 并列，
但不依赖 Browser 实现。

后端或 API 在保存配置前，可以通过 `Config::validate()` 校验完整规则，也可以通过
`middleware::check(&spec)` 单独校验一条中间件配置。

## 开发验证

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo test --workspace --all-targets --features runtime-tracing
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --features runtime-tracing -- -D warnings
cargo doc --workspace --no-deps
```
