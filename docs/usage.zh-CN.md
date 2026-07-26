# Crawler 使用手册

[项目 README](../README.zh-CN.md) | 简体中文 | [English](usage.md)

`crawler` 是一个使用 Rust 编写的爬虫运行时。默认单进程形态提供内存调度器、HTTP 下载器、中间件
生命周期、异步 Spider 处理器、规则模式、CSS Healing、AI Selector 和本地 JSONL 数据输出；`contrib`
还提供 Redis 与 Worker 侧 HTTP API Scheduler，用同一套 Engine 合同支持持久化的多 Worker 队列。
`master` crate 是基于 Axum/MySQL 的控制面，不是 Scheduler。运行时已完成 Task/Trace 运行种子、按
Worker 能力领取 Request，以及确定性的响应字符集解码。

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
Trace Snapshot、Request 状态、按 mode 分域的 processing 租约、结算和统计。每个
`processing:<mode>` ZSET 是唯一的活动执行权/租约投影，score 使用 `lease_time`；Request Hash 仍是状态
事实来源。Item 持久化属于独立的 `item::Store` 依赖，不进入 Redis Scheduler。

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

## Selector

在处理函数中通过 `self.tx.request(...)` 提交的新请求会进入同一个 Scheduler 队列；通过
`self.tx.item(...)` 提交的数据经过 Item 中间件后交给独立的 Item Store。

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
直接报错；JSON 选择没有 Healing，也不会触发 AI fallback。

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

## API Scheduler 与 Master

`contrib::scheduler::api::Api` 是远程 Worker 传给 `Engine::with_scheduler(...)` 的 Scheduler。
它把既有的 Scheduler 和 Init 合同转换为 Master Worker API，而不是对 MySQL client 的薄包装。
`Api::initializes_run()` 返回 `false`：远程代码 Worker 只消费 Master 已派发的运行，不会本地创建
Trace，也不会调用 `Spider.start()`；但它仍需要本地 Rust Spider，用稳定 node 解析代码 handler 并
执行业务逻辑。

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

`master` 是一个独立的 Axum 服务，使用私有 MySQL 8.0.19+ 存储。数据库 URL、namespace、Worker
token、control token 和运行限制统一写入严格校验的 `master/etc/master-api.yaml`，启动时显式传入该文件：

```bash
cargo run -p master -- --config master/etc/master-api.yaml
```

Master 自身监听明文 HTTP。生产环境必须由可信反向代理或负载均衡器在流量到达该监听端口前终止 TLS；
Bearer token 不能通过不可信网络明文传输。

仓库中的模板将两项 token 留空，部署方填入两套不同凭据前 Master 会拒绝启动。容量和保留时间使用固定单位的整数：

```yaml
api:
  max_size: 67108864
history:
  ttl: 172800
  cleanup_limit: 1000
```

`api.max_size` 的单位固定为字节，`history.ttl` 的单位固定为秒。两个字段只接受 YAML 正整数；
`"64MiB"`、`"48h"`、`"67108864"` 等字符串均会被拒绝。

控制面按职责分层：`master/src/handler/` 负责 Axum 提取器和路由 handler，
`master/src/logic/` 负责资源业务操作，`master/src/svc.rs` 持有共享 service context，
`master/src/types/` 统一 Worker/control DTO。Handler 不直接调用 MySQL；
`master/src/config/` 负责加载并校验运行时 YAML，`master/src/store/mysql/` 保持为控制面的私有持久化实现。

两端的 API 消息上限默认都是 64 MiB。Worker 在打开前通过
`Api::with_max_response_bytes(...)` 设置接收容量；Master 通过
YAML 的 `api.max_size` 字节整数或 `master::Config::with_api(...)` 设置请求/响应上限，允许范围为 1 KiB 到
4 GiB 减一字节。
启动时若 Master 上限大于 Worker 已配置的容量会被拒绝，因此 64 MiB 是默认值，不是全局固定上限。
Worker 会在发起网络请求前，把每条出站 JSON 消息序列化到同一个有界容量中，并在传输重试时复用这份
不可变 bytes。Master 在读取或解析请求体前先校验对应的 bearer 凭据与 namespace，因此未认证的非法或
超大 JSON 会先返回未认证错误，不会进入 JSON body 处理路径。

Worker token 与 control token 必须是两套不同且可组成 HTTP Bearer Header 的凭据。Worker token
只能调用 Scheduler Worker API；control token 用于发布 Task，并通过 `/v1/control/tasks`、
`/v1/control/traces`、`/v1/control/requests`、`/v1/control/workers` 和 `/v1/control/items` 读取控制面状态。
列表使用有上限的 keyset 分页，
大体积 Snapshot 与 Item data 只在详情响应返回。Worker 不会获得 MySQL 直连权限，Master 本身也
不能传给 `Engine::with_scheduler(...)`。

Master 的私有 MySQL 连接使用 `READ COMMITTED` 隔离级别；namespace 与身份类列使用二进制
`utf8mb4_0900_bin` collation，保证标识符和幂等 key 按字节区分。

Master 把 Task 保存为 Rules DSL 或序列化的代码 seeds，从不保存 Rust handler。它的 Cron 每次最多为
配置的派发上限内的到期 Task 创建新的 Trace 和初始 Request，通过正常重试状态机恢复过期租约或离线
Worker 的租约，但不启动、停止或监督 Worker。Task 发布只做静态校验；Cron 直接将保存的 Rules 或
代码 seed 实例化并入队，seed 派发期间不会执行任何 Worker 的 `before_scheduler`、Middleware 或 Dedup。
确定性损坏的持久化 Task 会被隔离为 `failed`，并通过 control API 暴露错误；它不会阻塞后续到期 Task，
重新发布修正后的定义即可再次进入调度。Claim 按 128 行的存储页完成候选校验，再为接受结果统一生成
租约时间；单次调用最多隔离 128 条非法候选后让出，后续领取继续清理。全部合法时可以持续跨页，直到达到
请求数量或响应容量。
一个响应对同一
`trace_id` 最多内嵌一次 Trace Snapshot。Worker 会独立读取并缓存省略的 Trace，因此不能仅因 Request
与 Trace 合并后超过响应上限，就把本可单独传输的 Request 判为坏数据。
未决的本地 Init/Item operation key 从创建起固定保留五分钟；Master 要求 `history.ttl` 至少为
`max(lease timeout, 5m30s)`，保证持久化的 operation 与 completion 重放记录覆盖该窗口。配置中的保留期
参数随后会驱动终态 Request、completion 和 operation 历史的有上限清理；Item、Trace、Task 和
trace-stat 的保留仍是独立工作。远程 Worker 是有限生命周期的 Engine：首次使用立即注册，只在能力
变化时重写 modes，在领取期间刷新过期心跳，兼容工作排空后退出，Scheduler close 时停止本地心跳。
Browser 下载、直接 MySQL Scheduler 与
`fasttrace` 运行期链路追踪不属于这套拓扑。

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
`.with_store(...)` 只替换 Item 持久化。

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

Scheduler 合同由 Engine 向 `next_requests` 与待处理判断传入 Worker ID 和支持的下载模式，能力筛选
必须和领取原子完成。Redis 与 Worker 侧 API Scheduler 是当前可用的持久化 Scheduler 实现；Master
是 API Scheduler 背后的独立 Axum/MySQL 控制面，不是另一个 Scheduler 实现。直接 MySQL Scheduler
和 `fasttrace` 运行期链路追踪仍是独立工作；真实 Browser Downloader 与 HTTP/browser 混合
端到端执行属于 v5。AI provider 配置已经收口为 Worker 本地配置：通过 `Engine::with_ai` 注入一个
可复用的 `ai::OpenAI` provider，Rules 只保留提示词。Redis 已通过共享 Scheduler 一致性测试。
Engine 默认使用 `worker-1` 和 HTTP 模式；`with_worker_id(...)` 与 `with_modes(...)` 可替换这些启动时冻结的值，
空 Worker ID 或空 mode 集合会在执行前被拒绝。

媒体对象规范化不会下载文件。Item 附件下载规划为独立的 v5 变更；它与 Browser Downloader 并列，
但不依赖 Browser 实现。

后端或 API 在保存配置前，可以通过 `Config::validate()` 校验完整规则，也可以通过
`middleware::check(&spec)` 单独校验一条中间件配置。

## 开发验证

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
```
