# Harbor Issues

这份清单记录当前 Harbor 在架构、功能边界、数据一致性、安全性和测试覆盖方面需要逐项确认或修复的问题。

处理规则：

- `P1`：会造成数据错误、任务丢失、重复执行或安全风险，优先讨论和修复。
- `P2`：不会立即破坏主流程，但会影响长期运行、扩展性或运维能力。
- 每项问题在边界确认后再修改实现。
- 设计文档不作为本清单的替代品；本文件只记录待处理问题和验证要求。

## P1

### 1. Request 重试重复累计统计

状态：已确认，已实现

当前实现：

- 每次 Request 执行都会创建新的统计增量，并增加节点 `total`。
- Request 失败后进入队列重试，下一次领取会再次增加 `total`。
- Memory、MySQL、Redis 都会在每次结算时合并当前 Payload 的统计。

相关代码：

- `src/engine/request/task.rs`
- `src/scheduler/memory.rs`
- `src/scheduler/memory/settle.rs`
- `contrib/src/scheduler/mysql/settle.rs`
- `contrib/src/scheduler/redis/scripts/failure.lua`

问题：

如果统计语义是“逻辑 Request 数量”，同一个 Request 多次失败会被重复计入 `total`、`download` 等字段，最终统计会大于真实 URL 数量。

已确定边界：

- Counter 按逻辑 Request 统计，不按执行尝试统计。
- 同一个 Request 的重试不重复增加 `total`。
- `total` 表示逻辑 Request 进入节点一次，只累计一次。
- `done`、`filter`、`dedup`、`validate` 和 `download` 只在该逻辑 Request 最终结算时累计。
- 一条逻辑 Request 的最终结果只进入 `done / filter / dedup / validate / download` 其中一个字段。
- Request 最终成功时累计 `done`；Request 最终失败时累计对应的最终失败原因。
- `items` 复用同一个 `Counter`；其中 `items.download` 只表示附件下载最终失败。
- Item 提交失败不新增 `store` 字段，继续通过 `error_item` 和日志表达。
- 重试中的中间失败不写入 `trace_stats`，只保留在当前执行过程和日志中。
- Memory、Redis、MySQL 必须使用相同的统计语义。

修复方向：

把执行尝试期间的临时统计与最终结算统计分开。Scheduler 只合并逻辑 Request 的最终统计增量，并使用 Request 的最终结算身份保证重复提交幂等。

验证：

- 一个 Request 失败两次后成功，节点 `total` 只能统计一次。
- 一个 Request 达到最大重试次数后失败，节点统计仍与逻辑 Request 数量一致。
- Memory、Redis、MySQL 的结果一致。

### 2. Load 消费配置后初始化失败会丢失任务

状态：已确认，已实现

当前实现：

- `Load` 只有 `load()`，没有确认、释放或重新入队操作。
- Redis Load 使用 `LPOP` 取出任务配置。
- 取出后如果 DSL 校验、初始 Request 生成或 `scheduler.init` 失败，Runtime 只记录日志。

相关代码：

- `src/load.rs`
- `contrib/src/load/redis.rs`
- `src/engine/runtime.rs`

问题：

配置从队列消费后，如果初始化失败，原消息不会自动恢复。当前项目明确保持“Worker 领取配置后即从配置队列移除”的边界，不新增配置 Ack、租约或处理中队列。

已确定边界：

- 配置入队前完成 `Message.validate()`。
- Worker 消费配置后执行 `init_task`。
- Scheduler 临时不可用时，Worker 本地最多重试 5 次，使用递增退避。
- DSL、配置、初始 Request 生成等非临时错误不重试，直接记录终态错误。
- 重试耗尽后记录 `task_id / trace_id / error`，不自动重新入队，由人工处理。

修复方向：

保留 Load 的消费即移除语义，只在 Worker 的任务初始化阶段增加有限的 Scheduler 临时错误重试。

验证：

- Scheduler 临时失败时按 5 次退避重试。
- 配置错误不会重试。
- 初始化成功只执行一轮。
- 多 Worker 同时消费时不会重复初始化同一配置。

### 3. 默认 Worker 无法领取 Browser Request

状态：已确认，已实现

当前实现：

- `Memory::new()` 默认只声明 HTTP 能力。
- Worker 默认 Downloader 同时包含 HTTP 和 Browser Downloader。
- Worker 配置通过 `download_modes` 声明默认 Memory Scheduler 支持的下载模式。

相关代码：

- `src/scheduler/memory.rs`
- `worker/src/builder.rs`
- `worker/src/config.rs`

问题：

DSL 生成 Browser Request 后，默认 Memory Scheduler 不会领取，Request 会留在队列中等待。

修复方向：

默认 Worker 使用 HTTP；需要 Browser 时通过 `Config::with_download_modes([Mode::Http, Mode::Browser])` 显式开启。用户传入自定义 Scheduler 后，由自定义 Scheduler 自己管理能力范围。

验证：

- HTTP Worker 不领取 Browser Request。
- Browser Worker 可以领取 Browser Request。
- 默认 Worker 的 `download_modes` 与 Memory Scheduler 能力保持一致。

### 4. Request 状态编码与数字状态约定不一致

状态：已确认，已实现

当前实现：

- Rust 内部继续使用 `State` 枚举，Wire 和 Snapshot 使用数字序列化。
- MySQL 的 Request 和 completion 状态使用 `TINYINT UNSIGNED`。
- Redis Hash 中使用数字字符串保存状态，Lua 状态判断与 Wire 数字一致。

相关代码：

- `src/net/request/contract.rs`
- `contrib/src/scheduler/mysql/settle.rs`
- `contrib/src/scheduler/redis`

问题：

之前约定过状态使用数字表示，旧字符串语义会让不同后端和传输结构不一致。

已确定边界：

- Request 状态固定为 `0=pending, 1=processing, 2=done, 3=failed`。
- Worker、Task、Event 等其他对象后续分别定义自己的状态合同，不复用 Request 状态。
- Core、Snapshot、Redis、MySQL 使用同一组 Request 数值状态。

修复方向：

已同步修改 Core、Redis、MySQL、Snapshot 和测试，不保留旧字符串兼容解析。

验证：

- 所有 Scheduler 的状态读写一致。
- 旧状态不会被静默解析成错误状态。
- 状态转换和存储测试覆盖 pending、processing、done、failed 全路径。

### 5. 租约自动回收规则需要统一

状态：已确认，已实现

当前实现：

- lease_time 只用于当前租约的所有权校验和 Worker 续租。
- Memory、MySQL、Redis 的领取流程都不会因为 lease_time 到期自动回收 Request。
- MySQL、Redis 提供显式 `recover_worker(worker_id)`，只处理已经标记为 offline 的 Worker 持有的 Request。

相关代码：

- `src/scheduler/memory/claim.rs`
- `src/scheduler/memory.rs`
- `contrib/src/scheduler/mysql/request.rs`
- `contrib/src/scheduler/redis/scripts/claim.lua`

边界：

- lease_time 不作为自动回收触发条件。
- 只有外部健康检查先把 Worker 标记为 offline，之后才能调用 `recover_worker(worker_id)`。
- 回收条件是 `state = processing` 且 `leased_by = worker_id`，不检查 lease_time。
- 回收会清空 `leased_by`、重置 `lease_time` 并重新入队；下一次 claim 递增 version。
- 未完成 ack 的 Request 回收不消耗 retry_count；已经 ack 的 Request 按队列重试规则处理。
- 旧 Worker 的 Ack 仍须通过 `leased_by + version` 校验，回收后的旧 Ack 必须失败。
- Worker offline 状态检测与 Request 回收是两个独立动作。
- Memory 是进程内存储，不维护可供外部恢复的 Worker 状态；进程退出后由进程状态丢失语义处理，不提供租约超时回收。

验证：

- 长任务在 lease 到期但 Worker 仍在线时不会被重复领取。
- Worker 离线后的 Request 能按约定被其他 Worker 接管。
- 旧 Worker 的 Ack 按 `leased_by + version` 正确失败。

### 6. 附件下载缺少目标地址安全策略

状态：已确认，已实现

当前实现：

- Local Attachment Downloader 只允许 `http` / `https`，并拒绝带凭据的附件 URL。
- 默认拒绝 `localhost`、回环、私有、链路本地、未指定、组播和保留地址。
- 域名会先解析并检查全部结果地址，解析失败或没有地址时拒绝下载。
- 关闭 `reqwest` 自动重定向，由下载逻辑逐跳处理并校验目标；重定向最多 10 跳。
- 超时、大小限制和重试仍沿用附件统一配置。

相关代码：

- `src/item/attachment.rs`

问题：

附件地址来自页面或 Item 字段时，恶意页面可能诱导 Worker 请求内部服务，形成 SSRF。

验证：

- localhost、私有 IP、非 HTTP(S) 地址被拒绝。
- 重定向到不允许的域名或内网地址在下一跳请求前被拒绝。
- 超时、大小限制和重试仍保持有效。

## P2

### 7. Memory Scheduler 缺少历史数据清理

状态：已确认，已实现

当前实现：

Memory 提供显式 `Memory::cleanup(before)`，清理超过截止时间的终态历史。
清理不会触碰排队中或 processing 中的 Request。

相关代码：

- `src/scheduler/memory/state.rs`
- `src/scheduler/memory.rs`

清理规则：

- 只清理已经完成或最终失败、且所有完成记录都早于 `before` 的 Request。
- 同步删除这些 Request 的 `completed`、`request_hashes` 和残留确认记录。
- 只有没有活动 Request 的 Trace 才会清理 `trace_snapshots`、`trace_stats` 和内部活动时间。
- `done / failed` 累计计数不回退。
- 清理是 Memory 后端的显式能力，不加入 `Scheduler` 公共 trait，也不自动启动 Cron。

### 8. 动态 Load 配置缺少完整的消费前校验

状态：已确认，已实现

当前实现：

- `Load.load()` 返回配置后，立即执行运行前校验。
- 校验通过后才进入 `Scheduler::init`。
- 动态 Rules 配置会检查 AI Provider、Item Function 和 Item Schema。
- `Load` 已确定的消费即移除边界保持不变。

相关代码：

- `src/engine/rules.rs`
- `src/engine/runtime.rs`

错误规则：

- 配置、DSL、AI、函数或 Schema 错误不重试，记录 task_id、trace_id 和错误。
- Scheduler 临时错误继续复用当前 Message，按初始化退避策略重试。
- 不修改 `Load` 或 `Scheduler` 公共接口，也不增加确认、死信或回队列机制。

### 9. `download` Middleware 与 AttachmentDownloader 的边界容易误解

状态：已确认

当前实现：

- `download` 作为 `before_item` 的内置 Middleware 名称注册。
- Registry 对 `download` 做特殊分支处理。
- 真正的下载实现通过独立 `AttachmentDownloader` 注入。

相关代码：

- `src/middleware/registry.rs`
- `src/item/attachment.rs`

边界：

- `download` Middleware 负责决定当前 Item 是否执行附件下载流程。
- `AttachmentDownloader` 负责具体的附件下载实现。
- `with_middleware("download", ...)` 只控制下载流程，不替换下载器。
- 用户需要替换下载实现时，使用 `with_attachment_downloader(...)`。

### 10. Cron 当前只有表达式解析能力

状态：已确认

职责边界：

- 支持五位 crontab 表达式。
- 支持 IANA 时区。
- 判断当前时间是否到期。
- 计算下一次执行时间。
- 支持一次性任务、周期性任务和工作日限制等表达式能力。
- Cron 不创建 Request，不解析 DSL，不调用 Worker，不管理 Scheduler，也不保存 Item。
- 任务配置读取、到期后的消息写入 Load 队列和失败恢复属于外部 Cron / Master 应用层。
- Worker 从 Load 队列读取任务配置后，再根据 DSL 初始化 Trace 并生成 Request。

相关代码：

- `contrib/src/cron.rs`

实现结论：

`contrib::cron` 只提供表达式解析、到期判断和 `next_time` 计算，不提供完整 Cron 控制面。

## 外部验收测试

状态：已确认

默认 Workspace 测试必须保持确定性，不依赖网络、ChromeDriver、Redis、MySQL、账号、Cookie 或 Token。当前 Workspace 测试和 Clippy 已通过。

以下测试作为独立外部验收测试维护，不阻塞默认测试和 Crates 发布：

- EastMoney 真实网络测试；
- X 真实页面测试；
- X 账号接口测试；
- Browser + ChromeDriver 测试；
- 部分真实 HTTP 测试。

外部验收测试通过显式命令或环境开关运行，只用于验证真实服务、浏览器和外部存储的兼容性。
