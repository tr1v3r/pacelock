# Pacelock 设计说明

状态：已实现已知消耗的联合准入、有保留期限的幂等准入与结果查询；输出预留及用量校正仍为提案。商用加固契约见 [commercial-readiness.md](commercial-readiness.md)。

## 已实现：可替换 Driver

核心模块只依赖标准库，公共构造函数接受 `Driver`，这是 pre-v1 破坏性变更。
Driver 以 `Admit` / `Lookup` 表达完整原子操作，Redis 和 PostgreSQL 是独立模块。
Redis 协议保持 v1；PG 用 domain 行锁、服务端时间和短事务，复用核心 `Evaluate`。
当前不支持调用者业务事务接入，不提供跨后端债务迁移或故障自动切换。
统一契约见 [drivers.md](drivers.md)，PG 部署边界见[指南](../drivers/postgres/README.zh-CN.md)。
下文 Redis 数据布局、Lua 故障与 TTL 说明专属于 Redis Driver。

## 目标与边界

多个实例调用同一个供应商账户时，各实例独立限流无法保证总量。Redis Driver 把额度状态放在 Redis，使用一次 Lua 脚本完成联合准入；PostgreSQL Driver 使用行锁与短事务。首版围绕已知消耗提供小而明确的接口，不执行工作、不管理队列、不统计批次完成、不持有并发 permit。

与通用单维限流器相比，首版着重支持请求数与 token 数的联合准入，以及便于后续扩展的明确错误语义。Redis Driver 中的 Go-Redis 是有意引入的运行时依赖：复用连接池、认证、TLS、Cluster 路由和脚本缓存，不自行实现 Redis 协议。

## 公开契约

```go
New(driver Driver, namespace, domain string) (*Limiter, error)
(*Limiter).Allow(ctx context.Context, rules ...Rule) (Decision, error)
(*Limiter).Wait(ctx context.Context, rules ...Rule) error
(*Limiter).WaitOnce(ctx context.Context, id string, retention time.Duration, rules ...Rule) (Receipt, error)
(*Limiter).AllowOnce(ctx context.Context, id string, retention time.Duration, rules ...Rule) (Receipt, error)
(*Limiter).Lookup(ctx context.Context, id string) (Decision, bool, error)
Validate(rules ...Rule) error
NewWithOptions(driver Driver, namespace, domain string, options Options) (*Limiter, error)
```

一次请求传入 1～32 条 Rule，每条包含 Name、Rate、Period、Burst、Cost。

- Rate、Period、Burst、Cost 必须为正；Cost 不得超过 Burst，否则这个请求永远不能通过该策略，直接返回 ErrInvalid。
- 同次调用不得出现重复 Name；不会偷偷合并重复额度。
- namespace 和 domain 非空、最多 256 字节；额度名称同样最多 256 字节。
- Limiter 持有调用者提供的并发安全客户端，不负责关闭。调用者必须提供实际非空客户端；包含 nil 指针的接口值也会返回 ErrInvalid。nil context 或未初始化 Limiter 同样会返回错误，不再 panic。
- 正常执行时，全部维度通过才扣减全部额度；任一维度不足则不扣减任何额度。`AllowOnce` 仍会保存拒绝的 attempt 记录，额度状态与准入记录不可混为一谈。
- Decision.Allowed=false 且 err=nil 只代表额度不足；Redis 故障不伪装成限流。
- RetryAfter 为本次观察下各维度等待时间的最大值。其他请求可能抢占额度，不保证等到该时间必定通过。
- Allow 不存储请求身份，不防重复扣减；AllowOnce 提供保留期内的准入去重。两者都不提供反向退款。

对同一额度漏传规则或使用不同 domain，会绕开共享限制。调用层必须负责规则覆盖和命名一致性。

## 已实现：阻塞等待封装

`Wait` 在调用方 goroutine 中立即尝试普通准入，仅在正常额度拒绝后按 `RetryAfter` 休眠并重新判定。所有后端错误立即返回，保留原始扣减确定性；两次尝试之间取消为 `NotCharged`，已确认扣减则返回 nil，不因并发取消而伪装成未扣减。错误的 `Op` 为 `wait`，Observer 为整次等待发出一个事件。

规则在调用开始时复制，支持原有多维与 Cost。休眠期间不持有后端事务、连接或锁；不增加队列、预留或公平性保证。调用方用 context deadline 限制等待。`Wait` 不生成可恢复的 ID。`WaitOnce` 使用同一 ID 等待，只保存成功结果；普通拒绝不扣减、不写记录，参数在成功或 pending 时才绑定。成功保留期从后端准入决策起算，不包含此前等待；成功可供 `Lookup` 查询并由两个 Once 接口重放。已有 `AllowOnce` 拒绝保持不可变，传给 `WaitOnce` 会返回 `ErrConflict` / `NotCharged`。记录过期或丢失后不得复用旧 ID。

可选 `WaitOnceDriver.AdmitWaitOnce` 表达一次立即的成功保留原子准入，不扩展基础 Driver；不支持时返回 `ErrUnsupported` / `NotCharged`。Redis 通过可选尾参数跳过拒绝写入，成功沿用 v1 pending/complete 记录与原始指纹，保证部分写入后不重放。PG 的拒绝回滚，成功与额度一起提交。未知结果不自动重试；整个 WaitOnce 调用只发出一个 `wait_once` 事件。同 ID 并发调用只去重准入，不能去重业务执行。

## GCRA 与加权消耗

定义：

- `I = ceil(Period / Rate)`，实现中以微秒为最小单位。
- `B = Burst`，代表空闲时能立即消耗的总单位数。
- `C = Cost`。
- `TAT` 为理论到达时间，首次使用时取当前 Redis 时间。

每次判定：

```text
candidate = max(TAT, now) + C * I
retry     = max(0, candidate - B * I - now)
allowed   = retry == 0
```

通过时保存 candidate；拒绝时保持原额度状态（`AllowOnce` 另存拒绝的准入记录）。每个额度只需常数大小的状态，不需要逐请求滑动日志。

例如每秒 10 单位、Burst=20：空闲时可立即消耗 20 单位；随后每 100ms 恢复 1 单位。它不是“任意一秒最多 10 次”，也不是固定时间窗计数。供应商若使用不同窗口规则，需要保守配置或另加对应策略，不能宣称 GCRA 精确模拟所有 RPM/TPM 限制。

## Redis 数据布局与原子性

每个额度一个字符串键：

```text
pacelock:v1:{sha256(length(namespace):namespace + domain)}:sha256(Name)
value = TAT_microseconds:effective_interval_microseconds:Burst
```

长度前缀避免 namespace/domain 连接歧义；哈希避免调用者输入干扰 Cluster hash tag。相同 domain 中所有额度同槽，所以可一次脚本判定。不同 domain 相互独立，也不能跨 domain 原子准入。

对于未命中已有准入记录的新尝试，脚本读取 Redis TIME，再读取并校验全部额度状态、计算全部候选值，全部通过后才写入额度；`AllowOnce` 的拒绝记录仍会保存。活跃状态的有效间隔或 Burst 不同会返回策略冲突，防止不同实例轮流用不同参数更新同一额度。有效速率相同的不同 Rate/Period 表达被视为同一策略。此校验仅在状态存在时有效；配置迁移需要应用自行协调，不能用换名字的方式无缝继承旧债务。

TTL 为 `ceil((newTAT - now) / 1ms)`，最少 1ms；状态完全恢复后即可删除。拒绝不刷新 TTL。Period 和 Burst × I 都限制在 24 小时以内，防止无界债务与持续时间溢出。TAT 使用整微秒并以整数格式序列化，避免 Lua 默认 tostring 丢失高位精度；服务端时钟还须处于 IEEE-754 精确整数范围内。

复杂度是每次 O(维度数)；单次最多 32 维，避免任意长脚本阻塞 Redis。不作通用吞吐量承诺；可复现基准及实测环境见 [verification.md](verification.md)。

Redis 的脚本原子执行保证其他请求不会插入中间。它**不保证运行时错误回滚**：若写阶段出现内存或权限等错误，应按未知结果处理，不能假定完全没扣减。所有预期的参数、状态和策略检查都在写入前完成。建议专用 Redis、noeviction、稳定 ACL、监控内存与复制延迟。

## Domain 与 LLM 额度建模

假设一个供应商账户有共享 RPM/TPM，同时每个租户有自身 TPM：

```text
namespace: llm-service
 domain:   provider-account-a
 rules:    account:rpm        cost=1
           model-a:input-tpm  cost=已知输入 token
           tenant-42:tpm     cost=该租户的本次已知消耗
```

所有需要共同判定的额度必须处于同一 domain。若租户跨多个供应商账户共享全局额度，就必须把相关额度合并到共同 domain，或者接受外部协调方案；不能既任意分片又无成本获得跨槽原子性。一个热 domain 会成为单分片热点，这是明确的取舍。

Cost 的单位由业务定义。库不把字符数、字节数或 SSE chunk 数当作 token，也不内置分词器。供应商如何处理 input、output、cached token 和 max_tokens，必须由适配层定义；当前示例只演示已知输入 token。

## 故障、取消和重试

| 结果 | 含义 | 调用者行为 |
|---|---|---|
| ErrInvalid | 本地参数无效，未派发 | 修正参数 |
| ErrPolicyMismatch | 活跃策略冲突，本次未写入 | 修正部署或协调迁移 |
| ErrState | 状态、准入记录或返回协议异常；既可能在写入前检出，也可能在扣减后解析响应时检出 | 必须检查 `OutcomeOf(err)`；`Unknown` 不盲目重试或退款，告警排查且不自动重置额度 |
| Allowed=false，err=nil | 正常拒绝，无扣减 | 拒绝或按业务队列稍后重试 |
| Allowed=true，err=nil | 本次全部额度扣减成功 | 执行对应业务尝试 |
| 其他错误 | 包括超时、网络、取消、Redis 运行时错误；可能无法确认扣减 | 不盲目重放或退款，显式处理失败 |

预先取消的 context 在本地返回；派发后取消无法撤销 Redis 写入。具体 go-redis 客户端支持默认、正数及禁用（-1）的 MaxRetries 配置，SDK 通过 v9.22.0 的逐命令 NoRetry 标记禁止不确定重放，同时允许有界 MOVED/ASK 重定向；共享客户端的其他命令保留原有重试策略。Cluster MaxRedirects 可用默认或正数，-1 仍表示关闭重定向。ASKING 与脚本在同一连接发送。构造不联网、不修改 options；自定义 Scripter、hooks、节点工厂和代理须保持标记及不重放契约。

Pacelock 在 NOSCRIPT 时从 EVALSHA 回退到 EVAL，这是缓存未命中的安全回退；不会自行重试不确定的传输错误。Redis failover 仍可能丢失已确认写入；持久化、复制和故障策略属于部署边界。

Redis TIME 是所有实例的共同时间基准，但不是单调时钟。前跳会提前恢复额度，后跳可能延迟恢复，严重后跳会报状态错误。首版不承诺跨时钟跳变严格限流。

## 与 Airlock 配合

Airlock 的并发准入限制正在执行的工作量；Pacelock 限制随时间恢复的消耗。二者没有联合事务。

调用者可以先进行非阻塞并发尝试，再执行 Pacelock 准入；额度不足或异常时立即释放并发 permit。不要在持有 permit 时长时间等待限流恢复。即使这样，Redis 调用期间也会短暂占用并发容量；如果先扣速率再获取并发，则并发失败可能浪费额度。首版明确接受由调用层选择该顺序，不提供伪原子的组合封装。

## 已实现：有保留期限的幂等准入与查询

`AllowOnce` 的 ID 在 namespace/domain 内生效，绑定有序的额度名称、有效发射间隔、Burst、Cost 和保留期限。相同有效策略的不同 Rate/Period 表达可重放，但规则重排或其他参数变化返回 ErrConflict。保留期限为 1ms～24h 的整毫秒，重放和查询不延长 TTL。

记录与额度同槽，使用独立的 `attempt:sha256(id)` 后缀。脚本先读记录：完整记录直接重放；参数不符报冲突；处理中记录报 ErrOutcomeUnknown。无记录时检查所有额度、编码结果，然后先写处理中标记，再写入允许的额度，最后用 KEEPTTL 保存完整结果。拒绝也保存，因此同一 ID 不会随恢复时间改变决策。

处理中标记解决运行时错误后重复扣减的问题，但不能消除部分写入。测试通过 Redis ACL selectors 允许脚本访问全部键、仅允许写第一个额度，在第二个额度触发真实运行时错误，验证同 ID 重试不会再次扣减。

`Lookup` 使用主节点路由，返回保留的原始决策；RetryAfter 是原始观测值。缺失不证明从未扣减：记录可能过期、被删除、被淘汰或在故障切换中丢失，原命令也可能仍在执行。幂等窗口仅覆盖记录存续期，不保证全时段 exactly-once，更不保证上游执行恰好一次。

准入传输恢复复用同一 ID；每次真正的新上游调用使用新 ID。业务执行需要自己的协调机制，不能见到 Replayed 或查到 Allowed 就再次调用模型。

## 已实现：错误与观测

准入错误包装为 `*Error`，保留 errors.Is/As 与底层错误链。Outcome 为 NotCharged 或 Unknown；前者只说明本次调用未扣减，不消除先前尝试的不确定性。返回协议损坏、传输错误、运行时错误和处理中记录均保守标记 Unknown。任何错误下忽略 Decision。

`NewWithOptions` 提供可选 Observer，同步执行且不创建后台 goroutine。事件包含操作、耗时、结果、规则数、重放标识和错误。调用方负责回调并发安全与指标低基数；panic 向外传播。SDK 不引入具体遥测运行时依赖。

## 后续设计：LLM 生命周期（未实现）

### 1. 预留与实际用量校正

输出 token 数通常事先未知。计划区分已知输入、输出预算与最终观测用量，而非只给 Allow 增加一个负数 Cost。

候选流程为 `Reserve → Commit/Reconcile`，取消或过期需要显式终态。必须定义预留期限、增量扣减、崩溃恢复、供应商未返回 usage 时的保守结算。预留并不代表后端一定执行，调用后断连也不代表后端停止产生 token。

不能直接从当前 TAT 减去旧请求的差额：旧债务会随时间消退，且其他请求可能已在其后入账。退款可能错误地创造当前容量。实现前要选择有独立 reservation 身份的账本/时间桶或其他能证明守恒的模型，并说明它如何与 GCRA 状态共同更新。

### 2. 流式处理

区分模型生成、网络传输和最终 usage；SSE chunk 并非 token。减慢读取第三方响应通常无法限制模型在供应商侧的生成速度。增量校验只能约束本系统继续转发/继续接受的行为；若要阻止超预算生成，需要供应商支持生成上限或可靠取消。

本地丢连接时保留未知状态，不直接释放全部预算。明确输入/输出/缓存 token 的口径后再做适配器。

### 演进顺序

已完成已知 Cost 的联合准入、故障契约及有保留期限的幂等身份与结果查询。后续先设计有守恒依据的预留/校正，再加入供应商适配。等待队列、调度公平性和计费系统保持独立。

## 验证与参考

测试覆盖参数与精度边界、联合拒绝不扣减、跨客户端加权竞争、策略冲突、状态损坏、过期恢复、命名隔离、派发前取消、并发幂等、拒绝重放、响应丢失后的恢复及 ACL 部分写入。还有三主节点 Cluster 路由、随机 token bucket 模型对照、模糊测试和性能基准。CI 配置真实 Redis 与 Go race detector。

本地验证记录见 [verification.md](verification.md)。未承诺生产故障切换持久性、OOM 下的可用性、生产吞吐或 LLM 供应商兼容性；这些需要目标部署验证。

- [Redis Lua scripting](https://redis.io/docs/latest/develop/programmability/eval-intro/)
- [Go-Redis script execution](https://github.com/redis/go-redis/blob/v9.22.0/script.go)
- [go-redis/redis_rate](https://github.com/go-redis/redis_rate)：通用 GCRA 实现，可用于对照 API 范围；Pacelock 未复用其代码。
