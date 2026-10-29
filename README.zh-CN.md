# Pacelock

[![CI](https://github.com/tr1v3r/pacelock/actions/workflows/ci.yml/badge.svg)](https://github.com/tr1v3r/pacelock/actions/workflows/ci.yml)
[English](README.md)

**Go 多维加权 GCRA 原子限流库，提供 Redis 和 PostgreSQL Driver。**

Pacelock 协调多个服务实例的共享额度。一次准入可以同时扣减请求数、已知输入 token 和租户额度：全部通过才扣减；任一额度不足则全部不扣减。LLM API 是主要场景，底层支持任意已知的正数消耗。

API 仍处于 pre-v1 阶段。本 SDK 提供速率准入、明确的故障契约，不实现 LLM 网关或计费账本。最低要求为 Go 1.26+，后端选择 Redis 7+ 或 PostgreSQL 16+；生产构建应使用仍在安全维护的 Go 分支及其最新修补版。已验证配置见[兼容性说明](docs/compatibility.md)和[验证记录](docs/verification.md)。

## 方便 agent 使用的完整注释 SDK

Pacelock 是一个**提供完整源码注释的 SDK**，方便编码 agent 理解并正确调用。
注释覆盖包、类型、接口、函数、方法、字段、常量和变量，说明参数约束、并发与生命周期规则，以及错误处理方式。
集成时，请结合示例阅读所固定版本的源码注释。

可从[默认动态 API](managed.go)、[Rule 与纯计算工具](pacelock.go)、[Driver 契约](driver.go)和[错误结果](errors.go)开始。

也可查看[历史交互式准入流程图](docs/workflows/README.zh-CN.md)，分别展开公共 API、PostgreSQL 事务和 Redis Lua 的具体操作。

## 能力

- 加权 GCRA，一次调用 1～32 个具名额度的原子准入。
- 控制面发布版本化策略，默认准入只提交具名 `Charge` 消耗。
- 后端服务端时间、共享状态、逻辑过期及显式物理清理。
- Redis Lua 最后一次 MSET 提交，由应用调度有界的 `Driver.Prune`。
- 核心仅依赖标准库，Redis/PostgreSQL Driver 为独立模块。
- 立即返回结果、建议等待时间 `RetryAfter` 和全部阻塞维度名称。
- 支持 context 取消的阻塞式 `Limiter.Wait`，复用多维加权准入。
- 可程序化判断的扣减结果、离线规则校验和同步观测回调。
- 真实 Redis/Cluster、race、模型对照、模糊测试、故障注入和基准测试。

不包含队列、分词、输出预留、退款、流式用量校正或并发许可管理。[Airlock](https://github.com/tr1v3r/airlock) 处理正在执行的工作数量；Pacelock 处理随时间恢复的消耗。两者不构成联合事务。

## 安装与使用

本 README 描述尚未发布的 `dev` 开发线，请显式安装该分支：

```sh
go get github.com/tr1v3r/pacelock@dev github.com/tr1v3r/pacelock/driver/redis@dev
```

默认 `@latest` 当前选中旧版 API，并不包含本页展示的全部 API。请在应用的 `go.mod` 中保留解析得到的伪版本以便复现构建；`@dev` 会随开发推进，不代表稳定发布。

构造函数接受 `pacelock.Driver`。
下例需导入 `redisdriver "github.com/tr1v3r/pacelock/driver/redis"`。

```go
client := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
    ContextTimeoutEnabled: true,
})
defer client.Close()

driver, err := redisdriver.New(client)
if err != nil {
    return err
}
limiter, err := pacelock.New(driver, "llm-service", "provider-account-a")
if err != nil {
    return err
}
rpm := pacelock.Quota{Name: "account:rpm", Policy: pacelock.Policy{
    Revision: 1, Rate: 60, Period: time.Minute, Burst: 10,
}}
tokens := pacelock.Quota{Name: "model-a:input-tpm", Policy: pacelock.Policy{
    Revision: 1, Rate: 60000, Period: time.Minute, Burst: 12000,
}}
// 控制面先发布；数据面只传名称与本次消耗。
if err := limiter.UpdateQuotas(ctx, rpm, tokens); err != nil { return err }
charges := []pacelock.Charge{{Name: rpm.Name, Cost: 1}, {Name: tokens.Name, Cost: 1500}}
decision, err := limiter.Allow(ctx, charges...)
if err != nil {
    // 忽略 decision。OutcomeOf(err) 为 NotCharged 或 Unknown。
    // 业务明确选择故障策略，不盲目重试。
    return err
}
if !decision.Allowed {
    // 拒绝请求，或由外部调度器参考 RetryAfter 和 Limited 安排新尝试。
    // RetryAfter 不会预留未来容量。
}
```

`Allow` 为本次尝试消耗容量，**不具备幂等性**。准入后业务失败不会退还容量。对于 Redis Driver，Limiter 不拥有或关闭 Redis 客户端；构造函数不建立连接、不修改客户端配置。客户端必须通过 go-redis 构造函数初始化，不支持零值客户端结构体。对这些已初始化的具体 `*redis.Client` / `*redis.ClusterClient`，`redisdriver.New` 支持默认、正数及禁用（`-1`）的 `MaxRetries` 配置；不支持的负数预算在本地返回 `ErrInvalid`。请先配置客户端再传入，不要在之后修改其 options。

不支持 `redis.Ring`，即使关闭重试也会返回 `ErrInvalid`。自定义 Scripter 包装仍须自行保证重放与路由行为。

使用 Cluster 时，将上方客户端构造替换为：

```go
client := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs: []string{"localhost:17000", "localhost:17001", "localhost:17002"},
    MaxRedirects: 3,
    ContextTimeoutEnabled: true,
})
defer client.Close()
```

Pacelock 对具体 Client/ClusterClient 的命令设置 go-redis `NoRetry`，阻止不确定网络错误重放，同时保留有界 MOVED/ASK 恢复，不依赖 `MaxRetries` 配置。共享客户端上的其他命令继续使用其配置的重试策略。Cluster 可使用默认或正数 MaxRedirects；`MaxRedirects: -1` 仍可禁用重定向。自定义 Scripter、hooks、节点工厂及代理必须遵守相同契约，详见[运维说明](docs/operations.md)。

共享额度的实例使用相同的 namespace、domain 和额度名称；后端持有当前动态策略，请求节点只传名称/cost。每次准入应包含适用的全部账号、模型和租户额度；库无法推断漏掉的额度。不同 namespace/domain 相互独立。

## 配置在代码中，执行以后端为准

`New` / `NewWithOptions` 只构造一种 `Limiter`，提供 `UpdateQuotas`、`Allow`、`Wait`。
配置可以继续写在应用代码里：先显式发布带正数 `Policy.Revision` 的 `Quota`，
请求只提交 `Charge{Name, Cost}`。后端权威策略使滚动发布的新旧 pod 共用最新配置。
旧版本发布返回 `ErrPolicyMismatch`，应用必须明确处理或上报，不能吞掉所有更新错误。
回滚应以**更高版本**发布旧参数，而不是降低版本或换额度名称。
每次包含全部适用额度，漏传额度不会检查或扣减；`Decision.Limited` 按请求 Charge 顺序返回。

`Rule`、`Validate` 和 `Evaluate` 仅保留作离线数学工具，不提供调用方策略的后端路径。
`EvaluateManaged` 同样保持纯计算。

## PostgreSQL 与 Driver 边界

使用 PostgreSQL 时改装 `github.com/tr1v3r/pacelock/driver/postgres@dev`。
应用注册自己选择的 `database/sql` 传输驱动，并管理指向主库的 DB：

```go
driver, err := postgres.New(db) // 导入 github.com/tr1v3r/pacelock/driver/postgres
if err != nil { return err }
if err := driver.Migrate(ctx); err != nil { return err } // 显式初始化，不在每次请求中调用
limiter, err := pacelock.New(driver, "llm-service", "provider-account-a")
```

两种 Driver 都支持动态 `Limiter`（`UpdateQuotas`、`Allow`、`Wait`），共用错误结果和观测接口。
PostgreSQL 锁定一行 domain，获得锁后采样一次 `clock_timestamp()`，用短事务
一起提交全部额度变更。提交前失败会回滚；提交错误保守标记为 `Unknown`。
当前不会加入调用者的业务事务。domain 竞争、WAL、连接池和 vacuum 需要目标环境验证，
不宣称性能优于 Redis。`Prune` 提供由应用调度的有界物理清理；不清理也会逻辑过期。
SDK 不启动后台任务。

详见 [PostgreSQL 指南](driver/postgres/README.zh-CN.md)、[Redis 指南](driver/redis/README.zh-CN.md)
和[自定义 Driver 契约](docs/driver.md)。切换后端不会迁移额度债务，不能当作自动故障降级。

## 默认动态策略与暂停

默认 `New` / `NewWithOptions` 构造动态 Limiter。`UpdatePolicies` → `UpdateQuotas`，`PolicyDriver` → `QuotaDriver`，`PolicyUpdate` → `QuotaUpdate`；旧名称已移除。`Policy` 和纯计算工具 `ApplyPolicies` 保持原名。详见[迁移指南](docs/compatibility.md)。

控制面使用 `UpdateQuotas`，数据面使用 `Allow`。Redis 和 PostgreSQL
将永久配置与可过期的 TAT（债务）分开存储。处理请求的实例只需要额度名和 cost，
不需要本地策略、版本、binding 或配置 watcher：

```go
// 控制面：应用自己的轮询/watch 回调发布配置源中的版本。
q := pacelock.Quota{Name: "tenant:rpm", Policy: pacelock.Policy{
    Revision: 1, Rate: 100, Period: time.Minute, Burst: 20,
}}
if err := limiter.UpdateQuotas(ctx, q); err != nil { return err }

// 数据面：调整速率、暂停、恢复时，这个 charge 都不需要修改。
charge := pacelock.Charge{Name: "tenant:rpm", Cost: 1}
decision, err := limiter.Allow(ctx, charge)
if err != nil { return err }
if !decision.Allowed { /* 拒绝或调度另一次尝试 */ }

// 后续配置发布：不重置旧债，无需刷新请求节点。
q.Policy.Revision = 2
q.Policy.Rate = 200
if err := limiter.UpdateQuotas(ctx, q); err != nil { return err }
q.Policy.Revision = 3
q.Policy.Rate = 0 // 紧急暂停，新准入返回 ErrPaused
if err := limiter.UpdateQuotas(ctx, q); err != nil { return err }
q.Policy.Revision = 4
q.Policy.Rate = 100 // 恢复；暂停期间旧债随时间自然恢复
if err := limiter.UpdateQuotas(ctx, q); err != nil { return err }
```

- 版本来自同一个权威配置源，不能由各进程独立递增。多实例可重复发布同一批配置：高版本生效，同版本同有效策略只读成功，旧版本或同版本冲突返回 `ErrPolicyMismatch`。回滚需以**更高版本**发布旧参数。同 scope 内最多 `MaxRules` 条配置原子更新。
- 额度配置变更不会补充容量或重置债务。更新不读取、不重标定、不清零 TAT，也不缩短已有债务寿命。准入在同一原子边界内读取当前配置和债务；旧债超过新 burst 时，保留到自然恢复。
- Redis 每个动态 quota 使用永久 P1 配置 key 与 D1 债务 key，所有 key 显式传入且同 slot。`Allow` 一次脚本往返完成，不是客户端先 GET 再写。PostgreSQL 使用独立策略表与原有 scope 事务锁。不增加分布式锁 key。
- D1/P1 的 `1` 表示首个动态存储格式，不是 `Policy.Revision`（仍为正数且单调递增）。D3/P3 与旧 tuple 债务保留严格只读兼容；同版本同有效策略不写入，P3 保持不变；新策略或更高版本写 P1，下次允许的债务写入使用 D1。**标记迁移前先停止旧二进制**：仅支持 D3/P3 的旧 reader 读不了 D1/P1。读取、写入及维护进程全部升级后再恢复，不能混用新旧 writer，也不能通过重置或删键迁移；永久版本栅栏与历史回执仍需保留。详见[Redis 迁移](docs/compatibility.md#redis-state)。
- `Rate=0` 时仍要求正的 Period 和 Burst。新准入返回 `ErrPaused` / `NotCharged`，不扣减。配置不存在返回 `ErrPolicyMissing`，不能默认无限额度。cost 超过当前 burst 返回 `ErrInvalid`。动态 `Wait` 遇到这些错误立即停止，不等待策略变更。配置发布与配置源轮询由应用负责，SDK 不启动轮询 goroutine。
- `Prune` 可以清理过期债务，但绝不删除配置/版本。需规划配置名称基数；删除配置会丢失版本防护。所有动态发布都应走 SDK/版本检查脚本，不能裸 SET。
- PostgreSQL 必须在新版请求进程接流量前显式 `Migrate`。安装策略前必须退役旧静态写入者或停止其静态路径；迁移保留旧债务和历史回执，并不会让旧写入程序自动兼容。
- 传输/提交异常可能导致发布结果不确定；SDK 不自动重试，可显式重试**同一版本/策略**以收敛。`Driver.Admit(ctx, Admission) (Decision, error)` 是必需接口，`QuotaDriver` 负责发布配置；不支持的可选能力返回 `ErrUnsupported`，不修改状态。

可运行 SDK 示例：[管理型准入](driver/redis/examples/managed/main.go)。

## 阻塞等待准入

`Limiter.Wait(ctx, charges...) error` 先立即尝试，仅在有效的普通容量拒绝后按建议的
`RetryAfter` 等待。派发前只复制一次调用者的名称/cost Charge 切片，每次尝试都原子读取
后端最新策略，不快照策略。

```go
// limiter 和 charge 沿用上方动态示例，对应策略已发布。
charges := []pacelock.Charge{charge}
waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
if err := limiter.Wait(waitCtx, charges...); err != nil {
    // 任何错误都会停止等待。Unknown 可能已经扣减，绝不自动重试。
    return err
}
// 已成功扣减一次额度，执行本次业务尝试。
```

返回 nil 表示准入且扣减成功，业务失败不会退款。两次尝试之间取消为 `NotCharged`；后端调用期间取消保留该次调用的扣减结果及不确定性。若确认已成功扣减，即使同时发生取消也返回 nil。任何错误都会结束等待，包括 `ErrPaused`、`ErrPolicyMissing`、`ErrInvalid`（例如新 Burst 小于 Cost）、传输错误（即使为 `NotCharged`）、`Unknown`、不支持的能力和无效 Driver 结果。可通过 `errors.Is` 和 `OutcomeOf` 判断错误；`Error.Op` 为 `wait`。

Wait 在调用方 goroutine 中使用私有等待循环，休眠期间不持有后端锁或连接，不增加队列、worker 或后台 goroutine。策略更新不会主动唤醒休眠者：先前的 `RetryAfter` 可能延迟使用放宽后的策略。`RetryAfter` 不预留容量，其他调用者可能持续抢先，因此不保证 FIFO 或免于饥饿。请用 context deadline 限制等待时长。

## Pre-v1 破坏性移除

准入不具备幂等性。已移除 `AllowOnce`、`Lookup`、`Receipt`、`LookupRequest`、
准入 ID/Retention/Fingerprint 及 `Event.Replayed`。唯一的 Limiter 提供
`Allow(ctx, ...Charge) (Decision, error)`、`Wait(ctx, ...Charge) error` 和 `UpdateQuotas`。
`Driver.Admit(ctx, Admission)` 返回 `(Decision, error)`。静态构造函数、Limiter、Driver
和 Admission 接口均已移除，不保留兼容别名。不确定写入后没有结果恢复接口：**不要盲目重试 Unknown**。
本库只负责速率准入，不承担计费、退款、业务执行、队列或 worker 所有权。

升级不会自动删除历史回执存储。旧 Redis attempt key 和 PostgreSQL admissions
表/行可能仍然存在；应先停止旧写入进程，再协调备份与显式运维清理。
详见[迁移指南](docs/compatibility.md)。

## 错误与观测

使用 `errors.Is` 判断 `ErrInvalid`、`ErrPolicyMismatch`、`ErrState`、`ErrBackend`，以及包装的 context/传输错误。准入错误提供 `*pacelock.Error`，包含 `Op` 和 `Outcome`：

| 结果 | 含义 | 业务处理 |
|---|---|---|
| `Allowed=false`、无错误 | 额度不足；没有扣减额度 | 拒绝或调度新尝试 |
| `NotCharged` 错误 | 确定本次调用未扣减 | 修正参数/策略或处理取消 |
| `Unknown` 错误 | 可能已经扣减，尚无法确认 | 不要盲目重试；采用明确的业务故障策略 |

`NotCharged` 不会消除先前调用的未知状态。**任何错误**都应忽略返回的决策。仅凭 `ErrState` 不能认定未扣减：扣减后收到损坏的响应也会返回它。必须检查 `OutcomeOf(err)`；它对不认识的错误保守地返回 Unknown。

`Validate(rules...)` 可离线检查数学规则。`NewWithOptions` 接受同步 `Observer`，每次准入触发一次；构造函数和 `UpdateQuotas` 不触发事件。操作名为 `allow`、`wait`。Wait 为整个调用产生一次事件，不另发内部尝试事件。事件包含数量、耗时、结果、等待时间和错误，不暴露原始额度名字段；错误可能包含标识，不可用作指标标签。回调须快速、并发安全；panic 向外传播。

## 额度与运行契约

- `Rate / Period` 是恢复速度；`Burst` 是总瞬时容量，不是额外容量。`0 < Cost <= Burst`；Period 和突发容量完全恢复的时间均不超过 24 小时。发射间隔向上取整到微秒，极高速率的有效速度会保守降低。
- GCRA 约束速率与突发包络，不是精确的固定/滑动分钟计数器。需要明确建模供应商规则；已知输入 token 不控制输出 TPM；字符数、字节数和 SSE chunk 数不等于 token 数。
- 策略必须通过 `UpdateQuotas` 显式发布，Revision 必须为正数。
- 一个 domain 占用一个 Redis Cluster 槽。共享全局额度可能让较大的原子判定域集中到一个分片；任意分片无法保留跨槽事务。
- SDK 脚本命令通过 `NoRetry` 禁止不确定重放，无需禁用客户端重试；Cluster 可保留重定向。只在 `NOSCRIPT` 时从 `EVALSHA` 回退到 `EVAL`。构造检查不能替代对自定义客户端、hooks、工厂及代理的审计。
- 派发前取消不扣减；派发后取消不能撤销 Redis 写入。SDK 不创建 goroutine，不设置隐藏超时。
- 使用独立的键、主节点路由、TLS/认证和 Redis `noeviction`。Redis 协议使用单命令提交，崩溃和故障切换持久性仍取决于部署；时钟跳变、淘汰和故障切换丢数据可能削弱限流。

参见[运维与故障恢复](docs/operations.md)、[设计及扩展提案](docs/design.md)和[兼容性说明](docs/compatibility.md)。

## 示例与验证

```sh
docker compose up -d --wait
(cd driver/redis && REDIS_ADDR=localhost:16379 go run ./examples/llm)
(cd examples/postgres && go run .)
(cd examples/redis && go run . -redis localhost:16379)
PACELOCK_REDIS_ADDR=localhost:16379 \
PACELOCK_REDIS_CLUSTER_ADDRS=localhost:17000,localhost:17001,localhost:17002 \
PACELOCK_POSTGRES_DSN='postgres://pacelock:pacelock-test-only@localhost:15439/pacelock?sslmode=disable' \
PACELOCK_REQUIRED_REDIS_BACKENDS=standalone,cluster \
PACELOCK_REQUIRED_DRIVER_BACKENDS=redis,postgres sh scripts/check.sh
(cd driver/redis/internal/engine && go test -run '^$' -fuzz FuzzInterval -fuzztime 10s .)
docker compose down
```

CI 覆盖 Go 1.26/stable、Redis 7.0/7/8、三主节点 Cluster 和 PostgreSQL 16/17/18。
共用契约及 PG 回滚/提交丢响应测试启用 race 检查。
默认测试包含 100/500/1000 个同时调用、最多 32 个额度维度、加权与共享额度、租户隔离、取消、连接池耗尽、响应丢失扣减语义和四进程竞争。
CI 还会启用 `-race`、随机排序，将并发测试额外重复执行三次。
参见[并发测试契约与复现方法](docs/concurrency-testing.md)。基准结果依赖部署环境，不承诺通用吞吐量或故障切换 SLO。参见[贡献与发布检查](CONTRIBUTING.md)和[安全策略](SECURITY.md)。

部署负载、内存与故障转移工具及本地原始记录见[部署验收说明](docs/deployment-validation.md)。这些结果只证明受控测试环境的行为，不代表生产 SLO。

MIT 许可证。

核心、两个运行时 Driver、可选的 pgx 示例及集成测试模块统一要求 Go 1.26+。生产应用应使用仍在维护的 Go 版本。
