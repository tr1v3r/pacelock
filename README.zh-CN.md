# Pacelock

[![CI](https://github.com/tr1v3r/pacelock/actions/workflows/ci.yml/badge.svg)](https://github.com/tr1v3r/pacelock/actions/workflows/ci.yml)
[English](README.md)

**Go 多维加权 GCRA 原子限流库，提供 Redis 和 PostgreSQL Driver。**

Pacelock 协调多个服务实例的共享额度。一次准入可以同时扣减请求数、已知输入 token 和租户额度：全部通过才扣减；任一额度不足则全部不扣减。LLM API 是主要场景，底层支持任意已知的正数消耗。

API 仍处于 pre-v1 阶段。本 SDK 提供速率准入、明确的故障契约和有保留期限的幂等能力，不实现 LLM 网关或计费账本。最低要求为 Go 1.26+，后端选择 Redis 7+ 或 PostgreSQL 16+；生产构建应使用仍在安全维护的 Go 分支及其最新修补版。已验证配置见[兼容性说明](docs/compatibility.md)和[验证记录](docs/verification.md)。

## 方便 agent 使用的完整注释 SDK

Pacelock 是一个**提供完整源码注释的 SDK**，方便编码 agent 理解并正确调用。
注释覆盖包、类型、接口、函数、方法、字段、常量和变量，说明参数约束、并发与生命周期规则，以及错误处理方式。
集成时，请结合示例阅读所固定版本的源码注释。

可从[准入 API](pacelock.go)、[Driver 与恢复契约](driver.go)和[错误结果](errors.go)开始。

## 能力

- 加权 GCRA，一次调用 1～32 个具名额度的原子准入。
- 后端服务端时间、共享状态，Redis TTL 或 PostgreSQL 逻辑过期。
- 核心仅依赖标准库，Redis/PostgreSQL Driver 为独立模块。
- 立即返回结果、建议等待时间 `RetryAfter` 和全部阻塞维度名称。
- 有保留期限的幂等准入、参数冲突检测和结果查询。
- 可程序化判断的扣减结果、离线规则校验和同步观测回调。
- 真实 Redis/Cluster、race、模型对照、模糊测试、故障注入和基准测试。

不包含队列、阻塞式 `Wait`、分词、输出预留、退款、流式用量校正或并发许可管理。[Airlock](https://github.com/tr1v3r/airlock) 处理正在执行的工作数量；Pacelock 处理随时间恢复的消耗。两者不构成联合事务。

## 安装与使用

本 README 描述尚未发布的 `dev` 开发线，请显式安装该分支：

```sh
go get github.com/tr1v3r/pacelock@dev github.com/tr1v3r/pacelock/drivers/redis@dev
```

默认 `@latest` 当前选中旧版 API，并不包含本页展示的全部 API。请在应用的 `go.mod` 中保留解析得到的伪版本以便复现构建；`@dev` 会随开发推进，不代表稳定发布。

构造函数现在接受 `pacelock.Driver`，属于 pre-v1 破坏性变更。
下例需导入 `redisdriver "github.com/tr1v3r/pacelock/drivers/redis"`。

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
rules := []pacelock.Rule{
    {Name: "account:rpm", Rate: 60, Period: time.Minute, Burst: 10, Cost: 1},
    {Name: "model-a:input-tpm", Rate: 60000, Period: time.Minute, Burst: 12000, Cost: 1500},
}
decision, err := limiter.Allow(ctx, rules...)
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

所有实例必须使用相同的 namespace、domain、额度名称和有效策略。每次调用都要包含适用的账号、模型和租户规则；库无法推断漏传的规则。不同 namespace/domain 相互独立。

## PostgreSQL 与 Driver 边界

使用 PostgreSQL 时改装 `github.com/tr1v3r/pacelock/drivers/postgres@dev`。
应用注册自己选择的 `database/sql` 传输驱动，并管理指向主库的 DB：

```go
driver, err := postgres.New(db) // 导入 github.com/tr1v3r/pacelock/drivers/postgres
if err != nil { return err }
if err := driver.Migrate(ctx); err != nil { return err } // 显式初始化，不在每次请求中调用
limiter, err := pacelock.New(driver, "llm-service", "provider-account-a")
```

两种 Driver 共用 `Allow`、`AllowOnce`、`Lookup`、错误结果和观测接口。
PostgreSQL 锁定一行 domain，获得锁后采样一次 `clock_timestamp()`，用短事务
一起提交全部额度与准入记录。提交前失败会回滚；提交错误保守标记为 `Unknown`。
当前不会加入调用者的业务事务。domain 竞争、WAL、连接池和 vacuum 需要目标环境验证，
不宣称性能优于 Redis。`Prune` 提供由应用调度的有界物理清理；不清理也会逻辑过期。
SDK 不启动后台任务。

详见 [PostgreSQL 指南](drivers/postgres/README.zh-CN.md)、[Redis 指南](drivers/redis/README.zh-CN.md)
和[自定义 Driver 契约](docs/drivers.md)。切换后端不会迁移额度债务或幂等历史，不能当作自动故障降级。

## 幂等准入与恢复

```go
// 派发前，将准入 ID 与一次业务尝试一起持久保存。
receipt, err := limiter.AllowOnce(ctx, admissionID, 10*time.Minute, rules...)
if err != nil {
    return err
}
// receipt.Decision 是原始决策；receipt.Replayed 标识是否重放。
// Replayed=true 不代表可以再次执行上游请求。
```

响应不确定时，使用新的 context 查询：

```go
decision, found, err := limiter.Lookup(recoveryCtx, admissionID)
```

- ID 在 namespace/domain 内生效，绑定**有序的**额度名称、有效间隔、突发容量、消耗及保留期限。参数变化返回 `ErrConflict`。`AllowOnce` 要求额度名称为有效 UTF-8；ID 长度为 1～256 字节。
- 保留期限必须为 1 毫秒到 24 小时之间的整毫秒，覆盖业务恢复窗口。重放和查询不延长期限。
- 允许和拒绝都会保存。保留期内被拒绝的 ID 始终重放拒绝；稍后发起真正的新尝试需要新 ID。重放的 `RetryAfter` 是原始等待时长，不是倒计时或最新容量估计。
- 扣减额度前先写入“处理中”标记。运行时错误可能留下部分扣减和该标记；后续重试返回 `ErrOutcomeUnknown`，不会再次扣减。不要删除标记或自动退款。
- `found=false` **不能证明未扣减**。原命令可能仍在执行，记录也可能已经过期或丢失。过期、淘汰、删除和故障切换丢数据都会终止去重保障，不应在此之后复用旧 ID。
- 准入去重不等于上游执行去重。业务尝试需要单独协调；每次真正的新模型调用都需要新的准入 ID。

完整应用接入见[持久化准入示例](drivers/redis/examples/durable/README.zh-CN.md)：可信规则装配、attempt ID 持久化、重启恢复，以及上游执行未知时的显式处理。

## 错误与观测

使用 `errors.Is` 判断 `ErrInvalid`、`ErrPolicyMismatch`、`ErrState`、`ErrConflict`、`ErrOutcomeUnknown`、`ErrBackend`，以及包装的 context/传输错误。准入错误提供 `*pacelock.Error`，包含 `Op` 和 `Outcome`：

| 结果 | 含义 | 业务处理 |
|---|---|---|
| `Allowed=false`、无错误 | 额度不足；没有扣减额度 | 拒绝或调度新尝试 |
| `NotCharged` 错误 | 确定本次调用未扣减 | 修正参数/策略或处理取消 |
| `Unknown` 错误 | 可能已经扣减，也可能部分写入 | 查询仍保留的 ID，执行明确的恢复策略 |
| `Lookup` 未找到 | 没有找到仍保留的完整结果 | 保留对先前执行结果的不确定性 |

`NotCharged` 不会消除先前调用的未知状态。**任何错误**都应忽略返回的决策。仅凭 `ErrState` 不能认定未扣减：扣减后收到损坏的响应也会返回它。必须检查 `OutcomeOf(err)`；它对不认识的错误保守地返回 Unknown。

`Validate(rules...)` 可离线检查规则边界。`NewWithOptions` 接受可选的 `Observer`，每次 `Allow`、`AllowOnce` 或 `Lookup` 调用触发一次。事件包含操作、耗时、结果、规则数、重放状态、延迟和错误，不以字段暴露原始 ID 或额度名称。回调必须快速且并发安全；panic 会向外传播。不要把错误全文或用户标识用作指标标签。参见[可运行的观测与恢复示例](drivers/redis/examples/idempotency/main.go)。

## 额度与运行契约

- `Rate / Period` 是恢复速度；`Burst` 是总瞬时容量，不是额外容量。`0 < Cost <= Burst`；Period 和突发容量完全恢复的时间均不超过 24 小时。发射间隔向上取整到微秒，极高速率的有效速度会保守降低。
- GCRA 约束速率与突发包络，不是精确的固定/滑动分钟计数器。需要明确建模供应商规则；已知输入 token 不控制输出 TPM；字符数、字节数和 SSE chunk 数不等于 token 数。
- 策略身份为有效间隔和 Burst。活跃状态策略变化返回 `ErrPolicyMismatch`。这是部署保护，不是配置存储。
- 一个 domain 占用一个 Redis Cluster 槽。共享全局额度可能让较大的原子判定域集中到一个分片；任意分片无法保留跨槽事务。
- SDK 脚本命令通过 `NoRetry` 禁止不确定重放，无需禁用客户端重试；Cluster 可保留重定向。只在 `NOSCRIPT` 时从 `EVALSHA` 回退到 `EVAL`。构造检查不能替代对自定义客户端、hooks、工厂及代理的审计。
- 派发前取消不扣减；派发后取消不能撤销 Redis 写入。SDK 不创建 goroutine，不设置隐藏超时。
- 使用独立的键、主节点路由、TLS/认证和 Redis `noeviction`。Lua 执行隔离不等于运行时错误回滚或崩溃持久性；时钟跳变、淘汰和故障切换丢数据可能削弱限流及去重。

参见[运维与故障恢复](docs/operations.md)、[设计及扩展提案](docs/design.md)和[兼容性说明](docs/compatibility.md)。

## 示例与验证

```sh
docker compose up -d --wait
(cd drivers/redis && REDIS_ADDR=localhost:16379 go run ./examples/llm)
(cd drivers/redis && REDIS_ADDR=localhost:16379 go run ./examples/idempotency)
(cd examples/postgres && go run .)
PACELOCK_REDIS_ADDR=localhost:16379 \
PACELOCK_REDIS_CLUSTER_ADDRS=localhost:17000,localhost:17001,localhost:17002 \
PACELOCK_POSTGRES_DSN='postgres://pacelock:pacelock-test-only@localhost:15439/pacelock?sslmode=disable' \
PACELOCK_REQUIRED_REDIS_BACKENDS=standalone,cluster \
PACELOCK_REQUIRED_DRIVER_BACKENDS=redis,postgres sh scripts/check.sh
(cd drivers/redis/internal/engine && go test -run '^$' -fuzz FuzzInterval -fuzztime 10s .)
(cd drivers/redis/internal/engine && go test -run '^$' -fuzz FuzzRecord -fuzztime 10s .)
docker compose down
```

CI 覆盖 Go 1.26/stable、Redis 7.0/7/8、三主节点 Cluster 和 PostgreSQL 16/17/18。
共用契约及 PG 回滚/提交恢复测试启用 race 检查。
默认测试包含 100/500/1000 个同时调用、最多 32 个额度维度、加权与共享额度、租户隔离、幂等竞争、取消、连接池耗尽、响应丢失恢复和四进程竞争。
CI 还会启用 `-race`、随机排序，将并发测试额外重复执行三次。
参见[并发测试契约与复现方法](docs/concurrency-testing.md)。基准结果依赖部署环境，不承诺通用吞吐量或故障切换 SLO。参见[贡献与发布检查](CONTRIBUTING.md)和[安全策略](SECURITY.md)。

部署负载、内存与故障转移工具及本地原始记录见[部署验收说明](docs/deployment-validation.md)。这些结果只证明受控测试环境的行为，不代表生产 SLO。

MIT 许可证。

核心、两个运行时 Driver、可选的 pgx 示例及集成测试模块统一要求 Go 1.26+
（pgx/text 依赖原要求 1.25，go-redis v9.23 将下限抬至 1.26）。生产应用应使用仍在维护的 Go 版本。
