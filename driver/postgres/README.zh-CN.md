# PostgreSQL Driver

[English](README.md)

独立模块：`github.com/tr1v3r/pacelock/driver/postgres`，目标版本 PostgreSQL 16+。
运行时仅依赖标准库与 Pacelock 核心。应用自行选择并注册 database/sql 传输驱动；
共用测试使用 pgx。

```go
// db 是已初始化、指向权威主库的 *sql.DB。
driver, err := postgres.New(db)
if err != nil { return err }
if err := driver.Migrate(setupCtx); err != nil { return err }
limiter, err := pacelock.New(driver, "llm-service", "provider-account-a")
if err != nil { return err }
rpm := pacelock.Quota{
    Name: "requests", Policy: pacelock.Policy{Revision: 1, Rate: 60, Period: time.Minute, Burst: 10},
}
tokens := pacelock.Quota{
    Name: "input-tokens", Policy: pacelock.Policy{Revision: 1, Rate: 60000, Period: time.Minute, Burst: 12000},
}
if err := limiter.UpdateQuotas(ctx, rpm, tokens); err != nil { return err }
decision, err := limiter.Allow(ctx, pacelock.Charge{Name: rpm.Name, Cost: 1}, pacelock.Charge{Name: tokens.Name, Cost: 1500})
// 错误时忽略 decision，并检查 pacelock.OutcomeOf(err)。
```

导入 `"github.com/tr1v3r/pacelock/driver/postgres"` 与核心包。使用 pgx 时，
应用注册 `_ "github.com/jackc/pgx/v5/stdlib"`，调用 `sql.Open("pgx", dsn)`，
并固定传输驱动版本。构造函数不联网、不迁移、不关闭 db。
在初始化阶段显式使用具备建表权限的角色调用 Migrate，按 schema.sql 创建当前有日志表，
包括独立的 `pacelock_policies` 配置表。仅支持当前表结构，不保证升级或历史结构兼容。
所有调用者须使用可信、稳定且一致的 search_path、schema 与主库，
不能传入读副本连接池。

先用 `UpdateQuotas` 发布正 revision 的策略，再用 `Allow(ctx, ...Charge{Name, Cost})`
或 `Wait(ctx, charges...)` 准入，无需本地绑定或策略刷新。`Limiter` 只提供动态准入；
`Rule` 与 `Evaluate` 保留为纯计算 API，不是后端准入入口。

## 动态策略

Driver 实现必需的动态 `pacelock.Driver`，以及可选 `pacelock.QuotaDriver` 接口。
控制面发布配置，数据面**只传名称与消耗**，获得 scope 锁后使用后端最新配置。
准入节点无需刷新本地策略或重新绑定，SDK 不启动后台 worker；配置源轮询与发布由调用者负责。

```go
// 控制面：发布配置源中的权威版本。
err = limiter.UpdateQuotas(ctx, pacelock.Quota{
    Name: "requests",
    Policy: pacelock.Policy{Revision: 1, Rate: 60, Period: time.Minute, Burst: 10},
})
if err != nil { return err }
// 数据面：相同调用会自动使用以后发布的新版本。
decision, err := limiter.Allow(ctx, pacelock.Charge{Name: "requests", Cost: 1})
_ = decision
```

发布正的单调递增版本（最大 9007199254740991）。更新与准入使用同一 scope 锁，
通过 `ApplyQuotas` 完整校验批次后写入。更新只读写独立策略表，绝不读写债务，
暂停与恢复也不修改 TAT 或债务过期时间。更高版本替换配置；相同版本且有效间隔/Burst 相同
是只读成功，不重写配置。旧版本或同版本不同配置返回 `ErrPolicyMismatch`。
缩小容量可能使债务暂时超出容量；债务随时间恢复，额度配置变更不会补充容量、重置或重标定债务。

任何策略缺失时，托管准入关闭放行并返回 `ErrPolicyMissing` / `NotCharged`。
消耗超过当前 Burst 返回 `ErrInvalid`，不是可以等待恢复的容量拒绝。
托管策略的 `Rate: 0` 表示暂停（Period、Burst 仍须为正），先于超 Burst 检查返回
`ErrPaused` / `NotCharged`，不扣减。以更高版本和正速率恢复。

`Limiter.Wait(ctx, charges...)` 在派发前只复制一次 Charge，每次普通尝试原子读取后端
最新策略，不快照策略。仅在有效容量拒绝后按建议的 `RetryAfter` 重试；任何错误都停止，
包括暂停、策略缺失、新 Burst 小于 Cost、传输错误（即使为 `NotCharged`）、`Unknown`、
不支持的能力及无效 Driver 结果。两次尝试间取消为 `NotCharged`；调用期间保留不确定性，
已确认成功不因并发取消变为失败。每次尝试均为独立准入。整次调用只产生一次 `wait` 观测。更新不会主动
唤醒休眠者，旧 RetryAfter 可能延迟使用放宽后的策略。休眠不持有事务、锁或连接，
不增加队列、worker、后台 goroutine、公平性或退款。请设置 context deadline；配置源
轮询与发布仍由调用者负责。详见[阻塞等待准入](../../README.zh-CN.md#阻塞等待准入)。

更新在提交前失败会回滚；提交错误为 `Unknown`，表示策略更新可能已经提交，不表示发生额度扣减。
显式重试相同版本可处理不确定性，但该版本也可能已被更新版本取代。不做隐藏重试，派发前取消不修改状态。

在接收流量前，于受控初始化阶段运行 `Migrate`，避免并发初始化；DDL 会持有表锁。

## 事务与故障契约

每次准入使用 Read Committed，通过 INSERT ON CONFLICT 建立永久 domain 锁行，
再用 SELECT FOR UPDATE 锁定它，覆盖额度尚不存在的情况。
获得锁后用 clock_timestamp 采样一次后端时间，全部额度在同一事务提交。
PostgreSQL 使用核心 EvaluateManaged 转换；Redis 的等价计算在 Lua 内执行。
托管准入先独立校验原债务字段，再在内存中应用当前配置。债务行的间隔/Burst 仅用于完整性校验，
不是权威策略。

准入用两条 SELECT 分别批量读取策略和债务，全部额度允许后才用一条多行 upsert 写入债务。
结果按额度 ID 映射回请求顺序；缺策略时拒绝准入，缺债务时从空状态开始。
上限仍为 `MaxRules = 32`。包含 domain 插入、加锁与取时，允许准入执行六条 SQL，
拒绝执行五条，另加 BEGIN/COMMIT，语句数不随额度数增长。
拒绝不写策略或债务（仍可能插入永久 domain 行）；SQL 长度和参数数量仍随额度数增长。
`UpdateQuotas` 同样只读一次策略，最多执行一次仅含变更行的 upsert：
有变更时五条 SQL，完全相同时四条，另加事务边界。同版本相同配置不重写，
完全相同的更新回滚只读事务而不提交。所有参数均为 `database/sql` 标量参数，
不依赖数组编解码、特定传输的批处理 API 或新增依赖。批处理减少往返，不消除 domain 串行化和锁等待。

发送 Commit 前的错误回滚并标记 NotCharged；任何 Commit 错误都保守标记 Unknown，
额度可能已经扣减。不提供记录查询、重放或自动事务重试；重试是新的准入，可能再次扣减。
业务幂等由应用负责，不属于限流器职责。

## 部署边界与清理

- 一个 domain 是一个串行点，可能等待行锁或连接池。配置 context 截止时间、服务端语句/
  锁超时以及有界连接池；不要在事务中调用供应商或等待 RetryAfter。
- 使用有日志表、fsync 和合适的 synchronous_commit/复制策略。异步提交或切换至落后副本
  可能丢失债务。PG 也不能消除墙上时钟跳变。
- 过期债务会被逻辑忽略，但托管策略身份永久保留。应用可调度 `driver.Prune(ctx, scope, limit)`，
  每次最多删除 limit 条过期债务（1～10000）。
  过期时间与 TAT 都必须不晚于数据库时间，避免删除仍有效的债务。
  它使用同一 domain 锁，绝不触碰策略表，需规划持久化策略基数。
  domain 锁行故意保留，需规划 domain 基数；不要独立删除锁行或活跃状态。
  额度频繁更新，需要关注 vacuum。
- 当前 Driver 自己管理事务，不能加入业务事务。不提供 Redis 状态自动迁移/故障切换、
  计费、队列或吞吐/SLO 保证；需要在目标部署验证延迟与热点 domain。

使用根目录 Compose 和 `scripts/check.sh` 验证。`tests/contracts` 覆盖共用行为、
真实 PG 回滚、提交响应丢失的计账语义、等锁取消、锁后取时和清理。
详见 [Driver 契约](../../docs/driver.md)。

[可运行的 pgx 示例](../../examples/postgres/main.go)：在仓库根目录执行
`(cd examples/postgres && go run .)`，连接 Compose 的 PostgreSQL 测试实例。

核心、两个运行时 Driver、可选的 pgx 示例和集成测试模块统一要求 Go 1.26+。
生产应用应使用仍在维护的 Go 分支及其最新修补版。

Prune 使用 `(scope_id, GREATEST(expires_us, tat_us), quota_id)` 索引并按恢复时间
选择候选，避免持 domain 锁遍历全部活跃额度。LIMIT 限制已恢复候选数量，不保证硬性
延迟上限（仍受 MVCC 可见性、锁和存储影响）。已有当前版本 schema 需在受控初始化
阶段运行 `Migrate` 添加索引；创建索引可能阻塞写入并消耗磁盘，应安排停流量窗口，
或在 Migrate 之外并发创建等价索引。额外索引也增加债务写入维护成本，需按部署测量。
