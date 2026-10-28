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
    Name: "requests", Policy: pacelock.Policy{Rate: 60, Period: time.Minute, Burst: 10},
}
tokens := pacelock.Quota{
    Name: "input-tokens", Policy: pacelock.Policy{Rate: 60000, Period: time.Minute, Burst: 12000},
}
bound, err := limiter.Bind(rpm, tokens)
if err != nil { return err }
decision, err := bound.Allow(ctx, rpm.Charge(1), tokens.Charge(1500))
// 错误时忽略 decision，并检查 pacelock.OutcomeOf(err)。
```

导入 `"github.com/tr1v3r/pacelock/driver/postgres"` 与核心包。使用 pgx 时，
应用注册 `_ "github.com/jackc/pgx/v5/stdlib"`，调用 `sql.Open("pgx", dsn)`，
并固定传输驱动版本。构造函数不联网、不迁移、不关闭 db。
在初始化阶段显式使用具备建表权限的角色调用 Migrate；它按 schema.sql 创建固定的有日志表，
不升级已有的不兼容表。所有调用者需使用可信、稳定且一致的 search_path、schema 和主库，
不能传入读副本连接池。

公共 `Bind` 接口在本地校验并复制固定额度。通过值接收者方法 `q.Charge(cost)` 将完整额度和消耗
捕获到私有字段，不校验、不做 I/O；不要使用 Charge 字段字面量。`Quota.Name` / `Policy` 仍公开，
之后修改 Quota 不影响已有 Charge 或绑定，变更策略需新建 binding。`Allow` / `AllowOnce` 仍接收
`...Charge`，每次接受已绑定额度的任意非空子集，每个选中额度仅提供一次消耗，且 `0 < cost <= Burst`。
未传入额度既不检查也不扣减。选中 Charge 按绑定顺序整理，幂等指纹只包含选中额度的名称、
整理后的顺序、有效策略、消耗及保留期限；同一仍保留的 ID 改变子集返回 `ErrConflict`，
调整传入 Charge 顺序不改变指纹。多次独立调用合起来不具备原子性。
捕获的完整策略必须合法，且名称、有效间隔（向上取整到微秒）和 Burst 须与
绑定一致；合法且有效策略相同的不同写法可接受。空调用、重复、未知、无效或不匹配的 Charge 在准入前本地返回
`ErrInvalid` / `NotCharged`，不是后端 `ErrPolicyMismatch`。Rule API 与绑定接口共用 Driver 和存储，等待
通过 `limiter.Wait` 传入规则，不提供绑定等待方法。
详见[策略与消耗契约](../../README.zh-CN.md#固定策略与单次消耗)。

## 事务与故障契约

每次准入使用 Read Committed，通过 INSERT ON CONFLICT 建立永久 domain 锁行，
再用 SELECT FOR UPDATE 锁定它，覆盖额度或准入记录尚不存在的情况。
获得锁后用 clock_timestamp 采样一次后端时间，全部额度与允许/拒绝记录在同一事务提交。
PostgreSQL 使用核心 Evaluate 转换；Redis 的等价计算在 Lua 内执行。

发送 Commit 前的错误回滚并标记 NotCharged；任何 Commit 错误都保守标记 Unknown，
使用新 context 按仍保留的 ID 查询恢复。不自动重试事务。Lookup 不加 domain 锁，
不等待进行中的准入，因此未找到不代表没有扣减。重放和查询不延长保留期。
上游业务执行仍需应用协调。

## 部署边界与清理

- 一个 domain 是一个串行点，可能等待行锁或连接池。配置 context 截止时间、服务端语句/
  锁超时以及有界连接池；不要在事务中调用供应商或等待 RetryAfter。
- 使用有日志表、fsync 和合适的 synchronous_commit/复制策略。异步提交或切换至落后副本
  可能丢失债务与准入记录。PG 也不能消除墙上时钟跳变。
- 过期行会被逻辑忽略。应用可调度 `driver.Prune(ctx, scope, limit)`，每次从两张数据表
  各最多删除 limit 行（1～10000）。它使用同一 domain 锁并保留活跃债务。
  domain 锁行故意保留，需规划 domain 基数；不要独立删除锁行或活跃状态。
  额度频繁更新，需要关注 vacuum。
- 当前 Driver 自己管理事务，不能加入业务事务。不提供 Redis 状态自动迁移/故障切换、
  计费、队列或吞吐/SLO 保证；需要在目标部署验证延迟与热点 domain。

使用根目录 Compose 和 `scripts/check.sh` 验证。`tests/contracts` 覆盖共用行为、
真实 PG 回滚、提交响应丢失恢复、等锁取消、锁后取时和清理。
详见 [Driver 契约](../../docs/driver.md)。

[可运行的 pgx 示例](../../examples/postgres/main.go)：在仓库根目录执行
`(cd examples/postgres && go run .)`，连接 Compose 的 PostgreSQL 测试实例。

保留期从采样的后端决策时间起算，SQL 执行和提交延迟会消耗一部分窗口；
配置时需覆盖派发与恢复所需时间。

核心、两个运行时 Driver、可选的 pgx 示例和集成测试模块统一要求 Go 1.26+。
生产应用应使用仍在维护的 Go 分支及其最新修补版。
