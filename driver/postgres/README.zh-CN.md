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
在初始化阶段显式使用具备迁移权限的角色调用 Migrate；它按 schema.sql 创建固定的有日志表，
并增加独立的 `pacelock_policies` 配置表。已有额度/债务表与准入记录表结构不变，保留全部债务与记录。
不支持不兼容的表结构。所有调用者需使用可信、稳定且一致的 search_path、schema 和主库，
不能传入读副本连接池。

默认先用 `UpdateQuotas` 发布正 revision 的策略，再用 `Allow(ctx, ...Charge{Name, Cost})` 或 `AllowOnce` 准入。无需本地绑定或策略刷新。显式静态替代路径是 `BindStatic` → `StaticLimiter`，通过 `q.StaticCharge(cost)` 构造封装的 `StaticCharge`，然后调用绑定对象的 `Allow` / `AllowOnce`。底层静态路径使用 `Rule` 与 `AllowStatic` / `AllowStaticOnce` / `WaitStatic`，Revision 必须为 0；没有默认动态 Wait。

## 默认动态策略

Driver 实现必需的动态 `pacelock.Driver`，以及可选 `pacelock.QuotaDriver` 与 `pacelock.StaticDriver` 接口。
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
// 或保留决策：ID 指纹包含名称与消耗，不包含策略版本。
receipt, err := limiter.AllowOnce(ctx, "operation-id", time.Minute,
    pacelock.Charge{Name: "requests", Cost: 1})
_ = decision
_ = receipt
```

发布正的单调递增版本（最大 9007199254740991）。更新与准入使用同一 scope 锁，
通过 `ApplyPolicies` 完整校验批次后写入。更新只读写独立策略表，绝不读写债务，
暂停与恢复也不修改 TAT 或债务过期时间。更高版本替换配置；相同版本且有效间隔/Burst 相同
是只读成功，不重写配置。旧版本或同版本不同配置返回 `ErrPolicyMismatch`。
缩小容量可能使债务暂时超出容量；债务随时间恢复，额度配置变更不会补充容量、重置或重标定债务。

任何策略缺失时，托管准入关闭放行并返回 `ErrPolicyMissing` / `NotCharged`。
消耗超过当前 Burst 返回 `ErrInvalid`，不是可以等待恢复的容量拒绝。
托管策略的 `Rate: 0` 表示暂停（Period、Burst 仍须为正），先于超 Burst 检查返回
`ErrPaused` / `NotCharged`，不扣减、不写准入记录。以更高版本和正速率恢复。
本次不提供托管 Wait API，重试与配置轮询由调用者负责。

已有保留记录在当前策略检查**之前**重放，不因暂停或更新改变。托管请求的名称/消耗不变时，
跨版本重试仍重放原决策。静态与托管请求复用同一 ID 会冲突。历史允许记录重放不代表可以再次执行上游。
静态 Rule/BindStatic 请求在本地拒绝托管 Revision；对已配置额度发起新的静态准入返回
`ErrPolicyMismatch`，但旧静态记录仍先于该防绕过检查重放。

更新在提交前失败会回滚；提交错误为 `Unknown`，表示策略更新可能已经提交，不表示发生额度扣减。
显式重试相同版本可处理不确定性，但该版本也可能已被更新版本取代。不做隐藏重试，派发前取消不修改状态。

**部署顺序：先迁移 → 升级全部二进制 → 激活托管策略。** 新版请求服务在接收任何流量前
就必须执行 `Migrate`，**仅有静态流量也不例外**：新版静态准入会查询策略表，防止绕过托管配置。
迁移只增加表，因此升级期间旧二进制仍可处理静态流量，但此时不能激活任何托管策略。
共享数据库的全部二进制升级完成后，才能发布托管配置。旧 PostgreSQL 代码无法与动态策略共存：
它不检查独立配置表，会绕过托管限额。Migrate 应在受控初始化阶段运行，避免并发迁移；DDL 会持有表锁。

公共 `BindStatic` 接口在本地校验并复制固定额度。通过值接收者方法 `q.StaticCharge(cost)` 将完整额度和消耗
捕获到私有字段，不校验、不做 I/O；不要使用 StaticCharge 字段字面量。`Quota.Name` / `Policy` 仍公开，
之后修改 Quota 不影响已有 StaticCharge 或绑定，变更策略需新建 binding。`Allow` / `AllowOnce` 仍接收
`...StaticCharge`，每次接受已绑定额度的任意非空子集，每个选中额度仅提供一次消耗，且 `0 < cost <= Burst`。
未传入额度既不检查也不扣减。选中 StaticCharge 按绑定顺序整理，幂等指纹只包含选中额度的名称、
整理后的顺序、有效策略、消耗及保留期限；同一仍保留的 ID 改变子集返回 `ErrConflict`，
调整传入 StaticCharge 顺序不改变指纹。多次独立调用合起来不具备原子性。
捕获的完整策略必须合法，且名称、有效间隔（向上取整到微秒）和 Burst 须与
绑定一致；合法且有效策略相同的不同写法可接受。空调用、重复、未知、无效或不匹配的 StaticCharge 在准入前本地返回
`ErrInvalid` / `NotCharged`，不是后端 `ErrPolicyMismatch`。Rule API 与绑定接口共用 Driver 和存储，等待
通过 `limiter.WaitStatic` 传入规则，不提供绑定等待方法。
详见[策略与消耗契约](../../README.zh-CN.md#固定策略与单次消耗)。

## 事务与故障契约

每次准入使用 Read Committed，通过 INSERT ON CONFLICT 建立永久 domain 锁行，
再用 SELECT FOR UPDATE 锁定它，覆盖额度或准入记录尚不存在的情况。
获得锁后用 clock_timestamp 采样一次后端时间，全部额度与允许/拒绝记录在同一事务提交。
PostgreSQL 使用核心 Evaluate / EvaluateManaged 转换；Redis 的等价计算在 Lua 内执行。
托管准入先独立校验原债务字段，再在内存中应用当前配置。债务行的间隔/Burst 仅用于完整性校验，
不是权威策略。

发送 Commit 前的错误回滚并标记 NotCharged；任何 Commit 错误都保守标记 Unknown，
使用新 context 按仍保留的 ID 查询恢复。不自动重试事务。Lookup 不加 domain 锁，
不等待进行中的准入，因此未找到不代表没有扣减。重放和查询不延长保留期。
上游业务执行仍需应用协调。

## 部署边界与清理

- 一个 domain 是一个串行点，可能等待行锁或连接池。配置 context 截止时间、服务端语句/
  锁超时以及有界连接池；不要在事务中调用供应商或等待 RetryAfter。
- 使用有日志表、fsync 和合适的 synchronous_commit/复制策略。异步提交或切换至落后副本
  可能丢失债务与准入记录。PG 也不能消除墙上时钟跳变。
- 过期债务会被逻辑忽略，但托管策略身份永久保留。应用可调度 `driver.Prune(ctx, scope, limit)`，
  每次最多删除 limit 条过期债务（静态或托管）与 limit 条过期准入记录（1～10000）。
  它使用同一 domain 锁，绝不触碰策略表，需规划持久化策略基数。
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
